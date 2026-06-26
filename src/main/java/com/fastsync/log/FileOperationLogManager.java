package com.fastsync.log;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the per-UUID append-only operation log using standard Java NIO.
 *
 * <p>This is the plug-and-play replacement for {@link ChronicleQueueLogManager},
 * which required {@code --add-opens} JVM arguments on JDK 16+ due to Chronicle
 * Queue's use of internal {@code sun.nio.ch} APIs. This implementation uses
 * only public Java APIs ({@link java.nio.file.FileChannel},
 * {@link java.io.DataOutputStream}, {@link java.io.DataInputStream}) and
 * requires <strong>zero JVM arguments</strong> — just drop the JAR in
 * {@code plugins/} and it works.
 *
 * <p>Design:
 * <ul>
 *   <li><b>One file per player:</b> each UUID gets its own binary log file at
 *       {@code {dataDir}/player-log/{uuid}.log}. Files are created lazily on
 *       first append.</li>
 *   <li><b>Binary format:</b> each entry is a length-prefixed record:
 *       <pre>
 *       [4 bytes: record length (big-endian int)]
 *       [N bytes: serialized fields via DataOutputStream]
 *         - long seq
 *         - UTF uuid
 *         - UTF type name
 *         - UTF serverName
 *         - long fencingToken
 *         - long version
 *         - int  dataSize
 *         - UTF detail
 *         - long timestamp
 *       </pre>
 *   <li><b>Append-only:</b> writes use {@code StandardOpenOption.APPEND},
 *       so the file only grows. {@link #prune} compacts by rewriting.</li>
 *   <li><b>Durability:</b> each append flushes to the OS via
 *       {@code OutputStream.flush()}. Full {@code fsync} is not called on every
 *       write (performance); the OS page cache provides crash consistency for
 *       most scenarios, and the SQL database remains the source of truth.</li>
 *   <li><b>Per-session seq:</b> sequence numbers are allocated by an
 *       {@link AtomicLong} per UUID, starting at 1. They reset on restart —
 *       the on-disk log retains the full audit trail, but new seqs only need
 *       to be monotonically increasing <em>within a session</em>.</li>
 *   <li><b>Async append:</b> {@link #append} runs on a background thread via
 *       {@link CompletableFuture}, so logging never blocks the save/load path.</li>
 * </ul>
 *
 * <p>Thread-safety: the manager is safe to call concurrently from multiple
 * threads. Appends for the same UUID are serialized by a per-UUID lock to
 * ensure append ordering. {@link #queryHistory} is read-only and never blocks
 * appends. {@link #prune} should not run concurrently with an in-flight
 * {@link #append} for the same UUID.
 */
public class FileOperationLogManager {

    private static final Logger logger = Logger.getLogger(FileOperationLogManager.class.getName());

    private static final String PLAYER_LOG_DIR = "player-log";
    private static final String FILE_SUFFIX = ".log";

    private final Path dataDir;
    private final Path playerLogRoot;
    private final int retention;

    /** Per-UUID locks to serialize appends and ensure ordering. */
    private final ConcurrentHashMap<UUID, Object> appendLocks = new ConcurrentHashMap<>();

    /** Per-UUID monotonic sequence counter (session-scoped, starts at 0). */
    private final ConcurrentHashMap<UUID, AtomicLong> seqCounters = new ConcurrentHashMap<>();

    /** Dedicated single-thread executor for async log appends.
     *  Avoids polluting ForkJoinPool.commonPool() with I/O-bound work. */
    private final ExecutorService appendExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "FastSync-OpLog-Writer");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean initialized = false;
    private volatile boolean closed = false;

    public FileOperationLogManager(Path dataDir, int retention) {
        this.dataDir = dataDir;
        this.playerLogRoot = dataDir.resolve(PLAYER_LOG_DIR);
        this.retention = retention;
    }

    public void initialize() {
        try {
            Files.createDirectories(playerLogRoot);
        } catch (IOException e) {
            logger.log(Level.WARNING,
                "[OpLog] Failed to create player-log directory: " + playerLogRoot, e);
        }
        initialized = true;
        logger.info("[OpLog] File operation log enabled (dir=" + playerLogRoot
            + ", retention=" + retention + ").");
    }

    public boolean isEnabled() {
        return initialized && !closed;
    }

    private Path getLogPath(UUID uuid) {
        return playerLogRoot.resolve(uuid.toString() + FILE_SUFFIX);
    }

    private long nextSeq(UUID uuid) {
        return seqCounters.computeIfAbsent(uuid, id -> new AtomicLong(0))
            .incrementAndGet();
    }

    private Object getLock(UUID uuid) {
        return appendLocks.computeIfAbsent(uuid, id -> new Object());
    }

    public CompletableFuture<Void> append(OperationLog entry) {
        if (!isEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(() -> {
            try {
                appendSync(entry);
            } catch (Exception e) {
                logger.log(Level.WARNING,
                    "[OpLog] Failed to append operation log for " + entry.uuid(), e);
            }
        }, appendExecutor);
    }

    private void appendSync(OperationLog entry) throws IOException {
        UUID uuid = entry.uuid();
        if (uuid == null) {
            return;
        }
        long seq = nextSeq(uuid);
        OperationLog stamped = new OperationLog(
            seq, entry.uuid(), entry.type(), entry.serverName(),
            entry.fencingToken(), entry.version(), entry.dataSize(),
            entry.detail(), entry.timestamp());

        Path path = getLogPath(uuid);
        synchronized (getLock(uuid)) {
            try (var out = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(path,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND)))) {
                writeRecord(out, stamped);
                out.flush();
            }
        }
    }

    public List<OperationLog> queryHistory(UUID uuid, int limit) {
        if (!isEnabled() || limit <= 0 || uuid == null) {
            return List.of();
        }
        Path path = getLogPath(uuid);
        if (!Files.exists(path)) {
            return List.of();
        }

        List<OperationLog> all = readAll(path);
        if (all.isEmpty()) {
            return List.of();
        }

        int from = Math.max(0, all.size() - limit);
        List<OperationLog> tail = all.subList(from, all.size());
        List<OperationLog> result = new ArrayList<>(tail.size());
        for (int i = tail.size() - 1; i >= 0; i--) {
            result.add(tail.get(i));
        }
        return result;
    }

    public void prune(UUID uuid, int keepCount) {
        if (!isEnabled() || uuid == null || keepCount < 0) {
            return;
        }
        try {
            pruneSync(uuid, keepCount);
        } catch (Exception e) {
            logger.log(Level.WARNING, "[OpLog] Failed to prune operation log for " + uuid, e);
        }
    }

    private void pruneSync(UUID uuid, int keepCount) throws IOException {
        Path path = getLogPath(uuid);
        if (!Files.exists(path)) {
            return;
        }

        List<OperationLog> all = readAll(path);
        if (all.size() <= keepCount) {
            return;
        }

        if (keepCount == 0) {
            synchronized (getLock(uuid)) {
                Files.deleteIfExists(path);
            }
            return;
        }

        List<OperationLog> keep = new ArrayList<>(
            all.subList(all.size() - keepCount, all.size()));

        Path tmp = path.resolveSibling(uuid.toString() + ".tmp");
        synchronized (getLock(uuid)) {
            try (var out = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(tmp,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING)))) {
                for (OperationLog log : keep) {
                    writeRecord(out, log);
                }
                out.flush();
            }
            // Atomic replace
            Files.move(tmp, path,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("[OpLog] Compacted " + uuid + ": kept " + keep.size()
                + " of " + all.size() + " entries.");
        }
    }

    private List<OperationLog> readAll(Path path) {
        List<OperationLog> all = new ArrayList<>();
        try (var in = new DataInputStream(
                new java.io.BufferedInputStream(Files.newInputStream(path)))) {
            while (true) {
                try {
                    int recordLen = in.readInt();
                    if (recordLen <= 0 || recordLen > 1024 * 1024) {
                        logger.warning("[OpLog] Invalid record length " + recordLen
                            + " in " + path + ", stopping scan.");
                        break;
                    }
                    byte[] buf = new byte[recordLen];
                    in.readFully(buf);
                    OperationLog log = deserializeRecord(
                        new DataInputStream(new java.io.ByteArrayInputStream(buf)));
                    if (log != null) {
                        all.add(log);
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "[OpLog] Failed to read operation log: " + path, e);
        }
        return all;
    }

    private void writeRecord(DataOutputStream out, OperationLog log) throws IOException {
        var baos = new java.io.ByteArrayOutputStream();
        var inner = new DataOutputStream(baos);
        inner.writeLong(log.seq());
        inner.writeUTF(log.uuid() == null ? "" : log.uuid().toString());
        inner.writeUTF(log.type() == null ? "" : log.type().name());
        inner.writeUTF(log.serverName() == null ? "" : log.serverName());
        inner.writeLong(log.fencingToken());
        inner.writeLong(log.version());
        inner.writeInt(log.dataSize());
        inner.writeUTF(log.detail() == null ? "" : log.detail());
        inner.writeLong(log.timestamp());
        inner.flush();

        byte[] payload = baos.toByteArray();
        out.writeInt(payload.length);
        out.write(payload);
    }

    private OperationLog deserializeRecord(DataInputStream in) {
        try {
            long seq = in.readLong();
            String uuidStr = in.readUTF();
            String typeName = in.readUTF();
            String server = in.readUTF();
            long fencingToken = in.readLong();
            long version = in.readLong();
            int dataSize = in.readInt();
            String detail = in.readUTF();
            long timestamp = in.readLong();

            UUID uuid = uuidStr.isEmpty() ? null : UUID.fromString(uuidStr);
            OperationType type = typeName.isEmpty() ? null : OperationType.valueOf(typeName);
            String detailVal = detail.isEmpty() ? null : detail;
            return new OperationLog(seq, uuid, type, server,
                fencingToken, version, dataSize, detailVal, timestamp);
        } catch (Exception e) {
            logger.log(Level.WARNING, "[OpLog] Failed to deserialize operation log entry", e);
            return null;
        }
    }

    public void close() {
        closed = true;
        appendExecutor.shutdown();
        try {
            if (!appendExecutor.awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS)) {
                appendExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            appendExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        appendLocks.clear();
        seqCounters.clear();
        logger.info("[OpLog] File operation log closed.");
    }
}
