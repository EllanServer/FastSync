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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * threads. Appends, reads and pruning for the same UUID are serialized by a
 * per-UUID lock so readers never observe a partially-written record and prune
 * cannot replace a file while an append is in flight.
 */
public class FileOperationLogManager {

    private static final Logger logger = Logger.getLogger(FileOperationLogManager.class.getName());

    private static final String PLAYER_LOG_DIR = "player-log";
    private static final String FILE_SUFFIX = ".log";

    private final Path dataDir;
    private final Path playerLogRoot;
    private final int retention;

    /**
     * Dedicated single-thread bounded executor for log appends.
     *
     * <p>Round 16 (P0 #2): previously {@code append()} used
     * {@code CompletableFuture.runAsync(task)} with no executor, which
     * dispatches to {@code ForkJoinPool.commonPool()}. That conflicts with
     * the project's thread-governance goal (bounded, named, manageable
     * pools) and lets log writes escape FastSync's shutdown ordering — a
     * plugin disabled callback could fire while commonPool still has pending
     * log tasks, producing writes to a partially-closed manager.
     *
     * <p>This executor is single-threaded (log writes are sequential per
     * process; per-UUID locks handle ordering across threads) with a large
     * bounded queue (4096) and a discard-oldest policy — under extreme
     * load we prefer to drop the oldest queued log entry rather than block
     * the save/load path or throw. Dropped tasks explicitly complete their
     * futures, avoiding callers waiting forever on work that left the queue.
     * The SQL DB remains the source of truth; the operation log is an audit aid.
     */
    private final ThreadPoolExecutor appendExecutor;

    /** Per-UUID locks to serialize appends and ensure ordering. */
    private final ConcurrentHashMap<UUID, Object> appendLocks = new ConcurrentHashMap<>();

    /** Per-UUID monotonic sequence counter (session-scoped, starts at 0). */
    private final ConcurrentHashMap<UUID, AtomicLong> seqCounters = new ConcurrentHashMap<>();

    /** Number of queued log entries dropped due to append queue saturation. */
    private final AtomicLong droppedCount = new AtomicLong();

    /** Wall-clock time of the last dropped log entry, or 0 if none have dropped. */
    private final AtomicLong lastDropTimestamp = new AtomicLong();

    private volatile boolean initialized = false;
    private volatile boolean closed = false;

    public FileOperationLogManager(Path dataDir, int retention) {
        this.dataDir = dataDir;
        this.playerLogRoot = dataDir.resolve(PLAYER_LOG_DIR);
        this.retention = retention;
        this.appendExecutor = createAppendExecutor();
    }

    private ThreadPoolExecutor createAppendExecutor() {
        AtomicInteger counter = new AtomicInteger(0);
        java.util.concurrent.ThreadFactory factory = r -> {
            Thread t = new Thread(r, "FastSync-OpLog-" + counter.getAndIncrement());
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY - 1);  // log writes should yield to save/load
            return t;
        };
        return new ThreadPoolExecutor(
            1, 1,
            30L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(4096),
            factory,
            (task, executor) -> {
                if (closed || executor.isShutdown()) {
                    discardTask(task);
                    return;
                }
                Runnable dropped = executor.getQueue().poll();
                if (dropped != null) {
                    discardTask(dropped);
                }
                if (!executor.getQueue().offer(task)) {
                    discardTask(task);
                }
            });
    }

    public void initialize() throws IOException {
        // Fail initialization instead of advertising an enabled audit log whose
        // directory could not be created. OperationLogDelegate catches this and
        // disables the optional subsystem cleanly.
        Files.createDirectories(playerLogRoot);
        initialized = true;
        logger.info("[OpLog] File operation log enabled (dir=" + playerLogRoot
            + ", retention=" + retention + ", executor=dedicated single-thread bounded).");
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

    /** Runnable with an observable completion even when queue policy drops it. */
    private final class AppendTask implements Runnable {
        private final OperationLog entry;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private AppendTask(OperationLog entry) {
            this.entry = entry;
        }

        @Override
        public void run() {
            try {
                appendSync(entry);
            } catch (Throwable t) {
                logger.log(Level.WARNING,
                    "[OpLog] Failed to append operation log for " + entry.uuid(), t);
            } finally {
                completion.complete(null);
            }
        }

        private void completeAsDropped() {
            completion.complete(null);
        }
    }

    private void discardTask(Runnable task) {
        if (task instanceof FileOperationLogManager.AppendTask appendTask) {
            appendTask.completeAsDropped();
        }
        long drops = droppedCount.incrementAndGet();
        lastDropTimestamp.set(System.currentTimeMillis());
        if (drops == 1 || drops % 100 == 0) {
            logger.warning("[OpLog] Append queue full; dropped " + drops
                + " queued log entr" + (drops == 1 ? "y" : "ies")
                + " so far. This log is best-effort only.");
        }
    }

    private void discardTasks(java.util.List<Runnable> tasks) {
        if (tasks.isEmpty()) return;
        for (Runnable task : tasks) {
            if (task instanceof FileOperationLogManager.AppendTask appendTask) {
                appendTask.completeAsDropped();
            }
        }
        droppedCount.addAndGet(tasks.size());
        lastDropTimestamp.set(System.currentTimeMillis());
    }

    public CompletableFuture<Void> append(OperationLog entry) {
        if (!isEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        // Round 16 (P0 #2): use the dedicated bounded appendExecutor instead
        // of ForkJoinPool.commonPool(). This keeps log writes under FastSync's
        // thread-governance and shutdown ordering. The discard-oldest policy ensures
        // this never blocks the save/load path — under extreme load the oldest
        // queued log entry is silently dropped (the DB remains the source of
        // truth; this log is an audit aid only). A custom task is used instead
        // of CompletableFuture.runAsync so the rejection policy can complete a
        // future whose queued task it discards.
        AppendTask task = new AppendTask(entry);
        try {
            appendExecutor.execute(task);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Defensive fallback for executor implementations/policies that may
            // still throw during a close race. Logging remains best-effort.
            discardTask(task);
        }
        return task.completion;
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
        synchronized (getLock(uuid)) {
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
        synchronized (getLock(uuid)) {
            Path path = getLogPath(uuid);
            if (!Files.exists(path)) {
                return;
            }

            List<OperationLog> all = readAll(path);
            if (all.size() <= keepCount) {
                return;
            }

            if (keepCount == 0) {
                Files.deleteIfExists(path);
                return;
            }

            List<OperationLog> keep = new ArrayList<>(
                all.subList(all.size() - keepCount, all.size()));
            Path tmp = path.resolveSibling(uuid.toString() + ".tmp");
            try (var out = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(tmp,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING)))) {
                for (OperationLog log : keep) {
                    writeRecord(out, log);
                }
                out.flush();
            }
            try {
                Files.move(tmp, path,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tmp, path,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("[OpLog] Compacted " + uuid + ": kept " + keep.size()
                    + " of " + all.size() + " entries.");
            }
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
        // Mark closed first so new append() calls short-circuit to a completed
        // future instead of submitting to a shutting-down executor (which
        // would throw RejectedExecutionException).
        closed = true;

        // Round 16 (P0 #2): explicitly drain the dedicated appendExecutor.
        // Previously append() ran on commonPool, so pending log writes could
        // outlive the plugin — writes to a closed manager, files left half-
        // flushed, etc. Now we own the executor and wait for in-flight appends
        // to finish (bounded wait so a stuck flush can't hang shutdown).
        appendExecutor.shutdown();
        try {
            if (!appendExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                java.util.List<Runnable> abandoned = appendExecutor.shutdownNow();
                int remaining = abandoned.size();
                discardTasks(abandoned);
                if (remaining > 0) {
                    logger.warning("[OpLog] Forced shutdown of append executor; "
                        + remaining + " pending log entries were discarded.");
                }
                if (!appendExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    logger.severe("[OpLog] Append executor did not terminate!");
                }
            }
        } catch (InterruptedException e) {
            discardTasks(appendExecutor.shutdownNow());
            Thread.currentThread().interrupt();
        }

        appendLocks.clear();
        seqCounters.clear();
        logger.info("[OpLog] File operation log closed.");
    }

    public long getDroppedCount() {
        return droppedCount.get();
    }

    public long getLastDropTimestamp() {
        return lastDropTimestamp.get();
    }

    public int getQueueSize() {
        return appendExecutor.getQueue().size();
    }

    public int getQueueCapacity() {
        return appendExecutor.getQueue().remainingCapacity() + appendExecutor.getQueue().size();
    }
}
