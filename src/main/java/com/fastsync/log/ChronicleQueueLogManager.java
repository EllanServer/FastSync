package com.fastsync.log;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireIn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the per-UUID append-only operation log using Chronicle Queue.
 *
 * <p>This is the local-journal replacement for {@link OperationLogManager},
 * which stored the same Raft-inspired per-UUID ordered log in a SQL table
 * ({@code fastsync_operation_log}). Chronicle Queue gives us an off-heap,
 * append-only, memory-mapped journal with no database round-trip on the hot
 * logging path.
 *
 * <p>Design:
 * <ul>
 *   <li><b>One queue per player:</b> each UUID gets its own
 *       {@link SingleChronicleQueue} rooted at
 *       {@code {dataDir}/player-log/{uuid}/}. Queue instances are created
 *       lazily on first access and cached in a
 *       {@link ConcurrentHashMap}.</li>
 *   <li><b>Binary journal:</b> each {@link OperationLog} is written as a
 *       single binary document using the
 *       {@code appender.writeDocument(w -> w.write("log").marshallable(m -> ...))}
 *       pattern, and read back with the symmetric
 *       {@code tailer.readDocument(r -> r.read("log").marshallable(m -> ...))}
 *       pattern.</li>
 *   <li><b>Daily roll cycle:</b> queues use {@link RollCycles#DAILY}, so the
 *       journal is naturally split into per-day files that can be cleaned
 *       independently. The queue is append-only; compaction is performed by
 *       {@link #prune}.</li>
 *   <li><b>Per-session seq:</b> sequence numbers are allocated by an
 *       {@link AtomicLong} per UUID. They reset to 0 on restart — the on-disk
 *       Chronicle Queue retains the full audit trail, but new seqs only need
 *       to be monotonically increasing <em>within a session</em>. This
 *       replaces the SQL {@code LAST_INSERT_ID} allocation used by
 *       {@link OperationLogManager}.</li>
 *   <li><b>Async append:</b> {@link #append} runs on a background thread via
 *       {@link CompletableFuture}, so logging never blocks the save/load
 *       path — matching the original manager's contract.</li>
 * </ul>
 *
 * <p>Thread-safety: the manager is safe to call concurrently from multiple
 * threads. {@code acquireAppender()} returns a per-thread appender and the
 * seq counter is an {@link AtomicLong}, so concurrent appends for the same
 * UUID are safe. {@link #prune} performs a close/delete/rewrite cycle and is
 * intended to be run as a best-effort background task (not concurrently with
 * an in-flight {@link #append} for the same UUID).
 */
public class ChronicleQueueLogManager {

    private static final Logger logger = Logger.getLogger(ChronicleQueueLogManager.class.getName());

    /** Sub-directory under {@code dataDir} where per-player queue folders live. */
    private static final String PLAYER_LOG_DIR = "player-log";

    /** Root data directory passed to the constructor. */
    private final Path dataDir;

    /** {@code {dataDir}/player-log} — parent of all per-UUID queue folders. */
    private final Path playerLogRoot;

    /** Max entries to keep per player (used by {@link #prune}). */
    private final int retention;

    /** Cached per-player queues, keyed by UUID. */
    private final ConcurrentHashMap<UUID, SingleChronicleQueue> queues = new ConcurrentHashMap<>();

    /** Per-UUID monotonic sequence counter (session-scoped, starts at 0). */
    private final ConcurrentHashMap<UUID, AtomicLong> seqCounters = new ConcurrentHashMap<>();

    private volatile boolean initialized = false;
    private volatile boolean closed = false;

    /**
     * @param dataDir   root data directory. Per-player queues are created under
     *                  {@code {dataDir}/player-log/{uuid}/}.
     * @param retention max number of entries to keep per player (applied by
     *                  {@link #prune}). Same semantics as the SQL manager's
     *                  {@code operation-log.retention} setting.
     */
    public ChronicleQueueLogManager(Path dataDir, int retention) {
        this.dataDir = dataDir;
        this.playerLogRoot = dataDir.resolve(PLAYER_LOG_DIR);
        this.retention = retention;
    }

    /**
     * Initialize the manager: ensures the {@code player-log} directory exists.
     *
     * <p>Unlike {@code OperationLogManager.initialize}, this performs no SQL
     * DDL and takes no data source — Chronicle Queue creates its store files
     * lazily on first append.
     */
    public void initialize() {
        try {
            Files.createDirectories(playerLogRoot);
        } catch (IOException e) {
            logger.log(Level.WARNING,
                "[OpLog] Failed to create player-log directory: " + playerLogRoot, e);
        }
        initialized = true;
        logger.info("[OpLog] Chronicle Queue operation log enabled (dir=" + playerLogRoot
            + ", retention=" + retention + ").");
    }

    /**
     * Whether the manager is open for business. A local file journal is always
     * "enabled" once initialized (there is no config toggle in this backend);
     * this method exists for drop-in compatibility with
     * {@link OperationLogManager#isEnabled()}.
     */
    public boolean isEnabled() {
        return initialized && !closed;
    }

    /**
     * Lazily create (and cache) the binary queue for a player.
     *
     * <p>Uses {@code SingleChronicleQueueBuilder.binary(dir)} with a daily
     * roll cycle, as specified by the replacement design.
     */
    private SingleChronicleQueue getQueue(UUID uuid) {
        if (closed) {
            return null;
        }
        return queues.computeIfAbsent(uuid, id -> {
            Path dir = playerLogRoot.resolve(id.toString());
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                logger.log(Level.WARNING, "[OpLog] Failed to create queue dir for " + id, e);
            }
            return SingleChronicleQueueBuilder.binary(dir)
                .build();
        });
    }

    /**
     * Allocate the next per-UUID sequence number (session-scoped).
     * The first call for a UUID returns 1.
     */
    private long nextSeq(UUID uuid) {
        return seqCounters.computeIfAbsent(uuid, id -> new AtomicLong(0))
            .incrementAndGet();
    }

    /**
     * Append an operation to the player's log. Async and non-blocking —
     * logging never blocks the main save path, matching the original manager.
     *
     * <p>The per-UUID {@code seq} is assigned here (overwriting any value on
     * the incoming entry), replacing the SQL {@code LAST_INSERT_ID}
     * allocation.
     *
     * @return CompletableFuture that completes when the entry is durably
     *         appended to the queue
     */
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
        });
    }

    private void appendSync(OperationLog entry) {
        UUID uuid = entry.uuid();
        if (uuid == null) {
            return;
        }
        long seq = nextSeq(uuid);
        // Stamp the allocated seq onto a copy; the incoming entry carries seq=0.
        OperationLog stamped = new OperationLog(
            seq, entry.uuid(), entry.type(), entry.serverName(),
            entry.fencingToken(), entry.version(), entry.dataSize(),
            entry.detail(), entry.timestamp());

        SingleChronicleQueue queue = getQueue(uuid);
        if (queue == null) {
            return; // manager closed
        }
        ExcerptAppender appender = queue.acquireAppender();
        writeOperationLog(appender, stamped);
    }

    /**
     * Query the operation history for a player, most recent first.
     *
     * <p>Chronicle Queue is append-only and read sequentially, so this scans
     * the queue forward from the start, collects all entries, and returns the
     * last {@code limit} in reverse (newest-first) order — equivalent to the
     * SQL manager's {@code ORDER BY seq DESC LIMIT ?} but sourced from the
     * local journal.
     *
     * <p>This is read-only: it never blocks concurrent appenders.
     *
     * @param uuid  player UUID
     * @param limit max number of entries to return (most recent first)
     * @return list of operation log entries, most recent first
     */
    public List<OperationLog> queryHistory(UUID uuid, int limit) {
        if (!isEnabled() || limit <= 0 || uuid == null) {
            return List.of();
        }
        SingleChronicleQueue queue = queues.get(uuid);
        if (queue == null) {
            return List.of();
        }

        List<OperationLog> all = readAllOldestFirst(queue);
        if (all.isEmpty()) {
            return List.of();
        }

        // Take the last `limit` entries (oldest-first list) and reverse to newest-first.
        int from = Math.max(0, all.size() - limit);
        List<OperationLog> tail = all.subList(from, all.size());
        List<OperationLog> result = new ArrayList<>(tail.size());
        for (int i = tail.size() - 1; i >= 0; i--) {
            result.add(tail.get(i));
        }
        return result;
    }

    /**
     * Read every entry in a queue, oldest-first, into a list.
     *
     * <p>Uses a fresh {@link ExcerptTailer} positioned at the start and reads
     * documents in {@code FORWARD} order until the queue is exhausted.
     * Malformed entries are skipped (best-effort) rather than aborting the
     * whole scan.
     */
    private List<OperationLog> readAllOldestFirst(SingleChronicleQueue queue) {
        List<OperationLog> all = new ArrayList<>();
        try (ExcerptTailer tailer = queue.createTailer()) {
            tailer.toStart();
            while (true) {
                final OperationLog[] holder = new OperationLog[1];
                boolean found = tailer.readDocument(r ->
                    r.read("log").marshallable(m -> holder[0] = readOperationLog(m)));
                if (!found) {
                    break;
                }
                if (holder[0] != null) {
                    all.add(holder[0]);
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "[OpLog] Failed to read operation log", e);
        }
        return all;
    }

    /**
     * Prune old log entries for a player, keeping only the most recent
     * {@code keepCount}.
     *
     * <p>Chronicle Queue is append-only, so pruning is implemented as a
     * compaction: read the retained (newest) entries, close and delete the
     * queue directory, recreate a fresh queue, and rewrite the retained
     * entries in chronological order. This is the "nuclear" compaction option
     * described in the replacement design.
     *
     * <p>If the queue already has at most {@code keepCount} entries, this is
     * a cheap forward scan that performs no rewrite — so calling it after
     * every append (the original usage pattern) stays inexpensive while the
     * log is within retention.
     *
     * <p>Best-effort: should not run concurrently with an in-flight
     * {@link #append} for the same UUID.
     *
     * @param uuid      player UUID
     * @param keepCount number of most-recent entries to retain
     */
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
        SingleChronicleQueue queue = queues.get(uuid);
        if (queue == null) {
            return; // nothing to prune
        }

        List<OperationLog> all = readAllOldestFirst(queue);

        // Wipe entirely when keeping nothing.
        if (keepCount == 0) {
            if (all.isEmpty()) {
                return;
            }
            closeAndRemove(uuid);
            deleteQueueDir(uuid);
            return;
        }

        // Within retention: no rewrite needed.
        if (all.size() <= keepCount) {
            return;
        }

        // Keep the last `keepCount` entries (already oldest-first).
        List<OperationLog> keep = new ArrayList<>(
            all.subList(all.size() - keepCount, all.size()));

        // Close, delete, recreate a fresh queue, and rewrite the retained entries.
        closeAndRemove(uuid);
        deleteQueueDir(uuid);

        SingleChronicleQueue fresh = getQueue(uuid);
        if (fresh == null) {
            return; // closed concurrently
        }
        ExcerptAppender appender = fresh.acquireAppender();
        for (OperationLog log : keep) {
            writeOperationLog(appender, log);
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("[OpLog] Compacted " + uuid + ": kept " + keep.size()
                + " of " + all.size() + " entries.");
        }
    }

    /**
     * Write a single {@link OperationLog} as a binary document containing a
     * nested {@code "log"} marshallable. Shared by {@link #appendSync} and
     * {@link #pruneSync}.
     */
    private void writeOperationLog(ExcerptAppender appender, OperationLog log) {
        appender.writeDocument(w -> w.write("log").marshallable(m -> {
            m.write("seq").int64(log.seq());
            m.write("uuid").text(log.uuid() == null ? null : log.uuid().toString());
            m.write("type").text(log.type() == null ? null : log.type().name());
            m.write("server").text(log.serverName());
            m.write("fencingToken").int64(log.fencingToken());
            m.write("version").int64(log.version());
            m.write("dataSize").int32(log.dataSize());
            m.write("detail").text(log.detail());
            m.write("timestamp").int64(log.timestamp());
        }));
    }

    /**
     * Deserialize an {@link OperationLog} from a nested marshallable's
     * {@link WireIn}. Returns {@code null} (and logs) if the entry is
     * malformed, so a single bad document never aborts a full scan.
     */
    private OperationLog readOperationLog(WireIn m) {
        try {
            long seq = m.read("seq").int64();
            String uuidStr = m.read("uuid").text();
            String typeName = m.read("type").text();
            String server = m.read("server").text();
            long fencingToken = m.read("fencingToken").int64();
            long version = m.read("version").int64();
            int dataSize = m.read("dataSize").int32();
            String detail = m.read("detail").text();
            long timestamp = m.read("timestamp").int64();

            UUID uuid = (uuidStr == null) ? null : UUID.fromString(uuidStr);
            OperationType type = (typeName == null) ? null : OperationType.valueOf(typeName);
            return new OperationLog(seq, uuid, type, server,
                fencingToken, version, dataSize, detail, timestamp);
        } catch (Exception e) {
            logger.log(Level.WARNING, "[OpLog] Failed to deserialize operation log entry", e);
            return null;
        }
    }

    /** Close and remove a single player's queue from the cache. */
    private void closeAndRemove(UUID uuid) {
        SingleChronicleQueue q = queues.remove(uuid);
        if (q != null) {
            try {
                q.close();
            } catch (Exception e) {
                logger.log(Level.FINE, "[OpLog] Error closing queue for " + uuid, e);
            }
        }
    }

    /** Recursively delete a player's queue directory (files before folders). */
    private void deleteQueueDir(UUID uuid) throws IOException {
        Path dir = playerLogRoot.resolve(uuid.toString());
        if (!Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        logger.log(Level.FINE, "[OpLog] Could not delete " + p, e);
                    }
                });
        }
        // The directory itself is the last element deleted by the walk above.
    }

    /**
     * Close all cached queues and release resources. Safe to call once on
     * plugin disable. After {@code close()}, {@link #isEnabled()} returns
     * {@code false} and further {@link #append} calls are no-ops.
     */
    public void close() {
        closed = true;
        for (UUID uuid : queues.keySet()) {
            closeAndRemove(uuid);
        }
        queues.clear();
        seqCounters.clear();
        logger.info("[OpLog] Chronicle Queue operation log closed.");
    }
}
