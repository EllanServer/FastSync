package com.fastsync.snapshot;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages player data snapshots (point-in-time backups) stored in a dedicated
 * database table separate from the live player_data table.
 *
 * <p>Each snapshot captures a compressed player data blob along with metadata
 * (timestamp, save cause, pinned flag). Snapshots can be listed, loaded, deleted,
 * pinned (to protect them from automatic pruning), and pruned down to a
 * configurable limit.</p>
 *
 * <p>All database operations are executed asynchronously via
 * {@link CompletableFuture#supplyAsync} and return a future that completes
 * exceptionally on failure (after the error has been logged). Connections are
 * borrowed from the {@link DatabaseManager}'s HikariCP pool
 * (via {@code db.getDataSource()}).</p>
 */
public class SnapshotManager {

    private final Logger logger;
    private final ConfigManager config;

    private DatabaseManager databaseManager;
    private String snapshotTable;

    /** Dedicated bounded executor for all snapshot DB work. Keeps snapshot
     *  I/O off the common ForkJoinPool and bounds the queue so a flood of
     *  snapshot creations can't OOM the JVM. */
    private ThreadPoolExecutor snapshotExecutor;
    private final AtomicLong rejectedSnapshotTasks = new AtomicLong();
    private java.util.concurrent.Semaphore dbWorkSemaphore;

    /** Hard cap on the snapshot work queue. */
    private static final int SNAPSHOT_QUEUE_CAPACITY = 4096;

    public SnapshotManager(Logger logger, ConfigManager config) {
        this.logger = logger;
        this.config = config;
    }

    /** Share SyncManager's non-critical DB budget with snapshot work. */
    public void setDbWorkSemaphore(java.util.concurrent.Semaphore semaphore) {
        this.dbWorkSemaphore = semaphore;
    }

    /**
     * Initialize the snapshot manager with an initialized {@link DatabaseManager}.
     * Creates the snapshots table if it does not exist.
     *
     * @param db the initialized database manager providing the HikariCP data source
     */
    public void initialize(DatabaseManager db) {
        this.databaseManager = db;
        this.snapshotTable = config.getTablePrefix() + "snapshots";
        this.snapshotExecutor = createSnapshotExecutor();
        createTable();
        logger.info("SnapshotManager initialized (table=" + snapshotTable
            + ", cluster_id=" + db.getClusterId() + ").");
    }

    /**
     * Build the dedicated bounded executor for snapshot work.
     *
     * <p>Core/max pool size is fixed at 2 — snapshot operations are I/O-bound
     * (single row INSERT/SELECT/DELETE) and largely independent, so 2 threads
     * is enough to overlap latency without overwhelming the DB pool. The queue
     * is bounded at {@link #SNAPSHOT_QUEUE_CAPACITY} (4096) so that a runaway
     * producer (e.g. mass disconnect during a crash) fails fast with a logged
     * rejection rather than OOM-ing the JVM.
     *
     * @return a configured, unstarted ThreadPoolExecutor
     */
    private ThreadPoolExecutor createSnapshotExecutor() {
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "FastSync-Snapshot-" + counter.getAndIncrement());
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY - 1);
            return t;
        };
        return new ThreadPoolExecutor(
            2, 2, 30L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(SNAPSHOT_QUEUE_CAPACITY),
            factory,
            (task, executor) -> {
                long rejected = rejectedSnapshotTasks.incrementAndGet();
                if (rejected == 1 || rejected % 100 == 0) {
                    logger.warning("[Snapshot] Queue full; rejected snapshot tasks=" + rejected);
                }
                throw new RejectedExecutionException("Snapshot queue full");
            }
        );
    }

    /**
     * Submit a supplier to the dedicated snapshot executor. If the executor
     * has been shut down (e.g. after {@link #close()}), the returned future
     * completes exceptionally with an {@link IllegalStateException}.
     *
     * @param supplier the work to run
     * @param <T> result type
     * @return a future that will complete with the supplier's result
     */
    private <T> CompletableFuture<T> supplySnapshotAsync(Supplier<T> supplier) {
        if (snapshotExecutor == null || snapshotExecutor.isShutdown()) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("SnapshotManager is closed"));
            return failed;
        }
        return CompletableFuture.supplyAsync(() -> {
            boolean acquired = false;
            try {
                if (dbWorkSemaphore != null) {
                    dbWorkSemaphore.acquire();
                    acquired = true;
                }
                return supplier.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for snapshot DB capacity", e);
            } finally {
                if (acquired) {
                    dbWorkSemaphore.release();
                }
            }
        }, snapshotExecutor);
    }

    /**
     * Shut down the snapshot executor, waiting up to 10 seconds for queued
     * and in-flight tasks to complete. Safe to call multiple times.
     */
    public void close() {
        if (snapshotExecutor == null) {
            return;
        }
        snapshotExecutor.shutdown();
        try {
            if (!snapshotExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                int remaining = snapshotExecutor.shutdownNow().size();
                logger.warning("[Snapshot] Executor did not terminate in 10s; "
                    + remaining + " task(s) cancelled.");
            }
        } catch (InterruptedException e) {
            snapshotExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        snapshotExecutor = null;
    }

    /** @return current depth of the snapshot work queue. */
    public int getQueueSize() {
        return snapshotExecutor != null ? snapshotExecutor.getQueue().size() : 0;
    }

    /** @return the configured queue capacity. */
    public int getQueueCapacity() {
        return SNAPSHOT_QUEUE_CAPACITY;
    }

    /** @return total number of snapshot tasks rejected since startup. */
    public long getRejectedCount() {
        return rejectedSnapshotTasks.get();
    }

    /**
     * Create the snapshots table if it does not exist.
     *
     * <p><b>InnoDB locking design (per MySQL 8.4 docs):</b>
     * <ul>
     *   <li>UUID index for direct access — avoids full table scans
     *   <li>No FOR UPDATE on hot path — snapshot creation is async
     *   <li>No updated_at-based queries — uses auto-increment id for ordering
     *   <li>Precise DELETE by id — single-row PK lookup, no gap locks
     * </ul>
     *
     * <p><b>Spanner lesson:</b> The {@code id} column (auto-increment) is the
     * authoritative monotonic sequence for ordering snapshots. The {@code timestamp}
     * column is kept only for <strong>display</strong> purposes — it must never
     * be used for ordering because wall-clock timestamps are unreliable across
     * servers with clock skew. Auto-increment IDs are assigned by MySQL in commit
     * order, providing a globally consistent sequence without TrueTime.
     */
    private void createTable() {
        // v2 schema: cluster_id is the first non-PK column and the leading
        // column of both secondary indexes. Every WHERE clause that filters
        // by uuid now also filters by cluster_id so the optimizer can use
        // the composite index and so rows from other clusters are never
        // touched by this instance.
        String sql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `id` BIGINT NOT NULL AUTO_INCREMENT,
                `cluster_id` VARCHAR(64) NOT NULL,
                `uuid` VARCHAR(36) NOT NULL,
                `data` LONGBLOB NOT NULL,
                `timestamp` BIGINT NOT NULL,
                `save_cause` VARCHAR(64) NOT NULL DEFAULT 'unknown',
                `pinned` BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (`id`),
                INDEX `idx_snapshots_cluster_uuid` (`cluster_id`, `uuid`),
                INDEX `idx_snapshots_cluster_uuid_id` (`cluster_id`, `uuid`, `id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, snapshotTable);

        try (Connection conn = getDataSource().getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to create snapshots table '" + snapshotTable + "'", e);
            throw new RuntimeException("Failed to create snapshots table", e);
        }
    }

    /**
     * Save a new snapshot of a player's compressed data.
     *
     * @param uuid the player UUID
     * @param compressedData the compressed player data blob
     * @param saveCause the cause of the save (e.g. disconnect, death, world_save, shutdown)
     * @return a future that completes with the newly created snapshot id
     */
    public CompletableFuture<Long> createSnapshot(UUID uuid, byte[] compressedData, String saveCause) {
        return supplySnapshotAsync(() -> {
            String sql = String.format("""
                INSERT INTO `%s` (cluster_id, uuid, data, timestamp, save_cause, pinned)
                VALUES (?, ?, ?, ?, ?, ?)
                """, snapshotTable);

            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, databaseManager.getClusterId());
                ps.setString(2, uuid.toString());
                ps.setBytes(3, compressedData);
                ps.setLong(4, System.currentTimeMillis());
                ps.setString(5, saveCause != null ? saveCause : "unknown");
                ps.setBoolean(6, false);
                ps.executeUpdate();

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        long id = rs.getLong(1);
                        if (config.isDebug()) {
                            logger.info("Created snapshot " + id + " for " + uuid +
                                " (" + (compressedData != null ? compressedData.length : 0) +
                                " bytes, cause=" + saveCause + ")");
                        }
                        return id;
                    }
                }
                throw new SQLException("Failed to obtain generated snapshot id for " + uuid);
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to create snapshot for " + uuid, e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * List all snapshot metadata for a player, newest first.
     *
     * <p>Only metadata is returned (no data blobs) to keep the result lightweight.</p>
     *
     * @param uuid the player UUID
     * @return a future that completes with the list of snapshot metadata
     */
    public CompletableFuture<List<SnapshotInfo>> listSnapshots(UUID uuid) {
        return supplySnapshotAsync(() -> {
            // ORDER BY id DESC, NOT timestamp DESC.
            // Per Spanner: wall-clock timestamps are unreliable for ordering
            // due to cross-server clock skew. Auto-increment id is a true
            // monotonic sequence assigned by MySQL in commit order.
            String sql = String.format("""
                SELECT id, timestamp, save_cause, pinned
                FROM `%s`
                WHERE cluster_id = ? AND uuid = ?
                ORDER BY id DESC
                """, snapshotTable);

            List<SnapshotInfo> snapshots = new ArrayList<>();
            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, databaseManager.getClusterId());
                ps.setString(2, uuid.toString());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        snapshots.add(new SnapshotInfo(
                            rs.getLong("id"),
                            rs.getLong("timestamp"),
                            rs.getString("save_cause"),
                            rs.getBoolean("pinned")
                        ));
                    }
                }
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to list snapshots for " + uuid, e);
                throw new RuntimeException(e);
            }
            return snapshots;
        });
    }

    /**
     * Load the raw compressed data of a snapshot.
     *
     * @param snapshotId the snapshot id
     * @return a future that completes with the compressed data, or {@code null} if no such snapshot exists
     */
    public CompletableFuture<byte[]> loadSnapshot(long snapshotId) {
        return supplySnapshotAsync(() -> {
            String sql = String.format("""
                SELECT data FROM `%s` WHERE cluster_id = ? AND id = ?
                """, snapshotTable);

            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, databaseManager.getClusterId());
                ps.setLong(2, snapshotId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getBytes("data");
                    }
                }
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to load snapshot " + snapshotId, e);
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Delete a snapshot by id.
     *
     * @param snapshotId the snapshot id
     * @return a future that completes when the snapshot has been deleted
     */
    public CompletableFuture<Void> deleteSnapshot(long snapshotId) {
        return supplySnapshotAsync(() -> {
            String sql = String.format("""
                DELETE FROM `%s` WHERE cluster_id = ? AND id = ?
                """, snapshotTable);

            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, databaseManager.getClusterId());
                ps.setLong(2, snapshotId);
                int affected = ps.executeUpdate();
                if (config.isDebug()) {
                    logger.info("Deleted snapshot " + snapshotId + " (affected=" + affected + ")");
                }
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to delete snapshot " + snapshotId, e);
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Pin or unpin a snapshot. Pinned snapshots are protected from automatic
     * pruning via {@link #pruneSnapshots(UUID, int)}.
     *
     * @param snapshotId the snapshot id
     * @param pinned {@code true} to pin, {@code false} to unpin
     * @return a future that completes when the pin state has been updated
     */
    public CompletableFuture<Void> pinSnapshot(long snapshotId, boolean pinned) {
        return supplySnapshotAsync(() -> {
            String sql = String.format("""
                UPDATE `%s` SET pinned = ? WHERE cluster_id = ? AND id = ?
                """, snapshotTable);

            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setBoolean(1, pinned);
                ps.setString(2, databaseManager.getClusterId());
                ps.setLong(3, snapshotId);
                int affected = ps.executeUpdate();
                if (config.isDebug()) {
                    logger.info("Snapshot " + snapshotId + " pinned=" + pinned
                        + " (affected=" + affected + ")");
                }
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to update pin state for snapshot " + snapshotId, e);
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Prune (delete) the oldest non-pinned snapshots for a player so that at most
     * {@code maxSnapshots} non-pinned snapshots remain. Pinned snapshots are never
     * pruned and do not count toward the limit.
     *
     * @param uuid the player UUID
     * @param maxSnapshots the maximum number of non-pinned snapshots to keep
     * @return a future that completes when pruning has finished
     */
    public CompletableFuture<Void> pruneSnapshots(UUID uuid, int maxSnapshots) {
        return supplySnapshotAsync(() -> {
            int keep = Math.max(0, maxSnapshots);

            // Fetch non-pinned snapshot ids, newest first. The newest `keep` are
            // retained; everything older is collected for deletion.
            // ORDER BY id DESC (not timestamp) — per Spanner, auto-increment id
            // is the reliable monotonic sequence, immune to clock skew.
            String selectSql = String.format("""
                SELECT id FROM `%s`
                WHERE cluster_id = ? AND uuid = ? AND pinned = FALSE
                ORDER BY id DESC
                """, snapshotTable);

            List<Long> toDelete = new ArrayList<>();
            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setString(1, databaseManager.getClusterId());
                ps.setString(2, uuid.toString());
                try (ResultSet rs = ps.executeQuery()) {
                    int kept = 0;
                    while (rs.next()) {
                        if (kept < keep) {
                            kept++;
                            continue;
                        }
                        toDelete.add(rs.getLong("id"));
                    }
                }

                if (toDelete.isEmpty()) {
                    return null;
                }

                // Delete the surplus snapshots on the same connection.
                deleteByIds(conn, toDelete);
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to prune snapshots for " + uuid, e);
                throw new RuntimeException(e);
            }

            if (config.isDebug()) {
                logger.info("Pruned " + toDelete.size() + " snapshot(s) for " + uuid +
                    " (maxSnapshots=" + maxSnapshots + ")");
            }
            return null;
        });
    }

    /**
     * Delete all snapshots matching the given ids using the provided connection.
     *
     * @param conn an open connection
     * @param ids the snapshot ids to delete
     */
    private void deleteByIds(Connection conn, List<Long> ids) throws SQLException {
        StringBuilder placeholders = new StringBuilder("?");
        for (int i = 1; i < ids.size(); i++) {
            placeholders.append(",?");
        }

        String sql = String.format("""
            DELETE FROM `%s` WHERE cluster_id = ? AND id IN (%s)
            """, snapshotTable, placeholders);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, databaseManager.getClusterId());
            for (int i = 0; i < ids.size(); i++) {
                ps.setLong(i + 2, ids.get(i));
            }
            ps.executeUpdate();
        }
    }

    /**
     * Get the HikariCP data source from the database manager.
     *
     * @return the data source
     * @throws IllegalStateException if the manager has not been initialized
     */
    private HikariDataSource getDataSource() {
        if (databaseManager == null || databaseManager.getDataSource() == null) {
            throw new IllegalStateException("SnapshotManager has not been initialized");
        }
        return databaseManager.getDataSource();
    }

    // ==================== SnapshotInfo ====================

    /**
     * Lightweight metadata for a snapshot (does not include the data blob).
     */
    public static class SnapshotInfo {

        private final long id;
        private final long timestamp;
        private final String saveCause;
        private final boolean pinned;

        public SnapshotInfo(long id, long timestamp, String saveCause, boolean pinned) {
            this.id = id;
            this.timestamp = timestamp;
            this.saveCause = saveCause;
            this.pinned = pinned;
        }

        public long getId() { return id; }

        public long getTimestamp() { return timestamp; }

        public String getSaveCause() { return saveCause; }

        public boolean isPinned() { return pinned; }
    }
}
