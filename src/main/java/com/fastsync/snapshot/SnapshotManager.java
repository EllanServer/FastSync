package com.fastsync.snapshot;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
    private final Executor executor;

    private DatabaseManager databaseManager;
    private String snapshotTable;

    /**
     * @param executor the dedicated executor for async DB operations.
     *                 Must NOT be ForkJoinPool.commonPool() — use the plugin's
     *                 AsyncExecutor to ensure bounded threads and backpressure.
     */
    public SnapshotManager(Logger logger, ConfigManager config, Executor executor) {
        this.logger = logger;
        this.config = config;
        this.executor = executor;
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
        createTable();
        logger.info("SnapshotManager initialized (table=" + snapshotTable + ").");
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
        String sql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `id` BIGINT NOT NULL AUTO_INCREMENT,
                `uuid` VARCHAR(36) NOT NULL,
                `data` LONGBLOB NOT NULL,
                `timestamp` BIGINT NOT NULL,
                `save_cause` VARCHAR(64) NOT NULL DEFAULT 'unknown',
                `pinned` BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (`id`),
                INDEX idx_snapshots_uuid (`uuid`),
                INDEX idx_snapshots_uuid_id (`uuid`, `id`)
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
        return CompletableFuture.supplyAsync(() -> {
            String sql = String.format("""
                INSERT INTO `%s` (uuid, data, timestamp, save_cause, pinned)
                VALUES (?, ?, ?, ?, ?)
                """, snapshotTable);

            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, uuid.toString());
                ps.setBytes(2, compressedData);
                ps.setLong(3, System.currentTimeMillis());
                ps.setString(4, saveCause != null ? saveCause : "unknown");
                ps.setBoolean(5, false);
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
        }, executor);
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
        return CompletableFuture.supplyAsync(() -> {
            // ORDER BY id DESC, NOT timestamp DESC.
            // Per Spanner: wall-clock timestamps are unreliable for ordering
            // due to cross-server clock skew. Auto-increment id is a true
            // monotonic sequence assigned by MySQL in commit order.
            String sql = String.format("""
                SELECT id, timestamp, save_cause, pinned
                FROM `%s`
                WHERE uuid = ?
                ORDER BY id DESC
                """, snapshotTable);

            List<SnapshotInfo> snapshots = new ArrayList<>();
            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, uuid.toString());
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
        }, executor);
    }

    /**
     * Load the raw compressed data of a snapshot.
     *
     * @param snapshotId the snapshot id
     * @return a future that completes with the compressed data, or {@code null} if no such snapshot exists
     */
    public CompletableFuture<byte[]> loadSnapshot(long snapshotId) {
        return CompletableFuture.supplyAsync(() -> {
            String sql = String.format("""
                SELECT data FROM `%s` WHERE id = ?
                """, snapshotTable);

            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, snapshotId);
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
        }, executor);
    }

    /**
     * Delete a snapshot by id.
     *
     * @param snapshotId the snapshot id
     * @return a future that completes when the snapshot has been deleted
     */
    public CompletableFuture<Void> deleteSnapshot(long snapshotId) {
        return CompletableFuture.supplyAsync(() -> {
            String sql = String.format("""
                DELETE FROM `%s` WHERE id = ?
                """, snapshotTable);

            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, snapshotId);
                int affected = ps.executeUpdate();
                if (config.isDebug()) {
                    logger.info("Deleted snapshot " + snapshotId + " (affected=" + affected + ")");
                }
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to delete snapshot " + snapshotId, e);
                throw new RuntimeException(e);
            }
            return null;
        }, executor);
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
        return CompletableFuture.supplyAsync(() -> {
            String sql = String.format("""
                UPDATE `%s` SET pinned = ? WHERE id = ?
                """, snapshotTable);

            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setBoolean(1, pinned);
                ps.setLong(2, snapshotId);
                ps.executeUpdate();
                if (config.isDebug()) {
                    logger.info("Snapshot " + snapshotId + " pinned=" + pinned);
                }
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to update pin state for snapshot " + snapshotId, e);
                throw new RuntimeException(e);
            }
            return null;
        }, executor);
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
        return CompletableFuture.supplyAsync(() -> {
            int keep = Math.max(0, maxSnapshots);

            // Fetch non-pinned snapshot ids, newest first. The newest `keep` are
            // retained; everything older is collected for deletion.
            // ORDER BY id DESC (not timestamp) — per Spanner, auto-increment id
            // is the reliable monotonic sequence, immune to clock skew.
            String selectSql = String.format("""
                SELECT id FROM `%s`
                WHERE uuid = ? AND pinned = FALSE
                ORDER BY id DESC
                """, snapshotTable);

            List<Long> toDelete = new ArrayList<>();
            try (Connection conn = getDataSource().getConnection();
                 PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setString(1, uuid.toString());
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
        }, executor);
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
            DELETE FROM `%s` WHERE id IN (%s)
            """, snapshotTable, placeholders);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setLong(i + 1, ids.get(i));
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
