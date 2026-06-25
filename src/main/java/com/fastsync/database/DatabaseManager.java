package com.fastsync.database;

import com.fastsync.config.ConfigManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 * Database manager using HikariCP connection pool.
 * Stores player data as LONGBLOB (raw compressed byte[]) - no base64 string encoding.
 * Implements cross-server lock mechanism to prevent data races.
 * Uses Dynamo-style optimistic concurrency control based on a version column.
 */
public class DatabaseManager {

    private final Logger logger;
    private final ConfigManager config;
    private HikariDataSource dataSource;
    private String dataTable;

    public DatabaseManager(Logger logger, ConfigManager config) {
        this.logger = logger;
        this.config = config;
    }

    /**
     * Initialize the database connection pool and create tables.
     */
    public void initialize() throws SQLException {
        dataTable = config.getTablePrefix() + "player_data";

        HikariConfig hikariConfig = new HikariConfig();

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?%s",
            config.getDbHost(),
            config.getDbPort(),
            config.getDbDatabase(),
            config.getDbParameters());

        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(config.getDbUsername());
        hikariConfig.setPassword(config.getDbPassword());
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");

        hikariConfig.setMaximumPoolSize(config.getPoolSize());
        hikariConfig.setConnectionTimeout(config.getConnectionTimeout());
        hikariConfig.setIdleTimeout(config.getIdleTimeout());
        hikariConfig.setMaxLifetime(config.getMaxLifetime());
        hikariConfig.setLeakDetectionThreshold(config.getLeakDetectionThreshold());

        hikariConfig.setPoolName("FastSync-HikariPool");

        // MySQL optimizations
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
        hikariConfig.addDataSourceProperty("useLocalSessionState", "true");
        hikariConfig.addDataSourceProperty("rewriteBatchedStatements", "true");
        hikariConfig.addDataSourceProperty("cacheResultSetMetadata", "true");
        hikariConfig.addDataSourceProperty("cacheServerConfiguration", "true");
        hikariConfig.addDataSourceProperty("elideSetAutoCommits", "true");
        hikariConfig.addDataSourceProperty("maintainTimeStats", "false");

        dataSource = new HikariDataSource(hikariConfig);

        createTables();
        migrateSchema();
        logger.info("Database connection established: " + config.getDbHost() + ":" + config.getDbPort() + "/" + config.getDbDatabase());
    }

    /**
     * Create the player data table if it doesn't exist.
     * Uses LONGBLOB to support large compressed data without the 64KB BLOB limit.
     * Includes version and checksum columns for optimistic concurrency control.
     */
    private void createTables() throws SQLException {
        String sql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `uuid` VARCHAR(36) NOT NULL,
                `data` LONGBLOB NOT NULL,
                `version` BIGINT NOT NULL DEFAULT 0,
                `checksum` BIGINT NOT NULL DEFAULT 0,
                `fencing_token` BIGINT NOT NULL DEFAULT 0,
                `op_seq` BIGINT NOT NULL DEFAULT 0,
                `locked_by` VARCHAR(64) DEFAULT NULL,
                `locked_at` BIGINT DEFAULT NULL,
                `last_server` VARCHAR(64) DEFAULT NULL,
                `last_updated` BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (`uuid`),
                INDEX idx_locked (`locked_by`, `locked_at`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, dataTable);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * Migrate the schema for existing tables.
     * Adds version and checksum columns if they don't already exist.
     */
    private void migrateSchema() throws SQLException {
        // Add version column if it doesn't exist (MySQL 8+ supports IF NOT EXISTS)
        // For older MySQL, use information_schema check
        String[] migrations = {
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `version` BIGINT NOT NULL DEFAULT 0", dataTable),
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `checksum` BIGINT NOT NULL DEFAULT 0", dataTable),
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `fencing_token` BIGINT NOT NULL DEFAULT 0", dataTable),
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `op_seq` BIGINT NOT NULL DEFAULT 0", dataTable)
        };
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String sql : migrations) {
                try {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    // Column may already exist (MySQL versions that don't support IF NOT EXISTS)
                    if (!e.getMessage().contains("Duplicate column")) {
                        logger.warning("Schema migration note: " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Acquire a lock for a player's data and generate a fencing token.
     *
     * <p>Per Kleppmann's fencing token pattern: every successful lock acquisition
     * increments a monotonically increasing {@code fencing_token} in the database.
     * This token must be presented on save; the storage layer rejects any write
     * whose fencing token is less than the stored value, preventing stale writes
     * from servers that experienced GC pauses, network delays, or clock skew.
     *
     * <p>This is stronger than Redis SET NX PX locks, which cannot generate
     * fencing tokens and thus cannot defend against stale writes at the storage layer.
     *
     * <p><b>InnoDB locking (per MySQL 8.4 docs):</b> The UPDATE uses
     * {@code WHERE uuid = ?} which hits the PRIMARY KEY index — a single-row
     * X lock with no gap locks or next-key locks. This is safe for high
     * concurrency: different UUIDs never contend on the same index entry.
     * The subsequent SELECT is also a PK point lookup (no index scan).
     *
     * @return LockResult with acquired=true and the fencing token, or acquired=false
     */
    public LockResult acquireLock(UUID uuid, String serverName) throws SQLException {
        long now = System.currentTimeMillis();
        long expiredTime = now - (config.getLockTimeout() * 1000L);

        // First, ensure the player row exists
        ensurePlayerExists(uuid);

        // Atomically acquire lock AND increment fencing token
        String updateSql = String.format("""
            UPDATE `%s` SET locked_by = ?, locked_at = ?, fencing_token = fencing_token + 1
            WHERE uuid = ? AND (locked_by IS NULL OR locked_at < ? OR locked_by = ?)
            """, dataTable);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(updateSql)) {
            ps.setString(1, serverName);
            ps.setLong(2, now);
            ps.setString(3, uuid.toString());
            ps.setLong(4, expiredTime);
            ps.setString(5, serverName);
            int updated = ps.executeUpdate();
            if (updated == 0) {
                return LockResult.FAILED;
            }
        }

        // Read back the new fencing token
        String selectSql = String.format(
            "SELECT fencing_token FROM `%s` WHERE uuid = ?", dataTable);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(selectSql)) {
            ps.setString(1, uuid.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return LockResult.success(rs.getLong("fencing_token"));
                }
            }
        }
        return LockResult.FAILED;
    }

    /**
     * Ensure a player row exists in the database.
     */
    private void ensurePlayerExists(UUID uuid) throws SQLException {
        String sql = String.format("""
            INSERT IGNORE INTO `%s` (uuid, data, version, checksum, fencing_token, op_seq, locked_by, locked_at, last_server, last_updated)
            VALUES (?, ?, 0, 0, 0, 0, NULL, NULL, NULL, 0)
            """, dataTable);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            ps.setBytes(2, new byte[0]);
            ps.executeUpdate();
        }
    }

    /**
     * Load player data from the database along with its version and checksum.
     * Returns VersionedData.EMPTY if no data exists or data is empty.
     */
    public VersionedData loadData(UUID uuid) throws SQLException {
        String sql = String.format(
            "SELECT data, version, checksum, fencing_token FROM `%s` WHERE uuid = ?", dataTable);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    byte[] data = rs.getBytes("data");
                    long version = rs.getLong("version");
                    long checksum = rs.getLong("checksum");
                    long fencingToken = rs.getLong("fencing_token");
                    if (data != null && data.length > 0) {
                        return new VersionedData(data, version, checksum, fencingToken);
                    }
                }
            }
        }
        return VersionedData.EMPTY;
    }

    /**
     * Save player data with optimistic concurrency control AND fencing token verification.
     *
     * <p>This implements two layers of defence against stale writes:
     *
     * <ol>
     *   <li><b>Version check (Dynamo-style):</b> {@code WHERE version = expectedVersion}
     *       ensures the row hasn't been modified since we loaded it.
     *
     *   <li><b>Fencing token check (Kleppmann/ZooKeeper-style):</b>
     *       {@code WHERE fencing_token <= fencingToken} ensures that even if the
     *       version somehow matches (e.g. row was reset), a stale lock holder
     *       whose fencing token is less than the current stored value cannot
     *       overwrite data written by a newer lock holder.
     * </ol>
     *
     * <p>The fencing token defence is critical: it handles the case where a server
     * acquires lock (token 33), pauses for GC, lease expires, another server
     * acquires lock (token 34) and writes, then the first server resumes and
     * tries to write with its stale token 33. The DB rejects: 33 < 34.
     *
     * @param uuid          player UUID
     * @param data          compressed data blob
     * @param checksum      CRC32 checksum of the uncompressed data
     * @param expectedVersion the version this data was loaded from
     * @param fencingToken  the fencing token assigned when the lock was acquired
     * @param serverName    server writing the data
     * @return true if saved successfully, false if version conflict or fencing token violation
     */
    public boolean saveData(UUID uuid, byte[] data, long checksum, long expectedVersion,
                            long fencingToken, String serverName) throws SQLException {
        long now = System.currentTimeMillis();
        // Both version AND fencing token must pass for the write to succeed.
        // - version = expectedVersion: optimistic concurrency (row not modified since load)
        // - fencing_token <= fencingToken: the writer's lock is not stale
        //   (a newer lock holder would have a higher stored fencing_token, rejecting us)
        String sql = String.format("""
            UPDATE `%s` SET data = ?, version = version + 1, checksum = ?,
            locked_by = NULL, locked_at = NULL, last_server = ?, last_updated = ?
            WHERE uuid = ? AND version = ? AND fencing_token <= ?
            """, dataTable);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setBytes(1, data);
            ps.setLong(2, checksum);
            ps.setString(3, serverName);
            ps.setLong(4, now);
            ps.setString(5, uuid.toString());
            ps.setLong(6, expectedVersion);
            ps.setLong(7, fencingToken);
            return ps.executeUpdate() > 0;
        }
    }

    /**
     * Get the current fencing token for a player in the database.
     * Used for conflict diagnosis (determining if a stale lock holder tried to write).
     */
    public long getCurrentFencingToken(UUID uuid) throws SQLException {
        String sql = String.format("SELECT fencing_token FROM `%s` WHERE uuid = ?", dataTable);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("fencing_token");
                }
            }
        }
        return -1;
    }

    /**
     * Atomically increment and return the per-UUID operation sequence number.
     *
     * <p><b>InnoDB locking note:</b> This uses MySQL's {@code LAST_INSERT_ID(expr)} trick
     * to atomically increment a counter and read back the new value in a single
     * connection. The {@code UPDATE} is a single-row primary-key update (X lock on
     * one row only — no gap locks, no next-key locks), and {@code SELECT LAST_INSERT_ID()}
     * reads the connection-local value. This replaces the unsafe
     * {@code SELECT MAX(seq) ... FOR UPDATE} pattern which caused gap locks on the
     * operation_log index, blocking concurrent inserts for adjacent UUIDs.
     *
     * <p>InnoDB locking rules followed:
     * <ol>
     *   <li>UUID primary key direct access — single-row X lock, no index scan
     *   <li>No range query + FOR UPDATE on hot path
     *   <li>Precise CAS on uuid — {@code WHERE uuid = ?} hits the PK index
     * </ol>
     *
     * @return the new sequence number (monotonically increasing per UUID)
     */
    public long incrementOpSeq(UUID uuid) throws SQLException {
        // Ensure the player row exists before incrementing; otherwise the UPDATE
        // would affect zero rows and LAST_INSERT_ID() would return 0.
        ensurePlayerExists(uuid);

        try (Connection conn = dataSource.getConnection()) {
            // Atomically increment op_seq and set it as LAST_INSERT_ID for this connection
            String updateSql = String.format(
                "UPDATE `%s` SET op_seq = LAST_INSERT_ID(op_seq + 1) WHERE uuid = ?", dataTable);
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                ps.setString(1, uuid.toString());
                ps.executeUpdate();
            }

            // Read back the connection-local LAST_INSERT_ID (no extra DB round-trip for locking)
            try (PreparedStatement ps = conn.prepareStatement("SELECT LAST_INSERT_ID()")) {
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
            }
        }
        return 1;
    }

    /**
     * Get the current version of player data in the database.
     * Used for conflict diagnosis.
     */
    public long getCurrentVersion(UUID uuid) throws SQLException {
        String sql = String.format("SELECT version FROM `%s` WHERE uuid = ?", dataTable);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("version");
                }
            }
        }
        return -1;
    }

    /**
     * Release the lock without saving data (e.g., on error).
     */
    public void releaseLock(UUID uuid, String serverName) throws SQLException {
        String sql = String.format("""
            UPDATE `%s` SET locked_by = NULL, locked_at = NULL
            WHERE uuid = ? AND (locked_by = ? OR locked_by IS NULL)
            """, dataTable);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            ps.setString(2, serverName);
            ps.executeUpdate();
        }
    }

    /**
     * Get the server that currently holds the lock for a player.
     * Returns null if not locked.
     */
    public String getLockHolder(UUID uuid) throws SQLException {
        String sql = String.format("""
            SELECT locked_by FROM `%s` WHERE uuid = ?
            """, dataTable);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, uuid.toString());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("locked_by");
                }
            }
        }
        return null;
    }

    /**
     * Close the connection pool.
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Database connection pool closed.");
        }
    }

    /**
     * Check if the connection pool is healthy.
     */
    public boolean isHealthy() {
        if (dataSource == null || dataSource.isClosed()) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            return conn.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Get the HikariCP data source (for stats).
     */
    public HikariDataSource getDataSource() {
        return dataSource;
    }

    /**
     * Compute CRC32 checksum of data for integrity verification.
     */
    public static long computeChecksum(byte[] data) {
        if (data == null || data.length == 0) return 0;
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    /**
     * Verify that data matches its stored checksum.
     * Returns true if checksum is valid (or checksum is 0 = not yet computed).
     */
    public static boolean verifyChecksum(byte[] data, long expectedChecksum) {
        if (expectedChecksum == 0) return true; // no checksum stored yet
        if (data == null || data.length == 0) return expectedChecksum == 0;
        return computeChecksum(data) == expectedChecksum;
    }
}
