package com.fastsync.database;

import com.fastsync.config.ConfigManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.zip.CRC32;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;

/**
 * Database manager using HikariCP connection pool.
 * Stores player data as LONGBLOB (raw compressed byte[]) - no base64 string encoding.
 * Implements cross-server lock mechanism to prevent data races.
 * Uses Dynamo-style optimistic concurrency control based on a version column.
 *
 * <p>All data access is expressed with jOOQ's type-safe DSL. No jOOQ code
 * generation is used: table and column references are built with the plain
 * {@code table(name(...))} / {@code field(name(...), type.class)} factory
 * methods. DDL (CREATE/ALTER TABLE) and the MySQL {@code LAST_INSERT_ID(expr)}
 * idiom are still issued as raw SQL through {@link DSLContext#execute(String)},
 * since jOOQ's DDL DSL is overly verbose and {@code LAST_INSERT_ID} has no DSL
 * equivalent. Each operation borrows a single connection from HikariCP and wraps
 * it in a per-call {@code DSLContext} via {@code DSL.using(connection, SQLDialect.MYSQL)}.
 */
public class DatabaseManager {

    // ---- jOOQ table/column references (no code generation) ----
    // Column names are prefix-independent; only the table name carries the
    // configured prefix, so the column Fields can be static constants while the
    // Table is resolved once the prefix is known in initialize().
    private static final Field<String> UUID_FIELD = field(name("uuid"), String.class);
    private static final Field<byte[]> DATA_FIELD = field(name("data"), byte[].class);
    private static final Field<Long> VERSION_FIELD = field(name("version"), Long.class);
    private static final Field<Long> CHECKSUM_FIELD = field(name("checksum"), Long.class);
    private static final Field<Long> FENCING_TOKEN_FIELD = field(name("fencing_token"), Long.class);
    private static final Field<String> LOCKED_BY_FIELD = field(name("locked_by"), String.class);
    private static final Field<Long> LOCKED_AT_FIELD = field(name("locked_at"), Long.class);
    private static final Field<String> LOCK_SESSION_ID_FIELD = field(name("lock_session_id"), String.class);
    private static final Field<String> LAST_SERVER_FIELD = field(name("last_server"), String.class);
    private static final Field<Long> LAST_UPDATED_FIELD = field(name("last_updated"), Long.class);

    // Phase 2: per-component storage fields (table: fastsync_player_component)
    private static final Field<String> COMPONENT_NAME_FIELD = field(name("component"), String.class);
    private static final Field<Long> COMPONENT_BITMAP_FIELD = field(name("component_bitmap"), Long.class);
    private static final Field<Long> COMPONENT_GENERATION_FIELD = field(name("component_generation"), Long.class);
    private static final Field<Long> COMPONENT_UPDATED_AT_FIELD = field(name("updated_at"), Long.class);
    private static final Field<Long> GENERATION_FIELD = field(name("generation"), Long.class);

    /**
     * Fail-closed guard for production lock operations. All save/release/
     * heartbeat/component-upsert methods MUST pass a non-null, non-blank
     * lockSessionId. Passing null would bypass the lock_session_id WHERE
     * clause and could clear/overwrite another session's lock — exactly the
     * race that lock_session_id was added to prevent.
     *
     * <p>Test/migration compatibility is handled by the deprecated overload
     * (e.g. {@link #releaseLock(UUID, String)}) which does not call this
     * guard. Production callers (SyncManager) must always track and pass the
     * session id from {@code playerLockSessions}.
     *
     * @throws IllegalArgumentException if lockSessionId is null or blank
     */
    private static void requireLockSession(String lockSessionId, String operation) {
        if (lockSessionId == null || lockSessionId.isBlank()) {
            throw new IllegalArgumentException(
                operation + " requires non-null, non-blank lockSessionId — refusing to bypass lock_session_id WHERE clause");
        }
    }

    private final Logger logger;
    private final ConfigManager config;
    private HikariDataSource dataSource;
    private String dataTable;
    private String componentTable;
    private Table<Record> playerData;
    private Table<Record> playerComponent;

    public DatabaseManager(Logger logger, ConfigManager config) {
        this.logger = logger;
        this.config = config;
    }

    /**
     * Wrap a single JDBC connection in a jOOQ {@link DSLContext} configured for
     * MySQL. A fresh context is created per operation so that each borrows
     * exactly one connection from the HikariCP pool.
     */
    private static DSLContext dsl(Connection connection) {
        return using(connection, SQLDialect.MYSQL);
    }

    /**
     * Initialize the database connection pool and create tables.
     */
    public void initialize() throws SQLException {
        // Validate table prefix — only alphanumeric + underscore allowed to
        // prevent SQL injection via config (e.g. backticks, spaces, semicolons).
        String prefix = config.getTablePrefix();
        if (prefix != null && !prefix.matches("[A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(
                "Invalid database.table-prefix: '" + prefix
                + "' — only alphanumeric characters and underscores are allowed.");
        }
        dataTable = config.getTablePrefix() + "player_data";
        playerData = table(name(dataTable));
        componentTable = config.getTablePrefix() + "player_component";
        playerComponent = table(name(componentTable));

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
        logger.info("Database connection established: " + config.getDbHost() + ":" + config.getDbPort() + "/" + config.getDbDatabase());
    }

    /**
     * Create the player data table if it doesn't exist.
     * Uses LONGBLOB to support large compressed data without the 64KB BLOB limit.
     * Includes version and checksum columns for optimistic concurrency control.
     *
     * <p>DDL is issued as raw SQL via {@link DSLContext#execute(String)} because
     * jOOQ's DDL DSL is far more verbose than the equivalent CREATE TABLE
     * statement for no type-safety benefit here.
     */
    private void createTables() throws SQLException {
        String sql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `uuid` VARCHAR(36) NOT NULL,
                `data` LONGBLOB NOT NULL,
                `version` BIGINT NOT NULL DEFAULT 0,
                `checksum` BIGINT NOT NULL DEFAULT 0,
                `fencing_token` BIGINT NOT NULL DEFAULT 0,
                `locked_by` VARCHAR(64) DEFAULT NULL,
                `locked_at` BIGINT DEFAULT NULL,
                `lock_session_id` VARCHAR(64) DEFAULT NULL,
                `last_server` VARCHAR(64) DEFAULT NULL,
                `last_updated` BIGINT NOT NULL DEFAULT 0,
                `component_bitmap` BIGINT NOT NULL DEFAULT 0,
                `component_generation` BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (`uuid`),
                INDEX idx_locked (`locked_by`, `locked_at`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, dataTable);

        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).execute(sql);
        }

        // Per-component storage table (phase 2). Each row holds one component
        // (inventory, vitals, food, etc.) for one player, with its own version
        // and checksum. The composite PK (uuid, component) gives O(1) point
        // lookups and lets us UPDATE only the changed component on save.
        //
        // The `generation` column tracks which full-Blob baseline this component
        // row belongs to. On load, only rows where generation == player_data.component_generation
        // are read — older generation rows are ignored (they belong to a previous
        // baseline that has been superseded by a full Blob save).
        String componentTableSql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `uuid` VARCHAR(36) NOT NULL,
                `component` VARCHAR(32) NOT NULL,
                `generation` BIGINT NOT NULL DEFAULT 0,
                `data` LONGBLOB NOT NULL,
                `version` BIGINT NOT NULL DEFAULT 0,
                `checksum` BIGINT NOT NULL DEFAULT 0,
                `updated_at` BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (`uuid`, `component`),
                INDEX idx_uuid_generation (`uuid`, `generation`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, componentTable);

        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).execute(componentTableSql);
        }
    }

    /**
     * Acquire a lock with an explicit per-session nonce.
     *
     * <p>The {@code lockSessionId} is a randomly-generated string (e.g. a UUID)
     * that uniquely identifies THIS acquire attempt. It is written to the
     * {@code lock_session_id} column alongside {@code locked_by}. The read-back
     * requires both {@code locked_by == serverName} AND
     * {@code lock_session_id == lockSessionId} — this prevents the
     * same-serverName quick-reconnect race where a prior session's lock is
     * still held (quit save in flight) and a naive {@code locked_by} check
     * would mistakenly return success.
     *
     * <p>All subsequent save / release / heartbeat / component-upsert calls
     * for this lock session MUST pass the same {@code lockSessionId} in their
     * WHERE clause, so that a stale session's writes are rejected even if
     * its fencing token has not yet been superseded.
     *
     * @param uuid          player UUID
     * @param serverName    server name (machine / backend identifier)
     * @param lockSessionId per-acquire nonce (caller-generated, e.g. UUID.randomUUID)
     * @return LockResult with acquired=true and the fencing token, or acquired=false
     */
    public LockResult acquireLock(UUID uuid, String serverName, String lockSessionId) throws SQLException {
        long now = System.currentTimeMillis();
        long expiredTime = now - (config.getLockTimeout() * 1000L);

        // INSERT ... ON DUPLICATE KEY UPDATE merges row creation and conditional
        // lock acquisition into one statement. The INSERT arm seeds a new player
        // row (fencing_token = 1, locked_by = us); the UPDATE arm bumps the token
        // and takes the lock only when it is free OR expired.
        //
        // CRITICAL: we do NOT allow `locked_by = serverName` re-acquisition.
        // The previous `OR locked_by = ?` clause let a player quit server A
        // (queuing an async quit save that still holds the DB lock) and
        // immediately reconnect to the SAME server A. The reconnect would
        // see `locked_by = "server-A"`, match the OR clause, bump the fencing
        // token, and let the new login read stale DB data. The pending quit
        // save would then CAS-fail against the new fencing token and the
        // player's final state would be lost.
        //
        // Without the OR clause, a quick reconnect to the same backend must
        // wait until either:
        //   (a) the quit save completes and calls releaseLock()/saveDataAndReleaseLock*
        //       which clears `locked_by` to NULL, OR
        //   (b) the lock naturally expires after lock-timeout seconds.
        //
        // (a) is the normal fast path — quit saves finish in milliseconds, so
        // the reconnect sees `locked_by IS NULL` on the first acquireLock
        // retry and proceeds immediately. (b) is the safety net for crashes.
        //
        // P0 SAFETY CHECK (round 10): lock_session_id — per-acquire nonce.
        //
        // The round-9 attempt to use locked_at == now broke CI (jOOQ Long
        // retrieval vs primitive long comparison subtlety). The round-8
        // removal of `OR locked_by = ?` was insufficient on its own because
        // the read-back only checked locked_by, which a prior same-serverName
        // session had already set.
        //
        // The robust fix: write a per-acquire nonce (lock_session_id) to a
        // new column. The read-back requires BOTH locked_by == serverName
        // AND lock_session_id == lockSessionId. When the UPDATE arm's IF is
        // false (lock held by prior session), lock_session_id is NOT updated
        // — it retains the prior session's nonce. Our nonce differs, so the
        // read-back returns FAILED. The login retries until the prior quit
        // save releases the lock (clearing lock_session_id to NULL) or the
        // lock expires.
        //
        // This is the reviewer-recommended "session_id / boot_id" approach.
        // We use a per-acquire nonce (not a per-JVM boot_id) because two
        // acquires from the same JVM (quit then quick reconnect) must also
        // be distinguished.
        String sql = String.format("""
            INSERT INTO `%s` (uuid, data, version, checksum, fencing_token, locked_by, locked_at, lock_session_id, last_server, last_updated)
            VALUES (?, '', 0, 0, 1, ?, ?, ?, NULL, 0)
            ON DUPLICATE KEY UPDATE
                fencing_token = IF(locked_by IS NULL OR locked_at < ?,
                                   LAST_INSERT_ID(fencing_token + 1),
                                   fencing_token),
                locked_by = IF(locked_by IS NULL OR locked_at < ?,
                               ?,
                               locked_by),
                locked_at = IF(locked_by IS NULL OR locked_at < ?,
                               ?,
                               locked_at),
                lock_session_id = IF(locked_by IS NULL OR locked_at < ?,
                                      ?,
                                      lock_session_id)
            """, dataTable);

        try (Connection conn = dataSource.getConnection()) {
            DSLContext dsl = dsl(conn);

            dsl.execute(sql,
                uuid.toString(),  // INSERT: uuid
                serverName,       // INSERT: locked_by
                now,              // INSERT: locked_at
                lockSessionId,    // INSERT: lock_session_id
                expiredTime,      // fencing_token IF predicate
                expiredTime,      // locked_by IF predicate
                serverName,       // locked_by new value
                expiredTime,      // locked_at IF predicate
                now,              // locked_at new value
                expiredTime,      // lock_session_id IF predicate
                lockSessionId     // lock_session_id new value
            );

            // Read back fencing_token, locked_by, AND lock_session_id using
            // raw JDBC (not jOOQ) to avoid Field type resolution issues that
            // caused null returns on CI MySQL.
            String readSql = String.format(
                "SELECT `fencing_token`, `locked_by`, `lock_session_id` FROM `%s` WHERE `uuid` = ?",
                dataTable);
            long readToken = 0;
            String readLockedBy = null;
            String readSessionId = null;
            try (var ps = conn.prepareStatement(readSql)) {
                ps.setString(1, uuid.toString());
                try (var rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return LockResult.FAILED;
                    }
                    readToken = rs.getLong("fencing_token");
                    readLockedBy = rs.getString("locked_by");
                    readSessionId = rs.getString("lock_session_id");
                }
            }

            if (!serverName.equals(readLockedBy)) {
                return LockResult.FAILED;
            }
            if (!lockSessionId.equals(readSessionId)) {
                return LockResult.FAILED;
            }
            return readToken > 0 ? LockResult.success(readToken) : LockResult.FAILED;
        }
    }

    /**
     * Load player data from the database along with its version and checksum.
     * Returns VersionedData.EMPTY if no data exists or data is empty.
     */
    public VersionedData loadData(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Record record = dsl(conn)
                .select(DATA_FIELD, VERSION_FIELD, CHECKSUM_FIELD, FENCING_TOKEN_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne();

            if (record != null) {
                byte[] data = record.get(DATA_FIELD);
                // Return real version/checksum/fencing_token even when data is
                // empty — see loadPlayerDataRow() for the full rationale.
                // Callers that depend on hasData() (which checks data.length>0)
                // still treat empty data as "no data", but the version is
                // preserved so the next save CAS-succeeds.
                if (data != null && data.length > 0) {
                    return new VersionedData(
                        data,
                        record.get(VERSION_FIELD),
                        record.get(CHECKSUM_FIELD),
                        record.get(FENCING_TOKEN_FIELD));
                }
                // Empty data but row exists — return a VersionedData with the
                // real version/fencing_token but empty data. hasData() returns
                // false, so callers treat this as "new player" for apply
                // purposes, but the version is correct for save CAS.
                return new VersionedData(
                    new byte[0],
                    record.get(VERSION_FIELD) != null ? record.get(VERSION_FIELD) : 0L,
                    record.get(CHECKSUM_FIELD) != null ? record.get(CHECKSUM_FIELD) : 0L,
                    record.get(FENCING_TOKEN_FIELD) != null ? record.get(FENCING_TOKEN_FIELD) : 0L);
            }
        }
        return VersionedData.EMPTY;
    }

    // ==================== Phase 2: Full Blob Save + Component Cleanup ====================

    /**
     * Full Blob save + release lock + atomically invalidate old component rows.
     *
     * <p><b>Why this exists:</b> When component-storage is enabled, periodic
     * saves write dirty components to the {@code player_component} table. If a
     * later full Blob save (QUIT/SHUTDOWN) writes the complete player state but
     * leaves old component rows behind, the next login would load the fresh
     * full Blob, then overlay it with stale component rows — silently rolling
     * back the player's state to when the component was last written.
     *
     * <p>This method prevents that by, in a single transaction:
     * <ol>
     *   <li>CAS-updating player_data with the new Blob + version+1 + clearing
     *       the lock (same as {@link #saveDataAndReleaseLock})</li>
     *   <li>Setting {@code component_bitmap = 0} (no components migrated)</li>
     *   <li>Incrementing {@code component_generation} by 1 — this makes all
     *       existing rows in {@code player_component} for this UUID
     *       <em>invisible</em> to future loads, because
     *       {@link #loadComponentsWithGeneration} only returns rows whose
     *       {@code generation} column matches the current
     *       {@code player_data.component_generation}.</li>
     * </ol>
     *
     * <p><b>Rows are NOT deleted</b> — only made invisible. A background
     * GC task ({@link #gcOldComponentRows}) can delete rows from old
     * generations later. This is intentional: deleting rows in the same
     * transaction as the CAS would hold row locks longer and complicate
     * rollback semantics.
     *
     * <p>If the CAS fails (version/fencing mismatch), the transaction is
     * rolled back and no rows are touched — preserving them for diagnosis.
     * Returns false in that case, same as {@link #saveDataAndReleaseLock}.
     *
     * @return true if the full Blob was saved and component_generation bumped;
     *         false if CAS failed (version/fencing conflict).
     */
    public boolean saveDataAndReleaseLockClearComponents(UUID uuid, byte[] data, long checksum,
            long expectedVersion, long fencingToken, String serverName, String lockSessionId) throws SQLException {
        requireLockSession(lockSessionId, "saveDataAndReleaseLockClearComponents");
        long now = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // CAS update player_data with new Blob + clear lock + reset bitmap
                // + increment component_generation. The generation increment
                // invalidates all old component rows without deleting them —
                // on load, only rows where generation == current generation are read.
                // Old rows can be GC'd later by a background task.
                //
                // P0 (round 10): WHERE now includes lock_session_id so a stale
                // session's quit save (whose lock was superseded by a new acquire)
                // cannot release the new session's lock or overwrite its data.
                int updated = dsl(conn).update(playerData)
                    .set(DATA_FIELD, data)
                    .set(VERSION_FIELD, VERSION_FIELD.plus(1))
                    .set(CHECKSUM_FIELD, checksum)
                    .set(LOCKED_BY_FIELD, (String) null)
                    .set(LOCKED_AT_FIELD, (Long) null)
                    .set(LOCK_SESSION_ID_FIELD, (String) null)
                    .set(LAST_SERVER_FIELD, serverName)
                    .set(LAST_UPDATED_FIELD, now)
                    .set(COMPONENT_BITMAP_FIELD, 0L)
                    .set(COMPONENT_GENERATION_FIELD, COMPONENT_GENERATION_FIELD.plus(1))
                    .where(UUID_FIELD.eq(uuid.toString())
                        .and(VERSION_FIELD.eq(expectedVersion))
                        .and(FENCING_TOKEN_FIELD.eq(fencingToken))
                        .and(LOCKED_BY_FIELD.eq(serverName))
                        .and(LOCK_SESSION_ID_FIELD.eq(lockSessionId)))
                    .execute();

                if (updated <= 0) {
                    conn.rollback();
                    return false;
                }

                // Note: we do NOT delete component rows here. The generation
                // increment makes them invisible to future loads. A background
                // GC task can clean them up: DELETE WHERE generation < current - 1.
                conn.commit();
                return true;
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    /**
     * Full Blob save + keep lock + invalidate old component rows via generation bump.
     *
     * <p>Same as {@link #saveDataAndReleaseLockClearComponents} but keeps the
     * lock (online/periodic save path). The full Blob becomes the new baseline,
     * so component_generation is incremented to invalidate old component rows.
     *
     * @return true if the full Blob was saved;
     *         false if CAS failed (version/fencing conflict).
     */
    public boolean saveDataKeepLockClearComponents(UUID uuid, byte[] data, long checksum,
            long expectedVersion, long fencingToken, String serverName, String lockSessionId) throws SQLException {
        requireLockSession(lockSessionId, "saveDataKeepLockClearComponents");
        long now = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                int updated = dsl(conn).update(playerData)
                    .set(DATA_FIELD, data)
                    .set(VERSION_FIELD, VERSION_FIELD.plus(1))
                    .set(CHECKSUM_FIELD, checksum)
                    .set(LAST_SERVER_FIELD, serverName)
                    .set(LAST_UPDATED_FIELD, now)
                    .set(LOCKED_AT_FIELD, now)
                    .set(COMPONENT_BITMAP_FIELD, 0L)
                    .set(COMPONENT_GENERATION_FIELD, COMPONENT_GENERATION_FIELD.plus(1))
                    // locked_by and lock_session_id are NOT cleared — we still hold the lock
                    .where(UUID_FIELD.eq(uuid.toString())
                        .and(VERSION_FIELD.eq(expectedVersion))
                        .and(FENCING_TOKEN_FIELD.eq(fencingToken))
                        .and(LOCKED_BY_FIELD.eq(serverName))
                        .and(LOCK_SESSION_ID_FIELD.eq(lockSessionId)))
                    .execute();

                if (updated <= 0) {
                    conn.rollback();
                    return false;
                }

                conn.commit();
                return true;
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    /**
     * Get the current fencing token for a player in the database.
     * Used for conflict diagnosis (determining if a stale lock holder tried to write).
     */
    public long getCurrentFencingToken(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Long token = dsl(conn).select(FENCING_TOKEN_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne(FENCING_TOKEN_FIELD);
            return token != null ? token : -1;
        }
    }

    /**
     * Get the current version of player data in the database.
     * Used for conflict diagnosis.
     */
    public long getCurrentVersion(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Long version = dsl(conn).select(VERSION_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne(VERSION_FIELD);
            return version != null ? version : -1;
        }
    }

    /**
     * Release a lock using a fencing token + lock-session id condition.
     *
     * <p>Clears the lock only if {@code locked_by = serverName AND
     * fencing_token = fencingToken AND lock_session_id = lockSessionId}.
     * This prevents a stale session (whose lock was superseded by a new acquire
     * on the same serverName) from releasing the new session's lock.
     *
     * @param uuid         player UUID
     * @param serverName   server that holds the lock
     * @param fencingToken fencing token assigned when the lock was acquired
     * @param lockSessionId per-acquire nonce that must match the stored value
     * @return true if the lock was released, false if the lock is no longer ours
     */
    public boolean releaseLock(UUID uuid, String serverName, long fencingToken, String lockSessionId) throws SQLException {
        requireLockSession(lockSessionId, "releaseLock");
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).update(playerData)
                .set(LOCKED_BY_FIELD, (String) null)
                .set(LOCKED_AT_FIELD, (Long) null)
                .set(LOCK_SESSION_ID_FIELD, (String) null)
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(LOCKED_BY_FIELD.eq(serverName))
                    .and(FENCING_TOKEN_FIELD.eq(fencingToken))
                    .and(LOCK_SESSION_ID_FIELD.eq(lockSessionId)))
                .execute() > 0;
        }
    }

    /**
     * Refresh the lock timestamp for an online player (heartbeat).
     *
     * <p>This is called periodically by the heartbeat task to prevent the lock
     * from expiring while the player is still online. The update is conditional
     * on {@code locked_by = serverName AND fencing_token = fencingToken} to
     * ensure we only refresh our own lock — if the lock was already taken over
     * by another server (stale lock expiry), this update affects 0 rows and
     * returns false, signalling a serious lock-infringement condition.
     *
     * @param uuid         player UUID
     * @param serverName   server that holds the lock
     * @param fencingToken fencing token assigned when the lock was acquired
     * @return true if the lock was refreshed (still ours), false if the lock
     *         is no longer held by us (possible infringement — must reload)
     */
    public boolean refreshLock(UUID uuid, String serverName, long fencingToken, String lockSessionId) throws SQLException {
        requireLockSession(lockSessionId, "refreshLock");
        long now = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).update(playerData)
                .set(LOCKED_AT_FIELD, now)
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(LOCKED_BY_FIELD.eq(serverName))
                    .and(FENCING_TOKEN_FIELD.eq(fencingToken))
                    .and(LOCK_SESSION_ID_FIELD.eq(lockSessionId)))
                .execute() > 0;
        }
    }

    /**
     * Batch refresh of lock timestamps for multiple players (heartbeat).
     *
     * <p>Uses a single JDBC connection and a single PreparedStatement with
     * {@code addBatch()} to refresh all players' {@code locked_at} in one
     * round-trip. Players whose refresh affects 0 rows (lock no longer ours)
     * are added to the {@code failedPlayers} set.
     *
     * <p>This is significantly more efficient than calling {@link #refreshLock}
     * per player when there are many online players, as it amortizes connection
     * acquisition and network round-trips.
     *
     * @param playersToRefresh map of UUID -> fencing token for each player
     * @param serverName       server that holds the locks
     * @param failedPlayers    output set — UUIDs whose refresh failed (0 rows updated)
     */
    public void refreshLockBatch(java.util.Map<UUID, Long> playersToRefresh,
                                  java.util.Map<UUID, String> playerLockSessions,
                                  String serverName,
                                  java.util.Set<UUID> failedPlayers) throws SQLException {
        if (playersToRefresh.isEmpty()) return;
        long now = System.currentTimeMillis();

        // Use raw JDBC for batch — jOOQ's batch API is more verbose for this use case.
        // P0 (round 10): WHERE now includes lock_session_id when available, so a
        // stale session's heartbeat cannot refresh a lock that a new session has
        // already acquired.
        String sql = "UPDATE `" + dataTable + "`"
            + " SET locked_at = ?"
            + " WHERE uuid = ? AND locked_by = ? AND fencing_token = ?"
            + " AND lock_session_id = ?";

        try (Connection conn = dataSource.getConnection();
             java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {

            for (java.util.Map.Entry<UUID, Long> entry : playersToRefresh.entrySet()) {
                UUID uuid = entry.getKey();
                String sessionId = playerLockSessions != null ? playerLockSessions.get(uuid) : null;
                ps.setLong(1, now);
                ps.setString(2, uuid.toString());
                ps.setString(3, serverName);
                ps.setLong(4, entry.getValue());
                ps.setString(5, sessionId);  // null matches the OR ... IS NULL arm
                ps.addBatch();
            }

            int[] results = ps.executeBatch();
            int i = 0;
            for (UUID uuid : playersToRefresh.keySet()) {
                // EXECUTE_FAILED (-3) means the driver couldn't execute this
                // particular statement in the batch. Treat it as a failure.
                // SUCCESS_NO_INFO (-2) means the driver executed the statement
                // but doesn't know the row count — we can't prove the lock
                // was refreshed. Conservative: verify with a single refreshLock().
                if (i >= results.length
                    || results[i] == java.sql.Statement.EXECUTE_FAILED
                    || results[i] == 0) {
                    failedPlayers.add(uuid);
                } else if (results[i] == java.sql.Statement.SUCCESS_NO_INFO) {
                    // Cannot prove that the lock row was actually refreshed.
                    // Conservative path: verify with a single conditional refresh.
                    Long token = playersToRefresh.get(uuid);
                    try {
                        if (token == null || !refreshLock(uuid, serverName, token, playerLockSessions != null ? playerLockSessions.get(uuid) : null)) {
                            failedPlayers.add(uuid);
                        }
                    } catch (SQLException verifyEx) {
                        failedPlayers.add(uuid);
                    }
                }
                i++;
            }
        }
    }

    /**
     * Get the server that currently holds the lock for a player.
     * Returns null if not locked.
     */
    public String getLockHolder(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).select(LOCKED_BY_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne(LOCKED_BY_FIELD);
        }
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
     *
     * <p>Issues a {@code SELECT 1} via jOOQ instead of {@link Connection#isValid(int)}
     * so the health probe exercises the same jOOQ execution path as real queries.
     * Both {@link SQLException} (pool/connection failures) and
     * {@link DataAccessException} (jOOQ-wrapped SQL errors) are treated as unhealthy.
     */
    public boolean isHealthy() {
        if (dataSource == null || dataSource.isClosed()) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).fetchValue("SELECT 1") != null;
        } catch (SQLException | DataAccessException e) {
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

    // ==================== Phase 2: Per-Component Storage ====================

    /**
     * Read the {@code component_bitmap} column for a player.
     *
     * <p>Each bit corresponds to one {@link com.fastsync.sync.dirty.ComponentDirtyMask.Component}
     * (bit position = {@code Component.ordinal()}). A set bit means that
     * component has been migrated to the {@code player_component} table and
     * should be read from there instead of the legacy single-Blob column.
     *
     * <p>Returns 0 for new players (no components migrated yet) or if the
     * column is not yet present (legacy installs before phase 2 migration).
     */
    public long getComponentBitmap(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Long bitmap = dsl(conn).select(COMPONENT_BITMAP_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne(COMPONENT_BITMAP_FIELD);
            return bitmap != null ? bitmap : 0L;
        }
    }

    /**
     * Delete all component rows for a player (used on data reset / conflict recovery).
     */
    public void deleteAllComponents(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).deleteFrom(playerComponent)
                .where(UUID_FIELD.eq(uuid.toString()))
                .execute();
        }
    }

    /**
     * Immutable holder for one component's stored data + metadata.
     */
    public record ComponentData(byte[] data, long version, long checksum) {
        public boolean hasData() {
            return data != null && data.length > 0;
        }
        public static ComponentData EMPTY = new ComponentData(new byte[0], 0L, 0L);
    }

    /**
     * Result of a batch component upsert with lock validation.
     *
     * @param success         whether the upsert succeeded
     * @param oldVersion      the player_data.version before this upsert
     * @param newVersion      the player_data.version after this upsert (oldVersion + 1)
     * @param componentBitmap the new component_bitmap (old | dirtyBits)
     * @param generation      the current component_generation
     * @param errorMessage    rejection reason if success=false
     */
    public record ComponentBatchResult(
            boolean success,
            long oldVersion,
            long newVersion,
            long componentBitmap,
            long generation,
            String errorMessage) {
        public static ComponentBatchResult rejected(String msg) {
            return new ComponentBatchResult(false, -1, -1, 0, 0, msg);
        }
        public static ComponentBatchResult success(long oldVersion, long newVersion,
                long bitmap, long generation) {
            return new ComponentBatchResult(true, oldVersion, newVersion, bitmap, generation, null);
        }
    }

    /**
     * Full player_data row for load path, including component storage metadata.
     *
     * <p>Extends {@link VersionedData} with component_bitmap and component_generation,
     * needed by the load path to decide whether to read component rows and at
     * which generation.
     */
    public record PlayerDataRow(
            byte[] data,
            long version,
            long checksum,
            long fencingToken,
            long componentBitmap,
            long componentGeneration) {
        public boolean hasData() {
            return data != null && data.length > 0;
        }
        public static PlayerDataRow EMPTY = new PlayerDataRow(
            new byte[0], 0, 0, 0, 0, 0);
    }

    /**
     * Load the full player_data row including component storage metadata.
     *
     * <p>This is the phase-3 replacement for {@link #loadData(UUID)} — it returns
     * component_bitmap and component_generation so the caller can decide whether
     * to load component rows and at which generation.
     */
    public PlayerDataRow loadPlayerDataRow(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Record record = dsl(conn)
                .select(DATA_FIELD, VERSION_FIELD, CHECKSUM_FIELD, FENCING_TOKEN_FIELD,
                        COMPONENT_BITMAP_FIELD, COMPONENT_GENERATION_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne();

            if (record != null) {
                // CRITICAL: return the row's real metadata even when the data
                // Blob is empty. The previous code fell through to
                // PlayerDataRow.EMPTY (all zeros) when data was null/empty,
                // which lost the row's actual version/checksum/fencing_token/
                // bitmap/generation.
                //
                // This matters for several real scenarios:
                //   - Migration residue: a row exists with empty data but
                //     version=5 from a previous plugin version. Returning
                //     version=0 would cause the next save to CAS-fail.
                //   - Manual clear: an operator cleared the data column but
                //     left version intact. Same CAS-fail risk.
                //   - Conflict recovery: a recovery script may have nulled
                //     the Blob but kept the version/fencing_token for audit.
                //   - Empty Blob after acquireLock INSERT: the INSERT arm of
                //     acquireLock seeds a row with data='' and version=0,
                //     which is correct here (version IS 0). But if a prior
                //     save had bumped version and then a full Blob save with
                //     empty data happened (shouldn't, but defensive), we'd
                //     want the real version.
                //
                // hasData() still returns false for empty data, so the load
                // path treats the player as "new" — but playerVersions gets
                // the REAL version, so the first save CAS-succeeds.
                byte[] data = record.get(DATA_FIELD);
                long realVersion = record.get(VERSION_FIELD) != null
                    ? record.get(VERSION_FIELD) : 0L;
                long realChecksum = record.get(CHECKSUM_FIELD) != null
                    ? record.get(CHECKSUM_FIELD) : 0L;
                long realFencing = record.get(FENCING_TOKEN_FIELD) != null
                    ? record.get(FENCING_TOKEN_FIELD) : 0L;
                long realBitmap = record.get(COMPONENT_BITMAP_FIELD) != null
                    ? record.get(COMPONENT_BITMAP_FIELD) : 0L;
                long realGen = record.get(COMPONENT_GENERATION_FIELD) != null
                    ? record.get(COMPONENT_GENERATION_FIELD) : 0L;
                return new PlayerDataRow(
                    data != null ? data : new byte[0],
                    realVersion,
                    realChecksum,
                    realFencing,
                    realBitmap,
                    realGen);
            }
        }
        // Row does not exist at all — genuinely new player. EMPTY is correct
        // here (version=0, fencing=0, etc.).
        return PlayerDataRow.EMPTY;
    }

    /**
     * Load components for a player at a specific generation.
     *
     * <p>Only rows where {@code generation = ?} are returned. This ensures that
     * stale component rows from a previous baseline (before a full Blob save
     * incremented the generation) are not loaded.
     *
     * @param uuid           player UUID
     * @param componentNames which components to load
     * @param generation     the current component_generation from player_data
     */
    public java.util.Map<String, ComponentData> loadComponentsWithGeneration(
            UUID uuid, java.util.Set<String> componentNames, long generation) throws SQLException {
        if (componentNames == null || componentNames.isEmpty()) {
            return java.util.Collections.emptyMap();
        }
        java.util.Map<String, ComponentData> result = new java.util.HashMap<>();
        try (Connection conn = dataSource.getConnection()) {
            String placeholders = String.join(",", java.util.Collections.nCopies(componentNames.size(), "?"));
            String sql = String.format(
                "SELECT `component`, `data`, `version`, `checksum` FROM `%s` " +
                "WHERE `uuid` = ? AND `generation` = ? AND `component` IN (%s)",
                componentTable, placeholders);

            try (var ps = conn.prepareStatement(sql)) {
                ps.setString(1, uuid.toString());
                ps.setLong(2, generation);
                int i = 3;
                for (String name : componentNames) {
                    ps.setString(i++, name);
                }
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.put(rs.getString("component"),
                            new ComponentData(
                                rs.getBytes("data"),
                                rs.getLong("version"),
                                rs.getLong("checksum")));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Get the current component_generation for a player.
     */
    public long getComponentGeneration(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Long gen = dsl(conn).select(COMPONENT_GENERATION_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne(COMPONENT_GENERATION_FIELD);
            return gen != null ? gen : 0L;
        }
    }

    /**
     * Phase 3: Single-transaction component upsert with full fencing validation.
     *
     * <p>This is the core safe component save method. In ONE database transaction:
     * <ol>
     *   <li>{@code SELECT player_data ... FOR UPDATE} — locks the row</li>
     *   <li>Validate {@code locked_by = serverName} AND {@code fencing_token = fencingToken}</li>
     *   <li>Upsert all dirty component rows WITH the current generation</li>
     *   <li>{@code UPDATE player_data} SET {@code version = version + 1},
     *       {@code component_bitmap = component_bitmap | dirtyBits},
     *       {@code locked_at = now}</li>
     *   <li>Commit</li>
     * </ol>
     *
     * <p>If any validation fails, the transaction is rolled back and no component
     * rows are written. This satisfies invariants 2, 3, and 5 from the RFC:
     * <ul>
     *   <li>Component writes cannot bypass locked_by + fencing_token</li>
     *   <li>Old fencing tokens can never write component rows</li>
     *   <li>component_bitmap and component rows are updated in the same transaction</li>
     * </ul>
     *
     * <p>The method also increments player_data.version (satisfying the RFC
     * requirement that component saves bump the global version), so the version
     * returned in the result must be used to update the local playerVersions map.
     *
     * @param dirtyBits  bitmask of dirty component ordinals (for bitmap update)
     * @return ComponentBatchResult with new version/bitmap/generation, or rejected
     */
    public ComponentBatchResult upsertComponentsIfLockHeld(
            UUID uuid,
            java.util.Map<String, byte[]> componentsWithData,
            java.util.Map<String, Long> componentsWithChecksum,
            String serverName,
            long fencingToken,
            String lockSessionId,
            long expectedVersion,
            long dirtyBits) throws SQLException {
        requireLockSession(lockSessionId, "upsertComponentsIfLockHeld");
        if (componentsWithData == null || componentsWithData.isEmpty()) {
            return ComponentBatchResult.rejected("no components to upsert");
        }
        long now = System.currentTimeMillis();

        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                // 1. Lock player_data row and read metadata (including lock_session_id)
                String lockSql = String.format(
                    "SELECT `version`, `fencing_token`, `locked_by`, `lock_session_id`, `component_bitmap`, `component_generation` " +
                    "FROM `%s` WHERE `uuid` = ? FOR UPDATE", dataTable);

                long currentVersion;
                long currentBitmap;
                long generation;
                String lockedBy;
                String dbSessionId;
                long dbFencing;

                try (var ps = conn.prepareStatement(lockSql)) {
                    ps.setString(1, uuid.toString());
                    try (var rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            conn.rollback();
                            conn.setAutoCommit(oldAutoCommit);
                            return ComponentBatchResult.rejected("missing player_data row");
                        }
                        currentVersion = rs.getLong("version");
                        dbFencing = rs.getLong("fencing_token");
                        lockedBy = rs.getString("locked_by");
                        dbSessionId = rs.getString("lock_session_id");
                        currentBitmap = rs.getLong("component_bitmap");
                        generation = rs.getLong("component_generation");
                    }
                }

                // 2. Validate lock + fencing token + session id
                if (!serverName.equals(lockedBy) || dbFencing != fencingToken) {
                    conn.rollback();
                    conn.setAutoCommit(oldAutoCommit);
                    return ComponentBatchResult.rejected("lock/fencing mismatch (expected: "
                        + serverName + "/ft" + fencingToken
                        + ", actual: " + lockedBy + "/ft" + dbFencing + ")");
                }
                // P0 (round 10): session id must match. A stale session whose
                // lock was superseded by a new acquire must not be able to
                // write component rows.
                if (lockSessionId != null && !lockSessionId.equals(dbSessionId)) {
                    conn.rollback();
                    conn.setAutoCommit(oldAutoCommit);
                    return ComponentBatchResult.rejected("lock_session_id mismatch (expected: "
                        + lockSessionId + ", actual: " + dbSessionId + ")");
                }

                // P0 (round 10): expectedVersion check. If the DB version has
                // advanced past what the caller collected against, this is a
                // stale component snapshot — reject so the caller can fall back
                // to full Blob save or re-collect. This prevents a stale
                // component save from bumping version and masking a newer
                // component save that's still in flight.
                if (expectedVersion >= 0 && currentVersion != expectedVersion) {
                    conn.rollback();
                    conn.setAutoCommit(oldAutoCommit);
                    return ComponentBatchResult.rejected("stale collected version (expected: "
                        + expectedVersion + ", actual: " + currentVersion + ")");
                }

                // 3. Upsert component rows with current generation
                String upsertSql = String.format("""
                    INSERT INTO `%s` (uuid, component, generation, data, version, checksum, updated_at)
                    VALUES (?, ?, ?, ?, 1, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        generation = VALUES(generation),
                        data = VALUES(data),
                        version = version + 1,
                        checksum = VALUES(checksum),
                        updated_at = VALUES(updated_at)
                    """, componentTable);

                try (var ps = conn.prepareStatement(upsertSql)) {
                    for (var entry : componentsWithData.entrySet()) {
                        String name = entry.getKey();
                        byte[] data = entry.getValue();
                        long checksum = componentsWithChecksum.getOrDefault(name, 0L);
                        ps.setString(1, uuid.toString());
                        ps.setString(2, name);
                        ps.setLong(3, generation);
                        ps.setBytes(4, data);
                        ps.setLong(5, checksum);
                        ps.setLong(6, now);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }

                // 4. Update player_data metadata: version+1, bitmap|=dirtyBits, locked_at=now
                long newVersion = currentVersion + 1;
                long newBitmap = currentBitmap | dirtyBits;
                String updateSql = String.format(
                    "UPDATE `%s` SET `version` = ?, `component_bitmap` = ?, `locked_at` = ?, " +
                    "`last_updated` = ?, `last_server` = ? " +
                    "WHERE `uuid` = ? AND `locked_by` = ? AND `fencing_token` = ? AND `lock_session_id` = ?",
                    dataTable);

                int updated;
                try (var ps = conn.prepareStatement(updateSql)) {
                    ps.setLong(1, newVersion);
                    ps.setLong(2, newBitmap);
                    ps.setLong(3, now);
                    ps.setLong(4, now);
                    ps.setString(5, serverName);
                    ps.setString(6, uuid.toString());
                    ps.setString(7, serverName);
                    ps.setLong(8, fencingToken);
                    ps.setString(9, lockSessionId);
                    updated = ps.executeUpdate();
                }

                if (updated <= 0) {
                    conn.rollback();
                    conn.setAutoCommit(oldAutoCommit);
                    return ComponentBatchResult.rejected("failed to update player_data metadata");
                }

                conn.commit();
                conn.setAutoCommit(oldAutoCommit);
                return ComponentBatchResult.success(currentVersion, newVersion, newBitmap, generation);

            } catch (SQLException | RuntimeException e) {
                try { conn.rollback(); } catch (SQLException ignored) {}
                try { conn.setAutoCommit(oldAutoCommit); } catch (SQLException ignored) {}
                throw e;
            }
        }
    }

    /**
     * Background GC: delete component rows from old generations.
     * Safe to call periodically — only deletes rows where generation < current - 1.
     */
    public int gcOldComponentRows(UUID uuid, long currentGeneration) throws SQLException {
        if (currentGeneration <= 1) return 0;
        try (Connection conn = dataSource.getConnection()) {
            String sql = String.format(
                "DELETE FROM `%s` WHERE `uuid` = ? AND `generation` < ?",
                componentTable);
            try (var ps = conn.prepareStatement(sql)) {
                ps.setString(1, uuid.toString());
                ps.setLong(2, currentGeneration - 1);
                return ps.executeUpdate();
            }
        }
    }
}
