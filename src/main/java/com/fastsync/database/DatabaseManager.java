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
    private static final Field<String> CLUSTER_ID_FIELD = field(name("cluster_id"), String.class);
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
     * <p>Production callers (SyncManager) must always track and pass the
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

    /**
     * Required schema version for this build of FastSync. The v2 schema adds
     * a {@code cluster_id} column to every table's PRIMARY KEY and introduces
     * a {@code schema_version} bookkeeping table. FastSync refuses to start
     * against a database whose schema_version is older (or newer) than this.
     */
    private static final int REQUIRED_SCHEMA_VERSION = 2;

    private final Logger logger;
    private final ConfigManager config;
    private HikariDataSource dataSource;
    private String dataTable;
    private String componentTable;
    private String clusterId;
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
     * Validate and normalize the configured cluster-id.
     *
     * <p>The cluster-id is a short opaque identifier (e.g. {@code "prod-eu-1"})
     * that becomes part of every PRIMARY KEY in the v2 schema. It MUST be
     * explicitly set — empty is rejected, because a missing cluster-id silently
     * collapses multiple clusters onto the same {@code cluster_id = ""} key
     * space and reintroduces exactly the cross-cluster collision the v2 schema
     * was designed to prevent.
     *
     * <p>The charset is restricted to filename-safe ASCII (letters, digits,
     * {@code _}, {@code .}, {@code -}) and capped at 64 characters — the same
     * width as the {@code VARCHAR(64)} column it lands in. There is no default
     * and no fallback; callers must configure {@code cluster-id} explicitly.
     *
     * @param raw the raw configured value (may be null/blank)
     * @return the trimmed, validated cluster-id
     * @throws IllegalArgumentException if {@code raw} is null/blank or contains
     *         characters outside the allowed set
     */
    private static String normalizeClusterId(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("cluster-id must be explicitly set.");
        }
        String value = raw.trim();
        if (!value.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new IllegalArgumentException("Invalid cluster-id '" + raw + "'. Allowed: A-Z a-z 0-9 _ . - , max 64 chars.");
        }
        return value;
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

        // Normalize the cluster-id BEFORE any DB work. This MUST be set
        // explicitly — the v2 schema keys every row by cluster_id, so an empty
        // value would silently collapse multiple clusters onto the same key
        // space. Throws IllegalArgumentException (propagates out of initialize)
        // if missing or malformed.
        clusterId = normalizeClusterId(config.getClusterId());

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

        // Validate the existing schema state BEFORE issuing any DDL.
        // This either (a) greenlights a fresh install (no old tables, no
        // schema_version table) — createTables() will then create everything,
        // or (b) greenlights an existing v2 install (schema_version row == 2),
        // or (c) refuses to start against an old v1 schema (no cluster_id) or
        // a mismatched schema_version.
        validateSchemaVersion();

        createTables();
        // P1 (round 15): greenfield schema self-check. Verifies that the DDL
        // actually produced the columns the runtime expects. This is NOT a
        // migration — it never ALTERs or DROPs anything. It only catches:
        //   - MySQL permission anomalies that silently skip CREATE TABLE
        //   - partial DDL failure (one table created, the other not)
        //   - table-prefix misconfiguration pointing at a different schema
        //   - MySQL/MariaDB dialect differences that reject a column
        //   - column-name typos introduced when editing the DDL above
        // Failure throws SQLException and stops plugin startup (FastSync will
        // disable itself in onEnable's catch block).
        validateGreenfieldSchemaStrict();
        logger.info("Database connection established: " + config.getDbHost() + ":" + config.getDbPort() + "/" + config.getDbDatabase());
    }

    /**
     * Get the current time from the MySQL server, in milliseconds since epoch.
     *
     * <p>Used for lock expiry calculations instead of {@code System.currentTimeMillis()}
     * to eliminate clock-skew issues between Paper/Folia nodes. All nodes share
     * the same MySQL instance, so using DB time ensures consistent expiry
     * evaluation regardless of individual node clock drift.
     *
     * @param conn an open JDBC connection
     * @return DB server time in milliseconds since Unix epoch
     */
    private long getDatabaseTimeMs(Connection conn) throws SQLException {
        try (var ps = conn.prepareStatement("SELECT UNIX_TIMESTAMP() * 1000");
             var rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        // Fallback: if the query somehow fails, use JVM time rather than
        // crashing the lock acquire path. This is a safety net, not the norm.
        return System.currentTimeMillis();
    }

    /**
     * Create the player_data, player_component, and schema_version tables.
     *
     * <p>v2 schema: every table's PRIMARY KEY starts with {@code cluster_id},
     * so multiple FastSync clusters can share the same MySQL database without
     * colliding on the same UUID. The {@code schema_version} table records
     * which schema generation this database is on; {@link #validateSchemaVersion()}
     * refuses to start against an incompatible generation.
     *
     * <p>DDL is issued as raw SQL via {@link DSLContext#execute(String)} because
     * jOOQ's DDL DSL is far more verbose than the equivalent CREATE TABLE
     * statement for no type-safety benefit here.
     */
    private void createTables() throws SQLException {
        String sql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `cluster_id` VARCHAR(64) NOT NULL,
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
                PRIMARY KEY (`cluster_id`, `uuid`),
                INDEX `idx_locked` (`cluster_id`, `locked_by`, `locked_at`),
                INDEX `idx_uuid` (`uuid`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, dataTable);

        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).execute(sql);
        }

        // Per-component storage table (phase 2). Each row holds one component
        // (inventory, vitals, food, etc.) for one player, with its own version
        // and checksum. The composite PK (cluster_id, uuid, component) gives
        // O(1) point lookups and lets us UPDATE only the changed component on
        // save. cluster_id is the first PK column so multiple clusters can
        // share the table without colliding.
        //
        // The `generation` column tracks which full-Blob baseline this component
        // row belongs to. On load, only rows where generation == player_data.component_generation
        // are read — older generation rows are ignored (they belong to a previous
        // baseline that has been superseded by a full Blob save).
        String componentTableSql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `cluster_id` VARCHAR(64) NOT NULL,
                `uuid` VARCHAR(36) NOT NULL,
                `component` VARCHAR(32) NOT NULL,
                `generation` BIGINT NOT NULL DEFAULT 0,
                `data` LONGBLOB NOT NULL,
                `version` BIGINT NOT NULL DEFAULT 0,
                `checksum` BIGINT NOT NULL DEFAULT 0,
                `updated_at` BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (`cluster_id`, `uuid`, `component`),
                INDEX `idx_uuid_generation` (`cluster_id`, `uuid`, `generation`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, componentTable);

        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).execute(componentTableSql);
        }

        // schema_version bookkeeping table. Single row (id=1) carrying the
        // schema generation this database was initialized with. Future
        // migrations would bump this; for v2 there is no migration path —
        // a mismatched version is a hard refusal (drop & re-init).
        String schemaVersionTable = dataTable.replace("player_data", "schema_version");
        String schemaVersionSql = String.format("""
            CREATE TABLE IF NOT EXISTS `%s` (
                `id` TINYINT NOT NULL PRIMARY KEY,
                `version` INT NOT NULL,
                `updated_at` BIGINT NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, schemaVersionTable);

        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).execute(schemaVersionSql);
            // Seed the version row if the table is empty. ON DUPLICATE KEY
            // UPDATE keeps this idempotent across restarts without bumping
            // updated_at on every boot — the row is only written once, when
            // the table is freshly created.
            String seedSql = String.format(
                "INSERT INTO `%s` (id, version, updated_at) VALUES (1, ?, ?)",
                schemaVersionTable);
            try (var ps = conn.prepareStatement(seedSql)) {
                ps.setInt(1, REQUIRED_SCHEMA_VERSION);
                ps.setLong(2, System.currentTimeMillis());
                ps.executeUpdate();
            }
        }
    }

    /**
     * Validate the existing schema state before issuing any DDL.
     *
     * <p>Three branches:
     * <ol>
     *   <li>No {@code schema_version} table AND no {@code player_data} table —
     *       fresh install. {@link #createTables()} will create everything.</li>
     *   <li>No {@code schema_version} table but {@code player_data} exists
     *       without a {@code cluster_id} column — old v1 schema. Refuse to
     *       start: this is a greenfield v2 deploy, the operator must DROP the
     *       stale tables first.</li>
     *   <li>{@code schema_version} table exists with a row — check the version
     *       matches {@link #REQUIRED_SCHEMA_VERSION}. Older or newer both refuse.</li>
     * </ol>
     *
     * <p>This is read-only: it never ALTERs or DROPs anything. The refusal
     * messages tell the operator exactly what to drop.
     */
    private void validateSchemaVersion() throws SQLException {
        String schemaVersionTable = dataTable.replace("player_data", "schema_version");
        String checkOldSql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        String checkVersionSql = "SELECT version FROM `" + schemaVersionTable + "` WHERE id = 1";

        try (Connection conn = dataSource.getConnection()) {
            // Check if schema_version table exists.
            boolean schemaVersionExists = false;
            try (var ps = conn.prepareStatement(checkOldSql)) {
                ps.setString(1, conn.getCatalog());
                ps.setString(2, schemaVersionTable);
                try (var rs = ps.executeQuery()) {
                    schemaVersionExists = rs.next();
                }
            }

            if (!schemaVersionExists) {
                // Check if old player_data table exists (without cluster_id).
                boolean oldPlayerDataExists = false;
                try (var ps = conn.prepareStatement(checkOldSql)) {
                    ps.setString(1, conn.getCatalog());
                    ps.setString(2, dataTable);
                    try (var rs = ps.executeQuery()) {
                        oldPlayerDataExists = rs.next();
                    }
                }

                if (oldPlayerDataExists) {
                    // Check if it has cluster_id column.
                    boolean hasClusterId = false;
                    try (var ps = conn.prepareStatement(
                        "SELECT 1 FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = 'cluster_id'")) {
                        ps.setString(1, conn.getCatalog());
                        ps.setString(2, dataTable);
                        try (var rs = ps.executeQuery()) {
                            hasClusterId = rs.next();
                        }
                    }
                    if (!hasClusterId) {
                        throw new SQLException("Old database tables detected (without cluster_id). " +
                            "This is a greenfield v2 deployment. Please DROP all fastsync tables first:\n" +
                            "DROP TABLE IF EXISTS " + componentTable + ";\n" +
                            "DROP TABLE IF EXISTS " + dataTable + ";\n" +
                            "DROP TABLE IF EXISTS " + dataTable.replace("player_data", "snapshots") + ";\n" +
                            "DROP TABLE IF EXISTS " + schemaVersionTable + ";");
                    }
                }
                // No old tables, no schema_version — fresh install.
                // createTables() will create everything.
                return;
            }

            // schema_version table exists — check version.
            try (var ps = conn.prepareStatement(checkVersionSql)) {
                try (var rs = ps.executeQuery()) {
                    if (rs.next()) {
                        int version = rs.getInt("version");
                        if (version < REQUIRED_SCHEMA_VERSION) {
                            throw new SQLException("Database schema version " + version + " is older than required version " +
                                REQUIRED_SCHEMA_VERSION + ". Please DROP all fastsync tables and restart.");
                        }
                        if (version > REQUIRED_SCHEMA_VERSION) {
                            throw new SQLException("Database schema version " + version + " is newer than supported version " +
                                REQUIRED_SCHEMA_VERSION + ". Downgrade not supported. Please update FastSync.");
                        }
                        // Version matches — OK.
                        return;
                    }
                }
            }
            // schema_version table exists but no row — fresh schema_version
            // table, will be populated by createTables().
        }
    }

    /**
     * Greenfield schema self-check (round 15, P1).
     *
     * <p>Verifies that both tables contain the exact column set the runtime
     * code reads/writes. This is a <b>read-only</b> validation: it never
     * issues ALTER, DROP, or any repair statement. On any missing column it
     * throws {@link SQLException}, which propagates out of {@link #initialize()}
     * and causes FastSync to disable itself in {@code onEnable}.
     *
     * <p>Rationale: CREATE TABLE IF NOT EXISTS is a no-op if a same-named table
     * already exists (e.g. created by a prior misconfigured deploy, or by a
     * different plugin sharing the prefix). Without this check, the plugin
     * would start "successfully" and then fail at runtime with cryptic
     * "Unknown column" errors during the first save — far worse than refusing
     * to start.
     */
    private void validateGreenfieldSchemaStrict() throws SQLException {
        requireColumns(dataTable,
            "cluster_id", "uuid", "data", "version", "checksum",
            "fencing_token", "locked_by", "locked_at", "lock_session_id",
            "last_server", "last_updated",
            "component_bitmap", "component_generation");
        requireColumns(componentTable,
            "cluster_id", "uuid", "component", "generation",
            "data", "version", "checksum", "updated_at");
    }

    /**
     * Throw {@link SQLException} if any of {@code requiredColumns} is absent
     * from {@code tableName}. Uses {@link Connection#getMetaData()} so the
     * check works across MySQL/MariaDB without dialect-specific SQL.
     */
    private void requireColumns(String tableName, String... requiredColumns) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            java.util.Set<String> actual = new java.util.HashSet<>();
            try (java.sql.ResultSet rs = conn.getMetaData().getColumns(
                    conn.getCatalog(), conn.getSchema(), tableName, null)) {
                while (rs.next()) {
                    actual.add(rs.getString("COLUMN_NAME"));
                }
            }
            for (String required : requiredColumns) {
                if (!actual.contains(required)) {
                    throw new SQLException(
                        "Greenfield schema validation failed: table '" + tableName
                        + "' is missing required column '" + required
                        + "'. Refusing to start. Found columns: " + actual
                        + ". This is a greenfield deploy — drop the table or fix the prefix and restart.");
                }
            }
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
        requireLockSession(lockSessionId, "acquireLock");
        // Use DB server time for lock expiry calculation, NOT JVM local time.
        // This eliminates clock-skew issues between Paper/Folia nodes: if node A's
        // clock is ahead, it won't prematurely consider node B's lock expired.
        // The extra SELECT is one round-trip but only on the login path (not hot).
        long now;
        try (Connection timeConn = dataSource.getConnection()) {
            now = getDatabaseTimeMs(timeConn);
        }
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
        // P0 SAFETY NOTE — MySQL left-to-right SET evaluation hazard.
        //
        // MySQL evaluates single-table UPDATE SET assignments left to right,
        // and a later assignment's RHS sees the NEW value of any column that
        // an earlier assignment in the SAME statement already wrote. The
        // previous form repeated `IF(locked_by IS NULL OR locked_at < ?, ...)`
        // for every column. Once the `locked_by = ...` assignment ran, the
        // following `locked_by IS NULL` checks evaluated against the just-
        // written (non-null) value, so for a just-released lock (locked_at
        // NULL, locked_at < ? => NULL => false) the `locked_at` and
        // `lock_session_id` IFs went false and left those columns at their
        // post-release NULL values. The read-back's `lock_session_id` match
        // then failed, breaking every acquire-after-release sequence.
        //
        // Fix: drive ALL four assignments off a single predicate that
        // references only `locked_at`, and assign `locked_at` LAST. The
        // earlier assignments (fencing_token, locked_by, lock_session_id)
        // then see the OLD `locked_at` (not yet written), and `locked_at`'s
        // own RHS is evaluated before its assignment, so it also sees OLD.
        // `locked_at IS NULL OR locked_at < ?` is exactly "lock is free or
        // expired": releaseLock / the save paths clear locked_at to NULL on
        // release, and acquireLock sets it to `now` while held. No reliance
        // on locked_by here, so the same-serverName quick-reconnect guard
        // continues to come from the read-back's lock_session_id check.
        String sql = String.format("""
            INSERT INTO `%s` (cluster_id, uuid, data, version, checksum, fencing_token, locked_by, locked_at, lock_session_id, last_server, last_updated)
            VALUES (?, ?, '', 0, 0, 1, ?, ?, ?, NULL, 0)
            ON DUPLICATE KEY UPDATE
                fencing_token = IF(locked_at IS NULL OR locked_at < ?,
                                   LAST_INSERT_ID(fencing_token + 1),
                                   fencing_token),
                locked_by = IF(locked_at IS NULL OR locked_at < ?,
                               ?,
                               locked_by),
                lock_session_id = IF(locked_at IS NULL OR locked_at < ?,
                                      ?,
                                      lock_session_id),
                locked_at = IF(locked_at IS NULL OR locked_at < ?,
                               ?,
                               locked_at)
            """, dataTable);

        try (Connection conn = dataSource.getConnection()) {
            DSLContext dsl = dsl(conn);

            dsl.execute(sql,
                clusterId,        // INSERT: cluster_id
                uuid.toString(),  // INSERT: uuid
                serverName,       // INSERT: locked_by
                now,              // INSERT: locked_at
                lockSessionId,    // INSERT: lock_session_id
                expiredTime,      // fencing_token IF predicate
                expiredTime,      // locked_by IF predicate
                serverName,       // locked_by new value
                expiredTime,      // lock_session_id IF predicate
                lockSessionId,    // lock_session_id new value
                expiredTime,      // locked_at IF predicate
                now               // locked_at new value
            );

            // Read back fencing_token, locked_by, AND lock_session_id using
            // raw JDBC (not jOOQ) to avoid Field type resolution issues that
            // caused null returns on CI MySQL.
            String readSql = String.format(
                "SELECT `fencing_token`, `locked_by`, `lock_session_id` FROM `%s` WHERE `cluster_id` = ? AND `uuid` = ?",
                dataTable);
            long readToken = 0;
            String readLockedBy = null;
            String readSessionId = null;
            try (var ps = conn.prepareStatement(readSql)) {
                ps.setString(1, clusterId);
                ps.setString(2, uuid.toString());
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
                // Raw JDBC for the CAS update — jOOQ Field type resolution
                // caused lock_session_id WHERE mismatches on CI MySQL.
                String sql = String.format(
                    "UPDATE `%s` SET `data` = ?, `version` = `version` + 1, `checksum` = ?, " +
                    "`locked_by` = NULL, `locked_at` = NULL, `lock_session_id` = NULL, " +
                    "`last_server` = ?, `last_updated` = ?, " +
                    "`component_bitmap` = 0, `component_generation` = `component_generation` + 1 " +
                    "WHERE `cluster_id` = ? AND `uuid` = ? AND `version` = ? AND `fencing_token` = ? " +
                    "AND `locked_by` = ? AND `lock_session_id` = ?",
                    dataTable);
                int updated;
                try (var ps = conn.prepareStatement(sql)) {
                    ps.setBytes(1, data);
                    ps.setLong(2, checksum);
                    ps.setString(3, serverName);
                    ps.setLong(4, now);
                    ps.setString(5, clusterId);
                    ps.setString(6, uuid.toString());
                    ps.setLong(7, expectedVersion);
                    ps.setLong(8, fencingToken);
                    ps.setString(9, serverName);
                    ps.setString(10, lockSessionId);
                    updated = ps.executeUpdate();
                }

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
                String sql = String.format(
                    "UPDATE `%s` SET `data` = ?, `version` = `version` + 1, `checksum` = ?, " +
                    "`last_server` = ?, `last_updated` = ?, `locked_at` = ?, " +
                    "`component_bitmap` = 0, `component_generation` = `component_generation` + 1 " +
                    "WHERE `cluster_id` = ? AND `uuid` = ? AND `version` = ? AND `fencing_token` = ? " +
                    "AND `locked_by` = ? AND `lock_session_id` = ?",
                    dataTable);
                int updated;
                try (var ps = conn.prepareStatement(sql)) {
                    ps.setBytes(1, data);
                    ps.setLong(2, checksum);
                    ps.setString(3, serverName);
                    ps.setLong(4, now);
                    ps.setLong(5, now);
                    ps.setString(6, clusterId);
                    ps.setString(7, uuid.toString());
                    ps.setLong(8, expectedVersion);
                    ps.setLong(9, fencingToken);
                    ps.setString(10, serverName);
                    ps.setString(11, lockSessionId);
                    updated = ps.executeUpdate();
                }

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
                .where(UUID_FIELD.eq(uuid.toString()).and(CLUSTER_ID_FIELD.eq(clusterId)))
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
                .where(UUID_FIELD.eq(uuid.toString()).and(CLUSTER_ID_FIELD.eq(clusterId)))
                .fetchOne(VERSION_FIELD);
            return version != null ? version : -1;
        }
    }

    /**
     * Snapshot of the lock-relevant columns for a player_data row.
     *
     * <p>Used by the final-save retry path to make a precise retry decision
     * in ONE round-trip instead of separate {@code getCurrentVersion} +
     * {@code getCurrentFencingToken} calls. Carries {@code lockedBy} and
     * {@code lockSessionId} so the caller can verify the lock is STILL ours
     * before retrying — a version bump alone is not enough, because the lock
     * could have been stolen by another server between our CAS failure and
     * the read.
     *
     * <p>Round 15 P1: the old retry path only compared version + fencing token.
     * If the lock was stolen (locked_by / lock_session_id changed) but the
     * fencing token happened to match, the retry would write through a CAS
     * that no longer belonged to us. The DB CAS would still reject it (the
     * WHERE clause includes locked_by + lock_session_id), but the retry
     * decision itself was imprecise and wasted an attempt.
     */
    public record LockState(
            long version,
            long fencingToken,
            String lockedBy,
            String lockSessionId,
            boolean rowExists) {
        /**
         * True if the lock is still held by {@code expectedServer} with the
         * given fencing token and session id.
         */
        public boolean isHeldBy(String expectedServer, long expectedFencing, String expectedSession) {
            if (!rowExists || expectedServer == null || expectedSession == null) return false;
            return expectedServer.equals(lockedBy)
                && fencingToken == expectedFencing
                && expectedSession.equals(lockSessionId);
        }
    }

    /**
     * Read version, fencing_token, locked_by, and lock_session_id in one query.
     * Returns {@code rowExists=false} if the player_data row does not exist.
     */
    public LockState getLockState(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Record r = dsl(conn)
                .select(VERSION_FIELD, FENCING_TOKEN_FIELD, LOCKED_BY_FIELD, LOCK_SESSION_ID_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()).and(CLUSTER_ID_FIELD.eq(clusterId)))
                .fetchOne();
            if (r == null) {
                return new LockState(-1, -1, null, null, false);
            }
            Long v = r.get(VERSION_FIELD);
            Long f = r.get(FENCING_TOKEN_FIELD);
            return new LockState(
                v != null ? v : -1,
                f != null ? f : -1,
                r.get(LOCKED_BY_FIELD),
                r.get(LOCK_SESSION_ID_FIELD),
                true);
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
        // Use raw JDBC to avoid jOOQ Field type resolution issues with
        // lock_session_id column on CI MySQL.
        String sql = String.format(
            "UPDATE `%s` SET `locked_by` = NULL, `locked_at` = NULL, `lock_session_id` = NULL " +
            "WHERE `cluster_id` = ? AND `uuid` = ? AND `locked_by` = ? AND `fencing_token` = ? AND `lock_session_id` = ?",
            dataTable);
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            ps.setString(1, clusterId);
            ps.setString(2, uuid.toString());
            ps.setString(3, serverName);
            ps.setLong(4, fencingToken);
            ps.setString(5, lockSessionId);
            return ps.executeUpdate() > 0;
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
        // Use DB time for locked_at — consistent with acquireLock's expiry check.
        String sql = String.format(
            "UPDATE `%s` SET `locked_at` = (SELECT UNIX_TIMESTAMP() * 1000) " +
            "WHERE `cluster_id` = ? AND `uuid` = ? AND `locked_by` = ? AND `fencing_token` = ? AND `lock_session_id` = ?",
            dataTable);
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            ps.setString(1, clusterId);
            ps.setString(2, uuid.toString());
            ps.setString(3, serverName);
            ps.setLong(4, fencingToken);
            ps.setString(5, lockSessionId);
            return ps.executeUpdate() > 0;
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
        // Use DB time inline — no need for a separate time query since
        // all rows in the batch share the same timestamp.
        String sql = "UPDATE `" + dataTable + "`"
            + " SET locked_at = (SELECT UNIX_TIMESTAMP() * 1000)"
            + " WHERE cluster_id = ? AND uuid = ? AND locked_by = ? AND fencing_token = ?"
            + " AND lock_session_id = ?";

        try (Connection conn = dataSource.getConnection();
             java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {

            for (java.util.Map.Entry<UUID, Long> entry : playersToRefresh.entrySet()) {
                UUID uuid = entry.getKey();
                String sessionId = playerLockSessions != null ? playerLockSessions.get(uuid) : null;
                if (sessionId == null || sessionId.isBlank()) {
                    // No recorded session for this player — we cannot safely
                    // refresh the lock. Mark failed (fail-closed) and skip the
                    // batch entry so we don't send a NULL bind that would
                    // update 0 rows and look like an ordinary heartbeat miss.
                    failedPlayers.add(uuid);
                    continue;
                }
                ps.setString(1, clusterId);
                ps.setString(2, uuid.toString());
                ps.setString(3, serverName);
                ps.setLong(4, entry.getValue());
                ps.setString(5, sessionId);
                ps.addBatch();
            }

            int[] results = ps.executeBatch();
            int i = 0;
            for (UUID uuid : playersToRefresh.keySet()) {
                // Players skipped above (missing session id) are already in
                // failedPlayers; skip them here so we don't index into results
                // at the wrong position (the batch only contains entries we
                // actually added).
                String sessionId = playerLockSessions != null ? playerLockSessions.get(uuid) : null;
                if (sessionId == null || sessionId.isBlank()) {
                    continue;
                }
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
                        if (token == null || !refreshLock(uuid, serverName, token, sessionId)) {
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
                .where(UUID_FIELD.eq(uuid.toString()).and(CLUSTER_ID_FIELD.eq(clusterId)))
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
     * component has a row in the {@code player_component} table at the current
     * generation and should be overlaid on top of the full Blob baseline.
     *
     * <p>Returns 0 for new players (no components written yet) or when the
     * column is null.
     */
    public long getComponentBitmap(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Long bitmap = dsl(conn).select(COMPONENT_BITMAP_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()).and(CLUSTER_ID_FIELD.eq(clusterId)))
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
                .where(UUID_FIELD.eq(uuid.toString()).and(CLUSTER_ID_FIELD.eq(clusterId)))
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
            String errorMessage,
            ComponentRejectReason reason) {
        public static ComponentBatchResult rejected(String msg, ComponentRejectReason reason) {
            return new ComponentBatchResult(false, -1, -1, 0, 0, msg, reason);
        }
        public static ComponentBatchResult success(long oldVersion, long newVersion,
                long bitmap, long generation) {
            return new ComponentBatchResult(true, oldVersion, newVersion, bitmap, generation, null,
                ComponentRejectReason.NONE);
        }
    }

    /**
     * Structured reason for a component-batch rejection. Lets the caller
     * ({@code persistComponentsOnly}) decide between:
     * <ul>
     *   <li>{@link #FALLBACK_FULL_BLOB} — safe to retry as a full Blob save</li>
     *   <li>{@link #SKIP_STALE_ONLINE_SAVE} — stale collected version; skip this
     *       online save, the next cycle re-collects</li>
     *   <li>{@link #FATAL_LOCK_CONFLICT} — lock/fencing/session mismatch; the
     *       lock is no longer ours, MUST NOT fall back to a full Blob save</li>
     *   <li>{@link #DB_UNAVAILABLE} — SQL error; caller decides retry/backoff</li>
     * </ul>
     *
     * <p>Round 3 directive #2 (round 15 P0): previously every rejection
     * returned {@code null} from {@code persistComponentsOnly}, which made
     * {@code persistCollectedData} blindly fall back to a full Blob save —
     * even when the rejection was a lock/fencing/session mismatch where
     * falling back would overwrite a newer session's data with a stale
     * snapshot.
     */
    public enum ComponentRejectReason {
        /** Save succeeded (not a rejection). */
        NONE,
        /** Caller passed an empty component map — fall back to full Blob. */
        NO_COMPONENTS,
        /** No player_data row exists yet — fall back to full Blob to establish baseline. */
        MISSING_BASELINE_ROW,
        /** locked_by or fencing_token mismatch — lock no longer ours. Fatal. */
        LOCK_OR_FENCING_MISMATCH,
        /** lock_session_id mismatch — stale session. Fatal. */
        SESSION_MISMATCH,
        /** DB version advanced past the collected version — stale snapshot. Skip. */
        STALE_VERSION,
        /** player_data metadata UPDATE affected 0 rows (lock lost mid-tx). Fatal. */
        METADATA_UPDATE_FAILED,
        /** SQL exception during the transaction. Retryable with backoff. */
        SQL_ERROR
    }

    /**
     * Full player_data row for load path, including component storage metadata.
     *
     * <p>Carries the data Blob plus version/checksum/fencing_token (for OCC +
     * fencing) and component_bitmap/component_generation (so the load path can
     * decide whether to read component rows and at which generation).
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
     * <p>Returns component_bitmap and component_generation so the caller can
     * decide whether to load component rows and at which generation.
     */
    public PlayerDataRow loadPlayerDataRow(UUID uuid) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Record record = dsl(conn)
                .select(DATA_FIELD, VERSION_FIELD, CHECKSUM_FIELD, FENCING_TOKEN_FIELD,
                        COMPONENT_BITMAP_FIELD, COMPONENT_GENERATION_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()).and(CLUSTER_ID_FIELD.eq(clusterId)))
                .fetchOne();

            if (record != null) {
                // Return the row's real metadata even when the data Blob is
                // empty. The INSERT arm of acquireLock seeds a row with
                // data='' and version=0; loadPlayerDataRow must hand back that
                // real version (0) so the first save CAS-succeeds. Falling
                // through to PlayerDataRow.EMPTY (all zeros) would lose the
                // row's actual version/checksum/fencing_token/bitmap/generation.
                // hasData() still returns false for empty data, so the load
                // path treats the player as "new".
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
                "WHERE `cluster_id` = ? AND `uuid` = ? AND `generation` = ? AND `component` IN (%s)",
                componentTable, placeholders);

            try (var ps = conn.prepareStatement(sql)) {
                ps.setString(1, clusterId);
                ps.setString(2, uuid.toString());
                ps.setLong(3, generation);
                int i = 4;
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
                .where(UUID_FIELD.eq(uuid.toString()).and(CLUSTER_ID_FIELD.eq(clusterId)))
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
            return ComponentBatchResult.rejected("no components to upsert", ComponentRejectReason.NO_COMPONENTS);
        }
        long now = System.currentTimeMillis();

        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                // 1. Lock player_data row and read metadata (including lock_session_id)
                String lockSql = String.format(
                    "SELECT `version`, `fencing_token`, `locked_by`, `lock_session_id`, `component_bitmap`, `component_generation` " +
                    "FROM `%s` WHERE `cluster_id` = ? AND `uuid` = ? FOR UPDATE", dataTable);

                long currentVersion;
                long currentBitmap;
                long generation;
                String lockedBy;
                String dbSessionId;
                long dbFencing;

                try (var ps = conn.prepareStatement(lockSql)) {
                    ps.setString(1, clusterId);
                    ps.setString(2, uuid.toString());
                    try (var rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            conn.rollback();
                            conn.setAutoCommit(oldAutoCommit);
                            return ComponentBatchResult.rejected("missing player_data row",
                                ComponentRejectReason.MISSING_BASELINE_ROW);
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
                        + ", actual: " + lockedBy + "/ft" + dbFencing + ")",
                        ComponentRejectReason.LOCK_OR_FENCING_MISMATCH);
                }
                // P0 (round 10): session id must match. A stale session whose
                // lock was superseded by a new acquire must not be able to
                // write component rows.
                if (lockSessionId != null && !lockSessionId.equals(dbSessionId)) {
                    conn.rollback();
                    conn.setAutoCommit(oldAutoCommit);
                    return ComponentBatchResult.rejected("lock_session_id mismatch (expected: "
                        + lockSessionId + ", actual: " + dbSessionId + ")",
                        ComponentRejectReason.SESSION_MISMATCH);
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
                        + expectedVersion + ", actual: " + currentVersion + ")",
                        ComponentRejectReason.STALE_VERSION);
                }

                // 3. Upsert component rows with current generation
                String upsertSql = String.format("""
                    INSERT INTO `%s` (cluster_id, uuid, component, generation, data, version, checksum, updated_at)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?)
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
                        ps.setString(1, clusterId);
                        ps.setString(2, uuid.toString());
                        ps.setString(3, name);
                        ps.setLong(4, generation);
                        ps.setBytes(5, data);
                        ps.setLong(6, checksum);
                        ps.setLong(7, now);
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
                    "WHERE `cluster_id` = ? AND `uuid` = ? AND `locked_by` = ? AND `fencing_token` = ? AND `lock_session_id` = ?",
                    dataTable);

                int updated;
                try (var ps = conn.prepareStatement(updateSql)) {
                    ps.setLong(1, newVersion);
                    ps.setLong(2, newBitmap);
                    ps.setLong(3, now);
                    ps.setLong(4, now);
                    ps.setString(5, serverName);
                    ps.setString(6, clusterId);
                    ps.setString(7, uuid.toString());
                    ps.setString(8, serverName);
                    ps.setLong(9, fencingToken);
                    ps.setString(10, lockSessionId);
                    updated = ps.executeUpdate();
                }

                if (updated <= 0) {
                    conn.rollback();
                    conn.setAutoCommit(oldAutoCommit);
                    return ComponentBatchResult.rejected("failed to update player_data metadata",
                        ComponentRejectReason.METADATA_UPDATE_FAILED);
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
                "DELETE FROM `%s` WHERE `cluster_id` = ? AND `uuid` = ? AND `generation` < ?",
                componentTable);
            try (var ps = conn.prepareStatement(sql)) {
                ps.setString(1, clusterId);
                ps.setString(2, uuid.toString());
                ps.setLong(3, currentGeneration - 1);
                return ps.executeUpdate();
            }
        }
    }

    /**
     * Return the normalized cluster-id this DatabaseManager is scoped to.
     *
     * <p>Every row written by this manager carries this value in its
     * {@code cluster_id} column. Exposed so callers (e.g. cross-cluster tooling,
     * diagnostics) can log / verify which cluster a running instance belongs
     * to without re-reading the config.
     *
     * @return the validated cluster-id (never null or blank)
     */
    public String getClusterId() {
        return clusterId;
    }
}
