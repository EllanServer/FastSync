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
    private static final Field<String> LAST_SERVER_FIELD = field(name("last_server"), String.class);
    private static final Field<Long> LAST_UPDATED_FIELD = field(name("last_updated"), Long.class);

    // Phase 2: per-component storage fields (table: fastsync_player_component)
    private static final Field<String> COMPONENT_NAME_FIELD = field(name("component"), String.class);
    private static final Field<Long> COMPONENT_BITMAP_FIELD = field(name("component_bitmap"), Long.class);
    private static final Field<Long> COMPONENT_GENERATION_FIELD = field(name("component_generation"), Long.class);
    private static final Field<Long> COMPONENT_UPDATED_AT_FIELD = field(name("updated_at"), Long.class);
    private static final Field<Long> GENERATION_FIELD = field(name("generation"), Long.class);

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
        migrateSchema();
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
     * Migrate the schema for existing tables.
     * Adds version and checksum columns if they don't already exist.
     *
     * <p>MySQL 8+ honours {@code ADD COLUMN IF NOT EXISTS}; older versions raise
     * a "Duplicate column" error which jOOQ surfaces as a
     * {@link DataAccessException} wrapping the underlying {@link SQLException}.
     * We walk the exception cause chain to detect that benign case and suppress
     * it, logging anything else as a migration note.
     */
    private void migrateSchema() throws SQLException {
        // Add columns if they don't exist (MySQL 8+ supports IF NOT EXISTS).
        // The trailing DROP COLUMN removes the orphaned `op_seq` column left over
        // from old installations; MySQL 8+ honours DROP COLUMN IF NOT EXISTS, so it
        // is a no-op on fresh installs (and on tables that never had the column).
        String[] migrations = {
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `version` BIGINT NOT NULL DEFAULT 0", dataTable),
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `checksum` BIGINT NOT NULL DEFAULT 0", dataTable),
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `fencing_token` BIGINT NOT NULL DEFAULT 0", dataTable),
            // Phase 2: component_bitmap tracks which components have been
            // migrated from the legacy single-Blob to the per-component table.
            // Default 0 = no components migrated yet (pure Blob mode).
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `component_bitmap` BIGINT NOT NULL DEFAULT 0", dataTable),
            // component_generation: incremented on every full Blob save. Component
            // rows are only read if their generation matches player_data.component_generation.
            // This prevents stale component rows from overriding a fresh full Blob.
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `component_generation` BIGINT NOT NULL DEFAULT 0", dataTable),
            // player_component: add generation column for baseline tracking
            String.format("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `generation` BIGINT NOT NULL DEFAULT 0", componentTable),
            String.format("ALTER TABLE `%s` DROP COLUMN IF NOT EXISTS `op_seq`", dataTable)
        };
        try (Connection conn = dataSource.getConnection()) {
            DSLContext dsl = dsl(conn);
            for (String sql : migrations) {
                try {
                    dsl.execute(sql);
                } catch (DataAccessException e) {
                    // ADD COLUMN: column may already exist on MySQL versions that
                    // don't support IF NOT EXISTS. DROP COLUMN: the column may not
                    // exist, or the server may not support DROP COLUMN IF NOT EXISTS.
                    // Either way the cause chain is walked and benign cases are
                    // suppressed; anything else is logged as a migration note.
                    if (!isDuplicateColumnError(e) && !isCannotDropColumnError(e)) {
                        logger.warning("Schema migration note: " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Walk the jOOQ exception cause chain looking for the MySQL
     * "Duplicate column" error, which is the expected outcome of adding a column
     * that already exists on MySQL versions without {@code IF NOT EXISTS} support.
     */
    private static boolean isDuplicateColumnError(Throwable e) {
        Throwable t = e;
        while (t != null) {
            String message = t.getMessage();
            if (message != null && message.contains("Duplicate column")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * Walk the jOOQ exception cause chain looking for the MySQL error raised when
     * attempting to drop a column that does not exist — either because the
     * {@code DROP COLUMN IF NOT EXISTS} syntax is unsupported (older MySQL) or
     * because the column was never present. MySQL surfaces this as error 1091
     * ("Can't DROP ...; check that column/key exists"); some variants report an
     * "Unknown column" error. Both are benign outcomes of the {@code op_seq}
     * cleanup migration and are suppressed.
     */
    private static boolean isCannotDropColumnError(Throwable e) {
        Throwable t = e;
        while (t != null) {
            String message = t.getMessage();
            if (message != null && (message.contains("check that column/key exists")
                    || message.contains("Can't DROP")
                    || message.contains("Unknown column"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
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
     * <p><b>Implementation:</b> The former three roundtrips — a separate
     * {@code INSERT IGNORE} to ensure the row exists, a conditional
     * {@code UPDATE} to take the lock and bump the token, and a
     * {@code SELECT} to read the token back — are collapsed into a single
     * {@code INSERT ... ON DUPLICATE KEY UPDATE} plus one read-back SELECT on the
     * same borrowed connection. The INSERT arm creates the row for brand-new
     * players with {@code fencing_token = 1}; the ON DUPLICATE KEY UPDATE arm
     * atomically increments {@code fencing_token} (via the
     * {@code LAST_INSERT_ID(expr)} idiom) and rewrites
     * {@code locked_by}/{@code locked_at} only when the lock is free, expired, or
     * already held by this server — the same predicate the old conditional UPDATE
     * used. The statement is issued as raw SQL via {@link DSLContext#execute(String)}
     * because jOOQ's DSL does not express {@code ON DUPLICATE KEY UPDATE} with
     * conditional {@code IF()} expressions cleanly.
     *
     * <p><b>InnoDB locking (per MySQL 8.4 docs):</b> The duplicate-key conflict is
     * resolved on the PRIMARY KEY ({@code uuid}) — a single-row X lock with no gap
     * locks or next-key locks, so different UUIDs never contend. The read-back
     * SELECT is a PK point lookup.
     *
     * <p><b>Why the read-back checks {@code locked_by} rather than relying on
     * {@code LAST_INSERT_ID()} alone:</b> The {@code LAST_INSERT_ID(expr)} idiom
     * only sets the connection's last-insert ID when the UPDATE arm actually
     * evaluates the expression. On a fresh INSERT (a new player) the UPDATE arm
     * never runs, and on a pooled connection {@code LAST_INSERT_ID()} may still
     * hold a stale value from a previous statement — so a zero return is not a
     * reliable failure signal. The committed {@code locked_by} column is
     * unambiguous: we hold the lock iff it equals {@code serverName}. Once we hold
     * the lock no other server can bump the token before the read-back, so the
     * {@code fencing_token} we read is the one we just wrote.
     *
     * @return LockResult with acquired=true and the fencing token, or acquired=false
     */
    public LockResult acquireLock(UUID uuid, String serverName) throws SQLException {
        long now = System.currentTimeMillis();
        long expiredTime = now - (config.getLockTimeout() * 1000L);

        // INSERT ... ON DUPLICATE KEY UPDATE merges row creation and conditional
        // lock acquisition into one statement. The INSERT arm seeds a new player
        // row (fencing_token = 1, locked_by = us); the UPDATE arm bumps the token
        // and takes the lock only when it is free, expired, or already ours.
        String sql = String.format("""
            INSERT INTO `%s` (uuid, data, version, checksum, fencing_token, locked_by, locked_at, last_server, last_updated)
            VALUES (?, '', 0, 0, 1, ?, ?, NULL, 0)
            ON DUPLICATE KEY UPDATE
                fencing_token = IF(locked_by IS NULL OR locked_at < ? OR locked_by = ?,
                                   LAST_INSERT_ID(fencing_token + 1),
                                   fencing_token),
                locked_by = IF(locked_by IS NULL OR locked_at < ? OR locked_by = ?,
                               ?,
                               locked_by),
                locked_at = IF(locked_by IS NULL OR locked_at < ? OR locked_by = ?,
                               ?,
                               locked_at)
            """, dataTable);

        try (Connection conn = dataSource.getConnection()) {
            DSLContext dsl = dsl(conn);

            dsl.execute(sql,
                uuid.toString(),  // INSERT: uuid
                serverName,       // INSERT: locked_by
                now,              // INSERT: locked_at
                expiredTime,      // fencing_token IF predicate
                serverName,       // fencing_token IF predicate
                expiredTime,      // locked_by IF predicate
                serverName,       // locked_by IF predicate
                serverName,       // locked_by new value
                expiredTime,      // locked_at IF predicate
                serverName,       // locked_at IF predicate
                now               // locked_at new value
            );

            // Read back the committed fencing_token and lock owner on the same
            // connection. We hold the lock iff locked_by == serverName; the
            // fencing_token is the one we just wrote (PK point lookup).
            Record record = dsl.select(FENCING_TOKEN_FIELD, LOCKED_BY_FIELD)
                .from(playerData)
                .where(UUID_FIELD.eq(uuid.toString()))
                .fetchOne();

            if (record == null || !serverName.equals(record.get(LOCKED_BY_FIELD))) {
                return LockResult.FAILED;
            }
            Long token = record.get(FENCING_TOKEN_FIELD);
            return token != null ? LockResult.success(token) : LockResult.FAILED;
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
                // version/checksum/fencing_token are NOT NULL columns, so the
                // boxed Long values are never null here (auto-unboxing is safe).
                if (data != null && data.length > 0) {
                    return new VersionedData(
                        data,
                        record.get(VERSION_FIELD),
                        record.get(CHECKSUM_FIELD),
                        record.get(FENCING_TOKEN_FIELD));
                }
            }
        }
        return VersionedData.EMPTY;
    }

    /**
     * Save player data AND RELEASE the lock (quit/final save).
     *
     * <p>This is the <b>final save</b> path (player quit / shutdown). After a
     * successful write the lock is cleared ({@code locked_by = NULL}) so another
     * server can immediately acquire it. Uses triple CAS:
     * <ol>
     *   <li>{@code version = expectedVersion} — optimistic concurrency</li>
     *   <li>{@code fencing_token = fencingToken} — stale-write defence (Kleppmann)</li>
     *   <li>{@code locked_by = serverName} — writer currently holds the DB lock</li>
     * </ol>
     *
     * @param uuid            player UUID
     * @param data            compressed data blob
     * @param checksum        CRC32 checksum of the uncompressed data
     * @param expectedVersion the version this data was loaded from
     * @param fencingToken    the fencing token assigned when the lock was acquired
     * @param serverName      server writing the data
     * @return true if saved successfully, false if version conflict or fencing token violation
     */
    public boolean saveDataAndReleaseLock(UUID uuid, byte[] data, long checksum, long expectedVersion,
                                          long fencingToken, String serverName) throws SQLException {
        long now = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).update(playerData)
                .set(DATA_FIELD, data)
                .set(VERSION_FIELD, VERSION_FIELD.plus(1))
                .set(CHECKSUM_FIELD, checksum)
                .set(LOCKED_BY_FIELD, (String) null)
                .set(LOCKED_AT_FIELD, (Long) null)
                .set(LAST_SERVER_FIELD, serverName)
                .set(LAST_UPDATED_FIELD, now)
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(VERSION_FIELD.eq(expectedVersion))
                    .and(FENCING_TOKEN_FIELD.eq(fencingToken))
                    .and(LOCKED_BY_FIELD.eq(serverName)))
                .execute() > 0;
        }
    }

    /**
     * Save player data AND KEEP the lock (online/periodic/bulk save).
     *
     * <p>This is the <b>online save</b> path (periodic save, world-save, bulk
     * save). The player is still online on this server, so the lock MUST be
     * retained after the write. We refresh {@code locked_at} to prevent the
     * lock from appearing stale while the player is actively playing.
     *
     * <p>Uses the same triple CAS as {@link #saveDataAndReleaseLock} but keeps
     * {@code locked_by} and refreshes {@code locked_at} instead of clearing them.
     *
     * @param uuid            player UUID
     * @param data            compressed data blob
     * @param checksum        CRC32 checksum of the uncompressed data
     * @param expectedVersion the version this data was loaded from
     * @param fencingToken    the fencing token assigned when the lock was acquired
     * @param serverName      server writing the data
     * @return true if saved successfully, false if version conflict or fencing token violation
     */
    public boolean saveDataKeepLock(UUID uuid, byte[] data, long checksum, long expectedVersion,
                                    long fencingToken, String serverName) throws SQLException {
        long now = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).update(playerData)
                .set(DATA_FIELD, data)
                .set(VERSION_FIELD, VERSION_FIELD.plus(1))
                .set(CHECKSUM_FIELD, checksum)
                .set(LAST_SERVER_FIELD, serverName)
                .set(LAST_UPDATED_FIELD, now)
                .set(LOCKED_AT_FIELD, now)  // refresh lock timestamp while player is online
                // locked_by is NOT cleared — we still hold the lock
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(VERSION_FIELD.eq(expectedVersion))
                    .and(FENCING_TOKEN_FIELD.eq(fencingToken))
                    .and(LOCKED_BY_FIELD.eq(serverName)))
                .execute() > 0;
        }
    }

    /**
     * Save player data and release the lock (legacy/backward-compatible alias).
     *
     * @deprecated Use {@link #saveDataAndReleaseLock} for quit saves or
     *             {@link #saveDataKeepLock} for online/periodic saves.
     */
    @Deprecated
    public boolean saveData(UUID uuid, byte[] data, long checksum, long expectedVersion,
                            long fencingToken, String serverName) throws SQLException {
        return saveDataAndReleaseLock(uuid, data, checksum, expectedVersion, fencingToken, serverName);
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
            long expectedVersion, long fencingToken, String serverName) throws SQLException {
        long now = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // CAS update player_data with new Blob + clear lock + reset bitmap
                // + increment component_generation. The generation increment
                // invalidates all old component rows without deleting them —
                // on load, only rows where generation == current generation are read.
                // Old rows can be GC'd later by a background task.
                int updated = dsl(conn).update(playerData)
                    .set(DATA_FIELD, data)
                    .set(VERSION_FIELD, VERSION_FIELD.plus(1))
                    .set(CHECKSUM_FIELD, checksum)
                    .set(LOCKED_BY_FIELD, (String) null)
                    .set(LOCKED_AT_FIELD, (Long) null)
                    .set(LAST_SERVER_FIELD, serverName)
                    .set(LAST_UPDATED_FIELD, now)
                    .set(COMPONENT_BITMAP_FIELD, 0L)
                    .set(COMPONENT_GENERATION_FIELD, COMPONENT_GENERATION_FIELD.plus(1))
                    .where(UUID_FIELD.eq(uuid.toString())
                        .and(VERSION_FIELD.eq(expectedVersion))
                        .and(FENCING_TOKEN_FIELD.eq(fencingToken))
                        .and(LOCKED_BY_FIELD.eq(serverName)))
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
            long expectedVersion, long fencingToken, String serverName) throws SQLException {
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
                    // locked_by is NOT cleared — we still hold the lock
                    .where(UUID_FIELD.eq(uuid.toString())
                        .and(VERSION_FIELD.eq(expectedVersion))
                        .and(FENCING_TOKEN_FIELD.eq(fencingToken))
                        .and(LOCKED_BY_FIELD.eq(serverName)))
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
     * Release the lock without saving data (e.g., on error).
     */
    /**
     * @deprecated Use {@link #releaseLock(UUID, String, long)} instead.
     *             This overload does not check the fencing token and can
     *             release another server's lock if server-names are duplicated.
     */
    @Deprecated(since = "1.0.0", forRemoval = true)
    public void releaseLock(UUID uuid, String serverName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).update(playerData)
                .set(LOCKED_BY_FIELD, (String) null)
                .set(LOCKED_AT_FIELD, (Long) null)
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(LOCKED_BY_FIELD.eq(serverName)))
                .execute();
        }
    }

    /**
     * Release a lock using a fencing token condition.
     *
     * <p>This is the safe variant of {@link #releaseLock(UUID, String)}: it only
     * clears the lock if {@code locked_by = serverName AND fencing_token = fencingToken}.
     * This prevents a misconfigured server (duplicate server-name) from releasing
     * a newer lock acquired by a different server instance that happened to use
     * the same name.
     *
     * @param uuid         player UUID
     * @param serverName   server that holds the lock
     * @param fencingToken fencing token assigned when the lock was acquired
     * @return true if the lock was released, false if the lock is no longer ours
     */
    public boolean releaseLock(UUID uuid, String serverName, long fencingToken) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).update(playerData)
                .set(LOCKED_BY_FIELD, (String) null)
                .set(LOCKED_AT_FIELD, (Long) null)
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(LOCKED_BY_FIELD.eq(serverName))
                    .and(FENCING_TOKEN_FIELD.eq(fencingToken)))
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
    public boolean refreshLock(UUID uuid, String serverName, long fencingToken) throws SQLException {
        long now = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            return dsl(conn).update(playerData)
                .set(LOCKED_AT_FIELD, now)
                .where(UUID_FIELD.eq(uuid.toString())
                    .and(LOCKED_BY_FIELD.eq(serverName))
                    .and(FENCING_TOKEN_FIELD.eq(fencingToken)))
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
                                  String serverName,
                                  java.util.Set<UUID> failedPlayers) throws SQLException {
        if (playersToRefresh.isEmpty()) return;
        long now = System.currentTimeMillis();

        // Use raw JDBC for batch — jOOQ's batch API is more verbose for this use case
        String sql = "UPDATE `" + dataTable + "`"
            + " SET locked_at = ?"
            + " WHERE uuid = ? AND locked_by = ? AND fencing_token = ?";

        try (Connection conn = dataSource.getConnection();
             java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {

            for (java.util.Map.Entry<UUID, Long> entry : playersToRefresh.entrySet()) {
                ps.setLong(1, now);
                ps.setString(2, entry.getKey().toString());
                ps.setString(3, serverName);
                ps.setLong(4, entry.getValue());
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
                        if (token == null || !refreshLock(uuid, serverName, token)) {
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
     * Update the {@code component_bitmap} column for a player.
     * Called after a component is written to {@code player_component} for the
     * first time, marking it as migrated.
     */
    public void setComponentBitmap(UUID uuid, long bitmap) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            dsl(conn).update(playerData)
                .set(COMPONENT_BITMAP_FIELD, bitmap)
                .where(UUID_FIELD.eq(uuid.toString()))
                .execute();
        }
    }

    /**
     * Load all migrated components for a player in a single round-trip.
     *
     * <p><b>DEPRECATED &amp; DANGEROUS.</b> This overload does NOT filter by
     * {@code component_generation}, so it can return stale component rows left
     * behind by a previous baseline (before a full Blob save incremented the
     * generation). Loading those rows and overlaying them on the current Blob
     * silently rolls back the player's state to whenever the component was
     * last written.
     *
     * <p>Use {@link #loadComponentsWithGeneration(UUID, java.util.Set, long)}
     * instead, which takes the current {@code component_generation} from
     * {@code player_data} and only returns rows matching that generation.
     *
     * @deprecated since 1.0.0, for removal. Use
     *             {@link #loadComponentsWithGeneration(UUID, java.util.Set, long)}.
     * @throws UnsupportedOperationException always — this method exists only to
     *         produce a clear compile-time deprecation and a loud runtime error
     *         if any caller still references it.
     */
    @Deprecated(since = "1.0.0", forRemoval = true)
    public java.util.Map<String, ComponentData> loadComponents(UUID uuid,
            java.util.Set<String> componentNames) throws SQLException {
        throw new UnsupportedOperationException(
            "loadComponents() is deprecated and unsafe — it does not filter by component_generation. "
            + "Use loadComponentsWithGeneration(uuid, names, generation) instead, where generation "
            + "is the current component_generation from player_data.");
    }

    /**
     * Upsert a single component: INSERT if not present, UPDATE if present.
     *
     * <p><b>DEPRECATED &amp; DANGEROUS.</b> This method has three correctness gaps:
     * <ol>
     *   <li>No {@code locked_by} / {@code fencing_token} validation — a stale
     *       lock holder can write component rows that later override the fresh
     *       full Blob on load.</li>
     *   <li>No {@code generation} column written — the row would have
     *       generation=0 (or NULL) and never match the current generation
     *       filter on load, so it would be silently invisible — OR if a
     *       previous valid row existed, it would corrupt the bitmap without
     *       bumping version.</li>
     *   <li>No {@code player_data.version} bump and no {@code component_bitmap}
     *       update — the playerVersions map drifts out of sync, and the next
     *       full Blob save CAS-fails with a stale-version conflict.</li>
     * </ol>
     *
     * <p>Use {@link #upsertComponentsIfLockHeld(UUID, java.util.Map, java.util.Map,
     * String, long, long)} instead — it does SELECT FOR UPDATE + fencing
     * validation + per-generation upsert + bitmap/version update all in one
     * transaction.
     *
     * @deprecated since 1.0.0, for removal. Use
     *             {@link #upsertComponentsIfLockHeld(UUID, java.util.Map, java.util.Map, String, long, long)}.
     * @throws UnsupportedOperationException always.
     */
    @Deprecated(since = "1.0.0", forRemoval = true)
    public long upsertComponent(UUID uuid, String componentName, byte[] data, long checksum)
            throws SQLException {
        throw new UnsupportedOperationException(
            "upsertComponent() is deprecated and unsafe — it has no fencing/generation/bitmap update. "
            + "Use upsertComponentsIfLockHeld(uuid, componentsWithData, componentsWithChecksum, "
            + "serverName, fencingToken, dirtyBits) instead.");
    }

    /**
     * Upsert multiple components in a single transaction.
     *
     * <p><b>DEPRECATED &amp; DANGEROUS.</b> Although this overload validates
     * {@code locked_by} + {@code fencing_token}, it still has two correctness gaps:
     * <ol>
     *   <li>It does NOT write the {@code generation} column on component rows —
     *       rows would be written with generation=0 (or NULL) and never match
     *       the current generation filter on load, so they would be invisible
     *       — OR if a previous valid row existed at the current generation, it
     *       would be overwritten with a wrong generation, corrupting the load
     *       path.</li>
     *   <li>It does NOT bump {@code player_data.version} and does NOT update
     *       {@code component_bitmap} in the same transaction. The local
     *       playerVersions map drifts out of sync with the DB, and components
     *       that were just written are not marked as migrated in the bitmap —
     *       so the next load would not even attempt to read them.</li>
     * </ol>
     *
     * <p>Use {@link #upsertComponentsIfLockHeld(UUID, java.util.Map, java.util.Map,
     * String, long, long)} instead — it does SELECT FOR UPDATE + fencing
     * validation + per-generation upsert + bitmap/version update all in one
     * transaction.
     *
     * @deprecated since 1.0.0, for removal. Use
     *             {@link #upsertComponentsIfLockHeld(UUID, java.util.Map, java.util.Map, String, long, long)}.
     * @throws UnsupportedOperationException always.
     */
    @Deprecated(since = "1.0.0", forRemoval = true)
    public java.util.Map<String, Long> upsertComponentsBatch(UUID uuid,
            java.util.Map<String, byte[]> componentsWithData,
            java.util.Map<String, Long> componentsWithChecksum,
            String serverName,
            long fencingToken) throws SQLException {
        throw new UnsupportedOperationException(
            "upsertComponentsBatch() is deprecated and unsafe — it does not write generation "
            + "or update player_data.version/component_bitmap. Use upsertComponentsIfLockHeld("
            + "uuid, componentsWithData, componentsWithChecksum, serverName, fencingToken, "
            + "dirtyBits) instead.");
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
                byte[] data = record.get(DATA_FIELD);
                if (data != null && data.length > 0) {
                    return new PlayerDataRow(
                        data,
                        record.get(VERSION_FIELD),
                        record.get(CHECKSUM_FIELD),
                        record.get(FENCING_TOKEN_FIELD),
                        record.get(COMPONENT_BITMAP_FIELD),
                        record.get(COMPONENT_GENERATION_FIELD));
                }
            }
        }
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
                componentTable);

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
            long dirtyBits) throws SQLException {
        if (componentsWithData == null || componentsWithData.isEmpty()) {
            return ComponentBatchResult.rejected("no components to upsert");
        }
        long now = System.currentTimeMillis();

        try (Connection conn = dataSource.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                // 1. Lock player_data row and read metadata
                String lockSql = String.format(
                    "SELECT `version`, `fencing_token`, `locked_by`, `component_bitmap`, `component_generation` " +
                    "FROM `%s` WHERE `uuid` = ? FOR UPDATE", dataTable);

                long currentVersion;
                long currentBitmap;
                long generation;
                String lockedBy;
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
                        currentBitmap = rs.getLong("component_bitmap");
                        generation = rs.getLong("component_generation");
                    }
                }

                // 2. Validate lock + fencing token
                if (!serverName.equals(lockedBy) || dbFencing != fencingToken) {
                    conn.rollback();
                    conn.setAutoCommit(oldAutoCommit);
                    return ComponentBatchResult.rejected("lock/fencing mismatch (expected: "
                        + serverName + "/ft" + fencingToken
                        + ", actual: " + lockedBy + "/ft" + dbFencing + ")");
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
                    "WHERE `uuid` = ? AND `locked_by` = ? AND `fencing_token` = ?",
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
