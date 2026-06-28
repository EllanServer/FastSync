package com.fastsync.config;

import net.momirealms.sparrow.yaml.SparrowYaml;
import net.momirealms.sparrow.yaml.YamlDocument;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Manages FastSync configuration loading and access.
 *
 * <p>Configuration is loaded from {@code config.yml} using the sparrow-yaml
 * library (which preserves comments). Clean-slate: there is no Bukkit
 * {@link FileConfiguration} fallback — a parse failure is a hard error and the
 * plugin refuses to start. The bundled sample values are additionally vetted by
 * {@link #validateProductionSafety()} before the database is opened.</p>
 */
public class ConfigManager {

    private final JavaPlugin plugin;
    private final SparrowYaml yaml;
    private YamlDocument doc;
    private final Logger logger;

    // Server
    private String serverName;

    // Database
    private String dbType;
    private String dbHost;
    private int dbPort;
    private String dbDatabase;
    private String dbUsername;
    private String dbPassword;
    private String tablePrefix;
    private int poolSize;
    private int queueCapacity;
    private long connectionTimeout;
    private long idleTimeout;
    private long maxLifetime;
    private long leakDetectionThreshold;
    private String dbParameters;
    // Explicit opt-in for plaintext JDBC to a non-localhost host. Default
    // false: validateProductionSafety() refuses to start when sslMode=DISABLED
    // is used against a remote DB. Operators on a fully trusted LAN can set
    // database.allow-insecure-remote: true to downgrade that to a warning.
    private boolean allowInsecureRemote;

    // Redis
    private boolean redisEnabled;
    private String redisHost;
    private int redisPort;
    private String redisPassword;
    private int redisDatabase;
    private boolean redisSsl;
    private int redisTimeout;
    private String redisChannelPrefix;
    private boolean redisCacheEnabled;

    // Sync
    private boolean syncInventory;
    private boolean syncEnderChest;
    private boolean syncHealth;
    private boolean syncFood;
    private boolean syncExperience;
    private boolean syncPotionEffects;
    private boolean syncGameMode;
    private boolean syncFireTicks;
    private boolean syncAir;
    private boolean syncExtraData;
    private int lockTimeout;
    private long lockRetryIntervalMs;
    private int lockMaxRetries;
    private int saveDelay;
    private boolean clearBeforeApply;
    private String loadFailKickMessage;
    private String lockTimeoutKickMessage;
    private boolean periodicSave;
    private int periodicSaveIntervalSeconds;
    private int periodicSaveBatchSize;
    private int heartbeatIntervalSeconds;
    private int maxConcurrentLoads;
    private String busyKickMessage;
    private long shutdownPendingSaveTimeoutMs;
    private int shutdownFinalSaveExecutorTimeoutSeconds;

    // Dirty tracking — skip serialization for unchanged components
    private boolean dirtyTrackingEnabled;
    private int dirtyValidationInterval;

    // Phase 2: per-component storage
    private boolean componentStorageEnabled;
    private int componentBatchSize;  // max components per upsertComponentsBatch transaction

    // Sync - new features
    private boolean syncAdvancements;
    private boolean syncStatistics;
    private boolean syncAttributes;
    private boolean syncFlight;
    private boolean syncPDC;
    private boolean syncLocation;

    // PDC strategy config
    private String pdcMode;                    // off | safe-all-paper | registered-only
    private boolean pdcClearBeforeRestore;     // safe-all-paper: clear PDC before restore (full sync vs merge)
    private java.util.List<String> registeredPdcKeys;  // list of "namespace:key=TYPE" strings

    // Typed statistics config
    private boolean typedStatsEnabled;         // default false
    private String typedStatsMode;             // whitelist | full
    private java.util.List<String> typedStatsWhitelist;  // list of "STATISTIC_NAME=MATERIAL_NAME" strings

    // Location validation config
    private boolean locationRequireSameWorldName;  // default true
    private boolean locationRequireSameWorldUuid;  // default true
    private boolean locationFallbackToSpawn;       // default true

    // Snapshot
    private boolean snapshotEnabled;
    private int maxSnapshots;
    private long snapshotBackupFrequencyMs;
    private String snapshotSaveTrigger;

    // Death save
    private boolean saveOnDeath;
    private boolean saveOnWorldSave;

    // Cluster
    private String clusterId;

    // Production mode
    private boolean productionEnabled;
    private boolean productionRequireRedis;
    private boolean productionRequireClusterId;

    // Final-save executor
    private int finalSaveThreads;
    private int finalSaveQueueCapacity;
    private int finalSaveShutdownTimeoutSeconds;
    private boolean finalSaveAllowSyncFallback;

    // Locked commands while loading
    private boolean cancelCommandsWhileLocked;

    // Compression
    private boolean compressionEnabled;
    private String compressionType;
    private int compressionMinSize;

    // Serialization
    private byte formatVersion;
    // Bounds enforced by CompressionUtil.unwrap() to prevent OOM on corrupted
    // blobs. Defaults are conservative (1 MiB raw / 2.5 MiB wrapped).
    private int serializationMaxRawBytes;
    private int serializationMaxWrappedBytes;

    // Debug
    private boolean debug;
    private boolean logTiming;

    // Dynamo-style conflict recovery
    private String conflictRecoveryStrategy; // "snapshot", "discard"
    private boolean verifyChecksum;
    private int latencyWindowSize;
    private boolean latencyTrackingEnabled;

    // Raft-inspired operation log
    private boolean operationLogEnabled;
    private int operationLogRetention; // max entries per UUID to keep

    // Redis Streams (critical event delivery)
    private boolean streamsEnabled;
    private int redisStreamMaxLen;
    private boolean redisStreamTrimApprox;

    public ConfigManager(JavaPlugin plugin) {
        this.plugin = plugin;
        this.yaml = SparrowYaml.builder().build();
        this.logger = plugin.getLogger();
    }

    /**
     * Test-only constructor. Skips the plugin reference so unit tests
     * can construct a ConfigManager without instantiating a real
     * {@link JavaPlugin} (which has many final methods in modern Paper).
     * <p>Do not use this in production code.</p>
     */
    public ConfigManager(boolean testMode) {
        if (!testMode) {
            throw new IllegalArgumentException("Use ConfigManager(JavaPlugin) for production");
        }
        this.plugin = null;
        this.yaml = null;
        this.logger = Logger.getLogger("FastSync");
    }

    public void load() {
        // Copy config.yml from the JAR if it does not already exist (Bukkit handles this)
        plugin.saveDefaultConfig();
        File configFile = new File(plugin.getDataFolder(), "config.yml");

        // Single path: parse with sparrow-yaml (preserves comments, typed access).
        // Clean-slate: there is NO Bukkit FileConfiguration fallback. A parse
        // failure is a hard error — the plugin cannot start with an unreadable
        // config. Previously a silent Bukkit fallback masked schema drift and
        // old-config migration residue; refusing to load surfaces it instead.
        YamlDocument loaded;
        try {
            loaded = yaml.load(configFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse config.yml with sparrow-yaml: "
                + e.getMessage(), e);
        }
        this.doc = loaded;

        assignValues(new SparrowConfigSource(loaded));
    }

    public void reload() {
        // Re-read the config file (sparrow-yaml re-parses it inside load())
        load();
    }

    /**
     * Refuses to let the plugin start when the loaded config still carries the
     * insecure bundled sample values. This is a clean-slate production guard:
     * the shipped {@code config.yml} uses {@code root/password} and
     * {@code sslMode=DISABLED} as *samples*, and silently running with them in
     * production is a footgun (open DB creds, plaintext DB traffic over the
     * network).
     *
     * <p>Rules (any violation throws {@link RuntimeException}):
     * <ul>
     *   <li>{@code database.username == "root"} <em>and</em>
     *       {@code database.password == "password"} — both still the sample
     *       defaults. The operator has not configured real credentials.</li>
     *   <li>{@code database.host} is not localhost/loopback <em>and</em> the
     *       JDBC {@code parameters} contain {@code sslMode=DISABLED} —
     *       plaintext auth over the network. Suppressed only when the operator
     *       explicitly sets {@code database.allow-insecure-remote: true}.</li>
     * </ul>
     *
     * <p>Called from {@link com.fastsync.FastSync#onEnable()} after
     * {@link #load()} succeeds and before the database is opened.
     */
    public void validateProductionSafety() {
        // 1) Sample credentials still in place. Checking BOTH fields avoids
        //    false positives for an operator who legitimately uses root with a
        //    real password, or "password" as a placeholder while changing the
        //    username.
        if ("root".equals(dbUsername) && "password".equals(dbPassword)) {
            throw new RuntimeException(
                "Insecure default database credentials detected (username='root', "
                    + "password='password'). Edit database.username and "
                    + "database.password in config.yml to real values before "
                    + "starting FastSync.");
        }

        // 2) Plaintext JDBC to a non-loopback host. sslMode=DISABLED (and its
        //    deprecated alias useSSL=false) is fine for a same-machine DB;
        //    over the network it leaks credentials.
        if (!isLoopbackHost(dbHost) && hasPlaintextSsl(dbParameters)) {
            if (allowInsecureRemote) {
                logger.warning("[Config] database.allow-insecure-remote=true: "
                    + "starting with plaintext DB TLS (sslMode=DISABLED / "
                    + "useSSL=false) against non-localhost host '"
                    + dbHost + "'. This sends DB credentials in plaintext. "
                    + "Use sslMode=REQUIRED or VERIFY_IDENTITY in production.");
            } else {
                throw new RuntimeException(
                    "Refusing to start: database.host='" + dbHost + "' is not "
                        + "loopback and database.parameters disables DB TLS "
                        + "(sslMode=DISABLED / useSSL=false). Set "
                        + "sslMode=REQUIRED (or VERIFY_IDENTITY) in "
                        + "database.parameters, or explicitly set "
                        + "database.allow-insecure-remote: true only if you are "
                        + "on a fully trusted network.");
            }
        }

        // 3) Multi-cluster DB isolation. v2 schema uses (cluster_id, uuid) PK,
        //    so different clusters sharing the same table-prefix is now safe
        //    at the DB level. But Redis pub/sub still needs distinct cluster-ids
        //    for namespace isolation. Warn if cluster-id is empty in a multi-server setup.
        if (clusterId == null || clusterId.isBlank()) {
            logger.warning("[Config] cluster-id is empty. Redis pub/sub will use the default namespace. "
                + "If running multiple server clusters on the same Redis, set a distinct cluster-id per cluster.");
        }

        // 4) Production mode checks (round 14)
        if (productionEnabled) {
            if (productionRequireClusterId && (clusterId == null || clusterId.isBlank())) {
                throw new RuntimeException(
                    "production.require-cluster-id=true but cluster-id is empty. "
                        + "Set a non-empty cluster-id in config.yml.");
            }
            if (productionRequireRedis && !redisEnabled) {
                throw new RuntimeException(
                    "production.require-redis=true but redis.enabled=false. "
                        + "Redis is mandatory in production mode for real-time lock coordination.");
            }
            logger.info("[Config] Production mode enabled. Redis required=" + productionRequireRedis
                + ", cluster-id required=" + productionRequireClusterId
                + ", final-save sync fallback allowed=" + finalSaveAllowSyncFallback);
        }
    }

    private static boolean isLoopbackHost(String host) {
        if (host == null) return false;
        String h = host.trim().toLowerCase(java.util.Locale.ROOT);
        return h.isEmpty() || h.equals("localhost")
            || h.equals("127.0.0.1") || h.equals("::1") || h.equals("[::1]");
    }

    private static boolean hasPlaintextSsl(String parameters) {
        if (parameters == null || parameters.isEmpty()) return false;
        // Match the whole parameter, case-insensitively, so sslMode=REQUIRED /
        // VERIFY_IDENTITY and useSSL=true do not match.
        String p = parameters.toLowerCase(java.util.Locale.ROOT);
        for (String token : p.split("&")) {
            String t = token.trim();
            if (t.equals("sslmode=disabled") || t.equals("usessl=false")) {
                return true;
            }
        }
        return false;
    }

    public boolean isAllowInsecureRemote() { return allowInsecureRemote; }

    /**
     * Reads every configuration value through the provided {@link ConfigSource}.
     */
    private void assignValues(ConfigSource source) {
        // Server
        serverName = source.getString("server-name", "survival-1");

        // Database
        dbType = source.getString("database.type", "mysql");
        dbHost = source.getString("database.host", "localhost");
        dbPort = source.getInt("database.port", 3306);
        dbDatabase = source.getString("database.database", "minecraft");
        dbUsername = source.getString("database.username", "root");
        dbPassword = source.getString("database.password", "password");
        tablePrefix = source.getString("database.table-prefix", "fastsync_");
        if (tablePrefix != null && !tablePrefix.matches("[A-Za-z0-9_]*")) {
            logger.warning("[Config] Invalid database.table-prefix '" + tablePrefix
                + "'. Only letters, numbers and underscore are allowed. Falling back to 'fastsync_'.");
            tablePrefix = "fastsync_";
        }
        poolSize = source.getInt("database.pool-size", 10);
        queueCapacity = source.getInt("database.queue-capacity", 256);
        connectionTimeout = source.getLong("database.connection-timeout", 10000);
        idleTimeout = source.getLong("database.idle-timeout", 300000);
        maxLifetime = source.getLong("database.max-lifetime", 1800000);
        leakDetectionThreshold = source.getLong("database.leak-detection-threshold", 60000);
        // Default mirrors the bundled config.yml exactly so the
        // validateProductionSafety() sslMode check sees the same value whether
        // the operator removed the key or left the sample value.
        dbParameters = source.getString("database.parameters",
            "sslMode=DISABLED&useUnicode=true&characterEncoding=UTF-8");
        allowInsecureRemote = source.getBoolean("database.allow-insecure-remote", false);

        // Redis
        redisEnabled = source.getBoolean("redis.enabled", false);
        redisHost = source.getString("redis.host", "localhost");
        redisPort = source.getInt("redis.port", 6379);
        redisPassword = source.getString("redis.password", "");
        redisDatabase = source.getInt("redis.database", 0);
        redisSsl = source.getBoolean("redis.ssl", false);
        redisTimeout = source.getInt("redis.timeout", 5000);
        redisChannelPrefix = source.getString("redis.channel-prefix", "fastsync:lock:");
        redisCacheEnabled = source.getBoolean("redis.cache-enabled", false);

        // Sync
        syncInventory = source.getBoolean("sync.sync-inventory", true);
        syncEnderChest = source.getBoolean("sync.sync-ender-chest", true);
        syncHealth = source.getBoolean("sync.sync-health", true);
        syncFood = source.getBoolean("sync.sync-food", true);
        syncExperience = source.getBoolean("sync.sync-experience", true);
        syncPotionEffects = source.getBoolean("sync.sync-potion-effects", true);
        syncGameMode = source.getBoolean("sync.sync-game-mode", true);
        syncFireTicks = source.getBoolean("sync.sync-fire-ticks", true);
        syncAir = source.getBoolean("sync.sync-air", true);
        syncExtraData = source.getBoolean("sync.sync-extra-data", true);
        lockTimeout = source.getInt("sync.lock-timeout", 60);
        // Round 16 (P1 #5): tightened defaults. Previously 1000ms x 30 = 30s
        // worst-case pre-login block, which caused severe UX under login storms
        // and DB hiccups. New defaults: 300ms x 15 = 4.5s worst-case. The
        // Velocity handoff path is faster and does not wait the full window.
        lockRetryIntervalMs = source.getLong("sync.lock-retry-interval-ms", 300);
        lockMaxRetries = source.getInt("sync.lock-max-retries", 15);
        saveDelay = source.getInt("sync.save-delay", 0);
        clearBeforeApply = source.getBoolean("sync.clear-before-apply", true);
        loadFailKickMessage = source.getString("sync.load-fail-kick-message",
            "&c[FastSync] Failed to load your player data. Please try reconnecting.");
        lockTimeoutKickMessage = source.getString("sync.lock-timeout-kick-message",
            "&c[FastSync] Your data is still being saved by another server.");
        periodicSave = source.getBoolean("sync.periodic-save", false);
        periodicSaveIntervalSeconds = source.getInt("sync.periodic-save-interval-seconds", 300);
        periodicSaveBatchSize = source.getInt("sync.periodic-save-batch-size", 10);
        heartbeatIntervalSeconds = source.getInt("sync.heartbeat-interval-seconds", 10);

        // Login backpressure: limit concurrent pre-login data loads to prevent
        // login storms from exhausting the DB connection pool. Round 16 (P1 #5):
        // default leaves 3 connections for heartbeat/quit/final-save threads
        // and caps at 6 so a login burst cannot starve save/heartbeat traffic.
        // Operators with a large pool can raise this explicitly in config.
        maxConcurrentLoads = source.getInt("sync.max-concurrent-loads",
            Math.max(2, Math.min(6, poolSize - 3)));
        if (maxConcurrentLoads < 1) {
            logger.warning("[Config] sync.max-concurrent-loads must be >= 1. Using 1.");
            maxConcurrentLoads = 1;
        }
        busyKickMessage = source.getString("sync.busy-kick-message",
            "&c[FastSync] Data service is busy. Please reconnect in a few seconds.");

        // Shutdown timeouts — configurable for large servers or slow DBs.
        // The pending-save wait happens BEFORE executors are shut down, so it
        // must be long enough for in-flight saves to drain. The final-save
        // executor timeout is for the finalSaveExecutor.shutdown() call.
        shutdownPendingSaveTimeoutMs = source.getLong("shutdown.pending-save-timeout-ms", 30_000L);
        if (shutdownPendingSaveTimeoutMs < 5_000L) {
            logger.warning("[Config] shutdown.pending-save-timeout-ms must be >= 5000. Using 5000.");
            shutdownPendingSaveTimeoutMs = 5_000L;
        }
        shutdownFinalSaveExecutorTimeoutSeconds = source.getInt("shutdown.final-save-executor-timeout-seconds", 30);
        if (shutdownFinalSaveExecutorTimeoutSeconds < 5) {
            logger.warning("[Config] shutdown.final-save-executor-timeout-seconds must be >= 5. Using 5.");
            shutdownFinalSaveExecutorTimeoutSeconds = 5;
        }

        // Dirty tracking — skip serialization + DB writes for unchanged
        // components. Event listeners mark components dirty; periodic saves
        // only serialize + write the dirty ones. Every Nth save forces a
        // full-collect + checksum comparison as a safety net.
        dirtyTrackingEnabled = source.getBoolean("sync.dirty-tracking.enabled", true);
        dirtyValidationInterval = source.getInt("sync.dirty-tracking.validation-interval", 5);
        if (dirtyValidationInterval < 1) {
            logger.warning("[Config] sync.dirty-tracking.validation-interval must be >= 1. Using 1.");
            dirtyValidationInterval = 1;
        }

        // Phase 2: per-component storage. When enabled, dirty components are
        // written to the player_component table instead of rewriting the full
        // player_data Blob. Reads check component_bitmap to decide which path.
        // Disabled by default — needs explicit opt-in until battle-tested.
        componentStorageEnabled = source.getBoolean("sync.component-storage.enabled", false);
        componentBatchSize = source.getInt("sync.component-storage.batch-size", 15);
        if (componentBatchSize < 1) componentBatchSize = 15;

        // Validate: lock-timeout must have a sane lower bound. A lock-timeout
        // of 0 or negative would make acquireLock's expiredTime computation
        // (now - lockTimeout*1000) produce a value >= now, causing the lock
        // to appear expired immediately — anyone could steal it. A lock-timeout
        // of 1-2s is also unsafe because heartbeat refresh + DB latency can
        // easily exceed that, causing spurious lock loss and player kicks.
        // Enforce a minimum of 15s, which is comfortably above any reasonable
        // heartbeat interval (heartbeat is also clamped below to <= lockTimeout/3).
        if (lockTimeout < 15) {
            logger.warning("[Config] lock-timeout (" + lockTimeout
                + ") is too small (must be >= 15s). Auto-correcting to 15s. "
                + "A smaller value would cause the lock to expire during normal "
                + "DB latency, leading to spurious lock loss and player kicks.");
            lockTimeout = 15;
        }

        // Validate: heartbeat must be <= lockTimeout / 3 to guarantee the lock
        // is refreshed well before it expires. If misconfigured, auto-correct
        // and warn so the server still starts safely.
        int maxHeartbeat = lockTimeout / 3;
        if (maxHeartbeat < 1) maxHeartbeat = 1;
        if (heartbeatIntervalSeconds > maxHeartbeat) {
            logger.warning("[Config] heartbeat-interval-seconds (" + heartbeatIntervalSeconds
                + ") is too large for lock-timeout (" + lockTimeout
                + "). Auto-correcting to " + maxHeartbeat + "s (lock-timeout/3).");
            heartbeatIntervalSeconds = maxHeartbeat;
        }
        if (heartbeatIntervalSeconds < 1) {
            heartbeatIntervalSeconds = 1;
        }

        // Sync - new features
        syncAdvancements = source.getBoolean("sync.sync-advancements", true);
        syncStatistics = source.getBoolean("sync.sync-statistics", true);
        syncAttributes = source.getBoolean("sync.sync-attributes", true);
        syncFlight = source.getBoolean("sync.sync-flight", true);
        syncPDC = source.getBoolean("sync.sync-pdc", true);
        syncLocation = source.getBoolean("sync.sync-location", false);

        // PDC strategy
        pdcMode = source.getString("pdc.mode", "registered-only");
        pdcClearBeforeRestore = source.getBoolean("pdc.clear-before-restore", true);
        registeredPdcKeys = source.getStringList("pdc.registered-keys");

        // Typed statistics
        typedStatsEnabled = source.getBoolean("statistics.typed.enabled", false);
        typedStatsMode = source.getString("statistics.typed.mode", "whitelist");
        typedStatsWhitelist = source.getStringList("statistics.typed.whitelist");

        // Location validation
        locationRequireSameWorldName = source.getBoolean("sync.location.require-same-world-name", true);
        locationRequireSameWorldUuid = source.getBoolean("sync.location.require-same-world-uuid", true);
        locationFallbackToSpawn = source.getBoolean("sync.location.fallback-to-spawn", true);

        // Snapshot
        snapshotEnabled = source.getBoolean("snapshot.enabled", true);
        maxSnapshots = source.getInt("snapshot.max-snapshots", 16);
        snapshotBackupFrequencyMs = source.getLong("snapshot.backup-frequency-ms", 14400000);
        snapshotSaveTrigger = source.getString("snapshot.save-trigger", "never");

        // Death save
        saveOnDeath = source.getBoolean("sync.save-on-death", false);
        saveOnWorldSave = source.getBoolean("sync.save-on-world-save", false);

        // Cluster — fail early if missing/invalid, before DB init
        clusterId = source.getString("cluster-id", "");
        if (clusterId == null || clusterId.isBlank()) {
            throw new RuntimeException(
                "cluster-id must be explicitly set in config.yml. " +
                "All servers in the same FastSync cluster must use the same non-empty cluster-id.");
        }
        clusterId = clusterId.trim();
        if (!clusterId.matches("[A-Za-z0-9_.-]{1,64}")) {
            throw new RuntimeException(
                "Invalid cluster-id '" + clusterId + "'. " +
                "Allowed characters: A-Z a-z 0-9 _ . - , max 64 characters.");
        }

        // Production mode
        productionEnabled = source.getBoolean("production.enabled", false);
        productionRequireRedis = source.getBoolean("production.require-redis", true);
        productionRequireClusterId = source.getBoolean("production.require-cluster-id", true);

        // Final-save executor (single source of truth for sync-fallback gate)
        finalSaveThreads = source.getInt("final-save.threads", 4);
        finalSaveQueueCapacity = source.getInt("final-save.queue-capacity", 8192);
        finalSaveShutdownTimeoutSeconds = source.getInt("final-save.shutdown-timeout-seconds", 60);
        finalSaveAllowSyncFallback = source.getBoolean("final-save.allow-sync-fallback", false);

        // Locked commands while loading
        cancelCommandsWhileLocked = source.getBoolean("sync.cancel-commands-while-locked", false);

        // Compression
        compressionEnabled = source.getBoolean("compression.enabled", true);
        compressionType = source.getString("compression.type", "LZ4");
        compressionMinSize = source.getInt("compression.min-size", 128);

        // Serialization
        formatVersion = (byte) source.getInt("serialization.format-version", 1);
        // Decompression bounds — guard against corrupted / poisoned blobs
        // triggering OOM on the login thread. Defaults match CompressionUtil.
        serializationMaxRawBytes = source.getInt("serialization.max-raw-bytes", 1 << 20);          // 1 MiB
        serializationMaxWrappedBytes = source.getInt("serialization.max-wrapped-bytes", 5 * (1 << 19)); // 2.5 MiB
        if (serializationMaxRawBytes <= 0) serializationMaxRawBytes = 1 << 20;
        if (serializationMaxWrappedBytes <= 0) serializationMaxWrappedBytes = 5 * (1 << 19);
        com.fastsync.serialization.CompressionUtil.configureLimits(
            serializationMaxRawBytes, serializationMaxWrappedBytes);

        // Debug
        debug = source.getBoolean("debug", false);
        logTiming = source.getBoolean("log-timing", false);

        // Dynamo-style conflict recovery
        conflictRecoveryStrategy = source.getString("conflict.recovery-strategy", "snapshot");
        verifyChecksum = source.getBoolean("conflict.verify-checksum", true);
        latencyWindowSize = source.getInt("latency.window-size", 1000);
        latencyTrackingEnabled = source.getBoolean("latency.enabled", false);

        // Raft-inspired operation log
        operationLogEnabled = source.getBoolean("operation-log.enabled", true);
        operationLogRetention = source.getInt("operation-log.retention", 100);
        streamsEnabled = source.getBoolean("redis.streams-enabled", true);
        redisStreamMaxLen = source.getInt("redis.stream-maxlen", 100000);
        redisStreamTrimApprox = source.getBoolean("redis.stream-trim-approx", true);
    }

    // ==================== Getters ====================

    public String getServerName() { return serverName; }

    public String getDbType() { return dbType; }
    public String getDbHost() { return dbHost; }
    public int getDbPort() { return dbPort; }
    public String getDbDatabase() { return dbDatabase; }
    public String getDbUsername() { return dbUsername; }
    public String getDbPassword() { return dbPassword; }
    public String getTablePrefix() { return tablePrefix; }
    public int getPoolSize() { return poolSize; }
    public int getQueueCapacity() { return queueCapacity; }
    public long getConnectionTimeout() { return connectionTimeout; }
    public long getIdleTimeout() { return idleTimeout; }
    public long getMaxLifetime() { return maxLifetime; }
    public long getLeakDetectionThreshold() { return leakDetectionThreshold; }
    public String getDbParameters() { return dbParameters; }

    public boolean isRedisEnabled() { return redisEnabled; }
    public String getRedisHost() { return redisHost; }
    public int getRedisPort() { return redisPort; }
    public String getRedisPassword() { return redisPassword; }
    public int getRedisDatabase() { return redisDatabase; }
    public boolean isRedisSsl() { return redisSsl; }
    public int getRedisTimeout() { return redisTimeout; }
    public String getRedisChannelPrefix() { return redisChannelPrefix; }
    public boolean isRedisCacheEnabled() { return redisCacheEnabled; }

    public boolean isSyncInventory() { return syncInventory; }
    public boolean isSyncEnderChest() { return syncEnderChest; }
    public boolean isSyncHealth() { return syncHealth; }
    public boolean isSyncFood() { return syncFood; }
    public boolean isSyncExperience() { return syncExperience; }
    public boolean isSyncPotionEffects() { return syncPotionEffects; }
    public boolean isSyncGameMode() { return syncGameMode; }
    public boolean isSyncFireTicks() { return syncFireTicks; }
    public boolean isSyncAir() { return syncAir; }
    public boolean isSyncExtraData() { return syncExtraData; }
    public int getLockTimeout() { return lockTimeout; }
    public long getLockRetryIntervalMs() { return lockRetryIntervalMs; }
    public int getLockMaxRetries() { return lockMaxRetries; }
    public int getSaveDelay() { return saveDelay; }
    public boolean isClearBeforeApply() { return clearBeforeApply; }
    public String getLoadFailKickMessage() { return loadFailKickMessage; }
    public String getLockTimeoutKickMessage() { return lockTimeoutKickMessage; }
    public boolean isPeriodicSave() { return periodicSave; }
    public int getPeriodicSaveIntervalSeconds() { return periodicSaveIntervalSeconds; }
    public int getPeriodicSaveBatchSize() { return periodicSaveBatchSize; }
    public int getHeartbeatIntervalSeconds() { return heartbeatIntervalSeconds; }
    public int getMaxConcurrentLoads() { return maxConcurrentLoads; }
    public String getBusyKickMessage() { return busyKickMessage; }
    public long getShutdownPendingSaveTimeoutMs() { return shutdownPendingSaveTimeoutMs; }
    public int getShutdownFinalSaveExecutorTimeoutSeconds() { return shutdownFinalSaveExecutorTimeoutSeconds; }

    /** Whether dirty-tracking-based skip is enabled for periodic saves. */
    public boolean isDirtyTrackingEnabled() { return dirtyTrackingEnabled; }
    /** How many saves between forced full-collect validations. */
    public int getDirtyValidationInterval() { return dirtyValidationInterval; }

    /** Phase 2: per-component storage (writes dirty components to player_component table). */
    public boolean isComponentStorageEnabled() { return componentStorageEnabled; }
    /** Max components per batch upsert transaction. */
    public int getComponentBatchSize() { return componentBatchSize; }

    public boolean isSyncAdvancements() { return syncAdvancements; }
    public boolean isSyncStatistics() { return syncStatistics; }
    public boolean isSyncAttributes() { return syncAttributes; }
    public boolean isSyncFlight() { return syncFlight; }
    public boolean isSyncPDC() { return syncPDC; }
    public boolean isSyncLocation() { return syncLocation; }

    public String getPdcMode() { return pdcMode; }
    public boolean isPdcClearBeforeRestore() { return pdcClearBeforeRestore; }

    public java.util.List<String> getRegisteredPdcKeys() { return registeredPdcKeys; }

    public boolean isTypedStatsEnabled() { return typedStatsEnabled; }
    public String getTypedStatsMode() { return typedStatsMode; }
    public java.util.List<String> getTypedStatsWhitelist() { return typedStatsWhitelist; }

    public boolean isLocationRequireSameWorldName() { return locationRequireSameWorldName; }
    public boolean isLocationRequireSameWorldUuid() { return locationRequireSameWorldUuid; }
    public boolean isLocationFallbackToSpawn() { return locationFallbackToSpawn; }

    public boolean isSnapshotEnabled() { return snapshotEnabled; }
    public int getMaxSnapshots() { return maxSnapshots; }
    public long getSnapshotBackupFrequencyMs() { return snapshotBackupFrequencyMs; }
    public String getSnapshotSaveTrigger() { return snapshotSaveTrigger; }

    public boolean isSaveOnDeath() { return saveOnDeath; }
    public boolean isSaveOnWorldSave() { return saveOnWorldSave; }

    public String getClusterId() { return clusterId; }

    // Production mode
    public boolean isProductionEnabled() { return productionEnabled; }
    public boolean isProductionRequireRedis() { return productionRequireRedis; }
    public boolean isProductionRequireClusterId() { return productionRequireClusterId; }

    // Final-save executor
    public int getFinalSaveThreads() { return finalSaveThreads; }
    public int getFinalSaveQueueCapacity() { return finalSaveQueueCapacity; }
    public int getFinalSaveShutdownTimeoutSeconds() { return finalSaveShutdownTimeoutSeconds; }
    public boolean isFinalSaveAllowSyncFallback() { return finalSaveAllowSyncFallback; }

    public boolean isCancelCommandsWhileLocked() { return cancelCommandsWhileLocked; }

    public boolean isCompressionEnabled() { return compressionEnabled; }
    public String getCompressionType() { return compressionType; }
    public int getCompressionMinSize() { return compressionMinSize; }

    public byte getFormatVersion() { return formatVersion; }
    public int getSerializationMaxRawBytes() { return serializationMaxRawBytes; }
    public int getSerializationMaxWrappedBytes() { return serializationMaxWrappedBytes; }

    public boolean isDebug() { return debug; }
    public boolean isLogTiming() { return logTiming; }

    public String getConflictRecoveryStrategy() { return conflictRecoveryStrategy; }
    public boolean isVerifyChecksum() { return verifyChecksum; }
    public int getLatencyWindowSize() { return latencyWindowSize; }
    public boolean isLatencyTrackingEnabled() { return latencyTrackingEnabled; }

    public boolean isOperationLogEnabled() { return operationLogEnabled; }
    public int getOperationLogRetention() { return operationLogRetention; }
    public boolean isStreamsEnabled() { return streamsEnabled; }
    public int getRedisStreamMaxLen() { return redisStreamMaxLen; }
    public boolean isRedisStreamTrimApprox() { return redisStreamTrimApprox; }

    // ==================== Internal config access abstraction ====================

    /**
     * Minimal read-only abstraction over the configuration source (sparrow-yaml
     * {@link YamlDocument}). Dotted keys are split into the varargs route that
     * sparrow-yaml expects.
     */
    private interface ConfigSource {
        String getString(String key, String def);
        int getInt(String key, int def);
        long getLong(String key, long def);
        boolean getBoolean(String key, boolean def);
        java.util.List<String> getStringList(String key);
    }

    /**
     * {@link ConfigSource} backed by a sparrow-yaml {@link YamlDocument}. Dotted
     * keys (e.g. {@code "database.host"}) are split into the varargs route that
     * sparrow-yaml expects (e.g. {@code "database", "host"}).
     */
    private static final class SparrowConfigSource implements ConfigSource {
        private final YamlDocument document;

        SparrowConfigSource(YamlDocument document) {
            this.document = document;
        }

        private static Object[] route(String key) {
            return key.split("\\.");
        }

        @Override
        public String getString(String key, String def) {
            return document.getOrDefault(String.class, def, route(key));
        }

        @Override
        public int getInt(String key, int def) {
            return document.getOrDefault(Integer.class, def, route(key));
        }

        @Override
        public long getLong(String key, long def) {
            return document.getOrDefault(Long.class, def, route(key));
        }

        @Override
        public boolean getBoolean(String key, boolean def) {
            return document.getOrDefault(Boolean.class, def, route(key));
        }

        @Override
        public java.util.List<String> getStringList(String key) {
            try {
                java.util.List<String> list = document.getOrDefault(
                    new net.momirealms.sparrow.yaml.serializer.TypeRef<java.util.List<String>>() {},
                    java.util.Collections.<String>emptyList(),
                    route(key));
                return list == null ? java.util.Collections.emptyList() : list;
            } catch (Exception e) {
                // Non-list value at path (or parse failure) -> treat as empty list.
                return java.util.Collections.emptyList();
            }
        }
    }
}
