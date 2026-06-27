package com.fastsync.config;

import net.momirealms.sparrow.yaml.SparrowYaml;
import net.momirealms.sparrow.yaml.YamlDocument;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages FastSync configuration loading and access.
 *
 * <p>Configuration is loaded from {@code config.yml} using the sparrow-yaml
 * library (which preserves comments and supports config version migration). If
 * sparrow-yaml fails to parse the file for any reason, the loader transparently
 * falls back to Bukkit's {@link FileConfiguration} as a safety net so the plugin
 * can still start.</p>
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
    private boolean syncLockedMaps;

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

    // Locked commands while loading
    private boolean cancelCommandsWhileLocked;

    // Compression
    private boolean compressionEnabled;
    private String compressionType;
    private int compressionMinSize;

    // Serialization
    private byte formatVersion;

    // Debug
    private boolean debug;
    private boolean logTiming;

    // Dynamo-style conflict recovery
    private String conflictRecoveryStrategy; // "snapshot", "discard", "overwrite"
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

        // Primary path: parse with sparrow-yaml (preserves comments, typed access)
        YamlDocument loaded = null;
        try {
            loaded = yaml.load(configFile);
        } catch (IOException e) {
            plugin.getLogger().log(Level.SEVERE,
                "Failed to parse config.yml with sparrow-yaml. Falling back to Bukkit config.", e);
        }
        this.doc = loaded;

        // Safety net: if sparrow-yaml could not parse the file, use Bukkit's FileConfiguration
        ConfigSource source;
        if (loaded != null) {
            source = new SparrowConfigSource(loaded);
        } else {
            source = new BukkitConfigSource(plugin.getConfig());
        }
        assignValues(source);
    }

    public void reload() {
        // Re-read the config file (sparrow-yaml re-parses it inside load())
        load();
    }

    /**
     * Reads every configuration value through the provided {@link ConfigSource}.
     * This keeps the sparrow-yaml and Bukkit fallback paths identical without
     * duplicating the field assignments.
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
        dbParameters = source.getString("database.parameters",
            "useSSL=false&useUnicode=true&characterEncoding=UTF-8");

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
        lockRetryIntervalMs = source.getLong("sync.lock-retry-interval-ms", 1000);
        lockMaxRetries = source.getInt("sync.lock-max-retries", 30);
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
        // login storms from exhausting the DB connection pool. Default leaves
        // 2 connections for heartbeat/quit saves.
        maxConcurrentLoads = source.getInt("sync.max-concurrent-loads",
            Math.max(2, poolSize - 2));
        if (maxConcurrentLoads < 1) {
            logger.warning("[Config] sync.max-concurrent-loads must be >= 1. Using 1.");
            maxConcurrentLoads = 1;
        }
        busyKickMessage = source.getString("sync.busy-kick-message",
            "&c[FastSync] Data service is busy. Please reconnect in a few seconds.");

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
        // sync-locked-maps is reserved / not implemented. Default MUST be false
        // to match the bundled config.yml and to avoid the status command
        // showing "Enabled" for a feature that does nothing. Old config files
        // missing this key previously got true, which was misleading.
        syncLockedMaps = source.getBoolean("sync.sync-locked-maps", false);

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

        // Cluster
        clusterId = source.getString("cluster-id", "");

        // Locked commands while loading
        cancelCommandsWhileLocked = source.getBoolean("sync.cancel-commands-while-locked", false);

        // Compression
        compressionEnabled = source.getBoolean("compression.enabled", true);
        compressionType = source.getString("compression.type", "LZ4");
        compressionMinSize = source.getInt("compression.min-size", 128);

        // Serialization
        formatVersion = (byte) source.getInt("serialization.format-version", 1);

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
    public boolean isSyncLockedMaps() { return syncLockedMaps; }

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

    public boolean isCancelCommandsWhileLocked() { return cancelCommandsWhileLocked; }

    public boolean isCompressionEnabled() { return compressionEnabled; }
    public String getCompressionType() { return compressionType; }
    public int getCompressionMinSize() { return compressionMinSize; }

    public byte getFormatVersion() { return formatVersion; }

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
     * Minimal read-only abstraction over a configuration source, so the value
     * assignment logic can be shared between the sparrow-yaml primary path and
     * the Bukkit fallback path.
     */
    private interface ConfigSource {
        String getString(String key, String def);
        int getInt(String key, int def);
        long getLong(String key, long def);
        boolean getBoolean(String key, boolean def);
        java.util.List<String> getStringList(String key);
        /**
         * Returns {@code true} only if a value is explicitly present at the
         * given path in the user's config (i.e. not merely a default). This is
         * the semantics required by old-config migration detection.
         */
        boolean contains(String key);
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
                // Non-list value at path (or parse failure) -> treat as empty list,
                // mirroring Bukkit's getStringList() behavior.
                return java.util.Collections.emptyList();
            }
        }

        @Override
        public boolean contains(String key) {
            // sparrow-yaml does not merge config defaults, so a non-null node
            // means the key is explicitly present in the user's file.
            return document.getNodeOrNull(route(key)) != null;
        }
    }

    /**
     * {@link ConfigSource} backed by Bukkit's {@link FileConfiguration}. Used
     * only as a safety net when sparrow-yaml cannot parse the file.
     */
    private static final class BukkitConfigSource implements ConfigSource {
        private final FileConfiguration config;

        BukkitConfigSource(FileConfiguration config) {
            this.config = config;
        }

        @Override
        public String getString(String key, String def) {
            return config.getString(key, def);
        }

        @Override
        public int getInt(String key, int def) {
            return config.getInt(key, def);
        }

        @Override
        public long getLong(String key, long def) {
            return config.getLong(key, def);
        }

        @Override
        public boolean getBoolean(String key, boolean def) {
            return config.getBoolean(key, def);
        }

        @Override
        public java.util.List<String> getStringList(String key) {
            return config.getStringList(key);
        }

        @Override
        public boolean contains(String key) {
            // isSet() returns true only when the key is explicitly present in
            // the user's file (not from the bundled default config), which is
            // what migration detection needs.
            return config.isSet(key);
        }
    }
}
