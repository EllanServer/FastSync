package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.api.FastSyncEvents;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;
import com.fastsync.concurrent.AsyncExecutor;
import com.fastsync.concurrent.LatencyTracker;
import com.fastsync.config.ConfigManager;
import com.fastsync.conflict.ConflictManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import com.fastsync.database.LockResult;
import com.fastsync.database.VersionedData;
import com.fastsync.log.OperationLog;
import com.fastsync.log.FileOperationLogManager;
import com.fastsync.log.OperationType;
import com.fastsync.redis.RedissonManager;
import com.fastsync.redis.stream.StreamEvent;
import com.fastsync.redis.stream.StreamEventType;
import com.fastsync.util.SchedulerUtil;
import com.fastsync.serialization.CompressionUtil;
import com.fastsync.serialization.PlayerDataSerializer;
import com.fastsync.snapshot.SnapshotManager;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.NamespacedKey;
import org.bukkit.attribute.Attribute;
import org.bukkit.attribute.AttributeInstance;
import org.bukkit.attribute.AttributeModifier;
import org.bukkit.entity.Player;
import org.bukkit.potion.PotionEffect;
import org.bukkit.Statistic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.SQLException;

/**
 * Core synchronization manager.
 *
 * Flow:
 *   1. AsyncPlayerPreLoginEvent (async) -> acquireLock + loadData -> cache in pendingData
 *   2. PlayerJoinEvent (sync)          -> applyPlayerData from cache
 *   3. PlayerQuitEvent (sync)          -> collectPlayerData -> async save + releaseLock + notifyRedis
 *
 * Data is loaded during the login phase (not after joining) to prevent
 * the item duplication bugs that plague other sync plugins.
 *
 * Lock coordination:
 *   - With Redis: pub/sub request/response (proper acknowledgment, not "force entry")
 *   - Without Redis: database polling with timeout fallback
 */
public class SyncManager {

    private final FastSync plugin;
    private final ConfigManager config;
    private final DatabaseManager databaseManager;
    private final Logger logger;

    private AsyncExecutor asyncExecutor;
    private RedissonManager redissonManager;
    private SnapshotManager snapshotManager;
    private ConflictManager conflictManager;
    private FileOperationLogManager operationLogManager;

    // Dynamo-style p99.9 latency tracking
    private LatencyTracker loadLatency;
    private LatencyTracker saveLatency;
    private LatencyTracker serializeLatency;

    // Data loaded during pre-login, waiting to be applied on join.
    // ConcurrentHashMap does not permit null values, so players with no saved
    // data are tracked explicitly in pendingEmptyData.
    private final ConcurrentHashMap<UUID, PlayerData> pendingData = new ConcurrentHashMap<>();
    private final java.util.Set<UUID> pendingEmptyData = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<UUID, Long> pendingLoadTimes = new ConcurrentHashMap<>();

    // Track players whose data has been applied (actively playing)
    private final ConcurrentHashMap<UUID, Boolean> activePlayers = new ConcurrentHashMap<>();

    // Track the DB version each player's data was loaded from (for optimistic concurrency)
    private final ConcurrentHashMap<UUID, Long> playerVersions = new ConcurrentHashMap<>();

    // Track the fencing token for each player (Kleppmann stale-write defence)
    private final ConcurrentHashMap<UUID, Long> playerFencingTokens = new ConcurrentHashMap<>();

    // Per-UUID save lock: ensures saves for the same player run sequentially.
    // Periodic saves use tryLock() and skip if a save is in flight (coalescing —
    // the next periodic save will pick up the latest data).
    // Quit saves use lock() and wait, because they MUST persist the final state.
    // This prevents unnecessary version conflicts when periodic + quit saves
    // race for the same player.
    private final ConcurrentHashMap<UUID, java.util.concurrent.locks.ReentrantLock> playerSaveLocks = new ConcurrentHashMap<>();

    // Players whose locks were lost (heartbeat refreshLock=false). These players
    // are being kicked and must NOT be saved via the normal QUIT path — their
    // fencing token is no longer valid. The quit handler checks this set and
    // skips the save, only releasing the lock if it somehow still belongs to us.
    private final java.util.Set<UUID> quarantinedPlayers = ConcurrentHashMap.newKeySet();

    // Players who failed the join handshake (no preloaded data, or missing
    // version/fencing token). They are kicked immediately and must NOT be
    // saved via the normal QUIT path — they never had a valid lock.
    private final java.util.Set<UUID> failedJoinPlayers = ConcurrentHashMap.newKeySet();

    // Anti-reentry guard for heartbeat — prevents overlapping heartbeat cycles
    // when DB latency causes the previous cycle to exceed the interval.
    private final AtomicBoolean heartbeatRunning = new AtomicBoolean(false);

    // Login backpressure: limits concurrent pre-login data loads to prevent
    // login storms from exhausting the DB connection pool.
    private java.util.concurrent.Semaphore loginLoadSemaphore;

    // Dirty tracking: marks which components changed since last save, so
    // periodic saves can skip serialization + DB writes for unchanged parts.
    private com.fastsync.sync.dirty.ComponentDirtyMask dirtyMask;

    // Consecutive heartbeat failure counter. When the DB is unreachable,
    // the heartbeat cycle throws SQLException. If this persists beyond
    // HEARTBEAT_FAILURE_THRESHOLD consecutive cycles, the plugin enters
    // protection mode: all active players are kicked to prevent them from
    // playing with potentially expired locks.
    private final AtomicInteger heartbeatFailureCount = new AtomicInteger(0);
    private static final int HEARTBEAT_FAILURE_THRESHOLD = 3;
    private volatile boolean protectionMode = false;

    // Track pending async saves for graceful shutdown
    private final AtomicInteger pendingSaveCount = new AtomicInteger(0);
    private final AtomicInteger pendingLoadCount = new AtomicInteger(0);

    // Cached Bukkit registries (immutable for the server lifetime) to avoid
    // re-iterating Bukkit.advancementIterator()/Attribute.values() on every save.
    private volatile org.bukkit.advancement.Advancement[] cachedAdvancements;
    private volatile Attribute[] cachedAttributes;

    // Pre-grouped statistic registries (avoids Statistic.values() + type check on every save).
    private volatile List<Statistic> untypedStats;
    private volatile List<Statistic> itemStats;
    private volatile List<Statistic> blockStats;
    private volatile List<Statistic> entityStats;
    // Pre-filtered material/entity registries (avoids Material.values() ~1300 entries on every save).
    private volatile List<org.bukkit.Material> itemMaterials;   // isItem() == true
    private volatile List<org.bukkit.Material> blockMaterials;  // isBlock() == true
    private volatile List<org.bukkit.entity.EntityType> aliveEntities; // isAlive() == true

    // Parsed snapshot trigger set (computed once at init/reload, O(1) lookup per save).
    private volatile java.util.Set<String> snapshotTriggerSet;

    // Sync strategies (initialized in initialize())
    private com.fastsync.sync.strategy.PdcSyncStrategy pdcStrategy;
    private com.fastsync.sync.strategy.TypedStatisticStrategy typedStatsStrategy;
    private com.fastsync.sync.strategy.LocationSyncStrategy locationStrategy;

    public SyncManager(FastSync plugin, ConfigManager config, DatabaseManager databaseManager) {
        this.plugin = plugin;
        this.config = config;
        this.databaseManager = databaseManager;
        this.logger = plugin.getLogger();
    }

    /**
     * Initialize the sync manager with async executor and optional Redis.
     */
    public void initialize() {
        // Create dedicated thread pool (NOT ForkJoinPool.commonPool)
        int poolSize = Math.max(2, config.getPoolSize() / 2);
        asyncExecutor = new AsyncExecutor(logger, "FastSync-Async", poolSize);

        // Login backpressure semaphore — limits concurrent pre-login loads
        loginLoadSemaphore = new java.util.concurrent.Semaphore(config.getMaxConcurrentLoads(), true);
        logger.info("Login load concurrency limit: " + config.getMaxConcurrentLoads());

        // Dirty tracking — event-driven component change detection.
        // When enabled, periodic saves only serialize + write dirty components.
        // Falls back to full save every Nth cycle as a safety net.
        if (config.isDirtyTrackingEnabled()) {
            dirtyMask = new com.fastsync.sync.dirty.ComponentDirtyMask(
                config.getDirtyValidationInterval());
            logger.info("Dirty tracking enabled (validation every " +
                config.getDirtyValidationInterval() + " saves)");
        }

        // Initialize snapshot system if enabled
        if (config.isSnapshotEnabled()) {
            snapshotManager = new SnapshotManager(logger, config);
            snapshotManager.initialize(databaseManager);
            logger.info("Snapshot/backup system enabled (max " + config.getMaxSnapshots() + " per player).");
        }

        // Initialize Redis if enabled (Redisson: Pub/Sub + Streams unified)
        if (config.isRedisEnabled()) {
            try {
                redissonManager = new RedissonManager(
                    config.getRedisHost(), config.getRedisPort(),
                    config.getRedisPassword(), config.getRedisDatabase(),
                    config.getServerName(), config.getClusterId(), config.getRedisChannelPrefix(),
                    config.isRedisSsl(), config.getRedisTimeout(), config.isStreamsEnabled(),
                    config.getRedisStreamMaxLen(), config.isRedisStreamTrimApprox());
                // Register listener BEFORE initialize() — initialize() starts the
                // consumer loop and recovers pending entries, which can dispatch
                // stream events immediately. If the listener isn't registered yet,
                // those events are silently ack'd and lost.
                redissonManager.addListener(this::handleStreamEvent);
                redissonManager.initialize();
                logger.info("Redis coordination enabled (Redisson: Pub/Sub + Streams).");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to connect to Redis! Falling back to database polling.", e);
                redissonManager = null;
            }
        } else {
            logger.info("Redis not enabled, using database polling for lock coordination.");
        }

        // Initialize conflict manager (Dynamo-style conflict recovery)
        conflictManager = new ConflictManager(logger, config, snapshotManager);

        // Initialize operation log (file-based append-only journal, no JVM args needed)
        if (config.isOperationLogEnabled()) {
            try {
                operationLogManager = new FileOperationLogManager(
                    plugin.getDataFolder().toPath(), config.getOperationLogRetention());
                operationLogManager.initialize();
                logger.info("Operation log enabled (file-based, retention=" +
                    config.getOperationLogRetention() + " per player).");
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to initialize operation log (file-based)", e);
            }
        }

        // Initialize latency trackers (Dynamo p99.9 SLA focus)
        if (config.isLatencyTrackingEnabled()) {
            int window = config.getLatencyWindowSize();
            loadLatency = new LatencyTracker("DB-Load", logger, window);
            saveLatency = new LatencyTracker("DB-Save", logger, window);
            serializeLatency = new LatencyTracker("Serialize", logger, window);
            logger.info("Latency tracking enabled (p50/p99/p99.9, window=" + window + ").");
        }

        // Parse snapshot trigger set for O(1) lookup during saves
        parseSnapshotTriggerSet();

        // Initialize sync strategies
        pdcStrategy = com.fastsync.sync.strategy.PdcStrategyFactory.create(config, logger);
        logger.info("PDC sync strategy: " + pdcStrategy.strategyName()
            + (pdcStrategy.isSafe() ? "" : " (UNSAFE)"));

        if (config.isTypedStatsEnabled()) {
            typedStatsStrategy = createTypedStatsStrategy();
        } else {
            typedStatsStrategy = null;
        }
        if (typedStatsStrategy != null) {
            logger.info("Typed statistics strategy: " + typedStatsStrategy.strategyName());
        } else {
            logger.info("Typed statistics: disabled (basic stats still synced if sync-statistics is on)");
        }

        locationStrategy = new com.fastsync.sync.strategy.LocationSyncStrategy(config, logger);
    }

    /**
     * Re-parse config-dependent caches after a config reload.
     * Called from {@code /fastsync reload}.
     */
    public void refreshConfigCache() {
        parseSnapshotTriggerSet();
        // Re-create strategies that depend on config
        pdcStrategy = com.fastsync.sync.strategy.PdcStrategyFactory.create(config, logger);
        if (config.isTypedStatsEnabled()) {
            typedStatsStrategy = createTypedStatsStrategy();
        } else {
            typedStatsStrategy = null;
        }
        locationStrategy = new com.fastsync.sync.strategy.LocationSyncStrategy(config, logger);
    }

    /**
     * Create the typed statistics strategy based on config.
     * Uses the cached registry lists (untypedStats/itemStats/blockStats/entityStats/
     * itemMaterials/blockMaterials/aliveEntities) populated by ensureRegistryCache().
     */
    private com.fastsync.sync.strategy.TypedStatisticStrategy createTypedStatsStrategy() {
        ensureRegistryCache();
        String mode = config.getTypedStatsMode();
        if ("full".equalsIgnoreCase(mode)) {
            return new com.fastsync.sync.strategy.FullTypedStatsStrategy(
                itemStats, blockStats, entityStats,
                itemMaterials, blockMaterials, aliveEntities);
        }
        // Default: whitelist
        return parseWhitelistStrategy();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private com.fastsync.sync.strategy.TypedStatisticStrategy parseWhitelistStrategy() {
        java.util.List<String> rawList = config.getTypedStatsWhitelist();
        if (rawList == null || rawList.isEmpty()) {
            logger.warning("[Stats] Whitelist mode but no entries configured. Typed stats will be empty.");
            return new com.fastsync.sync.strategy.WhitelistTypedStatsStrategy(null, null, null);
        }

        java.util.List<com.fastsync.sync.strategy.WhitelistTypedStatsStrategy.StatBinding<org.bukkit.Material>> itemBindings = new ArrayList<>();
        java.util.List<com.fastsync.sync.strategy.WhitelistTypedStatsStrategy.StatBinding<org.bukkit.Material>> blockBindings = new ArrayList<>();
        java.util.List<com.fastsync.sync.strategy.WhitelistTypedStatsStrategy.StatBinding<org.bukkit.entity.EntityType>> entityBindings = new ArrayList<>();

        for (String entry : rawList) {
            // Format: "STATISTIC_NAME=MATERIAL_NAME"
            String[] parts = entry.split("=", 2);
            if (parts.length != 2) continue;
            String statName = parts[0].trim().toUpperCase();
            String targetName = parts[1].trim().toUpperCase();

            try {
                org.bukkit.Statistic stat = org.bukkit.Statistic.valueOf(statName);
                if (stat.getType() == org.bukkit.Statistic.Type.ITEM) {
                    org.bukkit.Material mat = org.bukkit.Material.matchMaterial(targetName);
                    if (mat != null && mat.isItem()) {
                        itemBindings.add(new com.fastsync.sync.strategy.WhitelistTypedStatsStrategy.StatBinding<>(stat, mat));
                    }
                } else if (stat.getType() == org.bukkit.Statistic.Type.BLOCK) {
                    org.bukkit.Material mat = org.bukkit.Material.matchMaterial(targetName);
                    if (mat != null && mat.isBlock()) {
                        blockBindings.add(new com.fastsync.sync.strategy.WhitelistTypedStatsStrategy.StatBinding<>(stat, mat));
                    }
                } else if (stat.getType() == org.bukkit.Statistic.Type.ENTITY) {
                    org.bukkit.entity.EntityType ent = org.bukkit.entity.EntityType.valueOf(targetName);
                    entityBindings.add(new com.fastsync.sync.strategy.WhitelistTypedStatsStrategy.StatBinding<>(stat, ent));
                }
            } catch (IllegalArgumentException ignored) {
                logger.warning("[Stats] Invalid whitelist entry: " + entry);
            }
        }

        logger.info("[Stats] Whitelist: " + itemBindings.size() + " item, "
            + blockBindings.size() + " block, " + entityBindings.size() + " entity bindings.");
        return new com.fastsync.sync.strategy.WhitelistTypedStatsStrategy(
            itemBindings, blockBindings, entityBindings);
    }

    // ==================== Load (Pre-Login) ====================

    /**
     * Load player data during the async pre-login phase.
     * Acquires a cross-server lock and loads data from the database.
     *
     * Lock acquisition strategy:
     *   1. Try to acquire lock via database
     *   2. If locked by another server:
     *      a. With Redis: send REQUEST, wait for RELEASED notification (real-time)
     *      b. Without Redis: poll database with sleep intervals
     *   3. If still locked after max retries: return LOCKED (kick player)
     *
     * @return LoadResult indicating success, locked, or error
     */
    public LoadResult loadPlayerData(UUID uuid) {
        if (protectionMode) {
            return LoadResult.protection("FastSync protection mode is active");
        }
        // Backpressure: if too many players are loading simultaneously, reject
        // fast rather than queuing behind DB lock contention. This prevents
        // login storms from exhausting the HikariCP connection pool.
        if (loginLoadSemaphore != null && !loginLoadSemaphore.tryAcquire()) {
            return LoadResult.busy("Too many concurrent data loads");
        }
        pendingLoadCount.incrementAndGet();
        try {
            return loadPlayerDataInternal(uuid);
        } finally {
            pendingLoadCount.decrementAndGet();
            if (loginLoadSemaphore != null) {
                loginLoadSemaphore.release();
            }
        }
    }

    private LoadResult loadPlayerDataInternal(UUID uuid) {
        // Step 1: Try to acquire lock (returns fencing token on success)
        boolean locked = false;
        long fencingToken = 0;
        for (int i = 0; i < config.getLockMaxRetries(); i++) {
            try {
                LockResult lockResult = databaseManager.acquireLock(uuid, config.getServerName());
                if (lockResult.acquired()) {
                    locked = true;
                    fencingToken = lockResult.fencingToken();
                    break;
                }

                // Lock is held by another server
                if (config.isDebug()) {
                    String holder = databaseManager.getLockHolder(uuid);
                    logger.info("Lock held by " + holder + " for " + uuid +
                        " (attempt " + (i + 1) + "/" + config.getLockMaxRetries() + ")");
                }

                // If Redis is available, wait for real-time RELEASED notification
                // instead of blindly sleeping
                if (redissonManager != null && redissonManager.isHealthy()) {
                    boolean released = redissonManager.waitForLockRelease(uuid, config.getLockRetryIntervalMs());
                    if (released && config.isDebug()) {
                        logger.info("Received lock release notification for " + uuid);
                    }
                    // If the pub/sub notification was lost (e.g. listener registered
                    // after the message was published), the lock may have already been
                    // released in the DB. Do a quick DB probe before retrying — if the
                    // lock is free, skip the sleep and retry immediately, avoiding up
                    // to lockMaxRetries × lockRetryIntervalMs of unnecessary waiting.
                    if (!released) {
                        try {
                            String holder = databaseManager.getLockHolder(uuid);
                            if (holder == null || holder.isEmpty()) {
                                // Lock is actually free — retry immediately
                                if (config.isDebug()) {
                                    logger.info("Lock already released in DB for " + uuid + " (pub message missed)");
                                }
                                continue;
                            }
                        } catch (SQLException probeEx) {
                            // DB probe failed — fall through to normal retry
                        }
                    }
                    // Whether or not we got the notification, retry acquiring
                } else {
                    // Fallback: sleep and retry
                    Thread.sleep(config.getLockRetryIntervalMs());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return LoadResult.error("Interrupted while waiting for lock");
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Database error while acquiring lock for " + uuid, e);
                return LoadResult.error("Database error: " + e.getMessage());
            }
        }

        if (!locked) {
            return LoadResult.locked();
        }

        // Step 2: Load data from database
        try {
            long startTime = System.nanoTime();

            VersionedData loaded = databaseManager.loadData(uuid);
            long loadElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
            if (loadLatency != null) loadLatency.record(loadElapsedMs);

            if (config.isLogTiming()) {
                logger.info("[Timing] DB load for " + uuid + ": " + loadElapsedMs + "ms");
            }

            if (!loaded.hasData()) {
                // New player or no saved data - still store fencing token and version for save
                pendingEmptyData.add(uuid);
                pendingLoadTimes.put(uuid, System.currentTimeMillis());
                playerFencingTokens.put(uuid, fencingToken);
                // Explicitly set version=0 for new players (DB default, but don't rely on implicit behavior)
                playerVersions.put(uuid, loaded.version());
                if (config.isDebug()) {
                    logger.info("No saved data for " + uuid + " (new player, v" + loaded.version() + ", ft: " + fencingToken + ")");
                }
                return LoadResult.success();
            }

            // Step 3: Decompress
            startTime = System.nanoTime();

            byte[] decompressed = CompressionUtil.unwrap(loaded.data());

            // Verify checksum AFTER decompression — checksum is computed on
            // the raw serialized data (not the compressed blob). This prevents
            // false corruption warnings when compression changes the byte layout.
            if (config.isVerifyChecksum() && !DatabaseManager.verifyChecksum(decompressed, loaded.checksum())) {
                logger.warning("[Checksum] Data corruption detected for " + uuid +
                    "! Stored checksum: " + loaded.checksum() +
                    ". Rejecting load to prevent applying corrupted data.");

                // Log checksum failure
                logOperation(uuid, OperationType.CHECKSUM_FAIL, fencingToken, loaded.version(),
                    decompressed != null ? decompressed.length : 0,
                    "Checksum mismatch: stored=" + loaded.checksum());

                if (loadLatency != null) loadLatency.recordError();
            try {
                databaseManager.releaseLock(uuid, config.getServerName(), fencingToken);
                notifyLockReleased(uuid);
            } catch (SQLException ex) {
                logger.log(Level.WARNING, "Failed to release lock after checksum failure for " + uuid, ex);
            }
            return LoadResult.error("Data checksum mismatch - possible corruption");
            }

            PlayerData data = PlayerDataSerializer.deserialize(decompressed);

            // Phase 2: merge per-component overrides from player_component table.
            // If component_bitmap is non-zero, some components have been migrated to
            // the per-component table — load them and overwrite the corresponding
            // fields in PlayerData. This gives the freshest state: full Blob as
            // base, component rows as overrides on top.
            if (config.isComponentStorageEnabled()) {
                long bitmap = databaseManager.getComponentBitmap(uuid);
                if (bitmap != 0) {
                    java.util.Set<String> migratedNames = new java.util.HashSet<>();
                    for (com.fastsync.sync.dirty.ComponentDirtyMask.Component c :
                            com.fastsync.sync.dirty.ComponentDirtyMask.ALL) {
                        if ((bitmap & (1L << c.ordinal())) != 0) {
                            migratedNames.add(c.name());
                        }
                    }
                    if (!migratedNames.isEmpty()) {
                        java.util.Map<String, com.fastsync.database.DatabaseManager.ComponentData> components =
                            databaseManager.loadComponents(uuid, migratedNames);
                        for (var entry : components.entrySet()) {
                            String name = entry.getKey();
                            byte[] compData = entry.getValue().data();
                            if (entry.getValue().hasData()) {
                                try {
                                    byte[] decompressedComp = CompressionUtil.unwrap(compData);
                                    PlayerDataSerializer.deserializeComponent(name, decompressedComp, data);
                                } catch (Exception e) {
                                    logger.warning("[Load] Failed to deserialize component "
                                        + name + " for " + uuid + ": " + e.getMessage());
                                }
                            }
                        }
                        if (config.isDebug()) {
                            logger.info("Merged " + components.size() + " component overrides for " + uuid);
                        }
                    }
                }
            }

            // Set the version from DB for optimistic concurrency (Dynamo-style)
            data.setVersion(loaded.version());
            // Set the fencing token from lock acquisition (Kleppmann-style)
            data.setFencingToken(fencingToken);

            long deserElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
            if (serializeLatency != null) serializeLatency.record(deserElapsedMs);

            if (config.isLogTiming()) {
                logger.info("[Timing] Deserialize for " + uuid + ": " + deserElapsedMs + "ms" +
                    " (raw=" + loaded.data().length + " bytes, decompressed=" + decompressed.length + " bytes)");
            }

            pendingData.put(uuid, data);
            pendingLoadTimes.put(uuid, System.currentTimeMillis());

            if (config.isDebug()) {
                logger.info("Loaded data for " + uuid + " (v" + loaded.version() + ", ft=" + fencingToken + ", " + loaded.data().length + " bytes in DB)");
            }

            // Log operation (Raft-inspired per-UUID ordered log)
            logOperation(uuid, OperationType.LOAD, fencingToken, loaded.version(),
                loaded.data().length, "Loaded from DB");

            // Publish critical event: player has checked in (Streams — recoverable)
            publishCheckin(uuid, loaded.version(), fencingToken);

            return LoadResult.success();

        } catch (Exception e) {
        logger.log(Level.SEVERE, "Failed to load data for " + uuid, e);
        // Release lock on error — use token version if we have a valid token
        try {
            if (fencingToken > 0) {
                databaseManager.releaseLock(uuid, config.getServerName(), fencingToken);
            } else {
                databaseManager.releaseLock(uuid, config.getServerName());
            }
            notifyLockReleased(uuid);
        } catch (SQLException ex) {
            logger.log(Level.WARNING, "Failed to release lock after load error for " + uuid, ex);
        }
        return LoadResult.error(e.getMessage());
    }
    }

    // ==================== Apply (Join) ====================

    /**
     * Apply loaded player data to the player.
     * Must be called on the main thread (during PlayerJoinEvent).
     */
    public void applyPlayerData(Player player) {
        UUID uuid = player.getUniqueId();
        PlayerData data = pendingData.remove(uuid);
        boolean hadEmptyData = pendingEmptyData.remove(uuid);
        pendingLoadTimes.remove(uuid);

        if (data == null) {
        if (hadEmptyData) {
            // New player with empty data — verify we have a valid fencing token.
            // Without it, saves would use version=0/fencingToken=0 and corrupt data.
            Long version = playerVersions.get(uuid);
            Long fencingToken = playerFencingTokens.get(uuid);
            if (version == null || fencingToken == null || fencingToken <= 0) {
                logger.severe("[FastSync] Empty-data player joined without valid version/fencing token: " + uuid);
                failedJoinPlayers.add(uuid);
                activePlayers.remove(uuid);
                player.kick(net.kyori.adventure.text.Component.text(
                    "[FastSync] Failed to prepare your data. Please reconnect.",
                    net.kyori.adventure.text.format.NamedTextColor.RED));
                return;
            }
            activePlayers.put(uuid, true);
            if (config.isDebug()) {
                logger.info("No saved data to apply for " + uuid + " (new player, v"
                    + version + ", ft=" + fencingToken + ")");
            }
            return;
        }
        // No pending data AND not empty-data — this means the pre-login load
        // failed silently or the player bypassed the normal flow. Must NOT
        // mark active — the player has no valid fencing token.
        logger.severe("[FastSync] Player joined without preloaded data: " + uuid
            + " — refusing to mark active.");
        failedJoinPlayers.add(uuid);
        activePlayers.remove(uuid);
        playerVersions.remove(uuid);
        playerFencingTokens.remove(uuid);
        if (dirtyMask != null) {
            dirtyMask.remove(uuid);
        }
        player.kick(net.kyori.adventure.text.Component.text(
            "[FastSync] Failed to prepare your data. Please reconnect.",
            net.kyori.adventure.text.format.NamedTextColor.RED));
        return;
    }

        long startTime = config.isLogTiming() ? System.nanoTime() : 0;
        try {
        // Clear current state to prevent duplication
        if (config.isClearBeforeApply()) {
            player.getInventory().clear();
            player.getEnderChest().clear();
            player.getInventory().setArmorContents(null);
            player.getInventory().setItemInOffHand(null);
            for (PotionEffect effect : new ArrayList<>(player.getActivePotionEffects())) {
                player.removePotionEffect(effect.getType());
            }
        }

        // Inventory
        if (config.isSyncInventory() && data.getInventory() != null) {
            setInventoryContents(player, data.getInventory());
        }

        // Armor
        if (config.isSyncInventory() && data.getArmor() != null) {
            player.getInventory().setArmorContents(data.getArmor());
        }

        // Offhand
        if (config.isSyncInventory() && data.getOffhand() != null) {
            player.getInventory().setItemInOffHand(data.getOffhand());
        }

        // Ender chest
        if (config.isSyncEnderChest() && data.getEnderChest() != null) {
            setEnderChestContents(player, data.getEnderChest());
        }

        // Health
        if (config.isSyncHealth()) {
            try {
                player.setMaxHealth(data.getMaxHealth());
            } catch (Exception ignored) {
                // Some versions don't support setMaxHealth directly
            }
            double health = Math.min(data.getHealth(), data.getMaxHealth());
            if (health > 0) {
                player.setHealth(health);
            }
        }

        // Food
        if (config.isSyncFood()) {
            player.setFoodLevel(data.getFoodLevel());
            player.setSaturation(data.getSaturation());
            player.setExhaustion(data.getExhaustion());
        }

        // Experience
        if (config.isSyncExperience()) {
            player.setLevel(data.getExpLevel());
            player.setExp(data.getExpProgress());
            player.setTotalExperience(data.getTotalExperience());
        }

        // Potion effects
        if (config.isSyncPotionEffects() && data.getPotionEffects() != null) {
            for (PlayerData.PotionEffectData effectData : data.getPotionEffects()) {
                PotionEffect effect = PlayerDataSerializer.toPotionEffect(effectData);
                if (effect != null) {
                    player.addPotionEffect(effect);
                }
            }
        }

        // Game mode
        if (config.isSyncGameMode() && data.getGameMode() != null) {
            player.setGameMode(data.getGameMode());
        }

        // Fire ticks
        if (config.isSyncFireTicks()) {
            player.setFireTicks(data.getFireTicks());
        }

        // Air
        if (config.isSyncAir()) {
            player.setRemainingAir(data.getRemainingAir());
        }

        // Flight status
        if (config.isSyncFlight()) {
            player.setAllowFlight(data.isAllowFlight());
            if (data.isFlying() && data.isAllowFlight()) {
                player.setFlying(true);
            }
        }

        // Advancements
        if (config.isSyncAdvancements() && data.getAdvancements() != null) {
            applyAdvancements(player, data);
        }

        // Statistics
        if (config.isSyncStatistics() && data.getStatistics() != null) {
            applyStatistics(player, data);
        }

        // Attributes
        if (config.isSyncAttributes() && data.getAttributes() != null) {
            applyAttributes(player, data);
        }

        // Persistent Data Container (via strategy)
        if (pdcStrategy != null && !"off".equals(pdcStrategy.strategyName())
                && data.getPersistentDataContainer() != null
                && data.getPersistentDataContainer().containsKey("__pdc_bytes__")) {
            byte[] pdcBytes = data.getPersistentDataContainer().get("__pdc_bytes__");
            // Call restore even if pdcBytes is empty (length==0) — this is the
            // signal to clear the target PDC and remove ghost keys.
            pdcStrategy.restore(player, pdcBytes);
        }

        // Location (optional, with validation via LocationSyncStrategy)
        if (config.isSyncLocation() && data.getWorldName() != null) {
            locationStrategy.apply(player, data);
        }

        activePlayers.put(uuid, true);
        // Store the version for optimistic concurrency on save
        playerVersions.put(uuid, data.getVersion());
        // Store the fencing token for stale-write defence on save
        playerFencingTokens.put(uuid, data.getFencingToken());

        // Fire API event
        FastSyncEvents.FastSyncApplyEvent applyEvent = new FastSyncEvents.FastSyncApplyEvent(player, data);
        Bukkit.getPluginManager().callEvent(applyEvent);

        if (config.isLogTiming()) {
            long elapsed = (System.nanoTime() - startTime) / 1_000_000;
            logger.info("[Timing] Apply data for " + uuid + ": " + elapsed + "ms");
        }

        if (config.isDebug()) {
            logger.info("Applied data for " + uuid);
        }
        } catch (Throwable t) {
            // Apply failed midway — player may be in a partially-applied state.
            // Mark as failed-join so quit handler skips save, release the lock,
            // and kick the player to prevent playing with corrupted state.
            failedJoinPlayers.add(uuid);
            activePlayers.remove(uuid);
            playerVersions.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid);
            if (dirtyMask != null) {
                dirtyMask.remove(uuid);
            }
            if (ft != null && ft > 0) {
                final long fencingToken = ft;
                pendingSaveCount.incrementAndGet();
                try {
                    asyncExecutor.execute(() -> {
                        try {
                            databaseManager.releaseLock(uuid, config.getServerName(), fencingToken);
                            notifyLockReleased(uuid);
                        } catch (SQLException e) {
                            logger.log(Level.WARNING, "Failed to release lock after apply failure for " + uuid, e);
                        } finally {
                            pendingSaveCount.decrementAndGet();
                        }
                    });
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    pendingSaveCount.decrementAndGet();
                    try {
                        databaseManager.releaseLock(uuid, config.getServerName(), fencingToken);
                        notifyLockReleased(uuid);
                    } catch (SQLException ex) {
                        logger.log(Level.WARNING, "Failed to release lock after apply failure for " + uuid, ex);
                    }
                }
            }
            player.kick(net.kyori.adventure.text.Component.text(
                "[FastSync] Failed to apply your data. Please reconnect.",
                net.kyori.adventure.text.format.NamedTextColor.RED));
            logger.log(Level.SEVERE, "Failed to apply data for " + uuid
                + " — player kicked, lock released, state not saved.", t);
        }
    }

    // ==================== Save (Quit) ====================

    /**
     * Collect player data and save it asynchronously.
     * Collection happens on the main thread; serialization and DB save happen async.
     * After save, notifies Redis so waiting servers can acquire the lock immediately.
     */
    /**
     * Refresh the version and fencing token in a PlayerData object from the
     * current in-memory maps. This MUST be called after acquiring the per-UUID
     * saveLock but before calling {@link #persistCollectedData}, to ensure the
     * final save uses the latest version (which may have been advanced by an
     * in-flight periodic/death/world_save save that completed while we waited
     * for the lock).
     *
     * <p>Without this refresh, a QUIT save that was collected with version N
     * but waited behind a periodic save that advanced the DB to version N+1
     * would fail the CAS and release the lock without saving — losing the
     * player's final state.
     */
    private void refreshVersionAndFencingToken(UUID uuid, PlayerData data) {
        Long latestVersion = playerVersions.get(uuid);
        if (latestVersion != null) {
            data.setVersion(latestVersion);
        }
        Long latestFencing = playerFencingTokens.get(uuid);
        if (latestFencing != null) {
            data.setFencingToken(latestFencing);
        }
    }

    /**
     * Collect and save player data on quit (async path).
     *
     * <p>Collection happens on the main/region thread (PlayerQuitEvent), then
     * the save is dispatched to the async executor. The per-UUID saveLock
     * ensures this save runs AFTER any in-flight periodic/death/world_save save.
     *
     * <p><b>Critical:</b> After acquiring the saveLock, {@link #refreshVersionAndFencingToken}
     * is called to pick up the latest version/fencing token from any save that
     * completed while we waited for the lock. This prevents the final-save
     * version race where the QUIT save uses a stale version, fails the CAS,
     * and releases the lock without saving the player's final state.
     */
    public void collectAndSavePlayerData(Player player) {
        UUID uuid = player.getUniqueId();

        // Check failed-join players — they never became active with a valid lock.
        // The kick from applyPlayerData triggers PlayerQuitEvent, so we must
        // intercept here to prevent a save with invalid version/fencingToken.
        if (failedJoinPlayers.remove(uuid)) {
            logger.warning("[Quit] Skipping save for failed-join player " + uuid
                + " — player never became active with a valid lock.");
            activePlayers.remove(uuid);
            pendingData.remove(uuid);
            pendingEmptyData.remove(uuid);
            pendingLoadTimes.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid);
            if (dirtyMask != null) {
                dirtyMask.remove(uuid);
            }
            playerVersions.remove(uuid);
            if (ft != null && ft > 0) {
                pendingSaveCount.incrementAndGet();
                try {
                    asyncExecutor.execute(() -> {
                        try {
                            databaseManager.releaseLock(uuid, config.getServerName(), ft);
                            notifyLockReleased(uuid);
                        } catch (SQLException e) {
                            logger.log(Level.WARNING, "Failed to release lock for failed-join player " + uuid, e);
                        } finally {
                            pendingSaveCount.decrementAndGet();
                        }
                    });
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    pendingSaveCount.decrementAndGet();
                    try {
                        databaseManager.releaseLock(uuid, config.getServerName(), ft);
                        notifyLockReleased(uuid);
                    } catch (SQLException ex) {
                        logger.log(Level.WARNING, "Failed to release lock for failed-join player " + uuid, ex);
                    }
                }
            }
            return;
        }

        // Skip save for players who are not active, not pending, and not quarantined.
        // This catches edge cases where a quit event fires for a player who was
        // already cleaned up by another path.
        if (!activePlayers.containsKey(uuid)
            && !pendingData.containsKey(uuid)
            && !pendingEmptyData.contains(uuid)
            && !quarantinedPlayers.contains(uuid)) {
            logger.warning("[Quit] Skipping save for inactive/untracked player " + uuid);
            playerVersions.remove(uuid);
            playerFencingTokens.remove(uuid);
            return;
        }

        // Check if player has pending data (was kicked during pre-login, never joined)
        if (pendingData.containsKey(uuid) || pendingEmptyData.contains(uuid)) {
            pendingData.remove(uuid);
            pendingEmptyData.remove(uuid);
            pendingLoadTimes.remove(uuid);
            activePlayers.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid);
            if (dirtyMask != null) {
                dirtyMask.remove(uuid);
            }
            playerVersions.remove(uuid);
            // Release lock without saving — use token version if available
            pendingSaveCount.incrementAndGet();
            try {
                asyncExecutor.execute(() -> {
                    try {
                        if (ft != null && ft > 0) {
                            databaseManager.releaseLock(uuid, config.getServerName(), ft);
                        } else {
                            databaseManager.releaseLock(uuid, config.getServerName());
                        }
                        notifyLockReleased(uuid);
                        if (config.isDebug()) {
                            logger.info("Released lock for " + uuid + " (never joined)");
                        }
                    } catch (SQLException e) {
                        logger.log(Level.WARNING, "Failed to release lock for " + uuid, e);
                    } finally {
                        pendingSaveCount.decrementAndGet();
                    }
                });
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // Executor shut down — release lock synchronously
                pendingSaveCount.decrementAndGet();
                try {
                    if (ft != null && ft > 0) {
                        databaseManager.releaseLock(uuid, config.getServerName(), ft);
                    } else {
                        databaseManager.releaseLock(uuid, config.getServerName());
                    }
                    notifyLockReleased(uuid);
                } catch (SQLException ex) {
                    logger.log(Level.WARNING, "Failed to release lock for " + uuid + " (sync fallback)", ex);
                }
            }
            return;
        }

        // IMPORTANT: collect data BEFORE removing from maps.
        // collectPlayerData() reads version and fencing token from playerVersions
        // and playerFencingTokens. If we remove first, the save will use default
        // version=0 and fencingToken=0, causing saveData() to fail or overwrite.
        PlayerData data = collectPlayerData(player);

        // Now safe to remove from active tracking.
        // playerVersions and playerFencingTokens are NOT removed here — they
        // are cleaned up after the async save completes (in the finally block),
        // because the per-UUID lock check in periodic save still references them.
        activePlayers.remove(uuid);

        // Check quarantine — if heartbeat detected lock loss, skip normal save.
        if (quarantinedPlayers.remove(uuid)) {
            logger.warning("[Quit] Player " + uuid + " was quarantined (lock lost during session)."
                + " Skipping final save — data may be stale. Player should have been kicked.");
            Long ft = playerFencingTokens.remove(uuid);
            playerVersions.remove(uuid);
            releaseLockAsyncBestEffort(uuid, ft, "quarantined player quit");
            return;
        }

        // Save asynchronously using dedicated thread pool.
        // Per-UUID lock ensures this save runs AFTER any in-flight periodic save,
        // preventing version conflicts from concurrent saves for the same player.
        pendingSaveCount.incrementAndGet();
        java.util.concurrent.locks.ReentrantLock saveLock =
            playerSaveLocks.computeIfAbsent(uuid, k -> new java.util.concurrent.locks.ReentrantLock());
        try {
            asyncExecutor.execute(() -> {
                saveLock.lock(); // must wait — quit save must persist final state
                try {
                    // CRITICAL: refresh version/fencingToken after acquiring the lock.
                    // A periodic/death/world_save save may have completed while we
                    // waited, advancing the DB version. Without this refresh, the
                    // QUIT save would use the stale version collected before the
                    // lock was acquired, fail the CAS, and release the lock
                    // without saving — losing the player's final state.
                    refreshVersionAndFencingToken(uuid, data);
                    persistCollectedData(uuid, data, SaveKind.QUIT);
                } finally {
                    saveLock.unlock();
                    // Do NOT remove lock from map — prevents lock-object split if
                    // another thread is waiting on the same lock instance.
                    // Locks are cleaned up lazily by cleanupStaleEntries().
                    playerVersions.remove(uuid);
                    playerFencingTokens.remove(uuid);
                    pendingSaveCount.decrementAndGet();
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Queue full — quit save MUST NOT be lost.
            // Fallback: run synchronously on the current thread (PlayerQuitEvent
            // is on main/region thread, but this is better than losing data).
            logger.log(Level.SEVERE, "Async executor rejected quit save for " + uuid
                + " — running synchronously as fallback", e);
            saveLock.lock();
            try {
                refreshVersionAndFencingToken(uuid, data);
                persistCollectedData(uuid, data, SaveKind.QUIT);
            } finally {
                saveLock.unlock();
                playerVersions.remove(uuid);
                playerFencingTokens.remove(uuid);
                pendingSaveCount.decrementAndGet();
            }
        }
    }

    /**
     * Collect and save player data synchronously (fallback for retired entities).
     *
     * <p>This is called from the {@code retired} callback of
     * {@link SchedulerUtil#runAtEntity} when the entity scheduler can no longer
     * run tasks on the player's region thread (typically during Folia shutdown).
     * It performs a best-effort synchronous collection and save on the calling
     * thread, bypassing the async executor. This is a last-resort mechanism to
     * prevent silent data loss when the normal async save path is unavailable.
     *
     * <p><b>Thread safety note:</b> This method may be called from threads where
     * the Bukkit API is not fully safe. On Paper/Spigot during PlayerQuitEvent,
     * the main thread is acceptable for reading player state. On Folia during
     * shutdown, region threads are being torn down, so reads may fail with
     * exceptions — those are caught and logged, and a lock release is still
     * attempted to avoid stranding locks.
     */
    public void collectAndSavePlayerDataSync(Player player) {
        UUID uuid = player.getUniqueId();

        // Check if player has pending data (was kicked during pre-login, never joined)
        if (pendingData.containsKey(uuid) || pendingEmptyData.contains(uuid)) {
        pendingData.remove(uuid);
        pendingEmptyData.remove(uuid);
        pendingLoadTimes.remove(uuid);
        activePlayers.remove(uuid);
        Long ft = playerFencingTokens.remove(uuid);
        if (dirtyMask != null) {
            dirtyMask.remove(uuid);
        }
        playerVersions.remove(uuid);
        try {
            if (ft != null && ft > 0) {
                databaseManager.releaseLock(uuid, config.getServerName(), ft);
            } else {
                databaseManager.releaseLock(uuid, config.getServerName());
            }
            notifyLockReleased(uuid);
            logger.info("Released lock for " + uuid + " (never joined, sync fallback)");
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Failed to release lock for " + uuid, e);
        }
        return;
    }

        // Best-effort data collection
        PlayerData data;
        try {
            data = collectPlayerData(player);
        } catch (Exception e) {
            // Collection failed — DO NOT release the lock!
            // Releasing the lock would allow another server to load stale data,
            // silently losing the player's final state. Instead, leave the lock
            // held so the player cannot log in elsewhere until lock-timeout
            // expires. This gives operators a window to investigate and recover.
            // The DB still contains the last successfully saved version (from
            // periodic save or initial load), so no data is lost — it's just
            // not the very latest state.
            logger.log(Level.SEVERE, "Failed to collect data for " + uuid
                + " in sync fallback — NOT releasing lock. The lock will expire"
                + " after " + config.getLockTimeout() + "s. Manual recovery may be needed."
                + " The DB still contains the last saved version.", e);
            activePlayers.remove(uuid);
            // Do NOT remove playerVersions/playerFencingTokens — they identify
            // which lock we hold. The heartbeat task will skip this player
            // (no longer in activePlayers), and the lock will expire naturally.
            return;
        }

        activePlayers.remove(uuid);

        // Check quarantine — skip save if lock was lost
        if (quarantinedPlayers.remove(uuid)) {
            logger.warning("[Quit-Sync] Player " + uuid + " was quarantined. Skipping save.");
            Long ft = playerFencingTokens.remove(uuid);
            if (dirtyMask != null) {
                dirtyMask.remove(uuid);
            }
            playerVersions.remove(uuid);
            releaseLockAsyncBestEffort(uuid, ft, "quarantined player quit (sync)");
            return;
        }

        // Save synchronously with per-UUID lock to avoid races with any in-flight save
        java.util.concurrent.locks.ReentrantLock saveLock =
            playerSaveLocks.computeIfAbsent(uuid, k -> new java.util.concurrent.locks.ReentrantLock());
        saveLock.lock();
        try {
            // Refresh version/fencingToken after acquiring lock — same rationale
            // as the async QUIT path: an in-flight save may have advanced the version.
            refreshVersionAndFencingToken(uuid, data);
            persistCollectedData(uuid, data, SaveKind.QUIT);
        } finally {
            saveLock.unlock();
            playerVersions.remove(uuid);
            playerFencingTokens.remove(uuid);
        }
    }

    /**
     * Notify other servers via Redis that a lock has been released.
     * No-op if Redis is not enabled.
     */
    private void notifyLockReleased(UUID uuid) {
        if (redissonManager != null && redissonManager.isHealthy()) {
            redissonManager.notifyLockReleased(uuid);
        }
    }

    /**
     * Release a player's DB lock asynchronously on the async executor.
     * Used by quarantine and failed-join paths where we must NOT do JDBC on
     * the entity/region thread. Falls back to synchronous execution only when
     * the executor is shut down (acceptable during server shutdown).
     *
     * @param uuid         player UUID
     * @param fencingToken fencing token from playerFencingTokens (may be null)
     * @param reason       log label for diagnostics
     */
    private void releaseLockAsyncBestEffort(UUID uuid, Long fencingToken, String reason) {
        pendingSaveCount.incrementAndGet();
        Runnable task = () -> {
            try {
                if (fencingToken != null && fencingToken > 0) {
                    databaseManager.releaseLock(uuid, config.getServerName(), fencingToken);
                } else {
                    logger.warning("[LockRelease] " + reason + " for " + uuid
                        + " has no fencing token; refusing unsafe tokenless release.");
                    return;
                }
                notifyLockReleased(uuid);
                if (config.isDebug()) {
                    logger.info("[LockRelease] Released lock for " + uuid + " (" + reason + ")");
                }
            } catch (SQLException e) {
                logger.log(Level.WARNING, "[LockRelease] Failed to release lock for " + uuid
                    + " (" + reason + ")", e);
            } finally {
                pendingSaveCount.decrementAndGet();
            }
        };
        try {
            if (asyncExecutor != null) {
                asyncExecutor.execute(task);
            } else {
                task.run();
            }
        } catch (java.util.concurrent.RejectedExecutionException e) {
            logger.log(Level.WARNING, "[LockRelease] Async executor rejected lock release for " + uuid
                + " (" + reason + "); running fallback synchronously.", e);
            task.run();
        }
    }

    /**
     * Collect player data from the player's current state.
     * Full HuskSync feature parity - collects all synchronizable data.
     */
    private PlayerData collectPlayerData(Player player) {
        PlayerData data = new PlayerData();

        // Inherit version from the loaded data for optimistic concurrency
        Long version = playerVersions.get(player.getUniqueId());
        if (version != null) {
            data.setVersion(version);
        }
        // Inherit fencing token from lock acquisition (Kleppmann stale-write defence)
        Long fencingToken = playerFencingTokens.get(player.getUniqueId());
        if (fencingToken != null) {
            data.setFencingToken(fencingToken);
        }

        // Inventory - normalize empty/AIR slots to null so the serializer can treat
        // them uniformly and avoid storing meaningless AIR ItemStacks (sparse storage).
        // All basic fields are now gated by config checks — disabling sync items
        // genuinely reduces serialization cost, NBT size, and DB write size.
        if (config.isSyncInventory()) {
            data.setInventory(sparseContents(player.getInventory().getContents()));
            data.setArmor(sparseContents(player.getInventory().getArmorContents()));
            org.bukkit.inventory.ItemStack offhand = player.getInventory().getItemInOffHand();
            data.setOffhand(offhand != null && offhand.getType() == org.bukkit.Material.AIR ? null : offhand);
        }

        // Ender chest
        if (config.isSyncEnderChest()) {
            data.setEnderChest(sparseContents(player.getEnderChest().getContents()));
        }

        // Vitals
        if (config.isSyncHealth()) {
            data.setHealth(player.getHealth());
            data.setMaxHealth(player.getMaxHealth());
        }
        if (config.isSyncFood()) {
            data.setFoodLevel(player.getFoodLevel());
            data.setSaturation(player.getSaturation());
            data.setExhaustion(player.getExhaustion());
        }

        // Experience
        if (config.isSyncExperience()) {
            data.setExpLevel(player.getLevel());
            data.setExpProgress(player.getExp());
            data.setTotalExperience(player.getTotalExperience());
        }

        // Potion effects
        if (config.isSyncPotionEffects()) {
            List<PlayerData.PotionEffectData> effects = new ArrayList<>();
            for (PotionEffect effect : player.getActivePotionEffects()) {
                effects.add(PlayerDataSerializer.toPotionEffectData(effect));
            }
            data.setPotionEffects(effects);
        }

        // Extra
        if (config.isSyncGameMode()) {
            data.setGameMode(player.getGameMode());
        }
        if (config.isSyncFireTicks()) {
            data.setFireTicks(player.getFireTicks());
        }
        if (config.isSyncAir()) {
            data.setRemainingAir(player.getRemainingAir());
            data.setMaximumAir(player.getMaximumAir());
        }

        // Flight status
        if (config.isSyncFlight()) {
            data.setFlying(player.isFlying());
            data.setAllowFlight(player.getAllowFlight());
        }

        // Advancements (using Bukkit API - iterates all advancement criteria)
        if (config.isSyncAdvancements()) {
            collectAdvancements(player, data);
        }

        // Statistics (basic UNTYPED stats always synced; typed stats via strategy)
        if (config.isSyncStatistics()) {
            collectStatistics(player, data);
        }

        // Attributes
        if (config.isSyncAttributes()) {
            collectAttributes(player, data);
        }

        // Persistent Data Container (via strategy)
        if (pdcStrategy != null && !"off".equals(pdcStrategy.strategyName())) {
            byte[] pdcBytes = pdcStrategy.dump(player);
            if (pdcBytes != null) {
                // Even if pdcBytes.length == 0 (empty PDC), store it so
                // restore() is called on the target server to clear ghost keys.
                Map<String, byte[]> pdcMap = new HashMap<>();
                pdcMap.put("__pdc_bytes__", pdcBytes);
                data.setPersistentDataContainer(pdcMap);
            } else {
                data.setPersistentDataContainer(new HashMap<>());
            }
        }

        // Location (optional)
        if (config.isSyncLocation()) {
            Location loc = player.getLocation();
            data.setWorldName(loc.getWorld() != null ? loc.getWorld().getName() : "world");
            data.setX(loc.getX());
            data.setY(loc.getY());
            data.setZ(loc.getZ());
            data.setYaw(loc.getYaw());
            data.setPitch(loc.getPitch());
        }

        data.setTimestamp(System.currentTimeMillis());

        return data;
    }

    // ==================== Data Collection Helpers ====================

    /**
     * Lazily populate the cached Bukkit registry snapshots.
     *
     * <p>These registries are immutable for the lifetime of a server process,
     * so we compute them once and reuse across all subsequent save/load cycles.
     * This avoids rebuilding ~1300-entry Material arrays and re-checking
     * isItem()/isBlock() on every player save.
     */
    private void ensureRegistryCache() {
        if (untypedStats != null) return; // already cached
        List<Statistic> unt = new ArrayList<>(), it = new ArrayList<>(),
                        blk = new ArrayList<>(), ent = new ArrayList<>();
        for (Statistic s : Statistic.values()) {
            switch (s.getType()) {
                case UNTYPED -> unt.add(s);
                case ITEM -> it.add(s);
                case BLOCK -> blk.add(s);
                case ENTITY -> ent.add(s);
            }
        }
        this.untypedStats = List.copyOf(unt);
        this.itemStats = List.copyOf(it);
        this.blockStats = List.copyOf(blk);
        this.entityStats = List.copyOf(ent);

        List<org.bukkit.Material> items = new ArrayList<>();
        List<org.bukkit.Material> blocks = new ArrayList<>();
        for (org.bukkit.Material mat : org.bukkit.Material.values()) {
            if (mat.isItem()) items.add(mat);
            if (mat.isBlock()) blocks.add(mat);
        }
        this.itemMaterials = List.copyOf(items);
        this.blockMaterials = List.copyOf(blocks);

        List<org.bukkit.entity.EntityType> alive = new ArrayList<>();
        for (org.bukkit.entity.EntityType e : org.bukkit.entity.EntityType.values()) {
            if (e.isAlive()) alive.add(e);
        }
        this.aliveEntities = List.copyOf(alive);
    }

    @SuppressWarnings("deprecation")
    private void collectAdvancements(Player player, PlayerData data) {
        try {
            Map<String, Map<String, Long>> advancements = new HashMap<>();
            // The set of registered advancements is immutable for the server lifetime,
            // so cache it after the first collection instead of re-iterating every save.
            if (cachedAdvancements == null) {
                List<org.bukkit.advancement.Advancement> list = new ArrayList<>();
                java.util.Iterator<org.bukkit.advancement.Advancement> it = Bukkit.advancementIterator();
                while (it.hasNext()) list.add(it.next());
                cachedAdvancements = list.toArray(new org.bukkit.advancement.Advancement[0]);
            }
            for (org.bukkit.advancement.Advancement adv : cachedAdvancements) {
                org.bukkit.advancement.AdvancementProgress progress = player.getAdvancementProgress(adv);
                if (progress == null) continue;

                String key = adv.getKey().toString();
                Map<String, Long> criteria = new HashMap<>();
                for (String awarded : progress.getAwardedCriteria()) {
                    // Note: Bukkit doesn't expose per-criteria award timestamps; using current
                    // time as approximation. For precise timing, NMS-level hooks would be needed.
                    criteria.put(awarded, System.currentTimeMillis());
                }
                if (!criteria.isEmpty()) {
                    advancements.put(key, criteria);
                }
            }
            data.setAdvancements(advancements);
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect advancements: " + e.getMessage());
            }
        }
    }

    private void collectStatistics(Player player, PlayerData data) {
        ensureRegistryCache();
        try {
            Map<String, Map<String, Integer>> statistics = new HashMap<>();

            // UNTYPED statistics (always synced — cheap, server-agnostic)
            for (Statistic stat : untypedStats) {
                try {
                    int value = player.getStatistic(stat);
                    statistics.computeIfAbsent("UNTYPED", k -> new HashMap<>()).put(stat.name(), value);
                } catch (Exception ignored) {}
            }

            // Typed statistics (ITEM/BLOCK/ENTITY) — via strategy if enabled
            if (typedStatsStrategy != null) {
                Map<String, Map<String, Integer>> typed = typedStatsStrategy.dump(player);
                if (typed != null && !typed.isEmpty()) {
                    statistics.putAll(typed);
                }
            }

            data.setStatistics(statistics);
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect statistics: " + e.getMessage());
            }
        }
    }

    private void collectAttributes(Player player, PlayerData data) {
        try {
            List<PlayerData.AttributeData> attributes = new ArrayList<>();
            // Attribute.values() is immutable for the server lifetime; cache it to
            // avoid rebuilding the array on every save.
            if (cachedAttributes == null) {
                cachedAttributes = Attribute.values();
            }
            for (Attribute attr : cachedAttributes) {
                try {
                    AttributeInstance instance = player.getAttribute(attr);
                    if (instance == null) continue;

                    String key = attr.getKey().toString();
                    double baseValue = instance.getBaseValue();
                    List<PlayerData.ModifierData> modifiers = new ArrayList<>();

                    for (AttributeModifier mod : instance.getModifiers()) {
                        modifiers.add(new PlayerData.ModifierData(
                            mod.getUniqueId().toString(),
                            mod.getName(),
                            mod.getAmount(),
                            mod.getOperation().name(),
                            null
                        ));
                    }

                    attributes.add(new PlayerData.AttributeData(key, baseValue, modifiers));
                } catch (Exception ignored) {
                    // Some attributes may not exist on all versions
                }
            }
            data.setAttributes(attributes);
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect attributes: " + e.getMessage());
            }
        }
    }

    // ==================== Data Apply Helpers ====================

    @SuppressWarnings("deprecation")
    private void applyAdvancements(Player player, PlayerData data) {
        // NOTE: The Bukkit API's awardCriteria() does not accept a timestamp
        // parameter, so the saved criterion timestamps (Map<String, Long>) are
        // not restored — they are preserved for audit/diagnostic purposes only.
        // The current server time is implicitly used by Bukkit when awarding.
        try {
            // Use the cached advancement list (populated by collectAdvancements)
            // instead of calling Bukkit.advancementIterator() again on every join.
            if (cachedAdvancements == null) {
                java.util.List<org.bukkit.advancement.Advancement> list = new ArrayList<>();
                java.util.Iterator<org.bukkit.advancement.Advancement> it = Bukkit.advancementIterator();
                while (it.hasNext()) list.add(it.next());
                cachedAdvancements = list.toArray(new org.bukkit.advancement.Advancement[0]);
            }
            for (org.bukkit.advancement.Advancement adv : cachedAdvancements) {
                String key = adv.getKey().toString();
                Map<String, Long> criteria = data.getAdvancements().get(key);
                if (criteria == null) continue;

                org.bukkit.advancement.AdvancementProgress progress = player.getAdvancementProgress(adv);
                if (progress == null) continue;

                // Award criteria that are saved but not yet awarded
                for (String criterion : criteria.keySet()) {
                    if (!progress.getAwardedCriteria().contains(criterion)) {
                        try {
                            progress.awardCriteria(criterion);
                        } catch (Exception ignored) {}
                    }
                }
            }
        } catch (Exception e) {
            if (config.isDebug()) logger.warning("Failed to apply advancements: " + e.getMessage());
        }
    }

    private void applyStatistics(Player player, PlayerData data) {
        try {
            // Separate typed (ITEM_/BLOCK_/ENTITY_) from untyped stats
            Map<String, Map<String, Integer>> untypedStats = new HashMap<>();
            Map<String, Map<String, Integer>> typedStatsData = new HashMap<>();

            for (Map.Entry<String, Map<String, Integer>> cat : data.getStatistics().entrySet()) {
                String category = cat.getKey();
                if (category.startsWith("ITEM_") || category.startsWith("BLOCK_") || category.startsWith("ENTITY_")) {
                    typedStatsData.put(category, cat.getValue());
                } else {
                    untypedStats.put(category, cat.getValue());
                }
            }

            // Apply typed stats via strategy (if enabled) or inline
            if (!typedStatsData.isEmpty()) {
                if (typedStatsStrategy != null) {
                    typedStatsStrategy.restore(player, typedStatsData);
                } else {
                    // Typed stats present but strategy not enabled — apply inline (backward compat)
                    applyTypedStatsInline(player, typedStatsData);
                }
            }

            // Apply untyped stats
            for (Map.Entry<String, Map<String, Integer>> cat : untypedStats.entrySet()) {
                for (Map.Entry<String, Integer> stat : cat.getValue().entrySet()) {
                    try {
                        Statistic statistic = Statistic.valueOf(stat.getKey());
                        if (statistic.getType() == Statistic.Type.UNTYPED) {
                            player.setStatistic(statistic, stat.getValue());
                        }
                    } catch (Exception ignored) {}
                }
            }
        } catch (Exception e) {
            if (config.isDebug()) logger.warning("Failed to apply statistics: " + e.getMessage());
        }
    }

    /** Inline typed stats restore for backward compat (when no strategy is active). */
    private void applyTypedStatsInline(Player player, Map<String, Map<String, Integer>> typedStatsData) {
        for (Map.Entry<String, Map<String, Integer>> cat : typedStatsData.entrySet()) {
            String category = cat.getKey();
            try {
                if (category.startsWith("ITEM_") || category.startsWith("BLOCK_")) {
                    String prefix = category.startsWith("ITEM_") ? "ITEM_" : "BLOCK_";
                    Statistic statistic = Statistic.valueOf(category.substring(prefix.length()));
                    for (Map.Entry<String, Integer> stat : cat.getValue().entrySet()) {
                        try {
                            org.bukkit.Material mat = org.bukkit.Material.matchMaterial(stat.getKey());
                            if (mat != null) player.setStatistic(statistic, mat, stat.getValue());
                        } catch (Exception ignored) {}
                    }
                } else if (category.startsWith("ENTITY_")) {
                    Statistic statistic = Statistic.valueOf(category.substring("ENTITY_".length()));
                    for (Map.Entry<String, Integer> stat : cat.getValue().entrySet()) {
                        try {
                            org.bukkit.entity.EntityType ent = org.bukkit.entity.EntityType.valueOf(stat.getKey());
                            player.setStatistic(statistic, ent, stat.getValue());
                        } catch (Exception ignored) {}
                    }
                }
            } catch (Exception ignored) {}
        }
    }

    private void applyAttributes(Player player, PlayerData data) {
        try {
            for (PlayerData.AttributeData attrData : data.getAttributes()) {
                try {
                    NamespacedKey key = NamespacedKey.fromString(attrData.getAttributeKey());
                    if (key == null) continue;
                    Attribute attr = org.bukkit.Registry.ATTRIBUTE.get(key);
                    if (attr == null) continue;

                    AttributeInstance instance = player.getAttribute(attr);
                    if (instance == null) continue;

                    instance.setBaseValue(attrData.getBaseValue());

                    // Clear existing modifiers and reapply
                    for (AttributeModifier existing : new ArrayList<>(instance.getModifiers())) {
                        instance.removeModifier(existing);
                    }

                    if (attrData.getModifiers() != null) {
                        for (PlayerData.ModifierData modData : attrData.getModifiers()) {
                            try {
                                AttributeModifier modifier = new AttributeModifier(
                                    java.util.UUID.fromString(modData.getUuid()),
                                    modData.getName(),
                                    modData.getAmount(),
                                    AttributeModifier.Operation.valueOf(modData.getOperation())
                                );
                                instance.addModifier(modifier);
                            } catch (Exception ignored) {}
                        }
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            if (config.isDebug()) logger.warning("Failed to apply attributes: " + e.getMessage());
        }
    }

    // ==================== Periodic Save ====================

    /**
     * Save all online players' data synchronously (for shutdown / /fastsync saveall).
     *
     * <p><b>Folia-safe:</b> Data collection is dispatched per-player via
     * {@link SchedulerUtil#runAtEntity} to ensure it runs on the correct
     * region thread. Serialization + DB write happen async, and we wait
     * for all futures to complete (with a global deadline) before returning.
     *
     * <p><b>Dynamic deadline:</b> For SHUTDOWN saves, the deadline scales
     * with the number of online players: max(30s, online * 500ms) so that
     * large servers are not cut off prematurely. For BULK saves (/saveall),
     * the deadline is fixed at 30s since this is an operator command.
     *
     * <p><b>Sync fallback:</b> For SHUTDOWN saves, if the async queue is
     * full (RejectedExecutionException), the save runs synchronously on
     * the calling thread instead of failing — shutdown must persist data.
     *
     * @param kind BULK for /saveall (keep lock), SHUTDOWN for onDisable (release lock)
     * @return result with total/success/failed counts and per-UUID failure reasons
     */
    /**
     * Save all online players' data synchronously (for shutdown).
     * This method calls Bukkit.getOnlinePlayers() and MUST be called on the
     * main thread or global region thread.
     *
     * @see #savePlayersSnapshot(List, SaveKind) for the thread-safe variant
     *       that accepts a pre-collected player list.
     */
    public SaveAllResult saveAllOnlinePlayers(SaveKind kind) {
        return savePlayersSnapshot(new ArrayList<>(Bukkit.getOnlinePlayers()), kind);
    }

    /**
     * Save a snapshot of players' data. The player list must have been
     * collected on the main thread or global region thread (Folia-safe).
     *
     * <p>This method is safe to call from an async thread because it does NOT
     * call Bukkit.getOnlinePlayers() — it uses the pre-collected list and
     * dispatches per-player data collection via SchedulerUtil.runAtEntity().
     *
     * @param players pre-collected player list (from main/global thread)
     * @param kind    BULK for /saveall (keep lock), SHUTDOWN for onDisable (release lock)
     * @return result with total/success/failed counts
     */
    public SaveAllResult savePlayersSnapshot(List<Player> players, SaveKind kind) {
        int total = 0;
        int success = 0;
        int failed = 0;
        Map<UUID, String> failures = new HashMap<>();
        List<Map.Entry<UUID, CompletableFuture<SaveResult>>> futures = new ArrayList<>();
        Plugin plugin = JavaPlugin.getPlugin(FastSync.class);

        for (Player player : players) {
            if (!activePlayers.containsKey(player.getUniqueId())) {
                continue;
            }
            total++;
            UUID uuid = player.getUniqueId();

            CompletableFuture<SaveResult> future = new CompletableFuture<>();
            SchedulerUtil.runAtEntity(plugin, player, () -> {
                try {
                    PlayerData data = collectPlayerData(player);
                    pendingSaveCount.incrementAndGet();
                    java.util.concurrent.locks.ReentrantLock saveLock =
                        playerSaveLocks.computeIfAbsent(uuid, k -> new java.util.concurrent.locks.ReentrantLock());
                    try {
                        asyncExecutor.execute(() -> {
                            SaveResult result;
                            try {
                                saveLock.lock();
                                try {
                                    // Refresh version/fencingToken after acquiring lock —
                                    // an in-flight save may have advanced the version
                                    // while we waited for the lock.
                                    refreshVersionAndFencingToken(uuid, data);
                                    result = persistCollectedData(uuid, data, kind);
                                } finally {
                                    saveLock.unlock();
                                }
                            } catch (Exception e) {
                                result = SaveResult.error(e.getMessage());
                            } finally {
                                pendingSaveCount.decrementAndGet();
                            }
                            future.complete(result);
                        });
                    } catch (java.util.concurrent.RejectedExecutionException e) {
                        pendingSaveCount.decrementAndGet();
                        if (kind == SaveKind.SHUTDOWN) {
                            // SHUTDOWN: synchronous fallback — must persist data.
                            // The async queue is full, but shutdown cannot skip saves.
                            logger.warning("[Shutdown] Async queue full for " + uuid
                                + " — running synchronously as fallback");
                            SaveResult result;
                            try {
                                saveLock.lock();
                                try {
                                    refreshVersionAndFencingToken(uuid, data);
                                    result = persistCollectedData(uuid, data, kind);
                                } finally {
                                    saveLock.unlock();
                                }
                            } catch (Exception ex) {
                                result = SaveResult.error(ex.getMessage());
                            }
                            future.complete(result);
                        } else {
                            // BULK (/saveall): queue full is acceptable — skip.
                            logger.log(Level.WARNING, "Async queue full during " + kind + " save for " + uuid, e);
                            future.complete(SaveResult.error("async queue full"));
                        }
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to collect data for " + uuid + " during " + kind + " save", e);
                    future.complete(SaveResult.error(e.getMessage()));
                }
            }, () -> {
                future.complete(SaveResult.error("entity retired"));
            });
            futures.add(Map.entry(uuid, future));
        }

        // Wait for all DB saves with a dynamic deadline.
        // SHUTDOWN: max(30s, online * 500ms) — scales for large servers.
        // BULK: fixed 30s — operator command, can retry.
        int onlineCount = futures.size();
        long deadlineMs = (kind == SaveKind.SHUTDOWN)
            ? Math.max(30_000L, onlineCount * 500L)
            : 30_000L;
        long deadlineStart = System.currentTimeMillis();
        for (Map.Entry<UUID, CompletableFuture<SaveResult>> entry : futures) {
            UUID uuid = entry.getKey();
            CompletableFuture<SaveResult> f = entry.getValue();
            long remaining = deadlineMs - (System.currentTimeMillis() - deadlineStart);
            if (remaining <= 0) {
                failed++;
                failures.put(uuid, "global deadline exceeded");
                continue;
            }
            try {
                SaveResult result = f.get(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (result.success()) {
                    success++;
                } else {
                    failed++;
                    failures.put(uuid, result.errorMessage() != null ? result.errorMessage() : "unknown error");
                }
            } catch (java.util.concurrent.TimeoutException e) {
                logger.warning(kind + ": global deadline exceeded waiting for " + uuid);
                failed++;
                failures.put(uuid, "timeout");
            } catch (Exception e) {
                failed++;
                failures.put(uuid, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            }
        }

        if (failed > 0) {
            logger.warning(kind + ": " + success + "/" + total + " succeeded, " + failed + " failed.");
        } else {
            logger.info(kind + ": saved data for all " + total + " online players.");
        }

        return new SaveAllResult(total, success, failed, failures);
    }

    /** Backward-compatible overload: defaults to BULK (keep lock, for /saveall command). */
    public SaveAllResult saveAllOnlinePlayers() {
        return saveAllOnlinePlayers(SaveKind.BULK);
    }

    /**
     * Save a single player's data asynchronously (for periodic/death/world_save saves).
     *
     * <p><b>Folia compatibility:</b> On Folia, {@code collectPlayerData} must run
     * on the entity's region thread, not the global region thread. We dispatch
     * the data collection via {@link SchedulerUtil#runAtEntity}, then perform
     * the async DB save from the collected data.
     *
     * @param player the player to save
     * @param kind   the save kind (PERIODIC, DEATH, WORLD_SAVE — all online/keep-lock)
     */
    public void savePlayerAsync(Player player, SaveKind kind) {
        if (!activePlayers.containsKey(player.getUniqueId())) {
            return;
        }
        // Only online-save kinds are valid here; QUIT is handled by collectAndSavePlayerData
        if (kind.releaseLock) {
            logger.warning("savePlayerAsync called with release-lock kind " + kind + " — use collectAndSavePlayerData for quit saves");
            return;
        }

        UUID uuid = player.getUniqueId();
        Plugin plugin = JavaPlugin.getPlugin(FastSync.class);
        final SaveKind finalKind = kind;

        // Dirty-tracking fast path: if no component is dirty AND this is a
        // periodic save (not death/world_save which must always save), skip
        // the entire collect + serialize + DB write cycle. The lock heartbeat
        // keeps the lock alive independently.
        //
        // Validation safety: every Nth save (N = validationInterval) we still
        // do a full collect + checksum to catch any changes that slipped
        // through events. recordSaveAndCheckValidation() returns true when
        // validation is due.
        if (dirtyMask != null && finalKind == SaveKind.PERIODIC) {
            boolean shouldValidate = dirtyMask.recordSaveAndCheckValidation(uuid);
            if (!shouldValidate && !dirtyMask.isAnyDirty(uuid)) {
                if (config.isDebug()) {
                    logger.fine("Skipping periodic save for " + uuid + " — no dirty components");
                }
                return;
            }
            if (shouldValidate) {
                // Force full collect + dirty-mark everything to ensure checksum
                // comparison happens in persistCollectedData.
                dirtyMask.markAllDirty(uuid);
            }
        }

        // Collect player data on the entity's region thread (Folia-safe)
        SchedulerUtil.runAtEntity(plugin, player, () -> {
            PlayerData data = collectPlayerData(player);

            pendingSaveCount.incrementAndGet();
            java.util.concurrent.locks.ReentrantLock saveLock =
                playerSaveLocks.computeIfAbsent(uuid, k -> new java.util.concurrent.locks.ReentrantLock());
            try {
                asyncExecutor.execute(() -> {
                    // Online save: skip if a save is already in progress for this player.
                    // The next periodic tick will pick up the latest data — coalescing
                    // avoids unnecessary version conflicts and saves CPU/DB load.
                    if (!saveLock.tryLock()) {
                        pendingSaveCount.decrementAndGet();
                        if (config.isDebug()) {
                            logger.fine("Skipping " + finalKind + " save for " + uuid + " — save already in progress");
                        }
                        return;
                    }
                    try {
                        // CRITICAL: refresh version/fencingToken after acquiring the lock.
                        // Without this, if a previous periodic/death save completed first
                        // and advanced the DB version, this save will CAS-fail with a stale
                        // version and be treated as a serious lock-infringement conflict.
                        refreshVersionAndFencingToken(uuid, data);
                        persistCollectedData(uuid, data, finalKind);
                    } catch (Exception e) {
                        logger.log(Level.WARNING, finalKind + " save failed for " + uuid, e);
                    } finally {
                        saveLock.unlock();
                        pendingSaveCount.decrementAndGet();
                    }
            });
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // Queue full — online save can be skipped (coalesced to next tick).
                // The player is still online; quit save will persist final state.
                pendingSaveCount.decrementAndGet();
                if (config.isDebug()) {
                    logger.fine("Skipping " + finalKind + " save for " + uuid + " — async queue full");
                }
            }
        }, () -> {
            // retired callback: entity no longer valid (player logged out during save tick)
            if (config.isDebug()) {
                logger.fine(finalKind + " save skipped for " + uuid + " — entity retired (player offline?)");
            }
        });
    }

    /**
     * Save a single player's data asynchronously (periodic save default).
     */
    public void savePlayerAsync(Player player) {
        savePlayerAsync(player, SaveKind.PERIODIC);
    }

    // ==================== Cleanup ====================

    /**
     * Clean up stale pending data entries (players who connected but never joined).
     *
     * <p><b>Spanner note:</b> This uses wall-clock timestamps for a <b>liveness</b>
     * check (timeout-based cleanup), which is acceptable. Spanner's lesson is that
     * wall-clock must not be used for <b>safety</b> decisions (e.g., "which data
     * is newer"). Here we're only deciding "is this entry old enough to clean up?"
     * — a timeout, not an ordering. Clock skew may cause cleanup to happen slightly
     * early or late, but cannot cause data corruption.
     */
    /**
     * Heartbeat: refresh locked_at for all online players whose locks we hold.
     *
     * <p>This runs on a separate timer from periodic saves and is the PRIMARY
     * mechanism for keeping online locks alive. Periodic saves also refresh
     * locked_at as a side effect, but relying solely on saves is unsafe when
     * periodic-save is disabled (default) or the interval exceeds lock-timeout.
     *
     * <p><b>Anti-reentry:</b> Uses {@link AtomicBoolean} to prevent overlapping
     * heartbeat cycles. If the previous cycle is still running (DB latency),
     * the current tick is skipped with a warning.
     *
     * <p><b>Batch refresh:</b> Uses a single JDBC batch UPDATE to refresh all
     * players' locked_at in one connection, reducing DB pressure from O(N)
     * connections to O(1) per heartbeat cycle. Players whose refresh fails
     * (lock no longer ours) are quarantined and kicked.
     *
     * <p>If refreshLock returns false for a player, it means our lock was
     * already taken over by another server (stale expiry or infringement).
     * This is a SEVERE condition — the player is quarantined and kicked to
     * prevent data corruption from continued play with an invalid lock.
     */
    public void heartbeatOnlinePlayers() {
        if (!heartbeatRunning.compareAndSet(false, true)) {
            logger.warning("[Heartbeat] Previous heartbeat cycle still running; skipping this tick.");
            return;
        }
        try {
            // Protection mode check: if we've had too many consecutive DB
            // failures, kick all active players to prevent stale-lock play.
            if (protectionMode) {
                logger.log(Level.SEVERE, "[Heartbeat] Protection mode active — DB has been unreachable"
                    + " for " + HEARTBEAT_FAILURE_THRESHOLD + "+ heartbeat cycles."
                    + " Kicking all active players to prevent data corruption.");
                kickAllActivePlayers();
                return;
            }

            String serverName = config.getServerName();
            if (activePlayers.isEmpty()) {
                // Reset failure count when no players are online
                heartbeatFailureCount.set(0);
                return;
            }

            // Build a snapshot of (uuid -> fencingToken) for batch refresh
            java.util.Map<UUID, Long> playersToRefresh = new java.util.HashMap<>();
            for (UUID uuid : activePlayers.keySet()) {
                Long fencingToken = playerFencingTokens.get(uuid);
                if (fencingToken != null && !quarantinedPlayers.contains(uuid)) {
                    playersToRefresh.put(uuid, fencingToken);
                }
            }

            if (playersToRefresh.isEmpty()) {
                heartbeatFailureCount.set(0);
                return;
            }

            // Track whether the entire batch failed (DB unreachable)
            boolean batchFailed = false;
            java.util.Set<UUID> failedPlayers = new java.util.HashSet<>();

            try {
                databaseManager.refreshLockBatch(playersToRefresh, serverName, failedPlayers);
            } catch (SQLException e) {
                batchFailed = true;
                logger.log(Level.WARNING, "[Heartbeat] Batch refresh failed; falling back to per-player", e);
                // Fallback: per-player refresh
                for (java.util.Map.Entry<UUID, Long> entry : playersToRefresh.entrySet()) {
                    try {
                        if (!databaseManager.refreshLock(entry.getKey(), serverName, entry.getValue())) {
                            failedPlayers.add(entry.getKey());
                        }
                    } catch (SQLException ex) {
                        logger.log(Level.WARNING, "[Heartbeat] Per-player refresh failed for " + entry.getKey(), ex);
                        failedPlayers.add(entry.getKey());
                    }
                }
            }

            // If the batch failed AND most players failed per-player fallback,
            // the DB is likely unreachable. Increment failure counter.
            if (batchFailed && failedPlayers.size() == playersToRefresh.size()) {
                int failures = heartbeatFailureCount.incrementAndGet();
                logger.log(Level.WARNING, "[Heartbeat] DB unreachable — heartbeat failure count: "
                    + failures + "/" + HEARTBEAT_FAILURE_THRESHOLD);
                if (failures >= HEARTBEAT_FAILURE_THRESHOLD) {
                    protectionMode = true;
                    logger.log(Level.SEVERE, "[Heartbeat] Entering protection mode after "
                        + failures + " consecutive DB failures. All players will be kicked.");
                    kickAllActivePlayers();
                }
                return; // Skip per-player quarantine — it's a DB issue, not a lock issue
            }

            // DB is reachable — reset failure counter
            heartbeatFailureCount.set(0);

            // Handle failed players — quarantine and kick
            for (UUID uuid : failedPlayers) {
                Long fencingToken = playerFencingTokens.get(uuid);
                logger.log(Level.SEVERE, "[Heartbeat] Lock refresh failed for " + uuid
                    + " (ft=" + fencingToken + ") — lock may have been taken by another server!"
                    + " Quarantining player and scheduling kick.");

                // Mark as quarantined — quit handler will see this and skip normal save
                quarantinedPlayers.add(uuid);

                // Remove from active players to stop periodic saves
                activePlayers.remove(uuid);

                // Kick the player — they must reconnect to re-acquire their lock.
                // Do NOT remove playerVersions/playerFencingTokens — keep them for
                // diagnostics and so the quit handler can log the fencing token.
                kickPlayerForLockLoss(uuid);
            }
        } finally {
            heartbeatRunning.set(false);
        }
    }

    /**
     * Kick all active players — used when entering protection mode (DB unreachable
     * for too long, locks may have expired).
     */
    private void kickAllActivePlayers() {
        java.util.Set<UUID> toKick = new java.util.HashSet<>(activePlayers.keySet());
        for (UUID uuid : toKick) {
            quarantinedPlayers.add(uuid);
            activePlayers.remove(uuid);
            kickPlayerForLockLoss(uuid);
        }
    }

    /**
     * Reset protection mode — only succeeds if the database is healthy.
     * Called when the DB recovers or the plugin reloads.
     *
     * @return true if protection mode was reset, false if DB is still unhealthy
     */
    public boolean resetProtectionMode() {
        if (!databaseManager.isHealthy()) {
            logger.warning("[Protection] Cannot reset protection mode: database is not healthy.");
            return false;
        }
        protectionMode = false;
        heartbeatFailureCount.set(0);
        logger.info("[Protection] Protection mode reset.");
        return true;
    }

    /**
     * Kick a player whose lock was lost during heartbeat. Runs on the main
     * thread (or region thread for Folia) to safely interact with the player
     * entity.
     */
    private void kickPlayerForLockLoss(UUID uuid) {
        Plugin plugin = JavaPlugin.getPlugin(FastSync.class);
        SchedulerUtil.runGlobal(plugin, () -> {
            Player player = org.bukkit.Bukkit.getPlayer(uuid);
            if (player != null && player.isOnline()) {
                player.kick(net.kyori.adventure.text.Component.text(
                    "[FastSync] Your data lock was lost. Please reconnect to re-sync your data.",
                    net.kyori.adventure.text.format.NamedTextColor.RED));
            } else {
                if (config.isDebug()) {
                    logger.fine("[Heartbeat] Player " + uuid + " already offline; no kick needed.");
                }
            }
        });
    }

    public void cleanupStaleEntries() {
        long now = System.currentTimeMillis();
        long staleThreshold = 5 * 60 * 1000; // 5 minutes

        pendingLoadTimes.forEach((uuid, loadTime) -> {
        if (loadTime != null && (now - loadTime) > staleThreshold) {
            pendingData.remove(uuid);
            pendingEmptyData.remove(uuid);
            pendingLoadTimes.remove(uuid);
            // Clean up ALL tracking maps to prevent memory leaks during login storms
            playerVersions.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid);
            failedJoinPlayers.remove(uuid);
            quarantinedPlayers.remove(uuid);
            asyncExecutor.execute(() -> {
                try {
                    if (ft != null && ft > 0) {
                        boolean released = databaseManager.releaseLock(uuid, config.getServerName(), ft);
                        if (released) {
                            notifyLockReleased(uuid);
                        }
                    } else {
                        logger.warning("Stale pending data for " + uuid
                            + " has no fencing token; not releasing lock or notifying release.");
                    }
                    logger.warning("Cleaned up stale pending data for " + uuid);
                } catch (SQLException e) {
                    logger.log(Level.WARNING, "Failed to release stale lock for " + uuid, e);
                }
            });
        }
    });

        // Clean up save locks for players no longer active (quit > 5 min ago).
        // This prevents unbounded growth of the playerSaveLocks map.
        // Conservative: only remove locks that are neither held nor have waiting
        // threads, to avoid removing a lock that a concurrent save just obtained.
        playerSaveLocks.entrySet().removeIf(e -> {
            UUID uuid = e.getKey();
            if (activePlayers.containsKey(uuid)
                || pendingData.containsKey(uuid)
                || pendingEmptyData.contains(uuid)) {
                return false;
            }
            java.util.concurrent.locks.ReentrantLock lock = e.getValue();
            return !lock.isLocked() && !lock.hasQueuedThreads();
        });
    }

    // ==================== Shutdown ====================

    /**
     * Shut down the sync manager, closing Redis and thread pool.
     */
    public void shutdown() {
        // Log final latency stats before shutdown
        logLatencyStats();

        // Wait for pending saves first
        waitForPendingSaves(5000);

        // Close Redis (Redisson: Pub/Sub + Streams unified, publishes SERVER_STOP)
        if (redissonManager != null) {
            redissonManager.close();
            redissonManager = null;
        }

        // Close operation log (file-based)
        if (operationLogManager != null) {
            operationLogManager.close();
            operationLogManager = null;
        }

        // Shut down thread pool
        if (asyncExecutor != null) {
            asyncExecutor.shutdown(10);
            asyncExecutor = null;
        }
    }

    // ==================== Helpers ====================

    /**
     * Advance the local version tracking after a successful save.
     * Uses compute() with Math.max to handle the race between periodic save
     * advancing to v+1 and quit save having already set a higher version.
     */
    private void advanceVersion(UUID uuid, long savedVersion) {
        playerVersions.compute(uuid, (k, current) -> {
            if (current == null) return null; // player already cleaned up (quit)
            return Math.max(current, savedVersion + 1);
        });
    }

    // ==================== Unified Save Path ====================

    /** Save kind for logging and behavior differentiation. */
    public enum SaveKind {
        /** Player quit / final save — lock released after save. */
        QUIT("disconnect", true),
        /** Periodic online save — lock kept, data refreshed. */
        PERIODIC("periodic", false),
        /** Bulk save (/saveall command) — online players keep lock. */
        BULK("bulk", false),
        /** World-save triggered save — online save, lock kept. */
        WORLD_SAVE("world_save", false),
        /** Death-triggered save — online save, lock kept. */
        DEATH("death", false),
        /** Server shutdown / plugin disable — final save, lock released.
         *  Distinct from QUIT so audit logs and snapshot triggers can
         *  differentiate a graceful disconnect from a server stop. */
        SHUTDOWN("shutdown", true);

        final String causeName;
        final boolean releaseLock;

        SaveKind(String causeName, boolean releaseLock) {
            this.causeName = causeName;
            this.releaseLock = releaseLock;
        }
    }

    /** Result of a bulk saveAll operation, returned to the command layer. */
    public record SaveAllResult(int total, int success, int failed,
                                Map<UUID, String> failures) {
        public boolean allSucceeded() { return failed == 0; }
    }

    /** Result of a save operation. */
    public record SaveResult(boolean success, long expectedVersion, long actualVersion, int compressedSize, String errorMessage) {
        public static SaveResult success(long version, int size) {
            return new SaveResult(true, version, version + 1, size, null);
        }
        public static SaveResult conflict(long expected, long actual, int size) {
            return new SaveResult(false, expected, actual, size, "version conflict");
        }
        public static SaveResult error(String msg) {
            return new SaveResult(false, 0, 0, 0, msg);
        }
    }

    /**
     * Phase 2: per-component storage fast path.
     *
     * <p>Serializes only the dirty components and upserts them into the
     * {@code player_component} table. Does NOT rewrite the full player_data
     * Blob — the Blob keeps the last full-save state, and non-migrated
     * components continue to be read from there on next load.
     *
     * <p>After a successful component-only save, the dirty mask for this
     * player is cleared, and the component_bitmap on player_data is updated
     * to mark the newly-migrated components.
     *
     * <p>Limitations:
     * <ul>
     *   <li>Only called for online saves (PERIODIC/DEATH/WORLD_SAVE), never QUIT</li>
     *   <li>Returns null on any failure — caller falls back to full Blob save</li>
     *   <li>Lock heartbeat still runs independently (component save doesn't refresh locked_at;
     *       that's intentional — the lock is already held, heartbeat keeps it alive)</li>
     * </ul>
     *
     * @return SaveResult on success, null to fall back to full save
     */
    private SaveResult persistComponentsOnly(UUID uuid, PlayerData data, SaveKind kind) {
        try {
            java.util.Set<com.fastsync.sync.dirty.ComponentDirtyMask.Component> dirty =
                dirtyMask.getDirty(uuid);
            if (dirty.isEmpty()) {
                return null;  // nothing dirty, fall back (shouldn't happen — caller checks)
            }

            // Cap the batch size to avoid huge transactions
            if (dirty.size() > config.getComponentBatchSize()) {
                // Too many dirty components — full save is cheaper
                if (config.isDebug()) {
                    logger.fine("Component save for " + uuid + " skipped — too many dirty ("
                        + dirty.size() + " > " + config.getComponentBatchSize() + ")");
                }
                return null;
            }

            long startSer = System.nanoTime();
            java.util.Map<String, byte[]> componentBlobs = new java.util.HashMap<>();
            java.util.Map<String, Long> componentChecksums = new java.util.HashMap<>();
            int totalCompressedSize = 0;

            for (com.fastsync.sync.dirty.ComponentDirtyMask.Component c : dirty) {
                String name = c.name();
                // Skip components that are disabled in config
                if (!isComponentSyncEnabled(c)) continue;

                byte[] serialized = PlayerDataSerializer.serializeComponent(name, data);
                if (serialized == null) continue;  // component has no data

                byte[] compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize());
                long checksum = DatabaseManager.computeChecksum(serialized);

                componentBlobs.put(name, compressed);
                componentChecksums.put(name, checksum);
                totalCompressedSize += compressed.length;
            }

            if (componentBlobs.isEmpty()) {
                // All dirty components produced no data (e.g. all empty)
                dirtyMask.clearDirty(uuid, dirty);
                return SaveResult.success(data.getVersion(), 0);
            }

            long serElapsed = (System.nanoTime() - startSer) / 1_000_000;
            if (serializeLatency != null) serializeLatency.record(serElapsed);

            // Batch upsert all dirty components in one transaction
            long dbStart = System.nanoTime();
            java.util.Map<String, Long> newVersions =
                databaseManager.upsertComponentsBatch(uuid, componentBlobs, componentChecksums);
            long dbElapsed = (System.nanoTime() - dbStart) / 1_000_000;
            if (saveLatency != null) saveLatency.record(dbElapsed);

            // Update component_bitmap to mark newly-migrated components
            long bitmap = databaseManager.getComponentBitmap(uuid);
            long newBitmap = bitmap;
            for (com.fastsync.sync.dirty.ComponentDirtyMask.Component c : dirty) {
                if (componentBlobs.containsKey(c.name())) {
                    newBitmap |= (1L << c.ordinal());
                }
            }
            if (newBitmap != bitmap) {
                databaseManager.setComponentBitmap(uuid, newBitmap);
            }

            // Clear dirty flags for the components we just saved
            dirtyMask.clearDirty(uuid, dirty);

            if (config.isDebug()) {
                logger.info("Component save " + kind + " for " + uuid + ": "
                    + componentBlobs.size() + " components, "
                    + totalCompressedSize + " bytes, "
                    + "ser=" + serElapsed + "ms db=" + dbElapsed + "ms");
            }

            // Log operation
            logOperation(uuid, OperationType.SAVE, data.getFencingToken(),
                data.getVersion(), totalCompressedSize,
                "Component save " + kind + " (" + componentBlobs.size() + " components)");

            // Component saves don't increment player_data.version — that's only
            // bumped on full Blob saves. We report the unchanged version here.
            return SaveResult.success(data.getVersion(), totalCompressedSize);

        } catch (Exception e) {
            logger.log(Level.WARNING, "Component save failed for " + uuid + ", falling back to full save", e);
            return null;  // fall back to full save
        }
    }

    /**
     * Check whether a given component is enabled for sync in config.
     * Used by {@link #persistComponentsOnly} to skip disabled components.
     */
    private boolean isComponentSyncEnabled(com.fastsync.sync.dirty.ComponentDirtyMask.Component c) {
        return switch (c) {
            case INVENTORY, ENDER_CHEST -> config.isSyncInventory() || config.isSyncEnderChest();
            case VITALS -> config.isSyncHealth();
            case FOOD -> config.isSyncFood();
            case EXPERIENCE -> config.isSyncExperience();
            case POTION_EFFECTS -> config.isSyncPotionEffects();
            case GAME_MODE -> config.isSyncGameMode();
            case FIRE_TICKS -> config.isSyncFireTicks();
            case AIR -> config.isSyncAir();
            case FLIGHT -> config.isSyncFlight();
            case ADVANCEMENTS -> config.isSyncAdvancements();
            case STATISTICS -> config.isSyncStatistics();
            case ATTRIBUTES -> config.isSyncAttributes();
            case PDC -> config.isSyncPDC();
            case LOCATION -> config.isSyncLocation();
        };
    }

    /**
     * Unified save path: serialize → compress → checksum → DB CAS → conflict/advance/lock-release.
     *
     * <p>All save paths (quit, periodic, bulk, world_save, death) converge here to prevent
     * "fix one, forget the other" drift. The caller is responsible for:
     * <ul>
     *   <li>Per-UUID locking (quit: lock+wait, periodic: tryLock+skip)</li>
     *   <li>pendingSaveCount management</li>
     *   <li>playerVersions/playerFencingTokens cleanup (quit only)</li>
     * </ul>
     *
     * <p>Lock semantics are determined by {@link SaveKind#releaseLock}:
     * <ul>
     *   <li>{@code releaseLock=true} (QUIT): after save, lock is released and Redis notified.</li>
     *   <li>{@code releaseLock=false} (PERIODIC/BULK/WORLD_SAVE/DEATH): lock is kept,
     *       {@code locked_at} refreshed. No Redis notification — other servers should not
     *       attempt to acquire the lock while the player is still online on this server.</li>
     * </ul>
     */
    private SaveResult persistCollectedData(UUID uuid, PlayerData data, SaveKind kind) {
        long startTime = System.nanoTime();
        // Set save cause for snapshot trigger matching and audit logging
        data.setSaveCause(kind.causeName);

        // Phase 2: per-component storage fast path. If enabled AND this is an
        // online save (PERIODIC/DEATH/WORLD_SAVE — not QUIT which always
        // writes the full Blob for atomic lock release) AND there are dirty
        // components, write only those components to player_component and
        // skip the full Blob rewrite. Returns a SaveResult indicating
        // component-only path was taken.
        //
        // QUIT saves must still write the full Blob because:
        // 1. They release the lock atomically with the version CAS
        // 2. Future loads read the full Blob for non-migrated components
        // 3. Partial component writes on quit could lose non-dirty state
        if (config.isComponentStorageEnabled()
            && dirtyMask != null
            && !kind.releaseLock
            && dirtyMask.isAnyDirty(uuid)) {
            SaveResult componentResult = persistComponentsOnly(uuid, data, kind);
            if (componentResult != null) {
                return componentResult;
            }
            // Fall through to full save if component path failed
        }

        // Retry flag for same-fencing self-conflict (our own previous save advanced the version).
        boolean retried = false;

        try {
            // 1. Serialize
            byte[] serialized = PlayerDataSerializer.serialize(data);
            // 2. Compress (respects config.isCompressionEnabled())
            byte[] compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize());
            // 3. Checksum on raw serialized (not compressed)
            long checksum = DatabaseManager.computeChecksum(serialized);

            long serElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
            if (serializeLatency != null) serializeLatency.record(serElapsedMs);

            if (config.isLogTiming()) {
                logger.info("[Timing] Serialize " + kind + " for " + uuid + ": " + serElapsedMs + "ms"
                    + " (serialized=" + serialized.length + " bytes, stored=" + compressed.length + " bytes)");
            }

            // 4. DB CAS save with version + fencing token
            long expectedVersion = data.getVersion();
            long fencingToken = data.getFencingToken();
            long saveStart = System.nanoTime();

            boolean saved;
            if (kind.releaseLock) {
                // Final save (quit): release lock after successful write
                saved = databaseManager.saveDataAndReleaseLock(uuid, compressed, checksum, expectedVersion, fencingToken, config.getServerName());
            } else {
                // Online save (periodic/bulk/world_save/death): keep lock, refresh locked_at
                saved = databaseManager.saveDataKeepLock(uuid, compressed, checksum, expectedVersion, fencingToken, config.getServerName());
            }

            long saveElapsedMs = (System.nanoTime() - saveStart) / 1_000_000;
            if (saveLatency != null) saveLatency.record(saveElapsedMs);

            if (!saved) {
                // 5a. Conflict — check if this is a same-fencing self-conflict
                long actualVersion = databaseManager.getCurrentVersion(uuid);
                long actualFencingToken = databaseManager.getCurrentFencingToken(uuid);

                // Same-fencing retry: if the fencing token matches and the actual
                // version is higher, our own previous save (periodic/death/etc.)
                // advanced the version while we waited for the saveLock. This is
                // NOT an external conflict — retry with the actual version.
                // Applies to BOTH online saves (releaseLock=false) and final saves
                // (releaseLock=true), since multiple online saves can queue up
                // and the earlier one advances the version.
                if (!retried
                    && actualFencingToken == fencingToken
                    && actualVersion > expectedVersion) {

                    logger.info("[Fencing] " + kind + " save for " + uuid
                        + " — same-fencing self-conflict (expected v" + expectedVersion
                        + ", actual v" + actualVersion + ", ft=" + fencingToken
                        + "). Retrying with actual version.");

                    // Update data with the actual version and re-serialize
                    data.setVersion(actualVersion);
                    retried = true;

                    // Re-serialize with updated version
                    serialized = PlayerDataSerializer.serialize(data);
                    compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize());
                    checksum = DatabaseManager.computeChecksum(serialized);

                    // Retry the save with the actual version
                    if (kind.releaseLock) {
                        saved = databaseManager.saveDataAndReleaseLock(uuid, compressed, checksum, actualVersion, fencingToken, config.getServerName());
                    } else {
                        saved = databaseManager.saveDataKeepLock(uuid, compressed, checksum, actualVersion, fencingToken, config.getServerName());
                    }

                    if (saved) {
                        // Retry succeeded — treat as success with the actual version
                        expectedVersion = actualVersion;
                    }
                }

                if (!saved) {
                    // Genuine conflict (external fencing violation or retry also failed)
                    conflictManager.handleConflict(uuid, data, expectedVersion, actualVersion);
                    logger.warning("[Fencing] " + kind + " save rejected for " + uuid +
                        " (expected v" + expectedVersion + "/ft" + fencingToken +
                        ", actual v" + actualVersion + "/ft" + actualFencingToken + ")"
                        + (retried ? " [after retry]" : ""));

                    logOperation(uuid, OperationType.CONFLICT, fencingToken, expectedVersion,
                        compressed.length, kind + " conflict: expected v" + expectedVersion + "/ft" + fencingToken +
                        ", actual v" + actualVersion + "/ft" + actualFencingToken);

                    if (saveLatency != null) saveLatency.recordError();

                    if (kind.releaseLock) {
                        // Quit save: release lock even on conflict — player is leaving.
                        // Use fencing token condition to avoid releasing another
                        // server's lock in case of duplicate server-name config.
                        try {
                            databaseManager.releaseLock(uuid, config.getServerName(), fencingToken);
                        } catch (SQLException lockEx) {
                            logger.log(Level.WARNING, "Failed to release lock after " + kind + " conflict for " + uuid, lockEx);
                        }
                        notifyLockReleased(uuid);
                    }
                    // Online save: do NOT release lock — player is still on this server.
                    // The CAS failure means someone else wrote (fencing token violation),
                    // which is a serious bug. Log it at SEVERE and keep the lock.
                    if (!kind.releaseLock) {
                        logger.log(Level.SEVERE, "[Fencing] Online save conflict for " + uuid
                            + " — possible lock infringement! The lock should be held by us but CAS failed."
                            + " expected v" + expectedVersion + "/ft" + fencingToken
                            + ", actual v" + actualVersion + "/ft" + actualFencingToken);
                    }

                    return SaveResult.conflict(expectedVersion, actualVersion, compressed.length);
                }
            }

            // 5b. Success: advance version + log + snapshot + publish
            advanceVersion(uuid, expectedVersion);

            // Full Blob save contains the latest state of all enabled components,
            // so all dirty flags can be cleared. Without this, a player who was
            // once marked dirty would stay dirty forever (in component-storage=false
            // mode), causing every subsequent periodic save to do a full collect +
            // serialize + DB write — defeating the dirty tracking optimization.
            if (dirtyMask != null) {
                dirtyMask.clearAll(uuid);
            }

                if (snapshotManager != null && shouldCreateSnapshot(data.getSaveCause())) {
                    snapshotManager.createSnapshot(uuid, compressed, data.getSaveCause())
                        .thenRun(() -> snapshotManager.pruneSnapshots(uuid, config.getMaxSnapshots()));
                }

                logOperation(uuid, OperationType.SAVE, fencingToken, expectedVersion + 1,
                    compressed.length, kind + " saved v" + (expectedVersion + 1) + " cause=" + data.getSaveCause());

                if (kind.releaseLock) {
                    // Quit save: publish checkout event and notify waiting servers
                    publishCheckout(uuid, expectedVersion + 1, fencingToken, data.getSaveCause());
                    notifyLockReleased(uuid);
                }
                // Online save: do NOT publish checkout or notify — lock is still held by us.
                // Other servers should not try to acquire the lock while player is online here.

                if (config.isDebug()) {
                    logger.info(kind + " save for " + uuid + " (v" + expectedVersion + "->v" + (expectedVersion + 1)
                        + ", " + compressed.length + " bytes, lock=" + (kind.releaseLock ? "released" : "kept") + ")");
                }

                if (config.isLogTiming()) {
                    long totalElapsed = (System.nanoTime() - startTime) / 1_000_000;
                    logger.info("[Timing] Total " + kind + " save for " + uuid + ": " + totalElapsed + "ms");
                }

                return SaveResult.success(expectedVersion, compressed.length);
        } catch (Exception e) {
            logger.log(Level.SEVERE, kind + " save failed for " + uuid, e);

            if (kind.releaseLock) {
                // Final save (quit/shutdown) failed: DO NOT release the lock.
                // Releasing the lock on failure allows other servers to read
                // stale data and overwrite the player's final state.
                // Instead, let the lock expire naturally (lock-timeout) so
                // that the next server to acquire it will load the DB's
                // current (potentially stale) version rather than a version
                // that was never saved.
                //
                // The only exception is quarantined players — their lock is
                // already lost (detected by heartbeat), so releasing is safe.
                if (quarantinedPlayers.contains(uuid)) {
                    logger.info("[Quit] Releasing lock for quarantined player " + uuid
                        + " after save failure (lock already lost).");
                    try {
                        Long ft = playerFencingTokens.get(uuid);
                        if (ft != null) {
                            databaseManager.releaseLock(uuid, config.getServerName(), ft);
                        } else {
                            databaseManager.releaseLock(uuid, config.getServerName());
                        }
                        notifyLockReleased(uuid);
                    } catch (SQLException ex) {
                        logger.log(Level.WARNING, "Failed to release lock for quarantined player " + uuid, ex);
                    }
                } else {
                    logger.warning("[Quit] NOT releasing lock for " + uuid + " after " + kind
                        + " save failure — lock will expire after lock-timeout ("
                        + config.getLockTimeout() + "s) to protect the player's data.");
                }
            }
            // Online save: keep lock on error — will retry on next periodic save or quit
            return SaveResult.error(e.getMessage());
        }
    }

    // ==================== Snapshot Trigger Helper ====================

    /**
     * Determine whether a snapshot should be created for the given save cause,
     * based on the {@code snapshot.save-trigger} config value.
     *
     * <p>Supported values:
     * <ul>
     *   <li>{@code "never"} (default) — no snapshots on save; conflict-driven
     *       snapshots in ConflictManager are still created unconditionally.</li>
     *   <li>{@code "always"} — snapshot on every successful save.</li>
     *   <li>Comma-separated cause list, e.g. {@code "death,disconnect,shutdown,world_save"}
     *       — snapshot only when the save cause matches one of the listed values.</li>
     * </ul>
     *
     * <p>The trigger string is parsed into a {@link Set} once at
     * {@link #initialize()}/{@link #reload()} time, so this method is O(1).
     */
    private boolean shouldCreateSnapshot(String saveCause) {
        java.util.Set<String> triggers = snapshotTriggerSet;
        if (triggers.isEmpty()) return false;
        if (triggers.contains("always")) return true;
        return saveCause != null && triggers.contains(saveCause.toLowerCase());
    }

    /**
     * Parse the {@code snapshot.save-trigger} config value into a pre-computed
     * Set for O(1) lookup. Called from {@link #initialize()} and {@link #reload()}.
     */
    private void parseSnapshotTriggerSet() {
        String trigger = config.getSnapshotSaveTrigger();
        if (trigger == null || trigger.isBlank() || "never".equalsIgnoreCase(trigger)) {
            snapshotTriggerSet = java.util.Set.of();
        } else {
            java.util.Set<String> set = new java.util.HashSet<>();
            for (String c : trigger.split(",")) {
                String trimmed = c.trim().toLowerCase();
                if (!trimmed.isEmpty()) set.add(trimmed);
            }
            snapshotTriggerSet = java.util.Collections.unmodifiableSet(set);
        }
    }

    /**
     * Normalize an ItemStack[] so empty/AIR slots become null. The array length
     * (and thus slot positions) is preserved; only AIR entries are cleared. This
     * lets the serializer treat empty slots uniformly as null instead of storing
     * meaningless AIR ItemStacks.
     */
    private org.bukkit.inventory.ItemStack[] sparseContents(org.bukkit.inventory.ItemStack[] contents) {
        if (contents == null) return null;
        for (int i = 0; i < contents.length; i++) {
            if (contents[i] != null && contents[i].getType() == org.bukkit.Material.AIR) {
                contents[i] = null;
            }
        }
        return contents;
    }

    private void setInventoryContents(Player player, org.bukkit.inventory.ItemStack[] contents) {
        org.bukkit.inventory.ItemStack[] current = player.getInventory().getContents();
        int max = Math.min(contents.length, current.length);
        for (int i = 0; i < max; i++) {
            player.getInventory().setItem(i, contents[i]);
        }
    }

    private void setEnderChestContents(Player player, org.bukkit.inventory.ItemStack[] contents) {
        org.bukkit.inventory.ItemStack[] current = player.getEnderChest().getContents();
        int max = Math.min(contents.length, current.length);
        for (int i = 0; i < max; i++) {
            player.getEnderChest().setItem(i, contents[i]);
        }
    }

    public boolean isPlayerActive(UUID uuid) {
        return activePlayers.containsKey(uuid);
    }

    public int getPendingCount() {
        return pendingData.size() + pendingEmptyData.size();
    }

    public int getActiveCount() {
        return activePlayers.size();
    }

    /**
     * Wait for all pending async saves to complete (for graceful shutdown).
     *
     * <p>Uses a bounded busy-wait on {@code pendingSaveCount} — the async
     * executor is NOT shut down here (that happens in {@link #shutdown()}).
     * This separation prevents the race where saveAllOnlinePlayers() submits
     * tasks that haven't been picked up yet when waitForPendingSaves runs.
     */
    public void waitForPendingSaves(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (pendingSaveCount.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        int remaining = pendingSaveCount.get();
        if (remaining > 0) {
            logger.warning(remaining + " pending save(s) did not complete within " + timeoutMillis + "ms timeout.");
        }
    }

    public int getPendingSaveCount() {
        return pendingSaveCount.get();
    }

    public int getPendingLoadCount() {
        return pendingLoadCount.get();
    }

    /**
     * Returns the number of players currently quarantined (lock lost during
     * heartbeat). These players have been kicked and are pending reconnection.
     */
    public int getQuarantinedPlayerCount() {
        return quarantinedPlayers.size();
    }

    /**
     * Returns the number of players currently tracked as active (online with
     * a valid lock on this server).
     */
    public int getActivePlayerCount() {
        return activePlayers.size();
    }

    /**
     * Returns true if the plugin is in protection mode (DB has been unreachable
     * for too many consecutive heartbeat cycles). In this mode, all active
     * players are kicked to prevent them from playing with potentially expired
     * locks. Use /fastsync reload to reset.
     */
    public boolean isProtectionMode() {
        return protectionMode;
    }

    public boolean isRedisHealthy() {
        return redissonManager != null && redissonManager.isHealthy();
    }

    public boolean isRedisEnabled() {
        return redissonManager != null && redissonManager.isHealthy();
    }

    public int getAsyncActiveCount() {
        return asyncExecutor != null ? asyncExecutor.getActiveCount() : -1;
    }

    public int getAsyncQueueSize() {
        return asyncExecutor != null ? asyncExecutor.getQueueSize() : -1;
    }

    /**
     * Log an operation to the per-UUID operation log (Raft-inspired).
     * Non-blocking — logging never blocks the main save/load path.
     */
    private void logOperation(UUID uuid, OperationType type, long fencingToken,
                              long version, int dataSize, String detail) {
        if (operationLogManager != null) {
            OperationLog log = OperationLog.create(uuid, type, config.getServerName(),
                fencingToken, version, dataSize, detail);
            operationLogManager.append(log)
                .thenRun(() -> operationLogManager.prune(uuid, config.getOperationLogRetention()))
                .exceptionally(e -> {
                    logger.log(Level.WARNING, "[OpLog] Failed to log " + type + " for " + uuid, e);
                    return null;
                });
        }
    }

    /**
     * Query the operation history for a player.
     */
    public List<OperationLog> queryOperationLog(UUID uuid, int limit) {
        if (operationLogManager == null) {
            return List.of();
        }
        try {
            return operationLogManager.queryHistory(uuid, limit);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to query operation log for " + uuid, e);
            return List.of();
        }
    }

    // ==================== Redis Streams (critical events) ====================

    /**
     * Handle a critical stream event from another server.
     * This is called by the RedissonManager's consumer thread.
     */
    private void handleStreamEvent(StreamEvent event) {
        switch (event.type()) {
            case PLAYER_CHECKOUT -> {
                // Another server has saved player data and released the lock.
                // We can use this as a hint to retry lock acquisition faster.
                if (config.isDebug()) {
                    logger.info("[Stream] Player " + event.uuid() + " checked out from " +
                        event.server() + " (v" + event.version() + ", ft" + event.fencingToken() + ")");
                }
            }
            case PLAYER_CHECKIN -> {
                // Another server has loaded player data and acquired the lock.
                if (config.isDebug()) {
                    logger.info("[Stream] Player " + event.uuid() + " checked in to " +
                        event.server() + " (v" + event.version() + ")");
                }
            }
            case SERVER_START -> {
                logger.info("[Stream] Server started: " + event.server());
            }
            case SERVER_STOP -> {
                logger.info("[Stream] Server stopped: " + event.server());
            }
            case DATA_CONFLICT -> {
                logger.warning("[Stream] Data conflict reported by " + event.server() +
                    " for " + event.uuid() + ": " + event.detail());
            }
            case SNAPSHOT_CREATED -> {
                if (config.isDebug()) {
                    logger.info("[Stream] Snapshot created by " + event.server() +
                        " for " + event.uuid());
                }
            }
        }
    }

    /**
     * Publish a PLAYER_CHECKOUT event when player data is saved and lock released.
     */
    private void publishCheckout(UUID uuid, long version, long fencingToken, String cause) {
        if (redissonManager != null && config.isStreamsEnabled()) {
            redissonManager.publish(StreamEvent.create(
                StreamEventType.PLAYER_CHECKOUT, uuid, config.getServerName(),
                "", version, fencingToken, "cause=" + cause));
        }
    }

    /**
     * Publish a PLAYER_CHECKIN event when player data is loaded and lock acquired.
     */
    private void publishCheckin(UUID uuid, long version, long fencingToken) {
        if (redissonManager != null && config.isStreamsEnabled()) {
            redissonManager.publish(StreamEvent.create(
                StreamEventType.PLAYER_CHECKIN, uuid, config.getServerName(),
                "", version, fencingToken, "Player loaded"));
        }
    }

    /**
     * Log latency statistics (p50/p99/p99.9) for all tracked operations.
     * Call periodically or on shutdown to monitor tail latency.
     */
    public void logLatencyStats() {
        if (loadLatency != null) loadLatency.logStats();
        if (saveLatency != null) saveLatency.logStats();
        if (serializeLatency != null) serializeLatency.logStats();
    }

    /**
     * Get the conflict manager instance.
     */
    public ConflictManager getConflictManager() {
        return conflictManager;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public DatabaseManager getDatabaseManager() {
        return databaseManager;
    }

    /**
     * Get the dirty mask for component-level change tracking.
     * Returns null if dirty tracking is disabled in config.
     */
    public com.fastsync.sync.dirty.ComponentDirtyMask getDirtyMask() {
        return dirtyMask;
    }

    // ==================== LoadResult ====================

    public static class LoadResult {
        public enum Status { SUCCESS, LOCKED, ERROR, PROTECTION, BUSY }

        private final Status status;
        private final String message;

        private LoadResult(Status status, String message) {
            this.status = status;
            this.message = message;
        }

        public static LoadResult success() { return new LoadResult(Status.SUCCESS, null); }
        public static LoadResult locked() { return new LoadResult(Status.LOCKED, null); }
        public static LoadResult error(String message) { return new LoadResult(Status.ERROR, message); }
    public static LoadResult protection(String message) { return new LoadResult(Status.PROTECTION, message); }
    public static LoadResult busy(String message) { return new LoadResult(Status.BUSY, message); }

        public Status getStatus() { return status; }
        public String getMessage() { return message; }
        public boolean isSuccess() { return status == Status.SUCCESS; }
    }
}
