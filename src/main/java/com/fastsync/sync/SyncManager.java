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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
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
    /**
     * Dedicated executor for final saves (QUIT / SHUTDOWN). Round 16 (P0 #3):
     * previously, when the main asyncExecutor's bounded queue was full, a QUIT
     * save fell back to running {@code persistCollectedData} synchronously on
     * the PlayerQuitEvent thread — i.e. the Paper main thread or a Folia
     * region/entity thread. Under DB latency spikes this blocks the game tick
     * loop and amplifies the failure (DB slow → queue full → more synchronous
     * fallbacks → more blocked threads).
     *
     * <p>This dedicated executor has a small thread count (2) and its own
     * bounded queue sized generously (4x the main queue) so that QUIT saves
     * — which MUST persist the player's final state — get their own lane and
     * do not compete with periodic/death/world_save traffic. Only if THIS
     * executor is also saturated do we fall back to synchronous execution,
     * and that fallback now logs at SEVERE so operators see it.
     */
    private AsyncExecutor finalSaveExecutor;

    // Final-save spool (WAL for queue-full events)
    private com.fastsync.spool.FinalSaveSpool finalSaveSpool;
    private com.fastsync.spool.FinalSaveReplayService finalSaveReplayService;
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
    // Login loads explicitly cancelled through FastSyncPreLoadEvent. These
    // players bypass FastSync entirely: no DB lock, apply, heartbeat, or save.
    private final java.util.Set<UUID> pendingBypass = ConcurrentHashMap.newKeySet();
    private final java.util.Set<UUID> bypassedPlayers = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<UUID, DeathState> deathStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Long> pendingLoadTimes = new ConcurrentHashMap<>();

    // Final-save saturation telemetry. A queue-full event on finalSaveExecutor
    // immediately causes a synchronous fallback today, so we count both the
    // rejection and the fallback and surface them in /fastsync status.
    private final AtomicLong finalSaveQueueFullTotal = new AtomicLong();
    private final AtomicLong finalSaveLastQueueFullAt = new AtomicLong();
    private final AtomicLong finalSaveSpoolEnqueuedTotal = new AtomicLong();
    private final AtomicLong finalSaveLastSpoolEnqueuedAt = new AtomicLong();
    private final AtomicLong finalSaveSpoolRejectedTotal = new AtomicLong();
    private final AtomicLong finalSaveLastSpoolRejectedAt = new AtomicLong();
    private final AtomicLong finalSaveSyncFallbackTotal = new AtomicLong();
    private final AtomicLong finalSaveLastSyncFallbackAt = new AtomicLong();

    // Track players whose data has been applied (actively playing)
    private final ConcurrentHashMap<UUID, Boolean> activePlayers = new ConcurrentHashMap<>();

    // Track the DB version each player's data was loaded from (for optimistic concurrency)
    private final ConcurrentHashMap<UUID, Long> playerVersions = new ConcurrentHashMap<>();

    // Track the fencing token for each player (Kleppmann stale-write defence)
    private final ConcurrentHashMap<UUID, Long> playerFencingTokens = new ConcurrentHashMap<>();

    // P0 (round 10): per-acquire lock session id. Each successful acquireLock
    // generates a random nonce (UUID string) that is written to the
    // lock_session_id column. All subsequent save / release / heartbeat /
    // component-upsert calls for this session include the nonce in their WHERE
    // clause. This prevents a stale session (e.g. quit save still in flight
    // when the player quick-reconnects to the same backend) from releasing
    // or overwriting the new session's lock/data.
    private final ConcurrentHashMap<UUID, String> playerLockSessions = new ConcurrentHashMap<>();

    // Per-UUID save lock: ensures saves for the same player run sequentially.
    // Periodic saves use tryLock() and skip if a save is in flight (coalescing —
    // the next periodic save will pick up the latest data).
    // Quit saves use lock() and wait, because they MUST persist the final state.
    // This prevents unnecessary version conflicts when periodic + quit saves
    // race for the same player.
    private final ConcurrentHashMap<UUID, java.util.concurrent.locks.ReentrantLock> playerSaveLocks = new ConcurrentHashMap<>();

    // Per-UUID in-flight gate for ONLINE saves (PERIODIC/DEATH/WORLD_SAVE/BULK).
    // CRITICAL (round 9 P0 fix): prevents the "stale snapshot wins" race where
    // save A collects an old snapshot, save B collects a new snapshot and
    // commits first, then save A acquires the saveLock and overwrites B's new
    // state with its old snapshot.
    //
    // The saveLock alone does NOT prevent this — it serializes DB writes but
    // does NOT serialize the collect step. A's collect can happen before B's
    // collect, then B commits first, then A commits with stale data.
    //
    // The fix: online saves check this gate BEFORE calling collectPlayerData.
    // If an online save is already in-flight for this UUID, the new save skips
    // entirely (coalesces to the in-flight one, which will pick up the latest
    // state on its own collect). This ensures at most one online save is
    // collecting+serializing+writing per UUID at a time.
    //
    // QUIT/SHUTDOWN do NOT use this gate — they must save the final state and
    // are willing to wait for the saveLock. They are dispatched from
    // collectAndSavePlayerData / savePlayersSnapshot, which already handle the
    // saveLock wait. The quit save's collect happens on the entity thread
    // right before dispatch, and the saveLock ensures it commits after any
    // in-flight online save.
    //
    // The gate is set in savePlayerAsync BEFORE runAtEntity (so the next
    // periodic tick sees it in-flight and skips) and cleared in the async
    // executor's finally block (after the DB write completes or fails).
    private final ConcurrentHashMap<UUID, java.util.concurrent.atomic.AtomicBoolean> onlineSaveInFlight = new ConcurrentHashMap<>();

    // Players whose locks were lost (heartbeat refreshLock=false). These players
    // are being kicked and must NOT be saved via the normal QUIT path — their
    // fencing token is no longer valid. The quit handler checks this set and
    // skips the save, only releasing the lock if it somehow still belongs to us.
    private final java.util.Set<UUID> quarantinedPlayers = ConcurrentHashMap.newKeySet();

    // Players who failed the join handshake (no preloaded data, or missing
    // version/fencing token). They are kicked immediately and must NOT be
    // saved via the normal QUIT path — they never had a valid lock.
    private final java.util.Set<UUID> failedJoinPlayers = ConcurrentHashMap.newKeySet();

    // Players who currently have a non-empty full-Blob baseline in player_data.
    //
    // <p><b>Why this exists:</b> {@code persistComponentsOnly()} writes only
    // dirty component rows + bumps {@code player_data.version/component_bitmap}
    // WITHOUT touching the {@code player_data.data} Blob. If a brand-new player
    // (whose Blob is still empty because no full save has run yet) hits a
    // component-only save path, the component rows will be persisted but the
    // Blob baseline remains empty — and on the NEXT login, {@code loadPlayerDataRow()}
    // sees {@code data == null} and treats the player as brand new again,
    // completely skipping the component overlay. The player's components are
    // silently orphaned in the DB until the next full Blob save.
    //
    // <p>This set is the safety gate:
    // <ul>
    //   <li>{@code true} — a full Blob baseline exists in {@code player_data.data};
    //       component-only save is allowed.</li>
    //   <li>{@code false} (or absent) — no baseline yet; {@code persistComponentsOnly}
    //       returns {@code null} so the caller falls back to a full Blob save first.</li>
    // </ul>
    //
    // <p>Lifecycle:
    // <ul>
    //   <li>Set on load when {@code loaded.hasData() == true}</li>
    //   <li>Set on any successful full Blob save (releaseLock or keepLock)</li>
    //   <li>Removed on quit / failed-join cleanup / stale cleanup</li>
    // </ul>
    private final java.util.Set<UUID> playersWithBaseline = ConcurrentHashMap.newKeySet();
    // Component metadata loaded with the baseline row. Keeping this session-
    // local cursor lets the successful component-write hot path use a single
    // conditional UPDATE instead of SELECT ... FOR UPDATE + UPDATE. The DB
    // still validates version, fencing, session, and generation atomically.
    private final ConcurrentHashMap<UUID, ComponentCursor> componentCursors = new ConcurrentHashMap<>();

    private record ComponentCursor(long generation, long bitmap) {}

    private void clearComponentBaseline(UUID uuid) {
        playersWithBaseline.remove(uuid);
        componentCursors.remove(uuid);
    }

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

    // P0 (round 11): shutdown flag — once set, all new online saves are
    // rejected. SHUTDOWN saves still proceed (they hold finalSaveInProgress).
    private volatile boolean shuttingDown = false;

    // P0 (round 11): per-UUID final-save marker. When a QUIT or SHUTDOWN save
    // is in progress for a UUID, new online saves for that UUID are rejected
    // (they would collect a snapshot that races with the final save).
    private final java.util.Set<UUID> finalSaveInProgress = ConcurrentHashMap.newKeySet();

    // Track pending async saves for graceful shutdown
    private final AtomicInteger pendingSaveCount = new AtomicInteger(0);
    private final AtomicInteger pendingLoadCount = new AtomicInteger(0);

    // Cached Bukkit registries (immutable for the server lifetime) to avoid
    // re-iterating Bukkit.advancementIterator()/Attribute.values() on every save.
    private volatile org.bukkit.advancement.Advancement[] cachedAdvancements;
    private volatile Attribute[] cachedAttributes;
    // Paper 1.21.11 registry entry, resolved lazily to avoid class-load order
    // issues during plugin construction.
    private static volatile Attribute MAX_HEALTH_ATTR;
    private static Attribute loadMaxHealthAttribute() {
        Attribute cached = MAX_HEALTH_ATTR;
        if (cached != null) return cached;
        Attribute resolved = org.bukkit.Registry.ATTRIBUTE.get(
            org.bukkit.NamespacedKey.minecraft("max_health"));
        if (resolved == null) {
            throw new IllegalStateException("Paper attribute minecraft:max_health is unavailable");
        }
        MAX_HEALTH_ATTR = resolved;
        return resolved;
    }

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
        // Initialize final-save spool FIRST — in production mode, failure to
        // initialize the spool is a hard error (data loss on queue-full).
        // This must happen before executor creation so that a spool init
        // failure leaves executors null (no partial init).
        if (config.isFinalSaveSpoolEnabled()) {
            try {
                java.nio.file.Path spoolDir = plugin.getDataFolder().toPath()
                    .resolve(config.getFinalSaveSpoolDir());
                finalSaveSpool = new com.fastsync.spool.FinalSaveSpool(
                    logger, spoolDir, config.isFinalSaveSpoolFsync(),
                    config.getFinalSaveSpoolMaxFiles(),
                    config.getFinalSaveSpoolMaxBytes(),
                    config.getFinalSaveSpoolRetainFailedDays());
                if (config.isFinalSaveSpoolReplayOnStartup()) {
                    finalSaveReplayService = new com.fastsync.spool.FinalSaveReplayService(
                        logger, finalSaveSpool, databaseManager, plugin,
                        config.getFinalSaveSpoolReplayBatchSize(),
                        config.getFinalSaveSpoolReplayIntervalTicks(),
                        config.getServerName(),
                        this::notifyLockReleased);
                    finalSaveReplayService.start();
                }
            } catch (Exception e) {
                if (config.isProductionEnabled()) {
                    throw new IllegalStateException(
                        "Refusing to start: final-save spool could not be initialized in production mode", e);
                }
                logger.log(Level.SEVERE, "Failed to initialize final-save spool! "
                    + "Queue-full events will result in data loss.", e);
            }
        }

        // Create dedicated thread pool (NOT ForkJoinPool.commonPool).
        // Pool size: half of config poolSize (the other half is reserved for
        // Redis/heartbeat/cleanup tasks). Queue capacity: from config
        // database.queue-capacity (default 256). Bounded queue is CRITICAL —
        // see AsyncExecutor javadoc for why an unbounded queue would let
        // tasks pile up under DB latency / login storms and exhaust heap.
        int poolSize = Math.max(2, config.getPoolSize() / 2);
        int queueCapacity = Math.max(1, config.getQueueCapacity());
        asyncExecutor = new AsyncExecutor(logger, "FastSync-Async", poolSize, queueCapacity);

        // Round 16 (P0 #3): dedicated final-save executor. 2 threads so a
        // stuck save on one thread does not head-of-line block the next QUIT
        // save; queue is 4x the main queue so QUIT saves rarely fall back to
        // synchronous execution on the event thread.
        // Round 14: final-save executor uses configurable threads + queue capacity.
        int finalThreads = Math.max(2, config.getFinalSaveThreads());
        int finalQueue = Math.max(1024, config.getFinalSaveQueueCapacity());
        finalSaveExecutor = new AsyncExecutor(logger, "FastSync-FinalSave", finalThreads, finalQueue);

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
                if (config.isProductionEnabled() && config.isProductionRequireRedis()) {
                    throw new RuntimeException(
                        "Redis is required in production mode (production.require-redis=true) "
                            + "but initialization failed. Refusing to start. Error: " + e.getMessage(), e);
                }
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
     *
     * <p>P1 (issue #59): invalidate the cached Attribute / Advancement arrays
     * so any registry additions made by other plugins since the last reload
     * are picked up. Without this, custom attributes registered via
     * {@code Registry.ATTRIBUTE} after FastSync {@code onEnable} would never
     * be synced.
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
        // P1 (issue #59): invalidate registry caches so newly-registered
        // attributes / advancements are picked up on the next collect.
        cachedAttributes = null;
        cachedAdvancements = null;
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
        FastSyncEvents.FastSyncPreLoadEvent preLoadEvent =
            new FastSyncEvents.FastSyncPreLoadEvent(uuid, true);
        Bukkit.getPluginManager().callEvent(preLoadEvent);
        if (preLoadEvent.isCancelled()) {
            pendingBypass.add(uuid);
            pendingLoadTimes.put(uuid, System.currentTimeMillis());
            return LoadResult.bypassed();
        }
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
        // P0 (round 10): generate a per-acquire lockSessionId so this session's
        // saves/releases/heartbeats can be distinguished from a prior session's
        // in-flight quit save. The nonce is stored in playerLockSessions after
        // successful acquire and passed to every subsequent DB call.
        boolean locked = false;
        long fencingToken = 0;
        String lockSessionId = java.util.UUID.randomUUID().toString();
        for (int i = 0; i < config.getLockMaxRetries(); i++) {
            try {
                LockResult lockResult = databaseManager.acquireLock(uuid, config.getServerName(), lockSessionId);
                if (lockResult.acquired()) {
                    locked = true;
                    fencingToken = lockResult.fencingToken();
                    playerLockSessions.put(uuid, lockSessionId);
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

        // Step 2: Load data from database.
        // Use loadPlayerDataRow() unconditionally — it returns the full row
        // including component_bitmap and component_generation, so we avoid a
        // second metadata round-trip. Persisted component rows are honored
        // even when new component writes are disabled in the current config.
        try {
            long startTime = System.nanoTime();

            com.fastsync.database.DatabaseManager.PlayerDataRow loaded =
                databaseManager.loadPlayerDataRow(uuid);
            componentCursors.put(uuid,
                new ComponentCursor(loaded.componentGeneration(), loaded.componentBitmap()));
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
                // CRITICAL: this player has NO full-Blob baseline in player_data.
                // persistComponentsOnly() will refuse to run until a full Blob
                // save establishes the baseline. Otherwise component rows would
                // be orphaned (next login sees empty Blob → treats as new player
                // → skips component overlay).
                clearComponentBaseline(uuid);
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
            // Release the lock via the unified helper (fail-closed on null
            // token/session; catches SQLException AND RuntimeException so a
            // defensive IllegalArgumentException can't escape the load path).
            releaseOwnedLockAndNotify(uuid, fencingToken, lockSessionId, "checksum failure");
            clearComponentBaseline(uuid);
            return LoadResult.error("Data checksum mismatch - possible corruption");
            }

            PlayerData data = PlayerDataSerializer.deserialize(decompressed);

            // Phase 3: merge per-component overrides from player_component table.
            // loaded already contains component_bitmap AND component_generation
            // (from the single loadPlayerDataRow call above), so no second
            // round-trip is needed. Only load component rows that match the
            // current generation — this prevents stale component rows from a
            // previous baseline (before a full Blob save incremented generation)
            // from overriding the fresh Blob.
            long bitmap = loaded.componentBitmap();
            long generation = loaded.componentGeneration();
            // Persisted metadata is authoritative even if component storage is
            // disabled in the current config. A server may restart/roll back
            // configuration after its last online component save; ignoring a
            // non-zero bitmap would load the stale baseline and lose the newer
            // component rows. Disabled mode merely stops new component writes;
            // the next full save safely resets the bitmap/generation.
            if (bitmap != 0) {
                java.util.Set<String> migratedNames = componentNamesForBitmap(bitmap);
                if (!migratedNames.isEmpty()) {
                    // Load only components matching the current generation
                    java.util.Map<String, com.fastsync.database.DatabaseManager.ComponentData> components =
                        databaseManager.loadComponentsWithGeneration(uuid, migratedNames, generation);
                    verifyComponentOverlayCompleteness(uuid, migratedNames, components, generation);
                    // FAIL-CLOSED: any component checksum mismatch / unwrap
                    // failure / deserialize failure makes the WHOLE load fail.
                    // The old behavior (warn + continue) silently applied the
                    // stale full-Blob baseline while a component row that was
                    // supposed to override it was corrupt — then the next save
                    // could rewrite the stale baseline, causing a silent
                    // rollback. Instead: release the lock, reject the login,
                    // and do NOT apply baseline / allow a save.
                    for (var entry : components.entrySet()) {
                        String name = entry.getKey();
                        com.fastsync.database.DatabaseManager.ComponentData cd = entry.getValue();
                        if (!cd.hasData()) {
                            // Empty component row with a set bitmap bit is itself
                            // a corrupt/inconsistent state — treat it the same way.
                            throw new IOException("Component '" + name + "' for " + uuid
                                + " has a set bitmap bit but no data row (gen=" + generation + ")");
                        }
                        byte[] decompressedComp;
                        try {
                            decompressedComp = CompressionUtil.unwrap(cd.data());
                        } catch (com.fastsync.serialization.CorruptDataException e) {
                            throw new IOException("Corrupt component '" + name + "' for " + uuid
                                + ": unwrap failed — " + e.getMessage(), e);
                        }
                        if (config.isVerifyChecksum()
                                && !DatabaseManager.verifyChecksum(decompressedComp, cd.checksum())) {
                            throw new IOException("Component checksum mismatch for '"
                                + name + "' on " + uuid + " (stored=" + cd.checksum()
                                + ") — refusing to apply baseline over a corrupt override");
                        }
                        try {
                            PlayerDataSerializer.deserializeComponent(name, decompressedComp, data);
                        } catch (Exception e) {
                            // Includes ItemSerializationException from a corrupt
                            // item inside the component.
                            throw new IOException("Failed to deserialize component '"
                                + name + "' for " + uuid + ": " + e.getMessage(), e);
                        }
                    }
                    if (config.isDebug()) {
                        logger.info("Merged " + components.size() + " component overrides for " + uuid
                            + " (gen=" + generation + ", bitmap=0x" + Long.toHexString(bitmap) + ")");
                    }
                }
            }

            // Set the version from DB for optimistic concurrency (Dynamo-style)
            data.setVersion(loaded.version());
            // Set the fencing token from lock acquisition (Kleppmann-style)
            data.setFencingToken(fencingToken);
            // A non-empty full-Blob baseline exists in player_data — component-only
            // saves are safe for this player from now on.
            playersWithBaseline.add(uuid);

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
        // Release lock on error — fail-closed: require both fencingToken and lockSessionId.
        // No tokenless release fallback (the 2-arg releaseLock was deleted).
        releaseOwnedLockAndNotify(uuid, fencingToken, lockSessionId, "load error");
        clearComponentBaseline(uuid);
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
        boolean bypassed = pendingBypass.remove(uuid);
        pendingLoadTimes.remove(uuid);

        if (bypassed) {
            bypassedPlayers.add(uuid);
            if (config.isDebug()) {
                logger.info("FastSync load/apply bypassed by FastSyncPreLoadEvent for " + uuid);
            }
            return;
        }

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
                playerVersions.remove(uuid);
                playerFencingTokens.remove(uuid);
                String emptyLockSession = playerLockSessions.remove(uuid);
                if (dirtyMask != null) {
                    dirtyMask.remove(uuid);
                }
                // Best-effort release if a lock session exists. The helper is
                // fail-closed: if fencingToken is null/<=0 or emptyLockSession
                // is null/blank, it logs and refuses tokenless release rather
                // than clearing another session's lock.
                releaseLockAsyncBestEffort(uuid, fencingToken, emptyLockSession, "empty-data join without valid token");
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
        Long ft = playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
        clearComponentBaseline(uuid);
        if (dirtyMask != null) {
            dirtyMask.remove(uuid);
        }
        // Best-effort release if we hold lock metadata. The helper is
        // fail-closed: if ft is null/<=0 or lockSession is null/blank, it
        // logs and refuses tokenless release — the correct behaviour when
        // no lock was ever acquired (e.g. pre-login fully failed before
        // acquireLock). When metadata IS present, releasing here prevents
        // the lock from leaking until timeout.
        releaseLockAsyncBestEffort(uuid, ft, lockSession, "joined without preloaded data");
        player.kick(net.kyori.adventure.text.Component.text(
            "[FastSync] Failed to prepare your data. Please reconnect.",
            net.kyori.adventure.text.format.NamedTextColor.RED));
        return;
    }

        long startTime = config.isLogTiming() ? System.nanoTime() : 0;
        if (dirtyMask != null) {
            dirtyMask.beginApply(uuid);
        }
        try {
        // Greenfield payloads fully replace enabled components below. Avoid
        // clearing complete payloads first: CraftInventoryPlayer's
        // setStorageContents/setArmorContents implementations already walk and
        // replace every owned slot, so a pre-clear would double slot writes and
        // client update packets. Keep clear-before-apply only as a fail-closed
        // behavior for an unexpectedly absent component.
        if (config.isClearBeforeApply()) {
            if (config.isSyncInventory()) {
                if (data.getInventory() == null) player.getInventory().clear();
                if (data.getArmor() == null) player.getInventory().setArmorContents(null);
            }
            if (config.isSyncEnderChest() && data.getEnderChest() == null) {
                player.getEnderChest().clear();
            }
            if (config.isSyncPotionEffects() && data.getPotionEffects() == null) {
                java.util.Collection<PotionEffect> active = player.getActivePotionEffects();
                if (!active.isEmpty()) {
                    for (PotionEffect effect : active) {
                        player.removePotionEffect(effect.getType());
                    }
                }
            }
        }

        // Inventory
        if (config.isSyncInventory() && data.getInventory() != null) {
            setInventoryContents(player.getInventory(), data.getInventory());
        }

        // Armor
        if (config.isSyncInventory() && data.getArmor() != null) {
            player.getInventory().setArmorContents(data.getArmor());
        }

        // Offhand
        // When sync-inventory is enabled, the offhand slot is owned by FastSync.
        // We must apply the data's offhand state even when it is null (= empty),
        // otherwise the player's previous offhand item silently persists.
        // This matters even when clear-before-apply=false: without this, a
        // player who cleared their offhand on server A and saved (offhand=null)
        // would still see the old offhand on server B if server B's loaded data
        // came from a full Blob that did not contain an offhand field.
        //
        // The component-storage path's offhandPresent flag (see PlayerDataSerializer)
        // makes the field mandatory in the greenfield format; apply its value,
        // including null, directly.
        if (config.isSyncInventory()) {
            player.getInventory().setItemInOffHand(data.getOffhand());
        }

        // Ender chest
        if (config.isSyncEnderChest() && data.getEnderChest() != null) {
            setEnderChestContents(player.getEnderChest(), data.getEnderChest());
        }

        // Restore attribute base values before health. Minecraft validates
        // setHealth against the resulting effective value (base plus the
        // target player's live equipment/effect modifiers).
        if (config.isSyncAttributes() && data.getAttributes() != null) {
            applyAttributes(player, data);
        }

        // Health
        if (config.isSyncHealth()) {
            double targetMaxHealth = data.getMaxHealth();
            AttributeInstance maxHealth = player.getAttribute(loadMaxHealthAttribute());
            if (maxHealth == null) {
                throw new IllegalStateException("Player has no minecraft:max_health attribute instance");
            }
            if (data.getMaxHealth() > 0) {
                maxHealth.setBaseValue(data.getMaxHealth());
                targetMaxHealth = maxHealth.getValue();
            }
            double health = Math.min(data.getHealth(), targetMaxHealth);
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
        // When sync-potion-effects is enabled, the player's effect list is owned
        // by FastSync. We must clear existing effects BEFORE applying the data's
        // effects, otherwise effects that were present on the target server but
        // not in the loaded data would silently persist. This matters even when
        // clear-before-apply=false — the clear-before-apply block above only
        // runs when that config is true, so without this local clear, the
        // effect list would be a union of (target server's effects) ∪ (data's
        // effects) instead of exactly (data's effects).
        //
        // Note: data.getPotionEffects() == null means "not collected / unknown";
        // in that case we do NOT touch the player's effects (preserve target
        // state). data.getPotionEffects() == empty list means "explicitly no
        // effects" and we clear all.
        if (config.isSyncPotionEffects() && data.getPotionEffects() != null) {
            for (PotionEffect effect : new ArrayList<>(player.getActivePotionEffects())) {
                player.removePotionEffect(effect.getType());
            }
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
            // CraftLivingEntity stores maximumAir separately from the current
            // air supply. Restore the maximum first so custom capacities survive
            // the hop and the current value is interpreted against the right cap.
            player.setMaximumAir(data.getMaximumAir());
            player.setRemainingAir(data.getRemainingAir());
        }

        // Flight status
        if (config.isSyncFlight()) {
            player.setAllowFlight(data.isAllowFlight());
            // Full-replace semantics: false must clear a target server's stale
            // flying state just as true enables it. collectPlayerData guarantees
            // flying => allowFlight, so this call satisfies Paper's precondition.
            player.setFlying(data.isFlying());
        }

        // Advancements
        if (config.isSyncAdvancements() && data.getAdvancements() != null) {
            applyAdvancements(player, data);
        }

        // Statistics
        if (config.isSyncStatistics() && data.getStatistics() != null) {
            applyStatistics(player, data);
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

        // Internal setters above can fire Bukkit events and must not dirty the
        // just-loaded snapshot. End suppression before exposing the apply event:
        // listener-owned mutations are real gameplay state and must be saved.
        if (dirtyMask != null) {
            dirtyMask.endApply(uuid);
        }

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
            Long ft = playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
            if (dirtyMask != null) {
                dirtyMask.remove(uuid);
            }
            // Release via the unified async helper. It is fail-closed on null
            // ft/lockSession (logs + refuses tokenless release) and catches
            // SQLException | RuntimeException, so a defensive
            // IllegalArgumentException from releaseLock can no longer leak the
            // lock. We do NOT bypass the helper even when ft looks valid — the
            // helper's guards are the single source of truth for safe release.
            releaseLockAsyncBestEffort(uuid, ft, lockSession, "apply failure");
            player.kick(net.kyori.adventure.text.Component.text(
                "[FastSync] Failed to apply your data. Please reconnect.",
                net.kyori.adventure.text.format.NamedTextColor.RED));
            logger.log(Level.SEVERE, "Failed to apply data for " + uuid
                + " — player kicked, lock released, state not saved.", t);
        } finally {
            if (dirtyMask != null) {
                dirtyMask.endApply(uuid);
            }
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

        if (bypassedPlayers.remove(uuid) || pendingBypass.remove(uuid)) {
            pendingLoadTimes.remove(uuid);
            return;
        }

        // Check failed-join players — they never became active with a valid lock.
        // The kick from applyPlayerData triggers PlayerQuitEvent, so we must
        // intercept here to prevent a save with invalid version/fencingToken.
        if (failedJoinPlayers.remove(uuid)) {
            logger.warning("[Quit] Skipping save for failed-join player " + uuid
                + " — player never became active with a valid lock.");
            activePlayers.remove(uuid);
            pendingData.remove(uuid);
            pendingEmptyData.remove(uuid);
            pendingBypass.remove(uuid);
            pendingLoadTimes.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
            if (dirtyMask != null) {
                dirtyMask.remove(uuid);
            }
            playerVersions.remove(uuid);
            clearComponentBaseline(uuid);
            // Release via the unified async helper (fail-closed on null
            // ft/lockSession; catches SQLException | RuntimeException). A
            // failed-join player may legitimately have a null session if the
            // apply failed before the session was recorded — the helper logs
            // and refuses tokenless release rather than throwing.
            releaseLockAsyncBestEffort(uuid, ft, lockSession, "failed-join quit");
            return;
        }

        // Skip save for players who are not active, not pending, and not quarantined.
        // This catches edge cases where a quit event fires for a player who was
        // already cleaned up by another path. We still best-effort release any
        // lock metadata they hold (fencingToken + lockSession) — failing to do
        // so would leak the DB lock until it times out.
        if (!activePlayers.containsKey(uuid)
            && !pendingData.containsKey(uuid)
            && !pendingEmptyData.contains(uuid)
            && !quarantinedPlayers.contains(uuid)) {
            logger.warning("[Quit] Skipping save for inactive/untracked player " + uuid);
            playerVersions.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
            // Best-effort release if lock metadata is present; the helper is
            // fail-closed (logs + refuses tokenless release) so a null session
            // is safe. Without this, a player who joined without preloaded data
            // but somehow acquired a lock would leak it.
            releaseLockAsyncBestEffort(uuid, ft, lockSession, "inactive/untracked quit");
            return;
        }

        // Check if player has pending data (was kicked during pre-login, never joined)
        if (pendingData.containsKey(uuid) || pendingEmptyData.contains(uuid)) {
            pendingData.remove(uuid);
            pendingEmptyData.remove(uuid);
            pendingLoadTimes.remove(uuid);
            activePlayers.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
            if (dirtyMask != null) {
                dirtyMask.remove(uuid);
            }
            playerVersions.remove(uuid);
            clearComponentBaseline(uuid);
            // Release lock without saving — fail-closed: require both ft and lockSession.
            // No tokenless release fallback (the 2-arg releaseLock was deleted).
            releaseLockAsyncBestEffort(uuid, ft, lockSession, "pending but never joined");
            return;
        }

        // Heartbeat already proved that this session no longer owns the lock.
        // Do not collect or expose stale state through FastSyncSaveEvent.
        if (quarantinedPlayers.remove(uuid)) {
            logger.warning("[Quit] Player " + uuid + " was quarantined (lock lost during session)."
                + " Skipping final save; player should already have been kicked.");
            activePlayers.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid);
            String lockSession = playerLockSessions.remove(uuid);
            playerVersions.remove(uuid);
            clearComponentBaseline(uuid);
            if (dirtyMask != null) dirtyMask.remove(uuid);
            releaseLockAsyncBestEffort(uuid, ft, lockSession, "quarantined player quit");
            return;
        }

        // IMPORTANT: collect data BEFORE removing from maps.
        // collectPlayerData() reads version and fencing token from playerVersions
        // and playerFencingTokens. If we remove first, the save will use default
        // version=0 and fencingToken=0, causing saveData() to fail or overwrite.
        PlayerData data;
        try {
            data = collectPlayerData(player);
        } catch (Throwable collectFailure) {
            // The player is leaving, so there is no later event from which to
            // retry collection. Do not release the DB lock: doing so would let
            // another server load stale data as if this final state had saved.
            // Also stop heartbeats and remove local metadata so the abandoned
            // lock expires naturally instead of being refreshed forever.
            activePlayers.remove(uuid);
            quarantinedPlayers.remove(uuid);
            playerVersions.remove(uuid);
            playerFencingTokens.remove(uuid);
            playerLockSessions.remove(uuid);
            clearComponentBaseline(uuid);
            if (dirtyMask != null) {
                dirtyMask.remove(uuid);
            }
            logger.log(Level.SEVERE, "[FinalSave] Failed to collect final state for " + uuid
                + "; NOT releasing lock. It will expire after " + config.getLockTimeout()
                + "s so stale DB data cannot be loaded immediately.", collectFailure);
            return;
        }

        if (!fireSaveEvent(player, data, SaveKind.QUIT)) {
            activePlayers.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid);
            String lockSession = playerLockSessions.remove(uuid);
            playerVersions.remove(uuid);
            clearComponentBaseline(uuid);
            if (dirtyMask != null) dirtyMask.remove(uuid);
            // Cancellation delegates persistence to the listener. FastSync still
            // owns the coordination lock, so release it after the callback.
            releaseLockAsyncBestEffort(uuid, ft, lockSession, "cancelled FastSyncSaveEvent");
            return;
        }

        // Now safe to remove from active tracking.
        // playerVersions and playerFencingTokens are NOT removed here — they
        // are cleaned up after the async save completes (in the finally block),
        // because the per-UUID lock check in periodic save still references them.
        activePlayers.remove(uuid);

        // Save asynchronously using dedicated thread pool.
        // Per-UUID lock ensures this save runs AFTER any in-flight periodic save,
        // preventing version conflicts from concurrent saves for the same player.
        pendingSaveCount.incrementAndGet();
        java.util.concurrent.locks.ReentrantLock saveLock =
            playerSaveLocks.computeIfAbsent(uuid, k -> new java.util.concurrent.locks.ReentrantLock());

        // Round 16 (P0 #3): QUIT saves go to the dedicated finalSaveExecutor,
        // NOT the main asyncExecutor. This isolates final saves from periodic
        // / death / world_save traffic and gives them their own bounded queue,
        // so a saturated main pool does not force QUIT saves onto the event
        // thread. Only if the finalSaveExecutor is ALSO saturated do we fall
        // back to synchronous execution on the current thread — and that
        // fallback is now logged at SEVERE so operators see it.
        Runnable quitSaveTask = () -> {
            saveLock.lock(); // must wait — quit save must persist final state
            try {
                // CRITICAL: refresh version/fencingToken after acquiring the lock.
                // A periodic/death/world_save save may have completed while we
                // waited, advancing the DB version. Without this refresh, the
                // QUIT save would use the stale version collected before the
                // lock was acquired, fail the CAS, and release the lock
                // without saving — losing the player's final state.
                refreshVersionAndFencingToken(uuid, data);
                persistFinalSaveWithSpool(
                    uuid, data, SaveKind.QUIT,
                    com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.EMPTY,
                    "asynchronous quit save failed");
            } finally {
                saveLock.unlock();
                // Do NOT remove lock from map — prevents lock-object split if
                // another thread is waiting on the same lock instance.
                // Locks are cleaned up lazily by cleanupStaleEntries().
                playerVersions.remove(uuid);
                playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
                clearComponentBaseline(uuid);
                pendingSaveCount.decrementAndGet();
            }
        };
        try {
            finalSaveExecutor.execute(quitSaveTask);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Final-save executor also saturated.
            // Round 14b: if finalSaveAllowSyncFallback is false (production
            // default), do NOT run DB I/O on the event thread. Instead, log
            // SEVERE and let the lock expire naturally. The player's final
            // state will be missing until the next login triggers a full
            // save, but this is safer than blocking the game tick.
            if (!config.isFinalSaveAllowSyncFallback()) {
                // Spool the final save to disk for later replay.
                // This prevents data loss when the final-save executor is saturated.
                FinalSaveQueueFullOutcome outcome;
                String detail;
                try {
                    String session = playerLockSessions.get(uuid);
                    if (finalSaveSpool != null) {
                        com.fastsync.spool.EncodedFinalSave encoded = com.fastsync.spool.FinalSaveEncoder.encode(
                            uuid, data, SaveKind.QUIT,
                            config.getClusterId(), config.getServerName(),
                            session, config.getCompressionMinSize());
                        finalSaveSpool.append(encoded);
                        outcome = FinalSaveQueueFullOutcome.SPOOLED;
                        detail = "queue full — final save safely spooled to disk for replay";
                    } else {
                        outcome = FinalSaveQueueFullOutcome.SPOOL_UNAVAILABLE;
                        detail = "queue full — spool is not initialized; final state may be lost";
                    }
                } catch (Exception spoolError) {
                    outcome = FinalSaveQueueFullOutcome.SPOOL_FAILED;
                    detail = "queue full — failed to spool final save: "
                        + (spoolError.getMessage() != null ? spoolError.getMessage() : spoolError.getClass().getSimpleName());
                    logger.log(Level.SEVERE, "[FinalSave] CRITICAL: failed to spool final save for "
                        + uuid + ". Final state may be lost. Lock will expire naturally.", spoolError);
                }
                recordFinalSaveQueueFull("QUIT", uuid, detail, e, outcome);
                pendingSaveCount.decrementAndGet();
                // Do NOT release lock — replay will release it after CAS succeeds if spooled.
                // If spool failed/unavailable, lock expires naturally.
                playerVersions.remove(uuid);
                playerFencingTokens.remove(uuid);
                playerLockSessions.remove(uuid);
                clearComponentBaseline(uuid);
                return;
            }
            // Fallback allowed: run synchronously on the event thread.
            recordFinalSaveQueueFull(
                "QUIT", uuid,
                "running synchronously on event thread as last-resort fallback. "
                    + "This blocks the game tick; investigate DB latency or raise "
                    + "final-save.queue-capacity.",
                e, FinalSaveQueueFullOutcome.SYNC_FALLBACK);
            saveLock.lock();
            try {
                refreshVersionAndFencingToken(uuid, data);
                persistFinalSaveWithSpool(
                    uuid, data, SaveKind.QUIT,
                    com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.EMPTY,
                    "synchronous queue-full quit fallback failed");
            } finally {
                saveLock.unlock();
                playerVersions.remove(uuid);
                playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
                clearComponentBaseline(uuid);
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

        if (bypassedPlayers.remove(uuid) || pendingBypass.remove(uuid)) {
            pendingLoadTimes.remove(uuid);
            return;
        }

        // Check if player has pending data (was kicked during pre-login, never joined)
        if (pendingData.containsKey(uuid) || pendingEmptyData.contains(uuid)) {
        pendingData.remove(uuid);
        pendingEmptyData.remove(uuid);
        pendingLoadTimes.remove(uuid);
        activePlayers.remove(uuid);
        Long ft = playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
        if (dirtyMask != null) {
            dirtyMask.remove(uuid);
        }
        playerVersions.remove(uuid);
        clearComponentBaseline(uuid);
        // Fail-closed: require both ft and lockSession. No tokenless release.
        releaseOwnedLockAndNotify(uuid, ft, lockSession, "never joined, sync fallback");
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

        if (quarantinedPlayers.remove(uuid)) {
            logger.warning("[Quit-Sync] Player " + uuid + " was quarantined. Skipping save.");
            activePlayers.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid);
            String lockSession = playerLockSessions.remove(uuid);
            if (dirtyMask != null) dirtyMask.remove(uuid);
            playerVersions.remove(uuid);
            clearComponentBaseline(uuid);
            releaseOwnedLockAndNotify(uuid, ft, lockSession, "quarantined player quit (sync)");
            return;
        }

        if (!fireSaveEvent(player, data, SaveKind.QUIT)) {
            activePlayers.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid);
            String lockSession = playerLockSessions.remove(uuid);
            playerVersions.remove(uuid);
            clearComponentBaseline(uuid);
            if (dirtyMask != null) dirtyMask.remove(uuid);
            releaseOwnedLockAndNotify(uuid, ft, lockSession, "cancelled FastSyncSaveEvent (sync)");
            return;
        }

        activePlayers.remove(uuid);

        // Save synchronously with per-UUID lock to avoid races with any in-flight save
        java.util.concurrent.locks.ReentrantLock saveLock =
            playerSaveLocks.computeIfAbsent(uuid, k -> new java.util.concurrent.locks.ReentrantLock());
        saveLock.lock();
        try {
            // Refresh version/fencingToken after acquiring lock — same rationale
            // as the async QUIT path: an in-flight save may have advanced the version.
            refreshVersionAndFencingToken(uuid, data);
            persistFinalSaveWithSpool(
                uuid, data, SaveKind.QUIT,
                com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.EMPTY,
                "synchronous retired-entity quit save failed");
        } finally {
            saveLock.unlock();
            playerVersions.remove(uuid);
            playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
            clearComponentBaseline(uuid);
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
    private void releaseLockAsyncBestEffort(UUID uuid, Long fencingToken, String lockSessionId, String reason) {
        pendingSaveCount.incrementAndGet();
        Runnable task = () -> {
            try {
                if (fencingToken == null || fencingToken <= 0) {
                    logger.warning("[LockRelease] " + reason + " for " + uuid
                        + " has no fencing token; refusing unsafe tokenless release.");
                    return;
                }
                if (lockSessionId == null || lockSessionId.isBlank()) {
                    logger.severe("[LockRelease] " + reason + " for " + uuid
                        + " has no lockSessionId; refusing release to avoid clearing another session's lock.");
                    return;
                }
                boolean released = databaseManager.releaseLock(
                    uuid, config.getServerName(), fencingToken, lockSessionId);
                if (released) {
                    notifyLockReleased(uuid);
                    if (config.isDebug()) {
                        logger.info("[LockRelease] Released lock for " + uuid + " (" + reason + ")");
                    }
                } else {
                    logger.warning("[LockRelease] " + reason + " for " + uuid
                        + " did not release anything; no Redis RELEASED will be sent.");
                }
            } catch (SQLException | RuntimeException e) {
                // Catch RuntimeException too: releaseLock can throw
                // IllegalArgumentException on null/blank session ids (defensive
                // guards). Without catching it here, the pendingSaveCount
                // decrement in finally would still run, but the exception
                // would escape the async task and be swallowed by the
                // executor — better to log it explicitly.
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
     * Unified sync (non-async) lock release + notify. Used by paths that are
     * already on the async thread or that must release synchronously (e.g.
     * checksum failure during load, which holds a connection we want to free
     * before notifying Redis).
     *
     * <p>P0 (round 11): fail-closed on null fencingToken OR null lockSessionId.
     * Never calls releaseLock with a null session — that would bypass the
     * lock_session_id WHERE clause and could clear another session's lock.
     * Only notifies Redis if releaseLock returns true (lock was actually ours
     * and was cleared).
     */
    private void releaseOwnedLockAndNotify(UUID uuid, Long fencingToken, String lockSessionId, String reason) {
        if (fencingToken == null || fencingToken <= 0) {
            logger.warning("[LockRelease] " + reason + " for " + uuid
                + " has no fencing token; refusing unsafe tokenless release.");
            return;
        }
        if (lockSessionId == null || lockSessionId.isBlank()) {
            logger.severe("[LockRelease] " + reason + " for " + uuid
                + " has no lockSessionId; refusing release to avoid clearing another session's lock.");
            return;
        }
        try {
            boolean released = databaseManager.releaseLock(
                uuid, config.getServerName(), fencingToken, lockSessionId);
            if (released) {
                notifyLockReleased(uuid);
                if (config.isDebug()) {
                    logger.info("[LockRelease] Released lock for " + uuid + " (" + reason + ")");
                }
            } else {
                logger.warning("[LockRelease] " + reason + " for " + uuid
                    + " did not release anything; no Redis RELEASED will be sent.");
            }
        } catch (SQLException | RuntimeException e) {
            // Catch RuntimeException too: releaseLock can throw
            // IllegalArgumentException when a session id is null/blank (defensive
            // guards), and we must not let that escape the release helper and
            // skip the surrounding finally / pendingSaveCount decrement.
            logger.log(Level.WARNING, "[LockRelease] Failed to release lock for " + uuid
                + " (" + reason + ")", e);
        }
    }

    /**
     * Collect player data from the player's current state.
     * Full HuskSync feature parity - collects all synchronizable data.
     */
    private PlayerData collectPlayerData(Player player) {
        return collectPlayerData(player, null);
    }

    /**
     * Collect either the complete configured state or only the components in a
     * pre-save dirty snapshot. Paper/Folia requires every Player read here to
     * run on the entity thread, so avoiding unrelated reads is the main CPU win
     * of component storage; serializing fewer fields after a full collect is
     * too late.
     *
     * @param requested null for a full collection, otherwise the exact dirty
     *                  component snapshot that will be persisted
     */
    private PlayerData collectPlayerData(Player player,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot requested) {
        PlayerData data = requested == null ? new PlayerData() : PlayerData.forComponentSubset();
        UUID playerId = player.getUniqueId();
        DeathState deathState = deathStates.get(playerId);

        // Inherit version from the loaded data for optimistic concurrency
        Long version = playerVersions.get(playerId);
        if (version != null) {
            data.setVersion(version);
        }
        // Inherit fencing token from lock acquisition (Kleppmann stale-write defence)
        Long fencingToken = playerFencingTokens.get(playerId);
        if (fencingToken != null) {
            data.setFencingToken(fencingToken);
        }

        // Inventory — Paper returns a fresh array containing CraftItemStack
        // mirrors. Detach only the non-empty elements so async serialization
        // cannot race the live NMS stacks; empty slots remain allocation-free.
        // All basic fields are now gated by config checks — disabling sync items
        // genuinely reduces serialization cost, NBT size, and DB write size.
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.INVENTORY)
                && config.isSyncInventory()) {
            // Armor and offhand are persisted separately below. Collect only
            // storage slots here so the payload matches setStorageContents()
            // exactly, rather than the all-contents view that also contains
            // equipment slots. This also avoids serializing duplicates.
            org.bukkit.inventory.PlayerInventory inventory = player.getInventory();
            data.setInventory(snapshotItemContents(inventory.getStorageContents()));
            data.setArmor(snapshotItemContents(inventory.getArmorContents()));
            org.bukkit.inventory.ItemStack offhand = inventory.getItemInOffHand();
            data.setOffhand(offhand != null && !offhand.getType().isAir() ? offhand.clone() : null);
        }

        // Ender chest
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ENDER_CHEST)
                && config.isSyncEnderChest()) {
            data.setEnderChest(snapshotItemContents(player.getEnderChest().getStorageContents()));
        }

        // Vitals
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.VITALS)
                && config.isSyncHealth()) {
            AttributeInstance maxHealthAttr = player.getAttribute(loadMaxHealthAttribute());
            if (maxHealthAttr == null) {
                throw new IllegalStateException("Player has no minecraft:max_health attribute instance");
            }
            double baseMaxHealth = maxHealthAttr.getBaseValue();
            double effectiveMaxHealth = maxHealthAttr.getValue();
            double currentHealth = player.getHealth();
            // P1 (issue #60): when a player is dead (health <= 0) at collect
            // time — e.g. they died and immediately quit before respawn —
            // saving health=0 lets the death act as a free health refill on
            // cross-server hop: the target server's applyPlayerData skips
            // setHealth(0) (you cannot setHealth(0) on a joining player) and
            // the player spawns at full health.
            //
            // The fix: save the EXPECTED post-respawn health (= maxHealth)
            // when the player is dead. This matches what MC does on respawn
            // (PlayerList.respawn sets health to getMaxHealth()), so the saved
            // state reflects the player's "next alive" state rather than the
            // transient "currently dying" state.
            //
            // isDead() catches both natural death and /kill. We also guard
            // against the (theoretical) case where health <= 0 but isDead()
            // returns false (e.g. plugin-managed fake death).
            boolean dead = deathState != null || player.isDead() || currentHealth <= 0;
            data.setHealth(healthForSave(dead, currentHealth, effectiveMaxHealth));
            data.setMaxHealth(baseMaxHealth);
        }
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.FOOD)
                && config.isSyncFood()) {
            if (deathState != null || player.isDead()) {
                // ServerPlayer.reset(): foodData = new FoodData().
                data.setFoodLevel(20);
                data.setSaturation(5.0F);
                data.setExhaustion(0.0F);
            } else {
                data.setFoodLevel(player.getFoodLevel());
                data.setSaturation(player.getSaturation());
                data.setExhaustion(player.getExhaustion());
            }
        }

        // Experience
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.EXPERIENCE)
                && config.isSyncExperience()) {
            if (deathState != null) {
                data.setExpLevel(deathState.level());
                data.setExpProgress(deathState.progress());
                data.setTotalExperience(deathState.totalExperience());
            } else if (player.isDead()) {
                data.setExpLevel(0);
                data.setExpProgress(0);
                data.setTotalExperience(0);
            } else {
                data.setExpLevel(player.getLevel());
                data.setExpProgress(player.getExp());
                data.setTotalExperience(player.getTotalExperience());
            }
        }

        // Potion effects
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.POTION_EFFECTS)
                && config.isSyncPotionEffects()) {
            List<PlayerData.PotionEffectData> effects = new ArrayList<>();
            if (deathState == null && !player.isDead()) {
                for (PotionEffect effect : player.getActivePotionEffects()) {
                    effects.add(PlayerDataSerializer.toPotionEffectData(effect));
                }
            }
            data.setPotionEffects(effects);
        }

        // Extra
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.GAME_MODE)
                && config.isSyncGameMode()) {
            data.setGameMode(player.getGameMode());
        }
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.FIRE_TICKS)
                && config.isSyncFireTicks()) {
            data.setFireTicks(deathState != null || player.isDead() ? 0 : player.getFireTicks());
        }
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.AIR)
                && config.isSyncAir()) {
            int maximumAir = player.getMaximumAir();
            data.setMaximumAir(maximumAir);
            data.setRemainingAir(
                deathState != null || player.isDead() ? maximumAir : player.getRemainingAir());
        }

        // Flight status
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.FLIGHT)
                && config.isSyncFlight()) {
            boolean flying = player.isFlying();
            boolean allowFlight = player.getAllowFlight();
            // P1 (audit): data integrity check. A logically inconsistent state
            // (flying=true with allowFlight=false) cannot be applied on the
            // target server — Player.setFlying(true) without allowFlight=true
            // is silently ignored by Bukkit. If we save this state and apply
            // it elsewhere, the player would keep whatever flight state the
            // target server has, which is non-deterministic.
            //
            // Force the state to be self-consistent: if the player is currently
            // flying but allowFlight is false (a transient inconsistency from
            // a plugin setting allowFlight=false mid-flight), save allowFlight=true
            // so the apply path can actually re-enter flight. This matches MC
            // PlayerList behavior where setFlying is gated on abilities.flying
            // which is gated on abilities.allowFlight.
            if (flying && !allowFlight) {
                allowFlight = true;
            }
            data.setFlying(flying);
            data.setAllowFlight(allowFlight);
        }

        // Advancements (using Bukkit API - iterates all advancement criteria)
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ADVANCEMENTS)
                && config.isSyncAdvancements()) {
            collectAdvancements(player, data);
        }

        // Statistics (basic UNTYPED stats always synced; typed stats via strategy)
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.STATISTICS)
                && config.isSyncStatistics()) {
            collectStatistics(player, data);
        }

        // Attributes
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ATTRIBUTES)
                && config.isSyncAttributes()) {
            collectAttributes(player, data);
        }

        // Persistent Data Container (via strategy)
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.PDC)
                && pdcStrategy != null && !"off".equals(pdcStrategy.strategyName())) {
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
        if (collects(requested, com.fastsync.sync.dirty.ComponentDirtyMask.Component.LOCATION)
                && config.isSyncLocation()) {
            Location loc = player.getLocation();
            data.setWorldName(loc.getWorld() != null ? loc.getWorld().getName() : "world");
            data.setWorldUuid(loc.getWorld() != null ? loc.getWorld().getUID().toString() : "");
            data.setX(loc.getX());
            data.setY(loc.getY());
            data.setZ(loc.getZ());
            data.setYaw(loc.getYaw());
            data.setPitch(loc.getPitch());
        }

        data.setTimestamp(System.currentTimeMillis());

        return data;
    }

    private static boolean collects(
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot requested,
            com.fastsync.sync.dirty.ComponentDirtyMask.Component component) {
        return requested == null || requested.contains(component);
    }

    /**
     * Detach Paper's CraftItemStack mirrors while still on the entity thread.
     * CraftInventory#getStorageContents returns a new array, but Paper 1.21.11
     * fills it with mirrors of the live NMS stacks. The async encoder must not
     * retain those mutable handles while gameplay continues.
     */
    private static org.bukkit.inventory.ItemStack[] snapshotItemContents(
            org.bukkit.inventory.ItemStack[] contents) {
        if (contents == null) return null;
        for (int i = 0; i < contents.length; i++) {
            org.bukkit.inventory.ItemStack item = contents[i];
            contents[i] = item == null || item.getType().isAir() ? null : item.clone();
        }
        return contents;
    }

    /**
     * Decide on the entity thread whether the save is guaranteed to stay on
     * the component path. A partial PlayerData must never fall back to a full
     * Blob write, so every fallback condition is checked before collection.
     */
    private boolean canCollectComponentsOnly(UUID uuid, SaveKind kind,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot snapshot) {
        if (!config.isComponentStorageEnabled() || dirtyMask == null || kind.releaseLock
                || snapshot == null || snapshot.isEmpty()
                || !playersWithBaseline.contains(uuid) || !componentCursors.containsKey(uuid)) {
            return false;
        }
        // A validation/all-dirty cycle intentionally refreshes the full Blob.
        // One baseline row is also cheaper than a transaction containing every
        // component row.
        if (snapshot.size() >= com.fastsync.sync.dirty.ComponentDirtyMask.Component.values().length
                || snapshot.size() > config.getComponentBatchSize()) {
            return false;
        }
        for (com.fastsync.sync.dirty.ComponentDirtyMask.Component component :
                com.fastsync.sync.dirty.ComponentDirtyMask.Component.values()) {
            if (snapshot.contains(component) && isComponentSyncEnabled(component)) return true;
        }
        return false;
    }

    /** Snapshot Paper's event-derived state that ServerPlayer.reset applies. */
    public void recordDeathState(org.bukkit.event.entity.PlayerDeathEvent event) {
        Player player = event.getEntity();
        if (event.getKeepLevel()) {
            deathStates.put(player.getUniqueId(), new DeathState(
                player.getLevel(), player.getExp(), player.getTotalExperience()));
            return;
        }
        deathStates.put(player.getUniqueId(), experienceAfterDeath(
            event.getNewLevel(), event.getNewTotalExp(), event.getNewExp()));
    }

    public void clearDeathState(UUID uuid) {
        deathStates.remove(uuid);
    }

    static DeathState experienceAfterDeath(int initialLevel, int initialTotal, int points) {
        int level = Math.max(0, initialLevel);
        long total = Math.max(0, initialTotal);
        int remaining = Math.max(0, points);
        total = Math.min(Integer.MAX_VALUE, total + remaining);
        while (remaining > 0) {
            long needed = experienceToNextLevel(level);
            if (remaining < needed) {
                return new DeathState(level, (float) (remaining / (double) needed), (int) total);
            }
            remaining -= (int) needed;
            if (level == Integer.MAX_VALUE) {
                return new DeathState(level, 0.0F, (int) total);
            }
            level++;
        }
        return new DeathState(level, 0.0F, (int) total);
    }

    private static long experienceToNextLevel(int level) {
        if (level >= 30) return 112L + (level - 30L) * 9L;
        if (level >= 15) return 37L + (level - 15L) * 5L;
        return 7L + level * 2L;
    }

    record DeathState(int level, float progress, int totalExperience) {}

    private boolean fireSaveEvent(Player player, PlayerData data, SaveKind kind) {
        FastSyncEvents.FastSyncSaveEvent event =
            new FastSyncEvents.FastSyncSaveEvent(player, data, kind.causeName);
        Bukkit.getPluginManager().callEvent(event);
        return !event.isCancelled();
    }

    private com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot snapshotDirtyForSave(
            UUID uuid, SaveKind kind) {
        if (dirtyMask == null || kind.releaseLock) {
            return com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot.EMPTY;
        }
        // Save listeners may mutate any PlayerData field. When component
        // storage is enabled, restricting the write to the pre-existing dirty
        // subset would silently discard those mutations. Only pay the full-
        // snapshot cost when a third-party listener is actually registered.
        if (FastSyncEvents.FastSyncSaveEvent.getHandlerList().getRegisteredListeners().length > 0) {
            dirtyMask.markAllDirty(uuid);
        }
        return dirtyMask.snapshotDirty(uuid);
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

        // Paper 1.21.11 registry entries. This project intentionally targets
        // that API directly; there is no old-version values() fallback.
        List<org.bukkit.Material> items = new ArrayList<>();
        List<org.bukkit.Material> blocks = new ArrayList<>();
        org.bukkit.Registry.MATERIAL.forEach(mat -> {
            if (mat.isItem()) items.add(mat);
            if (mat.isBlock()) blocks.add(mat);
        });
        this.itemMaterials = List.copyOf(items);
        this.blockMaterials = List.copyOf(blocks);

        List<org.bukkit.entity.EntityType> alive = new ArrayList<>();
        org.bukkit.Registry.ENTITY_TYPE.forEach(e -> {
            if (e.isAlive()) alive.add(e);
        });
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
                    // Bukkit exposes no criterion timestamp and cannot restore
                    // one. Store a stable sentinel so an unchanged advancement
                    // snapshot remains byte-for-byte stable across saves.
                    criteria.put(awarded, 0L);
                }
                // Keep an explicit empty map. Minecraft's advancement file is
                // authoritative: an empty criterion set must revoke progress
                // that may exist on the target server.
                advancements.put(key, criteria);
            }
            data.setAdvancements(advancements);
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect advancements: " + e.getMessage());
            }
        }
    }

    /** Minecraft respawns dead players at the effective max-health value. */
    static double healthForSave(boolean dead, double currentHealth, double effectiveMaxHealth) {
        return dead ? effectiveMaxHealth : currentHealth;
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
            // P1 (issue #59): use Registry.ATTRIBUTE (the 1.21+ API) instead of
            // the deprecated Attribute.values(). Attribute is no longer an enum
            // — it is a Registry<Attribute> — and Attribute.values() may be
            // removed on a future Paper release. The cache is invalidated on
            // /fastsync reload so custom attributes registered by other plugins
            // after FastSync onEnable are picked up.
            if (cachedAttributes == null) {
                java.util.List<Attribute> list = new java.util.ArrayList<>();
                org.bukkit.Registry.ATTRIBUTE.forEach(list::add);
                cachedAttributes = list.toArray(new Attribute[0]);
            }
            for (Attribute attr : cachedAttributes) {
                try {
                    AttributeInstance instance = player.getAttribute(attr);
                    if (instance == null) continue;

                    String key = attr.getKey().toString();
                    double baseValue = instance.getBaseValue();
                    // Paper's public AttributeInstance#getModifiers() returns
                    // permanent AND transient modifiers together, while
                    // addModifier() always creates a permanent modifier. Copying
                    // that collection would turn equipment, potion and movement
                    // modifiers into persistent player data. The API exposes no
                    // way to identify the permanent subset, so FastSync safely
                    // synchronizes base values only and leaves runtime modifiers
                    // owned by their source systems.
                    attributes.add(new PlayerData.AttributeData(key, baseValue));
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
            // P0 (issue #53): apply symmetric award + revoke. Previously only
            // award was performed — criteria revoked on the source server (via
            // /advancement revoke or plugin API) were silently lost on cross-
            // server load. MC's PlayerAdvancements.load does the symmetric
            // operation; this mirrors it.
            //
            // If data.getAdvancements() is null (the collect path was skipped
            // entirely, e.g. sync-advancements=false at save time), do NOT
            // touch the player's advancements — preserve target state.
            if (data.getAdvancements() == null) return;
            for (org.bukkit.advancement.Advancement adv : cachedAdvancements) {
                String key = adv.getKey().toString();
                // savedCriteria is the authoritative set: criteria in this set
                // should be awarded; criteria NOT in this set should be revoked.
                // An empty set (key present, empty map) means "all revoked".
                // A missing key means "we have no info — skip" (legacy compat).
                Map<String, Long> criteriaMap = data.getAdvancements().get(key);
                if (criteriaMap == null) continue;
                java.util.Set<String> savedCriteria = criteriaMap.keySet();

                org.bukkit.advancement.AdvancementProgress progress = player.getAdvancementProgress(adv);
                if (progress == null) continue;

                java.util.Collection<String> currentCriteria = progress.getAwardedCriteria();

                // Award: saved - current
                for (String criterion : savedCriteria) {
                    if (!currentCriteria.contains(criterion)) {
                        try {
                            progress.awardCriteria(criterion);
                        } catch (Exception ignored) {}
                    }
                }
                // Revoke: current - saved (was missing — silent loss of revocations)
                for (String criterion : new java.util.ArrayList<>(currentCriteria)) {
                    if (!savedCriteria.contains(criterion)) {
                        try {
                            progress.revokeCriteria(criterion);
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

            // Apply typed stats via strategy (if enabled). The inline backward-
            // compat path that applied typed stats without a strategy was
            // removed (clean-slate): a payload should only carry typed stats
            // when the strategy was enabled at save time. If typed stats are
            // present but no strategy is active, warn and ignore rather than
            // silently re-applying them through a legacy code path.
            if (!typedStatsData.isEmpty()) {
                if (typedStatsStrategy != null) {
                    typedStatsStrategy.restore(player, typedStatsData);
                } else {
                    logger.warning("[Stats] Typed statistics present in payload for " + player.getUniqueId()
                        + " but typed-stats strategy is disabled — ignoring " + typedStatsData.size()
                        + " typed categories. Enable the strategy or resave the player.");
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

                    // Do not touch modifiers here. Paper exposes all modifiers as
                    // one collection but cannot tell callers which are transient;
                    // removing/re-adding them would corrupt equipment/potion state.
                    instance.setBaseValue(attrData.getBaseValue());
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
        if (kind == SaveKind.SHUTDOWN) {
            shuttingDown = true;
        }
        // S4 fix (round 13): reject /saveall (BULK) during shutdown to avoid
        // racing with the SHUTDOWN save path. The SHUTDOWN path already
        // dispatches saves for all online players; a concurrent /saveall
        // would duplicate work and could overwrite the SHUTDOWN save's
        // final state with a stale BULK snapshot.
        if (shuttingDown && kind != SaveKind.SHUTDOWN) {
            logger.warning("[FastSync] /saveall rejected — server is shutting down. "
                + "The shutdown save path will persist all online players.");
            return new SaveAllResult(0, 0, 0, new HashMap<>());
        }
        return savePlayersSnapshot(new ArrayList<>(Bukkit.getOnlinePlayers()), kind);
    }

    /**
     * Save a snapshot of players' data. The player list must have been
     * collected on the main thread or global region thread (Folia-safe).
     *
     * <p><b>Threading:</b> This method performs two phases:
     * <ol>
     *   <li><b>Dispatch phase</b> — iterates the player list, reads
     *       {@code player.getUniqueId()}, and calls
     *       {@link SchedulerUtil#runAtEntity}. These operations touch the
     *       Player object and MUST run on the global region (or main thread
     *       on Paper). On Folia, calling them from an async thread is
     *       forbidden — entity state is owned by the entity's region thread.</li>
     *   <li><b>Wait phase</b> — blocks on {@code future.get()} for each
     *       dispatched save. This phase does NOT touch any Player object and
     *       is safe to run on any thread (async is fine).</li>
     * </ol>
     *
     * <p>For the SHUTDOWN path (called from {@code onDisable}), both phases
     * run on the calling thread (main/global) — that's correct because
     * shutdown must block until saves complete.
     *
     * <p>For the /saveall command path, callers should use
     * {@link #dispatchPlayerSaves} on the global region, then
     * {@link #waitForPlayerSaves} on an async thread, so the global region is
     * not blocked for the duration of the DB waits. See {@code FastSync.java}
     * for the canonical usage.
     *
     * @param players pre-collected player list (from main/global thread)
     * @param kind    BULK for /saveall (keep lock), SHUTDOWN for onDisable (release lock)
     * @return result with total/success/failed counts
     */
    public SaveAllResult savePlayersSnapshot(List<Player> players, SaveKind kind) {
        // P0 (round 15): belt-and-suspenders guard. onDisable is supposed to
        // call beginShutdown() before this, but if a caller forgets, the
        // SHUTDOWN path itself closes the online-save gate so a late periodic
        // save cannot race with the final save.
        if (kind == SaveKind.SHUTDOWN) {
            shuttingDown = true;
        }
        // Phase 1: dispatch (touches Player — must be on global/main thread)
        List<Map.Entry<UUID, CompletableFuture<SaveResult>>> futures = dispatchPlayerSaves(players, kind);
        // Phase 2: wait (no Player access — safe on any thread, including the calling thread)
        return waitForPlayerSaves(futures, kind);
    }

    /**
     * Phase 1 of {@link #savePlayersSnapshot}: dispatch per-player saves via
     * the entity scheduler. Returns the list of (uuid, future) pairs to wait on.
     *
     * <p><b>Must be called on the global region (or main thread on Paper).</b>
     * This method reads {@code player.getUniqueId()} and dispatches via
     * {@link SchedulerUtil#runAtEntity} — both touch the Player object.
     */
    public List<Map.Entry<UUID, CompletableFuture<SaveResult>>> dispatchPlayerSaves(
            List<Player> players, SaveKind kind) {
        List<Map.Entry<UUID, CompletableFuture<SaveResult>>> futures = new ArrayList<>();
        Plugin plugin = JavaPlugin.getPlugin(FastSync.class);

        for (Player player : players) {
            if (!activePlayers.containsKey(player.getUniqueId())) {
                continue;
            }
            UUID uuid = player.getUniqueId();

            // P0 (round 11): BULK (/saveall) is an online save — claim the
            // onlineSaveInFlight gate before collecting, same as savePlayerAsync.
            // This prevents a BULK save's stale snapshot from overwriting a
            // periodic/death save's fresh snapshot that commits in between.
            //
            // SHUTDOWN is a final save — mark finalSaveInProgress so new online
            // saves for this UUID are rejected (savePlayerAsync checks this set).
            // SHUTDOWN does NOT use onlineSaveInFlight because it must complete
            // even if an online save is in flight (the online save will finish
            // first via saveLock, then SHUTDOWN proceeds).
            final boolean isBulk = (kind == SaveKind.BULK);
            final java.util.concurrent.atomic.AtomicBoolean bulkGate;
            if (isBulk) {
                bulkGate = onlineSaveInFlight.computeIfAbsent(uuid, k -> new java.util.concurrent.atomic.AtomicBoolean(false));
                if (!bulkGate.compareAndSet(false, true)) {
                    futures.add(Map.entry(uuid, CompletableFuture.completedFuture(
                        SaveResult.error("online save in flight", SaveFailureReason.QUEUE_FULL))));
                    continue;
                }
            } else {
                bulkGate = null;
            }
            if (kind.releaseLock) {
                finalSaveInProgress.add(uuid);
            }

            CompletableFuture<SaveResult> future = new CompletableFuture<>();
            SchedulerUtil.runAtEntity(plugin, player, () -> {
                try {
                    // Snapshot dirty state BEFORE collectPlayerData — same
                    // rationale as savePlayerAsync. BULK (/saveall) is an
                    // online save (releaseLock=false) and needs epoch
                    // protection. SHUTDOWN is releaseLock=true and uses
                    // clearAll() (player is leaving anyway).
                    final com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot preSaveSnapshot =
                        snapshotDirtyForSave(uuid, kind);

                    PlayerData data = canCollectComponentsOnly(uuid, kind, preSaveSnapshot)
                        ? collectPlayerData(player, preSaveSnapshot)
                        : collectPlayerData(player);
                    if (!fireSaveEvent(player, data, kind)) {
                        if (isBulk && bulkGate != null) {
                            bulkGate.set(false);
                        }
                        if (kind.releaseLock) {
                            finalSaveInProgress.remove(uuid);
                            activePlayers.remove(uuid);
                            Long ft = playerFencingTokens.remove(uuid);
                            String session = playerLockSessions.remove(uuid);
                            playerVersions.remove(uuid);
                            clearComponentBaseline(uuid);
                            if (dirtyMask != null) dirtyMask.remove(uuid);
                            releaseLockAsyncBestEffort(uuid, ft, session,
                                "cancelled " + kind + " FastSyncSaveEvent");
                        }
                        future.complete(SaveResult.error(
                            "save cancelled by FastSyncSaveEvent", SaveFailureReason.CANCELLED));
                        return;
                    }
                    pendingSaveCount.incrementAndGet();
                    java.util.concurrent.locks.ReentrantLock saveLock =
                        playerSaveLocks.computeIfAbsent(uuid, k -> new java.util.concurrent.locks.ReentrantLock());
                    // Round 16 (P0 #3): final saves (SHUTDOWN, releaseLock=true)
                    // go to the dedicated finalSaveExecutor so they are not
                    // blocked behind periodic traffic and do not fall back to
                    // synchronous execution on the entity/global thread when
                    // the main pool is saturated. BULK (/saveall) is an online
                    // save and stays on the main asyncExecutor.
                    AsyncExecutor targetExecutor = kind.releaseLock ? finalSaveExecutor : asyncExecutor;
                    try {
                        targetExecutor.execute(() -> {
                            SaveResult result;
                            try {
                                saveLock.lock();
                                try {
                                    // Refresh version/fencingToken after acquiring lock —
                                    // an in-flight save may have advanced the version
                                    // while we waited for the lock.
                                    refreshVersionAndFencingToken(uuid, data);
                                    result = kind.releaseLock
                                        ? persistFinalSaveWithSpool(
                                            uuid, data, kind, preSaveSnapshot,
                                            kind + " asynchronous save failed")
                                        : persistCollectedData(uuid, data, kind, preSaveSnapshot);
                                } finally {
                                    saveLock.unlock();
                                }
                            } catch (Exception e) {
                                result = SaveResult.error(e.getMessage(), SaveFailureReason.DB_UNAVAILABLE);
                            } finally {
                                pendingSaveCount.decrementAndGet();
                                // P0 (round 11): release BULK gate after DB commit.
                                if (isBulk && bulkGate != null) {
                                    bulkGate.set(false);
                                }
                                if (kind.releaseLock) {
                                    finalSaveInProgress.remove(uuid);
                                }
                            }
                            future.complete(result);
                        });
                    } catch (java.util.concurrent.RejectedExecutionException e) {
                        pendingSaveCount.decrementAndGet();
                        if (kind == SaveKind.SHUTDOWN) {
                            // SHUTDOWN: synchronous fallback — must persist data.
                            // The final-save queue is full, but shutdown cannot
                            // skip saves. Log at SEVERE: this runs DB I/O on the
                            // entity/global thread and blocks the tick loop.
                            recordFinalSaveQueueFull(
                                "SHUTDOWN", uuid,
                                "running synchronously on event thread as fallback. "
                                    + "Investigate DB latency or raise final-save.queue-capacity.",
                                e, FinalSaveQueueFullOutcome.SYNC_FALLBACK);
                            SaveResult result;
                            try {
                                saveLock.lock();
                                try {
                                    refreshVersionAndFencingToken(uuid, data);
                                    result = persistFinalSaveWithSpool(
                                        uuid, data, kind, preSaveSnapshot,
                                        kind + " synchronous queue-full fallback failed");
                                } finally {
                                    saveLock.unlock();
                                }
                            } catch (Exception ex) {
                                result = SaveResult.error(ex.getMessage(), SaveFailureReason.DB_UNAVAILABLE);
                            }
                            if (kind.releaseLock) {
                                finalSaveInProgress.remove(uuid);
                            }
                            future.complete(result);
                        } else {
                            // BULK (/saveall): queue full is acceptable — skip.
                            logger.log(Level.WARNING, "Async queue full during " + kind + " save for " + uuid, e);
                            if (isBulk && bulkGate != null) {
                                bulkGate.set(false);
                            }
                            future.complete(SaveResult.error("async queue full", SaveFailureReason.QUEUE_FULL));
                        }
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to collect data for " + uuid + " during " + kind + " save", e);
                    // Release gates on collect failure
                    if (isBulk && bulkGate != null) {
                        bulkGate.set(false);
                    }
                    if (kind.releaseLock) {
                        finalSaveInProgress.remove(uuid);
                    }
                    future.complete(SaveResult.error(e.getMessage(), SaveFailureReason.DB_UNAVAILABLE));
                }
            }, () -> {
                // retired callback — release gates
                if (isBulk && bulkGate != null) {
                    bulkGate.set(false);
                }
                if (kind.releaseLock) {
                    finalSaveInProgress.remove(uuid);
                }
                future.complete(SaveResult.error("entity retired", SaveFailureReason.ENTITY_RETIRED));
            }, kind == SaveKind.SHUTDOWN);
            futures.add(Map.entry(uuid, future));
        }
        return futures;
    }

    /**
     * Phase 2 of {@link #savePlayersSnapshot}: wait for all dispatched saves to
     * complete (with a deadline) and aggregate results.
     *
     * <p><b>Safe to call from any thread.</b> This method does NOT touch any
     * Player object — it only waits on {@link CompletableFuture}s.
     */
    public SaveAllResult waitForPlayerSaves(
            List<Map.Entry<UUID, CompletableFuture<SaveResult>>> futures, SaveKind kind) {
        int total = futures.size();
        int success = 0;
        int failed = 0;
        Map<UUID, String> failures = new HashMap<>();

        // Wait for all DB saves with a dynamic deadline.
        // SHUTDOWN: max(30s, online * 500ms) — scales for large servers.
        // BULK: fixed 30s — operator command, can retry.
        long deadlineMs = (kind == SaveKind.SHUTDOWN)
            ? Math.max(30_000L, total * 500L)
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

        // P0 (round 11): reject new online saves during shutdown or when a
        // final save is in progress for this UUID. The final save must be the
        // last write; a racing online save could collect a snapshot that
        // overwrites the final state.
        if (shuttingDown) {
            if (config.isDebug()) {
                logger.fine("Skipping " + kind + " save for " + uuid + " — server is shutting down");
            }
            return;
        }
        if (finalSaveInProgress.contains(uuid)) {
            if (config.isDebug()) {
                logger.fine("Skipping " + kind + " save for " + uuid + " — final save in progress");
            }
            return;
        }

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
        //
        // CRITICAL ORDERING: the conservative dirty marking for components
        // without event coverage (PDC, STATISTICS, ATTRIBUTES, ADVANCEMENTS)
        // MUST happen BEFORE the "no dirty → skip" check. Otherwise, when a
        // plugin modifies PDC/stats/attrs/advancements via API without firing
        // a Bukkit event, no dirty bit gets set, the skip check returns early,
        // and those changes are silently dropped until the next validation
        // cycle (default every 5th save). If the server crashes in that
        // window, the changes are lost.
        if (dirtyMask != null && finalKind == SaveKind.PERIODIC) {
            boolean shouldValidate = dirtyMask.recordSaveAndCheckValidation(uuid);
            if (shouldValidate) {
                dirtyMask.markAllDirty(uuid);
            } else {
                // API mutation safety: Bukkit events do not cover direct API
                // modifications (e.g. player.getInventory().setItem() by a
                // plugin). Without conservative marking, these changes would
                // be silently lost between validation cycles.
                //
                // This logic runs regardless of component-storage.enabled —
                // even with full Blob saves, a skipped periodic save means
                // the change is not persisted until quit/validation.
                switch (config.getApiMutationSafetyMode()) {
                    case STRICT -> markApiMutationRiskComponents(uuid);
                    case BALANCED -> {
                        int interval = config.getApiMutationFullComponentScanInterval();
                        if (dirtyMask.recordApiMutationScanAndCheck(uuid, interval)) {
                            markApiMutationRiskComponents(uuid);
                        } else {
                            markHighRiskApiMutationComponents(uuid);
                        }
                    }
                    case API_ONLY -> { /* rely on FastSyncApi.markDirty() from plugins */ }
                }
            }
            if (!shouldValidate && !dirtyMask.isAnyDirty(uuid)) {
                if (config.isDebug()) {
                    logger.fine("Skipping periodic save for " + uuid + " — no dirty components");
                }
                return;
            }
        }

        // P0 in-flight gate (round 9): claim the per-UUID online-save slot
        // BEFORE collecting. If another online save is already in-flight for
        // this UUID, skip this one entirely — the in-flight save will pick up
        // the latest state on its own collect. This prevents the "stale
        // snapshot wins" race:
        //   T1: save A collects old snapshot
        //   T2: save B collects new snapshot, commits first (v10→v11)
        //   T3: save A acquires saveLock, refreshVersion to v11, CAS-succeeds
        //       with old data → B's new state overwritten by A's old snapshot.
        // The saveLock alone does NOT prevent this because it serializes DB
        // writes, not collects. The in-flight gate ensures at most one online
        // save collects+writes per UUID at a time.
        //
        // Only for online saves (releaseLock=false). QUIT/SHUTDOWN must save
        // final state and are handled by collectAndSavePlayerData / savePlayersSnapshot,
        // which use the saveLock (lock+wait, not tryLock) and run their collect
        // immediately before dispatch.
        if (!finalKind.releaseLock) {
            java.util.concurrent.atomic.AtomicBoolean inFlight =
                onlineSaveInFlight.computeIfAbsent(uuid, k -> new java.util.concurrent.atomic.AtomicBoolean(false));
            if (!inFlight.compareAndSet(false, true)) {
                if (config.isDebug()) {
                    logger.fine("Skipping " + finalKind + " save for " + uuid + " — online save already in-flight (coalesced)");
                }
                return;
            }
        }

        // Collect player data on the entity's region thread (Folia-safe)
        SchedulerUtil.runAtEntity(plugin, player, () -> {
            // CRITICAL: snapshot dirty state BEFORE collectPlayerData runs.
            // Any markDirty() that arrives during collectPlayerData / serialize /
            // DB write (on another thread or via a Bukkit event fired from the
            // collect path itself) will bump the epoch and be PRESERVED by
            // clearDirty(snapshot) after the save. Without this pre-collect
            // snapshot, the save would clear dirty bits for changes that the
            // full Blob does NOT contain — losing them until the next event.
            //
            // Only online saves need this. QUIT/SHUTDOWN are releaseLock=true
            // and use clearAll() unconditionally (player is leaving, no more
            // changes will arrive).
            final com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot preSaveSnapshot =
                snapshotDirtyForSave(uuid, finalKind);

            PlayerData data;
            try {
                data = canCollectComponentsOnly(uuid, finalKind, preSaveSnapshot)
                    ? collectPlayerData(player, preSaveSnapshot)
                    : collectPlayerData(player);
                if (!fireSaveEvent(player, data, finalKind)) {
                    if (!finalKind.releaseLock) {
                        java.util.concurrent.atomic.AtomicBoolean inFlight = onlineSaveInFlight.get(uuid);
                        if (inFlight != null) inFlight.set(false);
                    }
                    return;
                }
            } catch (Throwable t) {
                // collect failed — release gate so future saves can proceed.
                // (The gate must NOT stay set on collect failure, otherwise
                // the player is stuck with no online saves until quit.)
                if (!finalKind.releaseLock) {
                    java.util.concurrent.atomic.AtomicBoolean inFlight = onlineSaveInFlight.get(uuid);
                    if (inFlight != null) inFlight.set(false);
                }
                throw t;
            }

            pendingSaveCount.incrementAndGet();
            java.util.concurrent.locks.ReentrantLock saveLock =
                playerSaveLocks.computeIfAbsent(uuid, k -> new java.util.concurrent.locks.ReentrantLock());
            try {
                asyncExecutor.execute(() -> {
                    // Online save: skip if a save is already in progress for this player.
                    // The next periodic tick will pick up the latest data — coalescing
                    // avoids unnecessary version conflicts and saves CPU/DB load.
                    boolean locked = false;
                    try {
                        if (!saveLock.tryLock()) {
                            if (config.isDebug()) {
                                logger.fine("Skipping " + finalKind + " save for " + uuid + " — save already in progress");
                            }
                            return;
                        }
                        locked = true;
                        // CRITICAL: refresh version/fencingToken after acquiring the lock.
                        // Without this, if a previous periodic/death save completed first
                        // and advanced the DB version, this save will CAS-fail with a stale
                        // version and be treated as a serious lock-infringement conflict.
                        refreshVersionAndFencingToken(uuid, data);
                        persistCollectedData(uuid, data, finalKind, preSaveSnapshot);
                    } catch (Exception e) {
                        logger.log(Level.WARNING, finalKind + " save failed for " + uuid, e);
                    } finally {
                        if (locked) {
                            saveLock.unlock();
                        }
                        pendingSaveCount.decrementAndGet();
                        // P0 (round 10): gate released HERE — in the async DB
                        // task's finally, AFTER the DB write completed (or
                        // failed/skipped). The previous code released the gate
                        // in the entity-thread lambda's finally, which ran as
                        // soon as the async task was SUBMITTED (not completed).
                        // That left a window where a second online save could
                        // collect a new snapshot and commit it BEFORE this
                        // save's async DB write ran — and because
                        // refreshVersionAndFencingToken() bumps the stale
                        // snapshot's version to the latest, this save would
                        // CAS-succeed with old data and overwrite the new state.
                        // Releasing the gate only after the DB write completes
                        // ensures collect+serialize+DB-commit are serialized
                        // per-UUID for online saves.
                        if (!finalKind.releaseLock) {
                            java.util.concurrent.atomic.AtomicBoolean inFlight = onlineSaveInFlight.get(uuid);
                            if (inFlight != null) inFlight.set(false);
                        }
                    }
                });
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // Queue full — online save can be skipped (coalesced to next tick).
                // The player is still online; quit save will persist final state.
                pendingSaveCount.decrementAndGet();
                if (config.isDebug()) {
                    logger.fine("Skipping " + finalKind + " save for " + uuid + " — async queue full");
                }
                // Release gate — async task never submitted, so its finally
                // never runs. Without this release the gate would stay set
                // and block all future online saves for this UUID.
                if (!finalKind.releaseLock) {
                    java.util.concurrent.atomic.AtomicBoolean inFlight = onlineSaveInFlight.get(uuid);
                    if (inFlight != null) inFlight.set(false);
                }
            }
        }, () -> {
            // retired callback: entity no longer valid (player logged out during save tick).
            // The entity-thread lambda above never ran, so its finally block
            // never released the in-flight gate. Release it here.
            if (!finalKind.releaseLock) {
                java.util.concurrent.atomic.AtomicBoolean inFlight = onlineSaveInFlight.get(uuid);
                if (inFlight != null) {
                    inFlight.set(false);
                }
            }
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
     * Determine whether a SQLException represents a connection-level failure
     * (as opposed to a statement-level error like a syntax error or deadlock).
     *
     * <p>SQLState class 08xxx covers connection exceptions as defined by the
     * SQL standard. When the connection itself is broken, per-player fallback
     * refreshes will also fail — so the caller can skip them and go straight
     * to failure counting.
     *
     * @param e the SQLException to classify
     * @return true if this is a connection-level failure
     */
    public static boolean isConnectionFailure(SQLException e) {
        if (e == null) return false;
        // SQLTransientConnectionException is the JDBC mapping for SQLState
        // class 08 (connection exception), but its constructors do not always
        // populate the SQLState field — so check the type as well.
        if (e instanceof java.sql.SQLTransientConnectionException) return true;
        String sqlState = e.getSQLState();
        return sqlState != null && sqlState.startsWith("08");
    }

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
                databaseManager.refreshLockBatch(playersToRefresh, playerLockSessions, serverName, failedPlayers);
            } catch (SQLException e) {
                batchFailed = true;
                if (isConnectionFailure(e)) {
                    // Connection-level failure — per-player refresh will also fail.
                    // Skip the fallback to avoid N pointless round-trips and go
                    // straight to failure counting.
                    logger.log(Level.WARNING, "[Heartbeat] Batch refresh failed with connection error; skipping per-player fallback", e);
                    failedPlayers.addAll(playersToRefresh.keySet());
                } else {
                    logger.log(Level.WARNING, "[Heartbeat] Batch refresh failed; falling back to per-player", e);
                    // Fallback: per-player refresh
                    for (java.util.Map.Entry<UUID, Long> entry : playersToRefresh.entrySet()) {
                        try {
                            if (!databaseManager.refreshLock(entry.getKey(), serverName, entry.getValue(), playerLockSessions.get(entry.getKey()))) {
                                failedPlayers.add(entry.getKey());
                            }
                        } catch (SQLException ex) {
                            logger.log(Level.WARNING, "[Heartbeat] Per-player refresh failed for " + entry.getKey(), ex);
                            failedPlayers.add(entry.getKey());
                        }
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
     * Kick a player whose lock was lost during heartbeat.
     *
     * <p><b>Folia-safety:</b> Two-phase dispatch:
     * <ol>
     *   <li>Global region — look up the Player by UUID. {@code Bukkit.getPlayer(uuid)}
     *       is safe on the global region; it does not require the entity's region
     *       thread. This phase verifies the player is still online before
     *       attempting the kick.</li>
     *   <li>Entity scheduler — call {@code player.kick(...)}. On Folia, kicking
     *       a player modifies entity state, which must happen on the entity's
     *       own region thread. The global region is NOT the same as the player's
     *       region (a player can be in any region of any world), so we must
     *       dispatch from global → entity scheduler.</li>
     * </ol>
     *
     * <p>The retired callback handles the case where the entity is no longer
     * valid by the time the scheduler tries to run the task (player already
     * logged out between the two phases).
     */
    private void kickPlayerForLockLoss(UUID uuid) {
        Plugin plugin = JavaPlugin.getPlugin(FastSync.class);
        SchedulerUtil.runGlobal(plugin, () -> {
            Player player = org.bukkit.Bukkit.getPlayer(uuid);
            if (player == null || !player.isOnline()) {
                if (config.isDebug()) {
                    logger.fine("[Heartbeat] Player " + uuid + " already offline; no kick needed.");
                }
                return;
            }
            // Dispatch the kick to the player's own region thread. On Paper,
            // runAtEntity falls back to the main thread, which is fine.
            SchedulerUtil.runAtEntity(plugin, player, () -> {
                if (player.isOnline()) {
                    player.kick(net.kyori.adventure.text.Component.text(
                        "[FastSync] Your data lock was lost. Please reconnect to re-sync your data.",
                        net.kyori.adventure.text.format.NamedTextColor.RED));
                }
            }, () -> {
                // Entity retired between lookup and dispatch — player is gone, nothing to kick.
                if (config.isDebug()) {
                    logger.fine("[Heartbeat] Player " + uuid
                        + " entity retired before kick; no kick needed.");
                }
            });
        });
    }

    public void cleanupStaleEntries() {
        long now = System.currentTimeMillis();
        long staleThreshold = 5 * 60 * 1000; // 5 minutes

        pendingLoadTimes.forEach((uuid, loadTime) -> {
        if (loadTime != null && (now - loadTime) > staleThreshold) {
            pendingData.remove(uuid);
            pendingEmptyData.remove(uuid);
            boolean wasBypassed = pendingBypass.remove(uuid);
            pendingLoadTimes.remove(uuid);
            // Clean up ALL tracking maps to prevent memory leaks during login storms
            playerVersions.remove(uuid);
            Long ft = playerFencingTokens.remove(uuid); String lockSession = playerLockSessions.remove(uuid);
            failedJoinPlayers.remove(uuid);
            quarantinedPlayers.remove(uuid);
            clearComponentBaseline(uuid);
            if (wasBypassed) {
                logger.warning("Cleaned up stale bypassed pre-login state for " + uuid);
                return;
            }
            asyncExecutor.execute(() -> {
                try {
                    // Release via the unified sync helper (already on the async
                    // thread). Fail-closed on null ft/lockSession; catches
                    // SQLException | RuntimeException so no defensive guard
                    // can leak the lock.
                    releaseOwnedLockAndNotify(uuid, ft, lockSession, "stale pending cleanup");
                    logger.warning("Cleaned up stale pending data for " + uuid);
                } catch (Exception e) {
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
                || pendingEmptyData.contains(uuid)
                || pendingBypass.contains(uuid)) {
                return false;
            }
            java.util.concurrent.locks.ReentrantLock lock = e.getValue();
            return !lock.isLocked() && !lock.hasQueuedThreads();
        });

        // Also clean up onlineSaveInFlight entries for players no longer
        // active/pending. The gate is normally released by the entity-thread
        // finally block or the retired callback, but if a player disconnects
        // mid-save and the entity scheduler neither ran the task nor invoked
        // the retired callback (edge case during shutdown), the entry would
        // leak. Removing entries for inactive players is safe — the gate is
        // only consulted by savePlayerAsync, which is gated on activePlayers.
        onlineSaveInFlight.entrySet().removeIf(e -> {
            UUID uuid = e.getKey();
            return !activePlayers.containsKey(uuid)
                && !pendingData.containsKey(uuid)
                && !pendingEmptyData.contains(uuid)
                && !pendingBypass.contains(uuid);
        });
    }

    // ==================== Shutdown ====================

    /**
     * Mark the manager as shutting down. Once set, all new online saves
     * (PERIODIC/DEATH/WORLD_SAVE/BULK) are rejected by {@link #savePlayerAsync}
     * so they cannot race with the SHUTDOWN save and overwrite a player's
     * final state.
     *
     * <p><b>Must be called before {@link #saveAllOnlinePlayers} / {@link #savePlayersSnapshot}
     * in {@code onDisable}.</b> Without this, a periodic save task that fires
     * during the shutdown-save window could collect a stale snapshot and
     * commit it after the SHUTDOWN save, rolling back the final state.
     *
     * <p>{@link #savePlayersSnapshot} with {@link SaveKind#SHUTDOWN} also sets
     * this flag as a belt-and-suspenders guard, so even if a caller forgets to
     * call {@code beginShutdown()} the SHUTDOWN path itself closes the gate.
     */
    public void beginShutdown() {
        shuttingDown = true;
    }

    /**
     * Shut down the sync manager, closing Redis and thread pool.
     */
    public void shutdown() {
        // Idempotent: ensure the shutdown flag is set even if beginShutdown()
        // was not called. This prevents any online save dispatched by a stale
        // scheduled task from racing with the final shutdown wait.
        shuttingDown = true;
        // Log final latency stats before shutdown
        logLatencyStats();

        // Stop disk-spool replay before closing Redis or the database. An
        // in-flight replay may publish RELEASED after its DB CAS succeeds.
        if (finalSaveReplayService != null) {
            finalSaveReplayService.stop();
            finalSaveReplayService = null;
        }

        // Wait for pending saves first. Timeout is configurable for large
        // servers or slow DBs (default 30s, min 5s).
        long pendingTimeout = config.getShutdownPendingSaveTimeoutMs();
        waitForPendingSaves(pendingTimeout);

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

        // Round 16 (P0 #3): shut down the final-save executor AFTER the main
        // pool. waitForPendingSaves() above waits on pendingSaveCount, which
        // is incremented by QUIT saves submitted to either executor, so by
        // the time we reach here all final saves have completed. The shutdown
        // timeout is longer than the main pool's because final saves may
        // still be retrying under a same-fencing self-conflict (up to 3
        // attempts).
        if (finalSaveExecutor != null) {
            finalSaveExecutor.shutdown(config.getShutdownFinalSaveExecutorTimeoutSeconds());
            finalSaveExecutor = null;
        }

        // Round 14: close SnapshotManager's dedicated executor
        if (snapshotManager != null) {
            snapshotManager.close();
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

        public final String causeName;
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

    /**
     * Structured failure reason for save operations.
     *
     * <p>Inspired by CockroachDB's retry error classification — conflicts are
     * normal paths, not exceptions. Callers can {@code switch} on the reason
     * to apply different retry strategies:
     * <ul>
     *   <li>{@link #SAME_FENCING_SELF_CONFLICT} — retry with actual DB version</li>
     *   <li>{@link #FENCING_MISMATCH} — do not retry; lock was lost</li>
     *   <li>{@link #VERSION_CONFLICT} — external write; trigger conflict recovery</li>
     *   <li>{@link #DB_UNAVAILABLE} — retry with backoff</li>
     *   <li>{@link #SERIALIZATION_ERROR} — do not retry; data is corrupt</li>
     * </ul>
     */
    public enum SaveFailureReason {
        /** Save succeeded. */
        NONE,
        /** Version CAS failed but fencing token matches — our own previous save advanced the version. Retryable. */
        SAME_FENCING_SELF_CONFLICT,
        /** Fencing token mismatch — we lost the lock to another server. NOT retryable. */
        FENCING_MISMATCH,
        /** Version conflict with an external writer. Triggers conflict recovery. */
        VERSION_CONFLICT,
        /** Lock was not held (locked_by mismatch). NOT retryable — must re-acquire lock. */
        LOCK_NOT_HELD,
        /** Component generation mismatch — stale component row tried to override. NOT retryable. */
        COMPONENT_GENERATION_MISMATCH,
        /** Database unreachable or SQL error. Retryable with backoff. */
        DB_UNAVAILABLE,
        /** Serialization/compression failed. NOT retryable — data is likely corrupt. */
        SERIALIZATION_ERROR,
        /** Async executor queue full. Retryable on next tick. */
        QUEUE_FULL,
        /** Entity retired (player offline). NOT retryable. */
        ENTITY_RETIRED,
        /** A FastSyncSaveEvent listener explicitly cancelled this save. */
        CANCELLED,
        /** Component save rejected by fencing validation. Fall back to full Blob save. */
        COMPONENT_SAVE_REJECTED,
        /** Catch-all for unexpected errors. */
        UNKNOWN
    }

    /** Result of a save operation. */
    public record SaveResult(boolean success, long expectedVersion, long actualVersion,
                             int compressedSize, String errorMessage,
                             SaveFailureReason failureReason) {
        public static SaveResult success(long version, int size) {
            return new SaveResult(true, version, version + 1, size, null, SaveFailureReason.NONE);
        }
        public static SaveResult success(long oldVersion, long newVersion, int size) {
            return new SaveResult(true, oldVersion, newVersion, size, null, SaveFailureReason.NONE);
        }
        public static SaveResult conflict(long expected, long actual, int size) {
            return new SaveResult(false, expected, actual, size, "version conflict", SaveFailureReason.VERSION_CONFLICT);
        }
        public static SaveResult conflict(long expected, long actual, int size, SaveFailureReason reason) {
            return new SaveResult(false, expected, actual, size, reason.name().toLowerCase().replace('_', ' '), reason);
        }
        public static SaveResult error(String msg) {
            return new SaveResult(false, 0, 0, 0, msg, SaveFailureReason.UNKNOWN);
        }
        public static SaveResult error(String msg, SaveFailureReason reason) {
            return new SaveResult(false, 0, 0, 0, msg, reason);
        }
        /** Convenience: returns true if this failure is retryable (same-fencing, DB, queue). */
        public boolean isRetryable() {
            return !success && switch (failureReason) {
                case SAME_FENCING_SELF_CONFLICT, DB_UNAVAILABLE, QUEUE_FULL -> true;
                default -> false;
            };
        }
    }

    /**
     * Classify an exception thrown during a save operation into a
     * {@link SaveFailureReason}.
     *
     * <ul>
     *   <li>{@link SaveFailureReason#DB_UNAVAILABLE} — SQLException (possibly
     *       wrapped in a RuntimeException)</li>
     *   <li>{@link SaveFailureReason#SERIALIZATION_ERROR} — IOException
     *       (encoding/compression failure)</li>
     *   <li>{@link SaveFailureReason#UNKNOWN} — anything else</li>
     * </ul>
     *
     * @param e the exception to classify
     * @return the appropriate SaveFailureReason
     */
    public static SaveFailureReason classifySaveException(Exception e) {
        if (e == null) return SaveFailureReason.UNKNOWN;
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof SQLException) return SaveFailureReason.DB_UNAVAILABLE;
            if (cause instanceof IOException) return SaveFailureReason.SERIALIZATION_ERROR;
            cause = cause.getCause();
        }
        return SaveFailureReason.UNKNOWN;
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
    /**
     * Component-only save with an explicit pre-collect dirty snapshot.
     *
     * <p>The snapshot MUST be taken BEFORE {@link #collectPlayerData(Player)}
     * runs. If it is taken after collect, a markDirty that arrived during
     * collect would already be in the snapshot, and clearDirty(snapshot)
     * after the DB write would clear it — losing the change (the component
     * row was written from the pre-collect PlayerData, which does not
     * contain the change).
     *
     * <p>See {@link #persistCollectedData(UUID, PlayerData, SaveKind,
     * com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot)} for the
     * full rationale.
     */
    private ComponentSaveOutcome persistComponentsOnly(UUID uuid, PlayerData data, SaveKind kind,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot dirtySnapshot) {
        try {
            // CRITICAL safety gate: refuse to do a component-only save unless the
            // player already has a non-empty full-Blob baseline in player_data.
            //
            // Without this gate, a brand-new player (whose player_data.data is
            // still empty because no full save has run yet) would have component
            // rows written successfully, but on next login loadPlayerDataRow() sees
            // data == null and treats the player as brand new — the component
            // overlay is skipped, and the player's state is silently lost.
            //
            // Returning FALLBACK_FULL_BLOB here forces the caller
            // (persistCollectedData) to fall back to a full Blob save, which
            // establishes the baseline. Subsequent periodic saves can then
            // safely take the component-only fast path because
            // playersWithBaseline now contains this UUID.
            if (!playersWithBaseline.contains(uuid)) {
                if (config.isDebug()) {
                    logger.fine("[Component] No baseline Blob for " + uuid
                        + " — falling back to full Blob save first.");
                }
                return ComponentSaveOutcome.fallbackFullBlob();
            }

            // Use the caller-provided snapshot (taken before collectPlayerData).
            // See method javadoc for why this must NOT be re-snapshot here.
            if (dirtySnapshot == null || dirtySnapshot.isEmpty()) {
                return ComponentSaveOutcome.fallbackFullBlob();  // nothing dirty (shouldn't happen — caller checks)
            }
            java.util.Set<com.fastsync.sync.dirty.ComponentDirtyMask.Component> dirty =
                dirtySnapshot.components();

            // Full validation and naturally all-dirty cycles refresh the
            // baseline in one row. Writing every component separately creates
            // a larger transaction and defeats validation's baseline refresh.
            if (dirty.size() >= com.fastsync.sync.dirty.ComponentDirtyMask.Component.values().length) {
                return ComponentSaveOutcome.fallbackFullBlob();
            }

            // Cap the batch size to avoid huge transactions
            if (dirty.size() > config.getComponentBatchSize()) {
                // Too many dirty components — full save is cheaper
                if (config.isDebug()) {
                    logger.fine("Component save for " + uuid + " skipped — too many dirty ("
                        + dirty.size() + " > " + config.getComponentBatchSize() + ")");
                }
                return ComponentSaveOutcome.fallbackFullBlob();
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
                // All dirty components produced no payload (e.g. all empty AND
                // the serializer returned null for them — currently INVENTORY's
                // offhand, ENDER_CHEST, POTION_EFFECTS, ADVANCEMENTS, STATS,
                // ATTRIBUTES, PDC all return null when empty).
                //
                // Do NOT clear dirty flags here and pretend success — that would
                // discard the dirty signal without bumping player_data.version
                // or writing anything to the DB, leaving the local
                // playerVersions map out of sync with reality and silently
                // masking future changes. Instead, fall back to a full Blob
                // save so the baseline gets rewritten with the current state
                // and version is properly advanced.
                if (config.isDebug()) {
                    logger.fine("[Component] All dirty components for " + uuid
                        + " produced empty payloads — falling back to full save.");
                }
                return ComponentSaveOutcome.fallbackFullBlob();
            }

            long serElapsed = (System.nanoTime() - startSer) / 1_000_000;
            if (serializeLatency != null) serializeLatency.record(serElapsed);

            // Calculate dirtyBits bitmask for component_bitmap update
            long dirtyBits = 0L;
            for (com.fastsync.sync.dirty.ComponentDirtyMask.Component c : dirty) {
                if (componentBlobs.containsKey(c.name())) {
                    dirtyBits |= c.storageMask();
                }
            }

            // Single-transaction component upsert with fencing validation.
            // This method does SELECT FOR UPDATE + validates locked_by/fencing_token
            // + upserts component rows + updates player_data.version/bitmap/locked_at
            // all in one DB transaction. If the lock was lost (GC pause, heartbeat
            // timeout), the transaction is rolled back and no component rows are written.
            long dbStart = System.nanoTime();
            ComponentCursor cursor = componentCursors.get(uuid);
            com.fastsync.database.DatabaseManager.ComponentBatchResult batchResult;
            if (cursor != null) {
                batchResult = databaseManager.upsertComponentsIfLockHeld(
                    uuid, componentBlobs, componentChecksums,
                    config.getServerName(), data.getFencingToken(),
                    playerLockSessions.get(uuid), data.getVersion(), dirtyBits,
                    cursor.generation(), cursor.bitmap());
            } else {
                // Defensive compatibility path for an in-memory session that
                // predates cursor initialization (e.g. hot reload). Normal
                // post-login traffic always takes the overload above.
                batchResult = databaseManager.upsertComponentsIfLockHeld(
                    uuid, componentBlobs, componentChecksums,
                    config.getServerName(), data.getFencingToken(),
                    playerLockSessions.get(uuid), data.getVersion(), dirtyBits);
            }
            long dbElapsed = (System.nanoTime() - dbStart) / 1_000_000;
            if (saveLatency != null) saveLatency.record(dbElapsed);

            if (!batchResult.success()) {
                // P0 (round 15): classify the rejection. Previously every
                // rejection fell back to a full Blob save — but a
                // lock/fencing/session mismatch means the lock is no longer
                // ours, and a full Blob save would overwrite a newer session's
                // data with our stale snapshot. A stale version means our
                // snapshot is behind the DB; falling back would also clobber
                // newer state. Only the "no baseline / no components" cases
                // are safe to fall back from.
                com.fastsync.database.DatabaseManager.ComponentRejectReason r = batchResult.reason();
                if (r == null) {
                    r = com.fastsync.database.DatabaseManager.ComponentRejectReason.SQL_ERROR;
                }
                switch (r) {
                    case NO_COMPONENTS, MISSING_BASELINE_ROW -> {
                        logger.warning("[Component] Save rejected for " + uuid
                            + " (" + r + "): " + batchResult.errorMessage()
                            + " — falling back to full Blob save.");
                        return ComponentSaveOutcome.fallbackFullBlob();
                    }
                    case STALE_VERSION -> {
                        // Our collected snapshot is behind the DB version.
                        // Skip THIS online save — do NOT fall back to a full
                        // Blob save with the stale snapshot (it would clobber
                        // the newer state). The next periodic cycle re-collects
                        // against the current version.
                        logger.warning("[Component] Save rejected for " + uuid
                            + " (STALE_VERSION): " + batchResult.errorMessage()
                            + " — skipping this online save; next cycle re-collects.");
                        return ComponentSaveOutcome.skipStaleOnlineSave(batchResult.errorMessage());
                    }
                    case STALE_GENERATION -> {
                        // The DB cold-path diagnostic returns the authoritative
                        // cursor. Repair local metadata and retry on the next
                        // periodic cycle without misclassifying this as lock loss.
                        componentCursors.put(uuid, new ComponentCursor(
                            batchResult.generation(), batchResult.componentBitmap()));
                        return ComponentSaveOutcome.skipStaleOnlineSave(batchResult.errorMessage());
                    }
                    case LOCK_OR_FENCING_MISMATCH, SESSION_MISMATCH, METADATA_UPDATE_FAILED -> {
                        // The lock is no longer ours (or was superseded). A
                        // full Blob save would be rejected by the same CAS
                        // anyway, but more importantly it would be a
                        // semantically wrong attempt to overwrite a newer
                        // session's data. Treat as a fatal lock conflict —
                        // do NOT fall back, do NOT release the lock (the
                        // caller's releaseLock path is gated on success).
                        logger.severe("[Component] Save rejected for " + uuid
                            + " (" + r + "): " + batchResult.errorMessage()
                            + " — lock no longer held by this session; NOT falling back to full Blob save.");
                        return ComponentSaveOutcome.fatalLockConflict(batchResult.errorMessage());
                    }
                    case SQL_ERROR, NONE -> {
                        // SQL_ERROR: the transaction blew up. A full Blob save
                        // would likely hit the same DB issue, but it is a
                        // different code path and MIGHT succeed (e.g. the
                        // component INSERT hit a constraint the Blob UPDATE
                        // would not). Treat as DB_UNAVAILABLE — the caller's
                        // online-save path will handle the error result.
                        logger.log(Level.WARNING, "[Component] Save rejected for " + uuid
                            + " (" + r + "): " + batchResult.errorMessage()
                            + " — treating as DB_UNAVAILABLE; not falling back to full Blob save.", 
                            batchResult.errorMessage());
                        return ComponentSaveOutcome.dbUnavailable(batchResult.errorMessage());
                    }
                }
            }

            // Update local version tracking with the new version from the DB.
            // upsertComponentsIfLockHeld incremented player_data.version, so
            // the local playerVersions map must be updated to match.
            playerVersions.put(uuid, batchResult.newVersion());
            data.setVersion(batchResult.newVersion());
            componentCursors.put(uuid,
                new ComponentCursor(batchResult.generation(), batchResult.componentBitmap()));

            // Clear dirty flags using the epoch-protected clear. Only components
            // whose epoch still matches the snapshot will be cleared — if a
            // concurrent markDirty bumped an epoch during the DB write, that
            // component stays dirty and the next periodic save will re-serialize
            // and re-write it with the latest state.
            dirtyMask.clearDirty(uuid, dirtySnapshot);

            if (config.isDebug()) {
                logger.info("Component save " + kind + " for " + uuid + ": "
                    + componentBlobs.size() + " components, "
                    + totalCompressedSize + " bytes, "
                    + "ser=" + serElapsed + "ms db=" + dbElapsed + "ms"
                    + " v" + batchResult.oldVersion() + "->v" + batchResult.newVersion()
                    + " bitmap=0x" + Long.toHexString(batchResult.componentBitmap())
                    + " gen=" + batchResult.generation());
            }

            // Log operation
            logOperation(uuid, OperationType.SAVE, data.getFencingToken(),
                batchResult.newVersion(), totalCompressedSize,
                "Component save " + kind + " (" + componentBlobs.size() + " components, gen=" + batchResult.generation() + ")");

            return ComponentSaveOutcome.success(
                SaveResult.success(batchResult.oldVersion(), batchResult.newVersion(), totalCompressedSize));

        } catch (java.sql.SQLException e) {
            logger.log(Level.WARNING, "[Component] Save failed (SQL) for " + uuid, e);
            return ComponentSaveOutcome.dbUnavailable(e.getMessage());
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Component] Save failed for " + uuid, e);
            return ComponentSaveOutcome.dbUnavailable(e.getMessage());
        }
    }

    /**
     * Outcome of a component-only save attempt. Replaces the old
     * {@code SaveResult | null} contract so the caller can distinguish
     * "fall back to full Blob" from "skip this online save" and
     * "fatal lock conflict — do NOT fall back".
     *
     * <p>Round 15 P0: the old {@code null} return made
     * {@code persistCollectedData} blindly fall back to a full Blob save for
     * EVERY rejection, including lock/fencing/session mismatches where the
     * lock was no longer ours — which would overwrite a newer session's data
     * with a stale snapshot.
     */
    public record ComponentSaveOutcome(Decision decision, SaveResult result, String message) {
        public enum Decision {
            /** Component save succeeded; {@code result} carries the SaveResult. */
            SUCCESS,
            /** Safe to fall back to a full Blob save (no baseline / no components). */
            FALLBACK_FULL_BLOB,
            /** Stale collected version — skip this online save, do NOT fall back. */
            SKIP_STALE_ONLINE_SAVE,
            /** Lock/fencing/session mismatch — do NOT fall back, do NOT release lock. */
            FATAL_LOCK_CONFLICT,
            /** DB unavailable / SQL error — caller handles as error, no fallback. */
            DB_UNAVAILABLE
        }

        public static ComponentSaveOutcome success(SaveResult result) {
            return new ComponentSaveOutcome(Decision.SUCCESS, result, null);
        }
        public static ComponentSaveOutcome fallbackFullBlob() {
            return new ComponentSaveOutcome(Decision.FALLBACK_FULL_BLOB, null, null);
        }
        public static ComponentSaveOutcome skipStaleOnlineSave(String msg) {
            return new ComponentSaveOutcome(Decision.SKIP_STALE_ONLINE_SAVE, null, msg);
        }
        public static ComponentSaveOutcome fatalLockConflict(String msg) {
            return new ComponentSaveOutcome(Decision.FATAL_LOCK_CONFLICT, null, msg);
        }
        public static ComponentSaveOutcome dbUnavailable(String msg) {
            return new ComponentSaveOutcome(Decision.DB_UNAVAILABLE, null, msg);
        }
    }

    /**
     * Check whether a given component is enabled for sync in config.
     * Used by {@link #persistComponentsOnly} to skip disabled components.
     */
    private boolean isComponentSyncEnabled(com.fastsync.sync.dirty.ComponentDirtyMask.Component c) {
        return switch (c) {
            case INVENTORY -> config.isSyncInventory();
            case ENDER_CHEST -> config.isSyncEnderChest();
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
     * Mark ALL enabled components as dirty — for strict mode or full-scan cycles.
     * Covers every component that can be mutated by Bukkit API without firing events.
     */
    private void markApiMutationRiskComponents(UUID uuid) {
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.INVENTORY);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ENDER_CHEST);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.VITALS);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.FOOD);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.EXPERIENCE);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.GAME_MODE);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.POTION_EFFECTS);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.FLIGHT);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.AIR);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.FIRE_TICKS);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.LOCATION);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.PDC);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.STATISTICS);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ATTRIBUTES);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ADVANCEMENTS);
    }

    /**
     * Mark only high-risk components — for balanced mode between full scans.
     * These are the components most likely to be changed by plugin API calls
     * and most likely to cause visible data loss (inventory, ender chest, PDC, etc).
     */
    private void markHighRiskApiMutationComponents(UUID uuid) {
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.INVENTORY);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ENDER_CHEST);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.FOOD);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.EXPERIENCE);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.PDC);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.STATISTICS);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ATTRIBUTES);
        markIfEnabled(uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component.ADVANCEMENTS);
    }

    private void markIfEnabled(UUID uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component component) {
        if (dirtyMask == null) return;
        if (!isComponentSyncEnabled(component)) return;
        dirtyMask.markDirty(uuid, component);
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
    /**
     * Full-Blob save with an explicit pre-collect dirty snapshot.
     *
     * <p>Callers that have access to the dirty mask BEFORE calling
     * {@link #collectPlayerData(Player)} should pass the snapshot they took
     * at that point — this gives the tightest epoch protection (covers the
     * collect window too). The 3-arg overload {@link #persistCollectedData(
     * UUID, PlayerData, SaveKind)} is a convenience that takes the snapshot
     * inside the method, which still protects the serialize + DB-write window
     * but not the collect window.
     *
     * @param preSaveSnapshot dirty state captured before collectPlayerData.
     *                        Ignored when {@code kind.releaseLock == true}
     *                        (QUIT/SHUTDOWN uses clearAll instead).
     */
    /** Encoded player-data blob: raw serialized bytes, compressed bytes, and checksum. */
    private record EncodedBlob(byte[] serialized, byte[] compressed, long checksum) {}

    /**
     * Serialize + compress + checksum a PlayerData into an {@link EncodedBlob}.
     *
     * <p>Extracted so the initial encode and the retry re-encode in
     * {@link #persistCollectedData} share one implementation, matching
     * {@link com.fastsync.spool.FinalSaveEncoder} which uses the same
     * three-step sequence for the spool path.
     */
    private EncodedBlob encodeBlob(PlayerData data) throws java.io.IOException {
        byte[] serialized = PlayerDataSerializer.serialize(data);
        byte[] compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize());
        long checksum = DatabaseManager.computeChecksum(serialized);
        return new EncodedBlob(serialized, compressed, checksum);
    }

    private SaveResult persistCollectedData(UUID uuid, PlayerData data, SaveKind kind,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot preSaveSnapshot) {
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
            // Pass the caller-provided snapshot (taken before collectPlayerData)
            // so persistComponentsOnly's clearDirty after the DB write protects
            // changes that arrived during collect/serialize/DB-write. If we
            // let persistComponentsOnly re-snapshot here, the snapshot would be
            // taken AFTER collect and could include changes the component row
            // does NOT contain — clearing them would lose data.
            SaveResult componentResult = null;
            ComponentSaveOutcome outcome = persistComponentsOnly(uuid, data, kind, preSaveSnapshot);
            switch (outcome.decision()) {
                case SUCCESS -> {
                    return outcome.result();
                }
                case FALLBACK_FULL_BLOB -> {
                    // A selectively collected PlayerData intentionally omits
                    // unrelated components. It is never safe to turn that into
                    // the authoritative full Blob if an invariant unexpectedly
                    // invalidates the component path after collection.
                    if (data.isComponentSubset()) {
                        logger.warning("[Component] Refusing full-Blob fallback for partial snapshot "
                            + uuid + "; retrying on the next save cycle.");
                        return SaveResult.error("component path invalidated after partial collection",
                            SaveFailureReason.DB_UNAVAILABLE);
                    }
                    // A full collection is available; fall through safely.
                    componentResult = null;
                }
                case SKIP_STALE_ONLINE_SAVE -> {
                    // Stale collected version — skip this online save entirely.
                    // Do NOT fall back to full Blob (would clobber newer state).
                    // Return an error result so the caller's audit/logging sees
                    // the skip; the next periodic cycle re-collects.
                    logger.info("[Save] " + kind + " for " + uuid
                        + " skipped — component path saw stale version: " + outcome.message());
                    return SaveResult.error("stale collected version (skipped)",
                        SaveFailureReason.VERSION_CONFLICT);
                }
                case FATAL_LOCK_CONFLICT -> {
                    // Lock/fencing/session mismatch — the lock is no longer ours.
                    // Do NOT fall back to full Blob (would overwrite newer data).
                    // Do NOT release the lock here — the caller's release path
                    // is gated on success. Return a FENCING_MISMATCH error so
                    // the conflict manager / audit path picks it up.
                    logger.severe("[Save] " + kind + " for " + uuid
                        + " aborted — component path detected fatal lock conflict: "
                        + outcome.message());
                    return SaveResult.error("fatal lock conflict: " + outcome.message(),
                        SaveFailureReason.FENCING_MISMATCH);
                }
                case DB_UNAVAILABLE -> {
                    // SQL error — do NOT fall back to full Blob (same DB, likely
                    // same failure). Return DB_UNAVAILABLE so the caller can
                    // retry with backoff.
                    logger.warning("[Save] " + kind + " for " + uuid
                        + " aborted — component path hit DB error: " + outcome.message());
                    return SaveResult.error("component DB error: " + outcome.message(),
                        SaveFailureReason.DB_UNAVAILABLE);
                }
            }
            // Fall through to full save if component path fell back
            if (componentResult != null) {
                return componentResult;
            }
        }

        // Configuration can be reloaded between entity-thread collection and
        // the async persistence task. Under no circumstances may a selective
        // carrier become the authoritative full Blob merely because the
        // component gate changed in that window.
        if (data.isComponentSubset()) {
            logger.warning("[Component] Component-only snapshot for " + uuid
                + " reached the full-Blob path; refusing unsafe partial write.");
            return SaveResult.error("component-only snapshot cannot be written as full Blob",
                SaveFailureReason.CANCELLED);
        }

        // P1 (round 15): final-save retry now uses a bounded loop with a full
        // LockState read (version + fencing + locked_by + lock_session_id) per
        // retry, instead of the old single-shot version+token comparison.
        //
        // Final saves (releaseLock=true: QUIT/SHUTDOWN) get up to 3 attempts
        // because they persist the player's FINAL state — a single transient
        // same-fencing self-conflict (our own previous online save advanced the
        // version while we waited for the saveLock) must not lose the final
        // state. Online saves (releaseLock=false) keep 1 attempt: they are
        // best-effort and the next periodic cycle will retry.
        int maxAttempts = kind.releaseLock ? 3 : 1;

        try {
            // Serialize + compress + checksum (shared with the spool encoder path)
            EncodedBlob encoded = encodeBlob(data);
            byte[] serialized = encoded.serialized();
            byte[] compressed = encoded.compressed();
            long checksum = encoded.checksum();

            long serElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
            if (serializeLatency != null) serializeLatency.record(serElapsedMs);

            if (config.isLogTiming()) {
                logger.info("[Timing] Serialize " + kind + " for " + uuid + ": " + serElapsedMs + "ms"
                    + " (serialized=" + serialized.length + " bytes, stored=" + compressed.length + " bytes)");
            }

            // 4. DB CAS save with version + fencing token
            long expectedVersion = data.getVersion();
            long fencingToken = data.getFencingToken();
            String session = playerLockSessions.get(uuid);
            String serverName = config.getServerName();
            long saveStart = System.nanoTime();

            // Full Blob save ALWAYS uses the ClearComponents variant, regardless
            // of whether component-storage is currently enabled. Rationale:
            // if the server EVER ran with component-storage enabled in the past,
            // there may be leftover component rows + non-zero component_bitmap in
            // player_data for this UUID. If we now save the full Blob WITHOUT
            // bumping component_generation + zeroing component_bitmap, those
            // stale rows remain visible to future loads and will silently
            // overlay the freshly written Blob — rolling back parts of the
            // player's state to whenever the component was last written.
            //
            // The ClearComponents variant only writes two extra columns
            // (component_bitmap=0, component_generation=generation+1) on a row
            // we are already CAS-updating. The cost is negligible, and it is
            // safe even when component-storage has never been enabled:
            //   - component_bitmap defaults to 0, so setting it to 0 is a no-op
            //   - component_generation defaults to 0, incrementing to 1 is harmless
            //   - no component rows exist, so the generation bump invalidates nothing
            boolean saved = false;
            int attempt = 0;
            // Conflict diagnostics from the LAST attempt (used if all attempts fail).
            long actualVersion = expectedVersion;
            long actualFencingToken = fencingToken;
            String actualLockedBy = serverName;
            String actualSession = session;

            while (attempt < maxAttempts) {
                attempt++;
                if (kind.releaseLock) {
                    saved = databaseManager.saveDataAndReleaseLockClearComponents(uuid, compressed, checksum, expectedVersion, fencingToken, serverName, session);
                } else {
                    saved = databaseManager.saveDataKeepLockClearComponents(uuid, compressed, checksum, expectedVersion, fencingToken, serverName, session);
                }

                if (saved) {
                    break;
                }

                // CAS failed. Read the full lock state ONCE to classify.
                com.fastsync.database.DatabaseManager.LockState state = databaseManager.getLockState(uuid);
                actualVersion = state.version();
                actualFencingToken = state.fencingToken();
                actualLockedBy = state.lockedBy();
                actualSession = state.lockSessionId();

                // Retryable condition: lock is STILL ours (server + fencing +
                // session all match) AND the DB version advanced past what we
                // expected (our own previous save won the race). This is a
                // same-fencing self-conflict — safe to retry with the actual
                // version.
                //
                // If ANY of these fail, the retry is NOT safe:
                //   - locked_by / session mismatch → lock was stolen; retry
                //     would be rejected by CAS anyway and is semantically wrong
                //   - fencing mismatch → lock was lost to another server
                //   - version <= expected → unexpected state (row vanished or
                //     version went backwards); do not loop
                boolean lockStillOurs = state.isHeldBy(serverName, fencingToken, session);
                if (!lockStillOurs || actualVersion <= expectedVersion) {
                    if (config.isDebug()) {
                        logger.fine("[Fencing] " + kind + " save for " + uuid
                            + " — not retrying (attempt " + attempt + "/" + maxAttempts
                            + "): lockStillOurs=" + lockStillOurs
                            + ", expectedV=" + expectedVersion + ", actualV=" + actualVersion
                            + ", actualLockedBy=" + actualLockedBy
                            + ", actualSession=" + actualSession);
                    }
                    break;  // fall through to conflict handling
                }

                // Same-fencing self-conflict: retry with the actual version.
                logger.info("[Fencing] " + kind + " save for " + uuid
                    + " — same-fencing self-conflict (attempt " + attempt + "/" + maxAttempts
                    + ", expected v" + expectedVersion + ", actual v" + actualVersion
                    + ", ft=" + fencingToken + "). Retrying with actual version.");

                expectedVersion = actualVersion;
                data.setVersion(actualVersion);

                // Re-serialize with the updated version so the checksum matches.
                EncodedBlob reEncoded = encodeBlob(data);
                serialized = reEncoded.serialized();
                compressed = reEncoded.compressed();
                checksum = reEncoded.checksum();
                // Loop continues → next attempt uses the new expectedVersion.
            }

            long saveElapsedMs = (System.nanoTime() - saveStart) / 1_000_000;
            if (saveLatency != null) saveLatency.record(saveElapsedMs);

            if (!saved) {
                // Genuine conflict (external fencing violation, lock lost, or
                // all retries exhausted).
                conflictManager.handleConflict(uuid, data, expectedVersion, actualVersion);
                logger.warning("[Fencing] " + kind + " save rejected for " + uuid +
                    " (expected v" + expectedVersion + "/ft" + fencingToken +
                    ", actual v" + actualVersion + "/ft" + actualFencingToken +
                    ", locked_by=" + actualLockedBy + ", session=" + actualSession + ")"
                    + (attempt > 1 ? " [after " + attempt + " attempts]" : ""));

                logOperation(uuid, OperationType.CONFLICT, fencingToken, expectedVersion,
                    compressed.length, kind + " conflict: expected v" + expectedVersion + "/ft" + fencingToken +
                    ", actual v" + actualVersion + "/ft" + actualFencingToken);

                if (saveLatency != null) saveLatency.recordError();

                if (kind.releaseLock) {
                    // P0 (round 11): final save CAS conflict — DO NOT release
                    // the lock. Releasing here would let the next server
                    // acquire immediately and load stale DB data (the final
                    // state was never saved). The lock will expire after
                    // lock-timeout, giving operators a window to investigate.
                    // Only saveDataAndReleaseLockClearComponents(...) == true
                    // (success path) may notifyLockReleased — see line ~3230.
                    logger.severe("[FinalSave] " + kind + " CAS failed for " + uuid
                        + " (expected v" + expectedVersion + "/ft" + fencingToken
                        + ", actual v" + actualVersion + "/ft" + actualFencingToken
                        + ", session=" + session + ")"
                        + "; NOT releasing lock. The lock will expire after "
                        + config.getLockTimeout() + "s to protect final state.");
                } else {
                    // Online save: do NOT release lock — player is still on this server.
                    // The CAS failure means someone else wrote (fencing token violation),
                    // which is a serious bug. Log it at SEVERE and keep the lock.
                    logger.log(Level.SEVERE, "[Fencing] Online save conflict for " + uuid
                        + " — possible lock infringement! The lock should be held by us but CAS failed."
                        + " expected v" + expectedVersion + "/ft" + fencingToken
                        + ", actual v" + actualVersion + "/ft" + actualFencingToken);
                }

                // Classify the conflict reason for structured retry handling.
                // Any of these means the lock was lost/stolen → FENCING_MISMATCH:
                //   - fencing tokens differ (another server took the lock)
                //   - locked_by differs (lock stolen by another server)
                //   - session differs (same server, different acquire — stale session)
                // Only when all three match (same-fencing self-conflict) is it
                // a benign VERSION_CONFLICT (our own previous save won the race).
                SaveFailureReason reason = (actualFencingToken != fencingToken
                        || !serverName.equals(actualLockedBy)
                        || !session.equals(actualSession))
                    ? SaveFailureReason.FENCING_MISMATCH
                    : SaveFailureReason.VERSION_CONFLICT;
                return SaveResult.conflict(expectedVersion, actualVersion, compressed.length, reason);
            }

            // 5b. Success: advance version + log + snapshot + publish
            advanceVersion(uuid, expectedVersion);

            // A successful full Blob save establishes (or refreshes) the baseline.
            // From this point on, component-only saves are safe for this player
            // for the remainder of the session.
            playersWithBaseline.add(uuid);
            componentCursors.compute(uuid, (ignored, current) ->
                new ComponentCursor(current == null ? 1L : current.generation() + 1L, 0L));

            // Clear dirty flags using the pre-save snapshot's epoch.
            //
            // QUIT / SHUTDOWN (releaseLock=true): the player is leaving and
            // will not produce more changes, so clearAll() is safe and avoids
            // leaving a stale mask entry that would never be cleaned.
            //
            // Online saves (PERIODIC/BULK/WORLD_SAVE/DEATH): use clearDirty(snapshot)
            // so that any markDirty() that arrived during collectPlayerData /
            // serialize / DB write (and bumped the epoch) is PRESERVED. The next
            // periodic save will re-collect and re-write those components.
            //
            // Without this epoch-protected clear, the following race would lose
            // data:
            //   T1: periodic save starts, snapshot dirty = {INVENTORY: e5}
            //   T1: collectPlayerData → serialize → DB write in flight
            //   T2: player clicks inventory, markDirty(INVENTORY) → e6
            //   T1: DB write succeeds
            //   T1: clearAll(uuid)  // ❌ clears INVENTORY even though e6 != e5
            //   T1: next periodic sees no dirty → skips save
            //   ⚠ if server crashes before next save, T2's change is lost
            //
            // The full Blob just written does NOT contain T2's change because
            // collectPlayerData ran before T2's event. The dirty bit IS the only
            // record that T2 happened, so it MUST survive the clear.
            if (dirtyMask != null) {
                if (kind.releaseLock || preSaveSnapshot == null) {
                    dirtyMask.clearAll(uuid);
                } else {
                    dirtyMask.clearDirty(uuid, preSaveSnapshot);
                }
            }

                if (snapshotManager != null && shouldCreateSnapshot(data.getSaveCause())) {
                    try {
                        snapshotManager.createSnapshot(uuid, compressed, data.getSaveCause())
                            .thenRun(() -> snapshotManager.pruneSnapshots(uuid, config.getMaxSnapshots()));
                    } catch (Exception snapshotEx) {
                        // Post-commit side effect failure must NOT turn a
                        // successful DB save into a retry/failure. The DB
                        // commit is authoritative — the snapshot is best-effort.
                        logger.log(Level.WARNING, kind + " save succeeded but snapshot creation failed for " + uuid, snapshotEx);
                    }
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
                    // Release via the unified sync helper. It is fail-closed on
                    // null ft/lockSession (logs + refuses tokenless release)
                    // and catches SQLException | RuntimeException, so the
                    // defensive guards in releaseLock can no longer leak the
                    // lock here. We snapshot the current values (not remove)
                    // because the surrounding save-failure path may still need
                    // them for the lock-timeout fallback below.
                    Long ft = playerFencingTokens.get(uuid);
                    String qSession = playerLockSessions.get(uuid);
                    releaseOwnedLockAndNotify(uuid, ft, qSession, "quarantined save failure");
                } else {
                    logger.warning("[Quit] NOT releasing lock for " + uuid + " after " + kind
                        + " save failure — lock will expire after lock-timeout ("
                        + config.getLockTimeout() + "s) to protect the player's data.");
                }
            }
            // Online save: keep lock on error — will retry on next periodic save or quit
            return SaveResult.error(e.getMessage(), classifySaveException(e));
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
     * Apply the saved inventory contents to the player with full-replace semantics
     * (matching MC {@code Inventory.load}).
     *
     * <p>P0 (issue #57): the old implementation used {@code Math.min(contents.length,
     * current.length)} for the loop bound, which meant slots beyond {@code contents.length}
     * kept their current items. This was safe under {@code clear-before-apply=true}
     * (the inventory is cleared first), but leaked items from the target server
     * when {@code clear-before-apply=false}. The new implementation always writes
     * every slot of the player's inventory: slots beyond the saved contents are
     * explicitly set to {@code null}, achieving full-replace regardless of the
     * clear-before-apply setting.
     *
     * <p>Null {@code contents} is a no-op — the caller is expected to have
     * cleared the inventory separately (e.g. via clear-before-apply) if needed.
     */
    private void setInventoryContents(
            org.bukkit.inventory.PlayerInventory inventory,
            org.bukkit.inventory.ItemStack[] contents) {
        if (contents == null) return;
        // Greenfield format stores exactly NMS non-equipment items. Paper's
        // storage-specific API keeps armor/offhand out of this write and clears
        // every storage slot; pass through without a compatibility copy.
        inventory.setStorageContents(contents);
    }

    /**
     * Apply the saved ender chest contents with full-replace semantics.
     * See {@link #setInventoryContents} for the P0 (issue #57) rationale.
     */
    private void setEnderChestContents(
            org.bukkit.inventory.Inventory enderChest,
            org.bukkit.inventory.ItemStack[] contents) {
        if (contents == null) return;
        enderChest.setStorageContents(contents);
    }

    /**
     * Mark a player's component dirty — public API for third-party plugins
     * that modify player state via Bukkit API without firing events.
     * Called by {@link com.fastsync.api.FastSyncApi#markDirty}.
     */
    public void markPlayerDirty(UUID uuid, com.fastsync.sync.dirty.ComponentDirtyMask.Component component) {
        if (uuid == null || component == null) return;
        if (dirtyMask == null) return;
        if (!activePlayers.containsKey(uuid)) return;
        if (!isComponentSyncEnabled(component)) return;
        dirtyMask.markDirty(uuid, component);
    }

    public void markPlayerDirty(UUID uuid, String componentName) {
        if (uuid == null || componentName == null) return;
        try {
            com.fastsync.sync.dirty.ComponentDirtyMask.Component component =
                com.fastsync.sync.dirty.ComponentDirtyMask.Component.valueOf(
                    componentName.trim().toUpperCase(java.util.Locale.ROOT));
            markPlayerDirty(uuid, component);
        } catch (IllegalArgumentException ignored) {
            if (config.isDebug()) {
                logger.fine("[DirtyAPI] Unknown component name: " + componentName);
            }
        }
    }

    public boolean isPlayerActive(UUID uuid) {
        return activePlayers.containsKey(uuid) || bypassedPlayers.contains(uuid);
    }

    public int getPendingCount() {
        return pendingData.size() + pendingEmptyData.size() + pendingBypass.size();
    }

    /** Available permits on the login-load semaphore (max-concurrent-loads). */
    public int getLoginLoadAvailablePermits() {
        return loginLoadSemaphore != null ? loginLoadSemaphore.availablePermits() : 0;
    }

    /** Max permits on the login-load semaphore (max-concurrent-loads). */
    public int getLoginLoadLimit() {
        return loginLoadSemaphore != null ? config.getMaxConcurrentLoads() : 0;
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

    public int getAsyncActiveCount() {
        return asyncExecutor != null ? asyncExecutor.getActiveCount() : -1;
    }

    public int getAsyncQueueSize() {
        return asyncExecutor != null ? asyncExecutor.getQueueSize() : -1;
    }

    public int getFinalSaveActiveCount() {
        return finalSaveExecutor != null ? finalSaveExecutor.getActiveCount() : -1;
    }

    public int getFinalSaveQueueSize() {
        return finalSaveExecutor != null ? finalSaveExecutor.getQueueSize() : -1;
    }

    public int getFinalSaveQueueCapacity() {
        return finalSaveExecutor != null ? finalSaveExecutor.getQueueCapacity() : -1;
    }

    public long getFinalSaveQueueFullTotal() {
        return finalSaveQueueFullTotal.get();
    }

    public long getFinalSaveLastQueueFullAt() {
        return finalSaveLastQueueFullAt.get();
    }

    public long getFinalSaveSpoolEnqueuedTotal() {
        return finalSaveSpoolEnqueuedTotal.get();
    }

    public long getFinalSaveLastSpoolEnqueuedAt() {
        return finalSaveLastSpoolEnqueuedAt.get();
    }

    public long getFinalSaveSpoolRejectedTotal() {
        return finalSaveSpoolRejectedTotal.get();
    }

    public long getFinalSaveLastSpoolRejectedAt() {
        return finalSaveLastSpoolRejectedAt.get();
    }

    public long getFinalSaveSyncFallbackTotal() {
        return finalSaveSyncFallbackTotal.get();
    }

    public long getFinalSaveLastSyncFallbackAt() {
        return finalSaveLastSyncFallbackAt.get();
    }

    public boolean hasFinalSaveAlert() {
        return finalSaveSyncFallbackTotal.get() > 0
            || finalSaveSpoolRejectedTotal.get() > 0
            || getFinalSaveSpoolFailedCount() > 0;
    }

    public boolean hasFinalSaveWarning() {
        return finalSaveQueueFullTotal.get() > 0
            || finalSaveSpoolEnqueuedTotal.get() > 0
            || getFinalSaveSpoolPendingCount() > 0;
    }

    // Final-save spool telemetry
    public long getFinalSaveSpoolPendingCount() {
        return finalSaveSpool != null ? finalSaveSpool.getPendingCount() : 0;
    }
    public long getFinalSaveSpoolFailedCount() {
        return finalSaveSpool != null ? finalSaveSpool.getFailedCount() : 0;
    }
    public long getFinalSaveSpoolBytes() {
        return finalSaveSpool != null ? finalSaveSpool.getTotalBytes() : 0;
    }
    public long getFinalSaveSpoolLastReplayAt() {
        return finalSaveSpool != null ? finalSaveSpool.getLastReplayAt() : 0;
    }
    public String getFinalSaveSpoolLastError() {
        return finalSaveSpool != null ? finalSaveSpool.getLastError() : null;
    }

    /**
     * Persist a final save and spool only failures that are safe to replay.
     *
     * <p>This wrapper is deliberately kept off the successful hot path beyond
     * one result branch. Encoding the spool record happens only after a
     * retryable failure, so normal quit/shutdown saves do not pay a second
     * serialization cost.
     */
    SaveResult persistFinalSaveWithSpool(
            UUID uuid, PlayerData data, SaveKind kind,
            com.fastsync.sync.dirty.ComponentDirtyMask.DirtySnapshot preSaveSnapshot,
            String detail) {
        if (!kind.releaseLock) {
            throw new IllegalArgumentException("persistFinalSaveWithSpool requires a final save kind");
        }
        SaveResult result = persistCollectedData(uuid, data, kind, preSaveSnapshot);
        if (result.isRetryable()) {
            spoolRetryableFinalSave(
                uuid, data, kind, result, playerLockSessions.get(uuid), detail);
        }
        return result;
    }

    /**
     * Spool a failed final save for later replay.
     *
     * <p>Only retryable failures ({@link SaveResult#isRetryable()}) are spooled.
     * Non-retryable failures (FENCING_MISMATCH, VERSION_CONFLICT) are NOT spooled
     * — the lock is lost and replaying the save would clobber newer data.
     *
     * @param uuid the player UUID
     * @param data the player data that failed to save
     * @param kind the save kind (typically QUIT)
     * @param result the failed save result
     * @param lockSessionId the lock session ID
     * @param detail a human-readable description of the failure
     * @return true if the save was spooled, false if it was not retryable or spooling failed
     */
    public boolean spoolRetryableFinalSave(
            UUID uuid, PlayerData data, SaveKind kind,
            SaveResult result, String lockSessionId, String detail) {
        if (!result.isRetryable()) {
            return false;
        }
        if (finalSaveSpool == null) {
            logger.warning("[FinalSave] Cannot spool retryable save for " + uuid
                + " — spool is not initialized: " + detail);
            return false;
        }
        try {
            com.fastsync.spool.EncodedFinalSave encoded = com.fastsync.spool.FinalSaveEncoder.encode(
                uuid, data, kind,
                config.getClusterId(), config.getServerName(),
                lockSessionId, config.getCompressionMinSize());
            finalSaveSpool.append(encoded);
            finalSaveSpoolEnqueuedTotal.incrementAndGet();
            finalSaveLastSpoolEnqueuedAt.set(System.currentTimeMillis());
            logger.info("[FinalSave] Spooled retryable " + kind + " save for " + uuid
                + " (" + result.failureReason() + "): " + detail);
            return true;
        } catch (Exception e) {
            finalSaveSpoolRejectedTotal.incrementAndGet();
            finalSaveLastSpoolRejectedAt.set(System.currentTimeMillis());
            logger.log(Level.SEVERE, "[FinalSave] CRITICAL: failed to spool retryable save for "
                + uuid + ". Final state may be lost. Lock will expire naturally.", e);
            return false;
        }
    }

    public boolean isOperationLogEnabled() {
        return operationLogManager != null && operationLogManager.isEnabled();
    }

    public int getOperationLogQueueSize() {
        return operationLogManager != null ? operationLogManager.getQueueSize() : -1;
    }

    public int getOperationLogQueueCapacity() {
        return operationLogManager != null ? operationLogManager.getQueueCapacity() : -1;
    }

    public long getOperationLogDroppedTotal() {
        return operationLogManager != null ? operationLogManager.getDroppedCount() : 0L;
    }

    public long getOperationLogLastDropAt() {
        return operationLogManager != null ? operationLogManager.getLastDropTimestamp() : 0L;
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

    static void verifyComponentOverlayCompleteness(
            UUID uuid,
            java.util.Set<String> expectedComponents,
            java.util.Map<String, com.fastsync.database.DatabaseManager.ComponentData> loadedComponents,
            long generation) throws IOException {
        java.util.Set<String> missingComponents = new java.util.HashSet<>(expectedComponents);
        missingComponents.removeAll(loadedComponents.keySet());
        if (!missingComponents.isEmpty()) {
            throw new IOException("component_bitmap references missing rows for " + uuid
                + " (gen=" + generation + "): " + missingComponents);
        }
    }

    static java.util.Set<String> componentNamesForBitmap(long bitmap) throws IOException {
        java.util.Set<String> names = new java.util.HashSet<>();
        long knownMask = 0L;
        for (com.fastsync.sync.dirty.ComponentDirtyMask.Component component :
                com.fastsync.sync.dirty.ComponentDirtyMask.Component.values()) {
            knownMask |= component.storageMask();
            if ((bitmap & component.storageMask()) != 0) names.add(component.name());
        }
        long unknownBits = bitmap & ~knownMask;
        if (unknownBits != 0) {
            throw new IOException("component_bitmap contains unknown storage bits: 0x"
                + Long.toHexString(unknownBits));
        }
        return names;
    }

    private enum FinalSaveQueueFullOutcome {
        SPOOLED, SPOOL_UNAVAILABLE, SPOOL_FAILED, SYNC_FALLBACK
    }

    private void recordFinalSaveQueueFull(
            String kind, UUID uuid, String detail,
            java.util.concurrent.RejectedExecutionException cause,
            FinalSaveQueueFullOutcome outcome) {
        long now = System.currentTimeMillis();
        long queueFullCount = finalSaveQueueFullTotal.incrementAndGet();
        finalSaveLastQueueFullAt.set(now);
        long spooled = finalSaveSpoolEnqueuedTotal.get();
        long spoolRejected = finalSaveSpoolRejectedTotal.get();
        long syncFallback = finalSaveSyncFallbackTotal.get();
        switch (outcome) {
            case SPOOLED -> {
                spooled = finalSaveSpoolEnqueuedTotal.incrementAndGet();
                finalSaveLastSpoolEnqueuedAt.set(now);
            }
            case SPOOL_UNAVAILABLE, SPOOL_FAILED -> {
                spoolRejected = finalSaveSpoolRejectedTotal.incrementAndGet();
                finalSaveLastSpoolRejectedAt.set(now);
            }
            case SYNC_FALLBACK -> {
                syncFallback = finalSaveSyncFallbackTotal.incrementAndGet();
                finalSaveLastSyncFallbackAt.set(now);
            }
        }
        Level level = switch (outcome) {
            case SPOOLED -> Level.WARNING;
            case SPOOL_UNAVAILABLE, SPOOL_FAILED, SYNC_FALLBACK -> Level.SEVERE;
        };
        logger.log(level, "[FinalSave] Final-save executor rejected " + kind + " save for "
            + uuid + " — " + detail
            + " (outcome=" + outcome
            + ", queueFullTotal=" + queueFullCount
            + ", spooledTotal=" + spooled
            + ", spoolRejectedTotal=" + spoolRejected
            + ", syncFallbackTotal=" + syncFallback + ")", cause);
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
     * Return latency status lines for /fastsync status display.
     */
    public java.util.List<String> getLatencyStatusLines() {
        java.util.List<String> lines = new java.util.ArrayList<>();
        if (loadLatency != null) lines.add(loadLatency.getStatusLine());
        if (saveLatency != null) lines.add(saveLatency.getStatusLine());
        if (serializeLatency != null) lines.add(serializeLatency.getStatusLine());
        return lines;
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
        public enum Status { SUCCESS, BYPASSED, LOCKED, ERROR, PROTECTION, BUSY }

        private final Status status;
        private final String message;

        private LoadResult(Status status, String message) {
            this.status = status;
            this.message = message;
        }

        public static LoadResult success() { return new LoadResult(Status.SUCCESS, null); }
        public static LoadResult bypassed() { return new LoadResult(Status.BYPASSED, null); }
        public static LoadResult locked() { return new LoadResult(Status.LOCKED, null); }
        public static LoadResult error(String message) { return new LoadResult(Status.ERROR, message); }
    public static LoadResult protection(String message) { return new LoadResult(Status.PROTECTION, message); }
    public static LoadResult busy(String message) { return new LoadResult(Status.BUSY, message); }

        public Status getStatus() { return status; }
        public String getMessage() { return message; }
        public boolean isSuccess() { return status == Status.SUCCESS || status == Status.BYPASSED; }
    }
}
