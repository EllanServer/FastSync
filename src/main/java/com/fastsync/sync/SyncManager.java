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

    // Data loaded during pre-login, waiting to be applied on join
    // null value = player exists but has no saved data (new player)
    private final ConcurrentHashMap<UUID, PlayerData> pendingData = new ConcurrentHashMap<>();

    // Track players whose data has been applied (actively playing)
    private final ConcurrentHashMap<UUID, Boolean> activePlayers = new ConcurrentHashMap<>();

    // Track the DB version each player's data was loaded from (for optimistic concurrency)
    private final ConcurrentHashMap<UUID, Long> playerVersions = new ConcurrentHashMap<>();

    // Track the fencing token for each player (Kleppmann stale-write defence)
    private final ConcurrentHashMap<UUID, Long> playerFencingTokens = new ConcurrentHashMap<>();

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
                    config.getServerName());
                redissonManager.initialize();
                // Register listener for incoming stream events from other servers
                redissonManager.addListener(this::handleStreamEvent);
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
                logger.log(Level.WARNING, "Failed to initialize operation log (Chronicle Queue)", e);
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
    }

    /**
     * Re-parse config-dependent caches after a config reload.
     * Called from {@code /fastsync reload}.
     */
    public void refreshConfigCache() {
        parseSnapshotTriggerSet();
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
        pendingLoadCount.incrementAndGet();
        try {
            return loadPlayerDataInternal(uuid);
        } finally {
            pendingLoadCount.decrementAndGet();
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
                // New player or no saved data - still store fencing token for save
                pendingData.put(uuid, null);
                playerFencingTokens.put(uuid, fencingToken);
                if (config.isDebug()) {
                    logger.info("No saved data for " + uuid + " (new player, fencing token: " + fencingToken + ")");
                }
                return LoadResult.success();
            }

            // Verify checksum (Dynamo-style data integrity)
            if (config.isVerifyChecksum() && !DatabaseManager.verifyChecksum(loaded.data(), loaded.checksum())) {
                logger.warning("[Checksum] Data corruption detected for " + uuid +
                    "! Stored checksum: " + loaded.checksum() +
                    ". Rejecting load to prevent applying corrupted data.");

                // Log checksum failure
                logOperation(uuid, OperationType.CHECKSUM_FAIL, fencingToken, loaded.version(),
                    loaded.data() != null ? loaded.data().length : 0,
                    "Checksum mismatch: stored=" + loaded.checksum());

                if (loadLatency != null) loadLatency.recordError();
                try {
                    databaseManager.releaseLock(uuid, config.getServerName());
                    notifyLockReleased(uuid);
                } catch (SQLException ex) {
                    logger.log(Level.WARNING, "Failed to release lock after checksum failure for " + uuid, ex);
                }
                return LoadResult.error("Data checksum mismatch - possible corruption");
            }

            // Step 3: Decompress and deserialize
            startTime = System.nanoTime();

            byte[] decompressed = CompressionUtil.unwrap(loaded.data());
            PlayerData data = PlayerDataSerializer.deserialize(decompressed);

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
            // Release lock on error
            try {
                databaseManager.releaseLock(uuid, config.getServerName());
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

        if (data == null) {
            // New player or no saved data - nothing to apply
            if (config.isDebug()) {
                logger.info("No pending data to apply for " + uuid + " (new player)");
            }
            activePlayers.put(uuid, true);
            return;
        }

        long startTime = config.isLogTiming() ? System.nanoTime() : 0;

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

        // Persistent Data Container
        if (config.isSyncPDC()) {
            applyPDC(player, data);
        }

        // Location (optional)
        if (config.isSyncLocation() && data.getWorldName() != null) {
            try {
                var world = Bukkit.getWorld(data.getWorldName());
                if (world != null) {
                    player.teleport(new Location(world, data.getX(), data.getY(), data.getZ(), data.getYaw(), data.getPitch()));
                }
            } catch (Exception e) {
                if (config.isDebug()) logger.warning("Failed to apply location: " + e.getMessage());
            }
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
    }

    // ==================== Save (Quit) ====================

    /**
     * Collect player data and save it asynchronously.
     * Collection happens on the main thread; serialization and DB save happen async.
     * After save, notifies Redis so waiting servers can acquire the lock immediately.
     */
    public void collectAndSavePlayerData(Player player) {
        UUID uuid = player.getUniqueId();

        // Remove from active players
        activePlayers.remove(uuid);
        playerVersions.remove(uuid);
        playerFencingTokens.remove(uuid);

        // Check if player has pending data (was kicked during pre-login, never joined)
        if (pendingData.containsKey(uuid)) {
            pendingData.remove(uuid);
            // Release lock without saving
            pendingSaveCount.incrementAndGet();
            asyncExecutor.execute(() -> {
                try {
                    databaseManager.releaseLock(uuid, config.getServerName());
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
            return;
        }

        // Collect data on main thread
        PlayerData data = collectPlayerData(player);

        // Save asynchronously using dedicated thread pool
        pendingSaveCount.incrementAndGet();
        asyncExecutor.execute(() -> {
            try {
                long startTime = System.nanoTime();

                byte[] serialized = PlayerDataSerializer.serialize(data);
                byte[] compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize());
                long checksum = DatabaseManager.computeChecksum(serialized);

                long serElapsedMs = (System.nanoTime() - startTime) / 1_000_000;
                if (serializeLatency != null) serializeLatency.record(serElapsedMs);

                if (config.isLogTiming()) {
                    logger.info("[Timing] Serialize for " + uuid + ": " + serElapsedMs + "ms" +
                        " (serialized=" + serialized.length + " bytes, stored=" + compressed.length + " bytes)");
                }

                // Optimistic concurrency save with fencing token (Dynamo + Kleppmann)
                long expectedVersion = data.getVersion();
                long fencingToken = data.getFencingToken();
                long saveStart = System.nanoTime();
                boolean saved = databaseManager.saveData(uuid, compressed, checksum, expectedVersion, fencingToken, config.getServerName());
                long saveElapsedMs = (System.nanoTime() - saveStart) / 1_000_000;
                if (saveLatency != null) saveLatency.record(saveElapsedMs);

                if (!saved) {
                    // Version conflict or fencing token violation!
                    // Another server wrote newer data, or a stale lock holder tried to write.
                    long actualVersion = databaseManager.getCurrentVersion(uuid);
                    long actualFencingToken = databaseManager.getCurrentFencingToken(uuid);
                    conflictManager.handleConflict(uuid, data, expectedVersion, actualVersion);
                    logger.warning("[Fencing] Save rejected for " + uuid +
                        " (expected v" + expectedVersion + "/ft" + fencingToken +
                        ", actual v" + actualVersion + "/ft" + actualFencingToken + ")");

                    // Log conflict (Raft-inspired per-UUID ordered log)
                    logOperation(uuid, OperationType.CONFLICT, fencingToken, expectedVersion,
                        compressed.length, "Conflict: expected v" + expectedVersion + "/ft" + fencingToken +
                        ", actual v" + actualVersion + "/ft" + actualFencingToken);

                    if (saveLatency != null) saveLatency.recordError();
                } else {
                    // Success - create snapshot only when configured to do so on save.
                    // Snapshot creation is controlled by snapshot.save-trigger config.
                    // Values: "never" (default, only conflict-driven snapshots),
                    // "always" (every save), or a comma-separated cause list like
                    // "death,disconnect,shutdown,world_save".
                    // Conflict-driven snapshots in ConflictManager are ALWAYS created
                    // regardless of this setting.
                    if (snapshotManager != null && shouldCreateSnapshot(data.getSaveCause())) {
                        snapshotManager.createSnapshot(uuid, compressed, data.getSaveCause())
                            .thenRun(() -> snapshotManager.pruneSnapshots(uuid, config.getMaxSnapshots()));
                    }

                    if (config.isLogTiming()) {
                        long totalElapsed = (System.nanoTime() - startTime) / 1_000_000;
                        logger.info("[Timing] Total save for " + uuid + ": " + totalElapsed + "ms (v" + expectedVersion + "->v" + (expectedVersion + 1) + ", ft=" + fencingToken + ")");
                    }

                    if (config.isDebug()) {
                        logger.info("Saved data for " + uuid + " (v" + (expectedVersion + 1) + ", " + compressed.length + " bytes in DB)");
                    }

                    // Log successful save (Raft-inspired per-UUID ordered log)
                    logOperation(uuid, OperationType.SAVE, fencingToken, expectedVersion + 1,
                        compressed.length, "Saved v" + (expectedVersion + 1) + " cause=" + data.getSaveCause());

                    // Publish critical event: player checked out (Streams — recoverable)
                    publishCheckout(uuid, expectedVersion + 1, fencingToken, data.getSaveCause());
                }

                // Notify other servers via Redis that the lock is released
                notifyLockReleased(uuid);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to save data for " + uuid, e);
                // Try to release lock even if save failed
                try {
                    databaseManager.releaseLock(uuid, config.getServerName());
                    notifyLockReleased(uuid);
                } catch (SQLException ex) {
                    logger.log(Level.WARNING, "Failed to release lock after save error for " + uuid, ex);
                }
            } finally {
                pendingSaveCount.decrementAndGet();
            }
        });
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
        data.setInventory(sparseContents(player.getInventory().getContents()));
        data.setArmor(sparseContents(player.getInventory().getArmorContents()));
        org.bukkit.inventory.ItemStack offhand = player.getInventory().getItemInOffHand();
        data.setOffhand(offhand != null && offhand.getType() == org.bukkit.Material.AIR ? null : offhand);

        // Ender chest
        data.setEnderChest(sparseContents(player.getEnderChest().getContents()));

        // Vitals
        data.setHealth(player.getHealth());
        data.setMaxHealth(player.getMaxHealth());
        data.setFoodLevel(player.getFoodLevel());
        data.setSaturation(player.getSaturation());
        data.setExhaustion(player.getExhaustion());

        // Experience
        data.setExpLevel(player.getLevel());
        data.setExpProgress(player.getExp());
        data.setTotalExperience(player.getTotalExperience());

        // Potion effects
        List<PlayerData.PotionEffectData> effects = new ArrayList<>();
        for (PotionEffect effect : player.getActivePotionEffects()) {
            effects.add(PlayerDataSerializer.toPotionEffectData(effect));
        }
        data.setPotionEffects(effects);

        // Extra
        data.setGameMode(player.getGameMode());
        data.setFireTicks(player.getFireTicks());
        data.setRemainingAir(player.getRemainingAir());
        data.setMaximumAir(player.getMaximumAir());

        // Flight status
        data.setFlying(player.isFlying());
        data.setAllowFlight(player.getAllowFlight());

        // Advancements (using Bukkit API - iterates all advancement criteria)
        if (config.isSyncAdvancements()) {
            collectAdvancements(player, data);
        }

        // Statistics
        if (config.isSyncStatistics()) {
            collectStatistics(player, data);
        }

        // Attributes
        if (config.isSyncAttributes()) {
            collectAttributes(player, data);
        }

        // Persistent Data Container
        if (config.isSyncPDC()) {
            collectPDC(player, data);
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

            // UNTYPED statistics
            for (Statistic stat : untypedStats) {
                try {
                    int value = player.getStatistic(stat);
                    statistics.computeIfAbsent("UNTYPED", k -> new HashMap<>()).put(stat.name(), value);
                } catch (Exception ignored) {}
            }

            // ITEM statistics
            for (Statistic stat : itemStats) {
                Map<String, Integer> itemStatMap = statistics.computeIfAbsent("ITEM_" + stat.name(), k -> new HashMap<>());
                for (org.bukkit.Material mat : itemMaterials) {
                    try {
                        int v = player.getStatistic(stat, mat);
                        if (v != 0) itemStatMap.put(mat.name(), v);
                    } catch (Exception ignored) {}
                }
            }

            // BLOCK statistics
            for (Statistic stat : blockStats) {
                Map<String, Integer> blockStatMap = statistics.computeIfAbsent("BLOCK_" + stat.name(), k -> new HashMap<>());
                for (org.bukkit.Material mat : blockMaterials) {
                    try {
                        int v = player.getStatistic(stat, mat);
                        if (v != 0) blockStatMap.put(mat.name(), v);
                    } catch (Exception ignored) {}
                }
            }

            // ENTITY statistics
            for (Statistic stat : entityStats) {
                Map<String, Integer> entityStatMap = statistics.computeIfAbsent("ENTITY_" + stat.name(), k -> new HashMap<>());
                for (org.bukkit.entity.EntityType ent : aliveEntities) {
                    try {
                        int v = player.getStatistic(stat, ent);
                        if (v != 0) entityStatMap.put(ent.name(), v);
                    } catch (Exception ignored) {}
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

    private void collectPDC(Player player, PlayerData data) {
        try {
            org.bukkit.persistence.PersistentDataContainer pdc = player.getPersistentDataContainer();
            if (pdc == null || pdc.isEmpty()) {
                data.setPersistentDataContainer(new HashMap<>());
                return;
            }
            java.lang.reflect.Method serialize = findMethod(pdc.getClass(), "serializeToBytes");
            if (serialize == null) {
                if (config.isDebug()) {
                    logger.fine("[PDC] serializeToBytes not found on " + pdc.getClass().getName() + ", PDC sync disabled");
                }
                data.setPersistentDataContainer(new HashMap<>());
                return;
            }
            serialize.setAccessible(true);
            byte[] bytes = (byte[]) serialize.invoke(pdc);
            if (bytes != null && bytes.length > 0) {
                Map<String, byte[]> pdcMap = new HashMap<>();
                pdcMap.put("__pdc_bytes__", bytes);
                data.setPersistentDataContainer(pdcMap);
            } else {
                data.setPersistentDataContainer(new HashMap<>());
            }
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect PDC: " + e.getMessage());
            }
            data.setPersistentDataContainer(new HashMap<>());
        }
    }

    /**
     * Walk the class hierarchy (including superclasses) to find a declared
     * method by name, even if it is package-private or protected.
     *
     * <p>{@code Class.getMethod()} only finds public methods, which fails for
     * Paper's {@code CraftPersistentDataContainer#serializeToBytes()} when it
     * is package-private on certain versions. This method uses
     * {@code getDeclaredMethod()} on each class in the hierarchy.
     */
    private static java.lang.reflect.Method findMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
        Class<?> c = clazz;
        while (c != null && c != Object.class) {
            try {
                return c.getDeclaredMethod(name, paramTypes);
            } catch (NoSuchMethodException ignored) {
                c = c.getSuperclass();
            }
        }
        return null;
    }

    // ==================== Data Apply Helpers ====================

    @SuppressWarnings("deprecation")
    private void applyAdvancements(Player player, PlayerData data) {
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
            for (Map.Entry<String, Map<String, Integer>> cat : data.getStatistics().entrySet()) {
                String category = cat.getKey();
                try {
                    if (category.startsWith("ITEM_")) {
                        Statistic statistic = Statistic.valueOf(category.substring("ITEM_".length()));
                        for (Map.Entry<String, Integer> stat : cat.getValue().entrySet()) {
                            try {
                                org.bukkit.Material mat = org.bukkit.Material.matchMaterial(stat.getKey());
                                if (mat != null) player.setStatistic(statistic, mat, stat.getValue());
                            } catch (Exception ignored) {}
                        }
                    } else if (category.startsWith("BLOCK_")) {
                        Statistic statistic = Statistic.valueOf(category.substring("BLOCK_".length()));
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
                    } else {
                        // Untyped statistics (category == stat.getType().name(), e.g. UNTYPED)
                        for (Map.Entry<String, Integer> stat : cat.getValue().entrySet()) {
                            try {
                                Statistic statistic = Statistic.valueOf(stat.getKey());
                                if (statistic.getType() == Statistic.Type.UNTYPED) {
                                    player.setStatistic(statistic, stat.getValue());
                                }
                            } catch (Exception ignored) {}
                        }
                    }
                } catch (Exception ignored) {
                    // Unknown statistic name on this version
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

    private void applyPDC(Player player, PlayerData data) {
        // Restore the player's PersistentDataContainer from the serialized bytes
        // captured by collectPDC(). Uses reflection to call CraftBukkit/Paper's
        // deserializeBytes(byte[]) on the implementation class.
        if (data.getPersistentDataContainer() == null || data.getPersistentDataContainer().isEmpty()) {
            return;
        }
        byte[] pdcBytes = data.getPersistentDataContainer().get("__pdc_bytes__");
        if (pdcBytes == null) {
            return;
        }
        try {
            org.bukkit.persistence.PersistentDataContainer pdc = player.getPersistentDataContainer();
            java.lang.reflect.Method deserialize = findMethod(pdc.getClass(), "deserializeBytes", byte[].class);
            if (deserialize == null) {
                if (config.isDebug()) {
                    logger.fine("[PDC] deserializeBytes not found on " + pdc.getClass().getName());
                }
                return;
            }
            deserialize.setAccessible(true);
            deserialize.invoke(pdc, pdcBytes);
        } catch (Exception e) {
            if (config.isDebug()) logger.warning("Failed to apply PDC: " + e.getMessage());
        }
    }

    // ==================== Periodic Save ====================

    /**
     * Save all online players' data (for periodic saves and shutdown).
     *
     * <p>Data collection happens on the calling thread because Bukkit's player API
     * is not thread-safe (during shutdown this is the main thread). The expensive
     * serialization and database write for each player is dispatched to the async
     * executor and run in parallel, and we block on
     * {@code CompletableFuture.allOf(...).join()} so that shutdown does not return
     * until every save has completed. This avoids saving every player synchronously
     * on the main thread, which would hang the server during shutdown.
     */
    public void saveAllOnlinePlayers() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Player player : Bukkit.getOnlinePlayers()) {
            if (!activePlayers.containsKey(player.getUniqueId())) {
                continue;
            }
            UUID uuid = player.getUniqueId();
            // Collect on the calling (main) thread - Bukkit API is not thread-safe.
            PlayerData data = collectPlayerData(player);

            // Serialize + DB write happen async, but we wait for all of them below.
            futures.add(asyncExecutor.submit(() -> {
                try {
                    byte[] serialized = PlayerDataSerializer.serialize(data);
                    byte[] compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize());
                    long checksum = DatabaseManager.computeChecksum(serialized);
                    long expectedVersion = data.getVersion();
                    long fencingToken = data.getFencingToken();
                    boolean saved = databaseManager.saveData(uuid, compressed, checksum, expectedVersion, fencingToken, config.getServerName());
                    if (!saved) {
                        long actualVersion = databaseManager.getCurrentVersion(uuid);
                        conflictManager.handleConflict(uuid, data, expectedVersion, actualVersion);
                    }
                    notifyLockReleased(uuid);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to save data for " + uuid + " during bulk save", e);
                }
            }));
        }

        // Wait for all async saves to complete before returning. This is critical
        // for shutdown: the caller (onDisable) must not proceed to tear down the
        // thread pool / database until every save has finished.
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        logger.info("Saved data for all online players.");
    }

    /**
     * Save a single player's data asynchronously (for periodic saves).
     */
    /**
     * Save a single player's data asynchronously (for periodic saves).
     *
     * <p><b>Folia compatibility:</b> On Folia, {@code collectPlayerData} must run
     * on the entity's region thread, not the global region thread. We dispatch
     * the data collection via {@link SchedulerUtil#runAtEntity}, then perform
     * the async DB save from the collected data.
     */
    public void savePlayerAsync(Player player) {
        if (!activePlayers.containsKey(player.getUniqueId())) {
            return;
        }

        UUID uuid = player.getUniqueId();
        Plugin plugin = JavaPlugin.getPlugin(FastSync.class);

        // Collect player data on the entity's region thread (Folia-safe)
        SchedulerUtil.runAtEntity(plugin, player, () -> {
            PlayerData data = collectPlayerData(player);

            pendingSaveCount.incrementAndGet();
            asyncExecutor.execute(() -> {
                try {
                    byte[] serialized = PlayerDataSerializer.serialize(data);
                    byte[] compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize());
                    long checksum = DatabaseManager.computeChecksum(serialized);
                long expectedVersion = data.getVersion();
                long fencingToken = data.getFencingToken();
                boolean saved = databaseManager.saveData(uuid, compressed, checksum, expectedVersion, fencingToken, config.getServerName());
                if (!saved) {
                    long actualVersion = databaseManager.getCurrentVersion(uuid);
                    conflictManager.handleConflict(uuid, data, expectedVersion, actualVersion);
                    if (saveLatency != null) saveLatency.recordError();
                }

                if (config.isDebug()) {
                    logger.info("Periodic save for " + uuid + " (v" + expectedVersion + "->v" + (expectedVersion + 1) + ", " + compressed.length + " bytes)");
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "Periodic save failed for " + uuid, e);
            } finally {
                pendingSaveCount.decrementAndGet();
            }
        });
        }, null); // retired callback: do nothing if entity is no longer valid
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
    public void cleanupStaleEntries() {
        long now = System.currentTimeMillis();
        long staleThreshold = 5 * 60 * 1000; // 5 minutes

        pendingData.forEach((uuid, data) -> {
            if (data != null && data.getTimestamp() > 0 && (now - data.getTimestamp()) > staleThreshold) {
                pendingData.remove(uuid);
                asyncExecutor.execute(() -> {
                    try {
                        databaseManager.releaseLock(uuid, config.getServerName());
                        notifyLockReleased(uuid);
                        logger.warning("Cleaned up stale pending data for " + uuid);
                    } catch (SQLException e) {
                        logger.log(Level.WARNING, "Failed to release stale lock for " + uuid, e);
                    }
                });
            }
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
        return pendingData.size();
    }

    public int getActiveCount() {
        return activePlayers.size();
    }

    /**
     * Wait for all pending async saves to complete (for graceful shutdown).
     *
     * <p>Replaces the previous busy-loop ({@code Thread.sleep(100)} polling
     * {@code pendingSaveCount}) with a graceful executor shutdown +
     * {@code awaitTermination}. This avoids CPU spin and provides cleaner
     * semantics. Safe to call only from the shutdown path — after this returns,
     * the async executor is no longer usable.
     */
    public void waitForPendingSaves(long timeoutMillis) {
        if (asyncExecutor == null) return;
        int remaining = pendingSaveCount.get();
        if (remaining == 0) return;
        // Gracefully stop accepting new tasks and wait for in-flight saves to drain.
        asyncExecutor.shutdown((int) Math.max(1, timeoutMillis / 1000));
        asyncExecutor = null;
        remaining = pendingSaveCount.get();
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
        if (redissonManager != null) {
            redissonManager.publish(StreamEvent.create(
                StreamEventType.PLAYER_CHECKOUT, uuid, config.getServerName(),
                "", version, fencingToken, "cause=" + cause));
        }
    }

    /**
     * Publish a PLAYER_CHECKIN event when player data is loaded and lock acquired.
     */
    private void publishCheckin(UUID uuid, long version, long fencingToken) {
        if (redissonManager != null) {
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

    // ==================== LoadResult ====================

    public static class LoadResult {
        public enum Status { SUCCESS, LOCKED, ERROR }

        private final Status status;
        private final String message;

        private LoadResult(Status status, String message) {
            this.status = status;
            this.message = message;
        }

        public static LoadResult success() { return new LoadResult(Status.SUCCESS, null); }
        public static LoadResult locked() { return new LoadResult(Status.LOCKED, null); }
        public static LoadResult error(String message) { return new LoadResult(Status.ERROR, message); }

        public Status getStatus() { return status; }
        public String getMessage() { return message; }
        public boolean isSuccess() { return status == Status.SUCCESS; }
    }
}
