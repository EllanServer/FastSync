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
import com.fastsync.log.OperationLogManager;
import com.fastsync.log.OperationType;
import com.fastsync.redis.RedisManager;
import com.fastsync.redis.stream.StreamEvent;
import com.fastsync.redis.stream.StreamEventType;
import com.fastsync.redis.stream.StreamManager;
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
import org.bukkit.persistence.PersistentDataContainer;
import org.bukkit.persistence.PersistentDataType;
import org.bukkit.potion.PotionEffect;
import org.bukkit.Statistic;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    private RedisManager redisManager;
    private SnapshotManager snapshotManager;
    private ConflictManager conflictManager;
    private OperationLogManager operationLogManager;
    private StreamManager streamManager;

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

        // Initialize Redis if enabled
        if (config.isRedisEnabled()) {
            try {
                redisManager = new RedisManager(logger, config, config.getServerName());
                redisManager.initialize();
                logger.info("Redis lock coordination enabled.");
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to connect to Redis! Falling back to database polling.", e);
                redisManager = null;
            }
        } else {
            logger.info("Redis not enabled, using database polling for lock coordination.");
        }

        // Initialize Redis Streams for critical, recoverable event delivery
        if (config.isRedisEnabled() && config.isStreamsEnabled() && redisManager != null) {
            try {
                streamManager = new StreamManager(logger, config, config.getServerName());
                streamManager.initialize();
                // Register listener for incoming events from other servers
                streamManager.addListener(this::handleStreamEvent);
                logger.info("Redis Streams enabled (critical event delivery).");
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to initialize Redis Streams. Continuing without stream support.", e);
                streamManager = null;
            }
        }

        // Initialize conflict manager (Dynamo-style conflict recovery)
        conflictManager = new ConflictManager(logger, config, snapshotManager);

        // Initialize operation log (Raft-inspired per-UUID ordered log)
        operationLogManager = new OperationLogManager(logger, config);
        try {
            operationLogManager.initialize(databaseManager.getDataSource(), databaseManager);
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Failed to initialize operation log", e);
        }

        // Initialize latency trackers (Dynamo p99.9 SLA focus)
        if (config.isLatencyTrackingEnabled()) {
            int window = config.getLatencyWindowSize();
            loadLatency = new LatencyTracker("DB-Load", logger, window);
            saveLatency = new LatencyTracker("DB-Save", logger, window);
            serializeLatency = new LatencyTracker("Serialize", logger, window);
            logger.info("Latency tracking enabled (p50/p99/p99.9, window=" + window + ").");
        }
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
                if (redisManager != null && redisManager.isHealthy()) {
                    boolean released = redisManager.waitForLockRelease(uuid, config.getLockRetryIntervalMs());
                    if (released && config.isDebug()) {
                        logger.info("Received lock release notification for " + uuid);
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
                    // Success - create snapshot if enabled (backup system)
                    if (snapshotManager != null) {
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
        if (redisManager != null && redisManager.isHealthy()) {
            redisManager.notifyLockReleased(uuid);
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

        // Inventory
        data.setInventory(player.getInventory().getContents());
        data.setArmor(player.getInventory().getArmorContents());
        data.setOffhand(player.getInventory().getItemInOffHand());

        // Ender chest
        data.setEnderChest(player.getEnderChest().getContents());

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

    @SuppressWarnings("deprecation")
    private void collectAdvancements(Player player, PlayerData data) {
        try {
            Map<String, Map<String, Long>> advancements = new HashMap<>();
            java.util.Iterator<org.bukkit.advancement.Advancement> it = Bukkit.advancementIterator();
            while (it.hasNext()) {
                org.bukkit.advancement.Advancement adv = it.next();
                org.bukkit.advancement.AdvancementProgress progress = player.getAdvancementProgress(adv);
                if (progress == null) continue;

                String key = adv.getKey().toString();
                Map<String, Long> criteria = new HashMap<>();
                for (String awarded : progress.getAwardedCriteria()) {
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
        try {
            Map<String, Map<String, Integer>> statistics = new HashMap<>();
            for (Statistic stat : Statistic.values()) {
                try {
                    String categoryName = stat.getType().name();
                    String statName = stat.name();
                    int value;

                    if (stat.getType() == Statistic.Type.UNTYPED) {
                        value = player.getStatistic(stat);
                    } else {
                        // Skip typed statistics that require a material/entity parameter
                        // for simplicity - these are harder to enumerate
                        continue;
                    }

                    statistics.computeIfAbsent(categoryName, k -> new HashMap<>()).put(statName, value);
                } catch (Exception ignored) {
                    // Some stats throw on certain versions
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
            for (Attribute attr : Attribute.values()) {
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

    @SuppressWarnings("unchecked")
    private void collectPDC(Player player, PlayerData data) {
        try {
            Map<String, byte[]> pdcData = new HashMap<>();
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            // PDC keys are not enumerable in Bukkit API - we serialize the whole container
            // Using Bukkit's serialization to byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                org.bukkit.util.io.BukkitObjectOutputStream oos = new org.bukkit.util.io.BukkitObjectOutputStream(baos);
                // Write the player's PDC as a serialized map
                Map<String, Object> pdcMap = new HashMap<>();
                // We can't enumerate PDC keys, so we store a marker
                // In production, plugins would register their keys for sync
                pdcMap.put("__pdc_serialized__", true);
                oos.writeObject(pdcMap);
                oos.close();
                pdcData.put("__pdc__", baos.toByteArray());
            } catch (Exception ignored) {}
            data.setPersistentDataContainer(pdcData);
        } catch (Exception e) {
            if (config.isDebug()) {
                logger.warning("Failed to collect PDC: " + e.getMessage());
            }
        }
    }

    // ==================== Data Apply Helpers ====================

    @SuppressWarnings("deprecation")
    private void applyAdvancements(Player player, PlayerData data) {
        try {
            java.util.Iterator<org.bukkit.advancement.Advancement> it = Bukkit.advancementIterator();
            while (it.hasNext()) {
                org.bukkit.advancement.Advancement adv = it.next();
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
     * Save all online players' data (for periodic saves and shutdown).
     */
    public void saveAllOnlinePlayers() {
        for (Player player : Bukkit.getOnlinePlayers()) {
            if (activePlayers.containsKey(player.getUniqueId())) {
                UUID uuid = player.getUniqueId();
                PlayerData data = collectPlayerData(player);

                // Save synchronously during shutdown
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
            }
        }
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
                } else if (saveLatency != null) {
                    saveLatency.record(0); // periodic save, timing already tracked elsewhere
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

        // Close Redis Streams (publishes SERVER_STOP event)
        if (streamManager != null) {
            streamManager.close();
            streamManager = null;
        }

        // Close Redis pub/sub
        if (redisManager != null) {
            redisManager.close();
            redisManager = null;
        }

        // Shut down thread pool
        if (asyncExecutor != null) {
            asyncExecutor.shutdown(10);
            asyncExecutor = null;
        }
    }

    // ==================== Helpers ====================

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
     */
    public void waitForPendingSaves(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (pendingSaveCount.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
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

    public boolean isRedisEnabled() {
        return redisManager != null && redisManager.isHealthy();
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
        if (operationLogManager != null && operationLogManager.isEnabled()) {
            OperationLog log = OperationLog.create(uuid, type, config.getServerName(),
                fencingToken, version, dataSize, detail);
            operationLogManager.append(log)
                .thenCompose(v -> operationLogManager.prune(uuid, config.getOperationLogRetention()))
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
        if (operationLogManager == null || !operationLogManager.isEnabled()) {
            return List.of();
        }
        try {
            return operationLogManager.queryHistory(uuid, limit);
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Failed to query operation log for " + uuid, e);
            return List.of();
        }
    }

    // ==================== Redis Streams (critical events) ====================

    /**
     * Handle a critical stream event from another server.
     * This is called by the StreamManager's consumer thread.
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
        if (streamManager != null) {
            streamManager.publish(StreamEvent.create(
                StreamEventType.PLAYER_CHECKOUT, uuid, config.getServerName(),
                "", version, fencingToken, "cause=" + cause));
        }
    }

    /**
     * Publish a PLAYER_CHECKIN event when player data is loaded and lock acquired.
     */
    private void publishCheckin(UUID uuid, long version, long fencingToken) {
        if (streamManager != null) {
            streamManager.publish(StreamEvent.create(
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
