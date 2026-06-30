package com.fastsync;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.fastsync.listeners.PlayerListener;
import com.fastsync.log.OperationLog;
import com.fastsync.sync.SyncManager;
import com.fastsync.util.SchedulerUtil;
import org.bukkit.Bukkit;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

/**
 * FastSync - High-performance cross-server player data synchronization.
 *
 * Design principles (based on community discussion):
 *   1. NBT byte[] serialization - NO base64 string encoding, NO Kryo, NO Gson
 *      ItemStack.serializeAsBytes() (Paper 1.21.11+ native API; no fallback)
 *   2. LZ4 compression to reduce database storage and network transfer
 *   3. Data loaded during login phase (AsyncPlayerPreLoginEvent) - not after joining
 *      Prevents item duplication bugs from "enter server then load" approach
 *   4. Cross-server lock with proper acknowledgment (Redis pub/sub)
 *      NOT HuskSync's broken "petition" that forces entry after timeout
 *   5. Dedicated thread pool for async operations (NOT ForkJoinPool.commonPool)
 *   6. Version byte prefix for future serialization format migration
 */
public class FastSync extends JavaPlugin implements CommandExecutor, TabCompleter {

    private static final LegacyComponentSerializer MESSAGE_SERIALIZER =
        LegacyComponentSerializer.legacySection();
    private static final String RED = "\u00a7c";
    private static final String GREEN = "\u00a7a";
    private static final String YELLOW = "\u00a7e";
    private static final String GRAY = "\u00a77";
    private static final String GOLD = "\u00a76";
    private static final String AQUA = "\u00a7b";
    private static final String WHITE = "\u00a7f";

    private static void sendMessage(CommandSender sender, String message) {
        sender.sendMessage(MESSAGE_SERIALIZER.deserialize(message));
    }

    private static String formatTime(long epochMillis) {
        return epochMillis > 0 ? new java.util.Date(epochMillis).toString() : "never";
    }

    private ConfigManager configManager;
    private DatabaseManager databaseManager;
    private SyncManager syncManager;

    private static FastSync instance;

    public static FastSync getInstance() { return instance; }

    private Object cleanupTask;
    private Object periodicSaveTask;
    private Object heartbeatTask;

    /**
     * Start (or restart) the heartbeat task. Called from onEnable and from
     * the reload command when heartbeat-interval-seconds changes.
     *
     * <p>Without this, /fastsync reload would change the config value but
     * the old timer would keep running at the old interval — a subtle
     * production trap that could cause lock expiry if the new interval is
     * shorter than expected.
     */
    private void restartHeartbeatTask() {
        // Cancel old task if running
        if (heartbeatTask != null) {
            SchedulerUtil.cancel(heartbeatTask);
            heartbeatTask = null;
        }
        // Start new task with current config
        long heartbeatTicks = configManager.getHeartbeatIntervalSeconds() * 20L;
        heartbeatTask = SchedulerUtil.runAsyncTimer(this, () -> {
            syncManager.heartbeatOnlinePlayers();
        }, heartbeatTicks, heartbeatTicks);
        getLogger().info("Lock heartbeat " + (heartbeatTask != null ? "restarted" : "started")
            + ": every " + configManager.getHeartbeatIntervalSeconds() + " seconds"
            + " (lock-timeout=" + configManager.getLockTimeout() + "s).");
    }

    /** Start, stop, or reschedule periodic saves after a validated reload. */
    private void restartPeriodicSaveTask() {
        if (periodicSaveTask != null) {
            SchedulerUtil.cancel(periodicSaveTask);
            periodicSaveTask = null;
        }
        if (!configManager.isPeriodicSave()) {
            getLogger().info("Periodic save disabled.");
            return;
        }

        long intervalTicks = configManager.getPeriodicSaveIntervalSeconds() * 20L;
        periodicSaveTask = SchedulerUtil.runGlobalTimer(this, () -> {
            List<Player> players = new ArrayList<>(Bukkit.getOnlinePlayers());
            final int batchSize = configManager.getPeriodicSaveBatchSize();
            for (int i = 0; i < players.size(); i += batchSize) {
                final int start = i;
                final int end = Math.min(i + batchSize, players.size());
                long delayTicks = i / batchSize;
                SchedulerUtil.runGlobalDelayed(this, () -> {
                    for (int j = start; j < end; j++) {
                        syncManager.savePlayerAsync(players.get(j));
                    }
                }, delayTicks);
            }
        }, intervalTicks, intervalTicks);
        getLogger().info("Periodic save scheduled: every "
            + configManager.getPeriodicSaveIntervalSeconds() + " seconds, batch="
            + configManager.getPeriodicSaveBatchSize() + ".");
    }

    @Override
    public void onEnable() {
        instance = this;
        // Initialize config
        saveDefaultConfig();
        configManager = new ConfigManager(this);
        try {
            configManager.load();
        } catch (RuntimeException e) {
            // Clean-slate: config parse failure is a hard error (no Bukkit
            // fallback). Fail startup explicitly rather than letting the
            // RuntimeException propagate into Bukkit's loader.
            getLogger().log(Level.SEVERE, "Failed to load config.yml — refusing to start: " + e.getMessage(), e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // Validate insecure default configuration before touching the DB.
        // Clean-slate: the bundled config ships sample (root/password,
        // sslMode=DISABLED) values; running with those in production is a
        // footgun. Fail startup unless the operator has explicitly opted in.
        try {
            configManager.validateProductionSafety();
        } catch (RuntimeException e) {
            getLogger().log(Level.SEVERE, "Refusing to start: " + e.getMessage(), e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // Initialize database
        databaseManager = new DatabaseManager(getLogger(), configManager);
        try {
            databaseManager.initialize();
        } catch (SQLException | RuntimeException e) {
            getLogger().log(Level.SEVERE, "Failed to initialize database! Check your config.yml.", e);
            try {
                databaseManager.close();
            } catch (RuntimeException cleanupError) {
                e.addSuppressed(cleanupError);
                getLogger().log(Level.WARNING, "Database cleanup also failed", cleanupError);
            }
            databaseManager = null;
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // Initialize sync manager (creates thread pool + optional Redis)
        syncManager = new SyncManager(this, configManager, databaseManager);
        try {
            syncManager.initialize();
        } catch (RuntimeException e) {
            getLogger().log(Level.SEVERE, "Failed to initialize synchronization services — refusing to start.", e);
            try {
                syncManager.beginShutdown();
                syncManager.shutdown();
            } catch (RuntimeException cleanupError) {
                e.addSuppressed(cleanupError);
                getLogger().log(Level.WARNING, "Synchronization cleanup also failed", cleanupError);
            } finally {
                syncManager = null;
                try {
                    databaseManager.close();
                } catch (RuntimeException cleanupError) {
                    e.addSuppressed(cleanupError);
                    getLogger().log(Level.WARNING, "Database cleanup also failed", cleanupError);
                }
                databaseManager = null;
            }
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // Register plugin messaging channel for proxy handoff communication
        // This is optional — if no Velocity proxy with FastSync Proxy is installed,
        // the channel simply never receives messages. The backend works standalone.
        Bukkit.getMessenger().registerIncomingPluginChannel(
            this, com.fastsync.messaging.HandoffMessageListener.CHANNEL,
            new com.fastsync.messaging.HandoffMessageListener(
                this, configManager, databaseManager, syncManager));
        Bukkit.getMessenger().registerOutgoingPluginChannel(
            this, com.fastsync.messaging.HandoffMessageListener.CHANNEL);
        getLogger().info("Registered fastsync:handoff plugin messaging channel (optional: for Velocity proxy integration)");

        // Register listeners
        getServer().getPluginManager().registerEvents(
            new PlayerListener(this, syncManager), this);
        getServer().getPluginManager().registerEvents(
            new com.fastsync.listeners.DataListener(syncManager, configManager), this);

        // Register dirty-tracking listener (component-level change detection)
        if (configManager.isDirtyTrackingEnabled() && syncManager.getDirtyMask() != null) {
            getServer().getPluginManager().registerEvents(
                new com.fastsync.listeners.dirty.DirtyTrackingListener(syncManager.getDirtyMask()), this);
            getLogger().info("Dirty tracking enabled: validation every " +
                configManager.getDirtyValidationInterval() + " saves");
        }

        // Register command
        if (getCommand("fastsync") != null) {
            getCommand("fastsync").setExecutor(this);
            getCommand("fastsync").setTabCompleter(this);
        }

        // Start cleanup task (every 5 minutes = 6000 ticks)
        cleanupTask = SchedulerUtil.runAsyncTimer(this, () -> {
            syncManager.cleanupStaleEntries();
        }, 6000L, 6000L);

        // Start heartbeat task — refreshes locked_at for all online players.
        // This is the PRIMARY mechanism for keeping online locks alive.
        // Runs on async thread (DB I/O only, no Bukkit API calls).
        restartHeartbeatTask();

        // Start periodic save task (the same helper safely reschedules it on reload).
        restartPeriodicSaveTask();

        getLogger().info("FastSync v" + getPluginMeta().getVersion() + " enabled!");
        getLogger().info("Server ID: " + configManager.getServerName());
        getLogger().info("Serialization: Paper native ItemStack byte serialization");
        getLogger().info("Compression: " + (configManager.isCompressionEnabled()
            ? configManager.getCompressionType() : "Disabled"));
        getLogger().info("Redis: " + (configManager.isRedisEnabled() ? "Enabled" : "Disabled (DB polling)"));
    }

    @Override
    public void onDisable() {
        // P0 (round 15): close the online-save gate FIRST. Any periodic/death/
        // world_save task that fires during the shutdown-save window must be
        // rejected by savePlayerAsync, otherwise it could collect a stale
        // snapshot and commit it after the SHUTDOWN save — rolling back the
        // player's final state.
        if (syncManager != null) {
            syncManager.beginShutdown();
        }

        // Cancel scheduled tasks (Paper/Folia compatible)
        SchedulerUtil.cancel(cleanupTask);
        SchedulerUtil.cancel(periodicSaveTask);
        SchedulerUtil.cancel(heartbeatTask);

        // Save all online players synchronously (release locks — server is stopping)
        if (syncManager != null) {
            getLogger().info("Saving all online players (shutdown)...");
            SyncManager.SaveAllResult result = syncManager.saveAllOnlinePlayers(SyncManager.SaveKind.SHUTDOWN);
            getLogger().info("Shutdown save: " + result.success() + "/" + result.total()
                + " succeeded" + (result.failed() > 0 ? ", " + result.failed() + " failed" : "") + ".");
        }

        // Shut down sync manager (waits for pending saves, closes Redis + thread pool)
        if (syncManager != null) {
            syncManager.shutdown();
        }

        // Close database
        if (databaseManager != null) {
            databaseManager.close();
        }

        getLogger().info("FastSync disabled!");
        instance = null;
    }

    // ==================== Command Handler ====================

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command,
                             @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("fastsync.admin")) {
            sendMessage(sender, RED + "You don't have permission to use this command.");
            return true;
        }

        if (args.length == 0) {
            sendHelp(sender, label);
            return true;
        }

        switch (args[0].toLowerCase()) {
            case "reload" -> {
                ConfigManager.ReloadResult reloadResult;
                try {
                    reloadResult = configManager.reloadSafely();
                } catch (RuntimeException e) {
                    getLogger().log(Level.WARNING, "Configuration reload rejected", e);
                    sendMessage(sender, RED + "[FastSync] Reload rejected: " + e.getMessage());
                    return true;
                }
                if (!reloadResult.applied()) {
                    sendMessage(sender, YELLOW + "[FastSync] Reload not applied; restart required for: "
                        + String.join(", ", reloadResult.restartRequiredChanges()));
                    return true;
                }
                // Refresh SyncManager caches that depend on config (e.g. snapshot trigger set)
                if (syncManager != null) {
                    syncManager.refreshConfigCache();
                }
                // Restart heartbeat task — interval may have changed
                restartHeartbeatTask();
                // Periodic-save enable/interval/batch are live-reloadable.
                restartPeriodicSaveTask();
                // Reset protection mode on reload — only if DB is healthy
                boolean resetOk = syncManager.resetProtectionMode();
                if (resetOk) {
                    sendMessage(sender, GREEN + "[FastSync] Protection mode reset.");
                } else {
                    sendMessage(sender, RED + "[FastSync] Protection mode still active: database is unhealthy.");
                }
                sendMessage(sender, GREEN + "[FastSync] Configuration reloaded.");
                sendMessage(sender, GRAY + "Server: " + configManager.getServerName());
                sendMessage(sender, GRAY + "Compression: " +
                    (configManager.isCompressionEnabled()
                        ? configManager.getCompressionType() : "Disabled"));
                sendMessage(sender, GRAY + "Redis: " +
                    (configManager.isRedisEnabled() ? "Enabled" : "Disabled"));
                sendMessage(sender, GRAY + "Heartbeat: every " +
                    configManager.getHeartbeatIntervalSeconds() + "s (timer restarted)");
            }
            case "status" -> sendStatus(sender);
            case "debug" -> {
                boolean newDebug = !configManager.isDebug();
                // Runtime-only by design. Re-saving Bukkit's cached config here
                // used to overwrite external edits and Sparrow migrations with
                // a stale in-memory copy of the whole YAML document.
                configManager.setDebug(newDebug);
                syncManager.refreshConfigCache();
                sendMessage(sender, GREEN + "[FastSync] Debug mode: " +
                    (newDebug ? GREEN + "ON" : RED + "OFF"));
                sendMessage(sender, GRAY + "Runtime only; edit config.yml to persist across restarts.");
            }
            case "saveall" -> {
                sendMessage(sender, YELLOW + "[FastSync] Saving all online players...");
                // CRITICAL (Folia-safety): the save flow is split into two phases:
                //
                //   Phase 1 (dispatch) — runs on the global region. Captures the
                //   player list via Bukkit.getOnlinePlayers(), then for each
                //   player reads player.getUniqueId() and dispatches via
                //   SchedulerUtil.runAtEntity. Both operations touch the Player
                //   object, which is forbidden from async threads on Folia.
                //
                //   Phase 2 (wait) — runs on the async executor. Iterates the
                //   futures returned by phase 1 and blocks on future.get() with
                //   a deadline. This phase does NOT touch any Player object, so
                //   it is safe to run on async. Moving it off the global region
                //   prevents /saveall from blocking global ticks while DB writes
                //   complete (up to 30s on large servers).
                SchedulerUtil.runGlobal(this, () -> {
                    List<Player> players = new ArrayList<>(Bukkit.getOnlinePlayers());
                    List<Map.Entry<UUID, java.util.concurrent.CompletableFuture<SyncManager.SaveResult>>> futures =
                        syncManager.dispatchPlayerSaves(players, SyncManager.SaveKind.BULK);
                    SchedulerUtil.runAsync(this, () -> {
                        try {
                            SyncManager.SaveAllResult result = syncManager.waitForPlayerSaves(futures, SyncManager.SaveKind.BULK);
                            SchedulerUtil.runGlobal(this, () -> {
                                if (result.allSucceeded()) {
                                    sendMessage(sender, GREEN + "[FastSync] All " + result.total() + " players saved!");
                                } else {
                                    sendMessage(sender, YELLOW + "[FastSync] Saved " + result.success()
                                        + "/" + result.total() + " players. " + RED + result.failed() + " failed.");
                                    if (!result.failures().isEmpty()) {
                                        sendMessage(sender, GRAY + "Failed players:");
                                        result.failures().forEach((uuid, reason) ->
                                            sendMessage(sender, GRAY + "  " + uuid + ": " + RED + reason));
                                    }
                                }
                            });
                        } catch (Exception e) {
                            getLogger().log(Level.SEVERE, "Saveall failed", e);
                            SchedulerUtil.runGlobal(this, () ->
                                sendMessage(sender, RED + "[FastSync] Saveall failed: " + e.getMessage())
                            );
                        }
                    });
                });
            }
            case "log" -> {
                if (args.length < 2) {
                    sendMessage(sender, RED + "Usage: /" + label + " log <player|uuid> [limit]");
                    return true;
                }
                int limit;
                if (args.length >= 3) {
                    try {
                        limit = Math.min(Integer.parseInt(args[2]), 50);
                    } catch (NumberFormatException e) {
                        sendMessage(sender, RED + "Invalid number: " + args[2]);
                        return true;
                    }
                } else {
                    limit = 20;
                }
                UUID targetUuid;
                Player target = Bukkit.getPlayer(args[1]);
                if (target != null) {
                    targetUuid = target.getUniqueId();
                } else {
                    try {
                        targetUuid = UUID.fromString(args[1]);
                    } catch (IllegalArgumentException e) {
                        sendMessage(sender, RED + "Player not found or invalid UUID: " + args[1]);
                        return true;
                    }
                }
                final UUID fuuid = targetUuid;
                SchedulerUtil.runAsync(this, () -> {
                    List<OperationLog> logs = syncManager.queryOperationLog(fuuid, limit);
                    // Build all messages on async thread, then send on global thread
                    List<String> messages = new java.util.ArrayList<>();
                    if (logs.isEmpty()) {
                        messages.add(YELLOW + "[FastSync] No operation log entries for " + args[1]);
                    } else {
                        messages.add(GOLD + "===== Operation Log: " + args[1] + " (" + logs.size() + " entries) =====");
                        for (OperationLog log : logs) {
                            String typeColor = switch (log.type()) {
                                case CONFLICT, CHECKSUM_FAIL, LOCK_EXPIRE -> RED;
                                case SAVE, SNAPSHOT, RESTORE -> GREEN;
                                case LOAD, LOCK_ACQUIRE, LOCK_RELEASE -> AQUA;
                            };
                            messages.add(GRAY + "#" + log.seq() + " " +
                                typeColor + log.type() + GRAY +
                                " | server=" + log.serverName() +
                                " v=" + log.version() + " ft=" + log.fencingToken() +
                                " sz=" + log.dataSize() + "B" +
                                (log.detail() != null ? " | " + WHITE + log.detail() : ""));
                        }
                    }
                    SchedulerUtil.runGlobal(this, () -> {
                        for (String msg : messages) {
                            sendMessage(sender, msg);
                        }
                    });
                });
            }
            default -> sendHelp(sender, label);
        }
        return true;
    }

    @Override
    public List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command,
                                      @NotNull String alias, @NotNull String[] args) {
        if (args.length == 1) {
            List<String> suggestions = Arrays.asList("reload", "status", "debug", "saveall", "log");
            List<String> result = new ArrayList<>();
            for (String s : suggestions) {
                if (s.startsWith(args[0].toLowerCase())) {
                    result.add(s);
                }
            }
            return result;
        }
        return new ArrayList<>();
    }

    private void sendHelp(CommandSender sender, String label) {
        sendMessage(sender, GOLD + "===== FastSync =====");
        sendMessage(sender, YELLOW + "/" + label + " reload " + GRAY + "- Reload configuration");
        sendMessage(sender, YELLOW + "/" + label + " status " + GRAY + "- Show plugin status");
        sendMessage(sender, YELLOW + "/" + label + " debug " + GRAY + "- Toggle debug mode");
        sendMessage(sender, YELLOW + "/" + label + " saveall " + GRAY + "- Save all online players");
        sendMessage(sender, YELLOW + "/" + label + " log <player> [n] " + GRAY + "- View operation log for a player");
    }

    private void sendStatus(CommandSender sender) {
        sendMessage(sender, GOLD + "===== FastSync Status =====");
        sendMessage(sender, YELLOW + "Server: " + WHITE + configManager.getServerName());
        sendMessage(sender, YELLOW + "Database: " +
            (databaseManager.isHealthy() ? GREEN + "Connected" : RED + "Disconnected"));
        sendMessage(sender, YELLOW + "Redis: " +
            (configManager.isRedisEnabled()
                ? (syncManager.isRedisHealthy() ? GREEN + "Connected" : RED + "Failed")
                : GRAY + "Disabled"));
        sendMessage(sender, YELLOW + "Serialization: " + WHITE +
            "Paper native ItemStack byte serialization");
        sendMessage(sender, YELLOW + "Active players: " + WHITE + syncManager.getActiveCount());
        sendMessage(sender, YELLOW + "Pending loads: " + WHITE + syncManager.getPendingCount());
        sendMessage(sender, YELLOW + "Pending saves: " + WHITE + syncManager.getPendingSaveCount());
        sendMessage(sender, YELLOW + "Quarantined: " + WHITE + syncManager.getQuarantinedPlayerCount());
        sendMessage(sender, YELLOW + "Protection mode: " +
            (syncManager.isProtectionMode() ? RED + "ACTIVE (DB failures detected)" : GREEN + "Off"));
        sendMessage(sender, YELLOW + "Heartbeat: " + WHITE +
            "every " + configManager.getHeartbeatIntervalSeconds() + "s" +
            " (lock-timeout=" + configManager.getLockTimeout() + "s)");
        sendMessage(sender, YELLOW + "Async threads: " + WHITE +
            "active=" + syncManager.getAsyncActiveCount() +
            ", queue=" + syncManager.getAsyncQueueSize());
        sendMessage(sender, YELLOW + "Login-load semaphore: " + WHITE
            + "available=" + syncManager.getLoginLoadAvailablePermits()
            + "/" + syncManager.getLoginLoadLimit()
            + " (max-concurrent-loads)");
        String finalSaveColor = syncManager.hasFinalSaveAlert() ? RED
            : (syncManager.hasFinalSaveWarning() ? YELLOW : GREEN);
        sendMessage(sender, YELLOW + "Final-save executor: " + finalSaveColor +
            "active=" + syncManager.getFinalSaveActiveCount() +
            ", queue=" + syncManager.getFinalSaveQueueSize() + "/" + syncManager.getFinalSaveQueueCapacity() +
            ", queueFull=" + syncManager.getFinalSaveQueueFullTotal() +
            ", lastQueueFull=" + formatTime(syncManager.getFinalSaveLastQueueFullAt()));
        sendMessage(sender, YELLOW + "Final-save spool events: " + WHITE +
            "spooled=" + syncManager.getFinalSaveSpoolEnqueuedTotal() +
            ", lastSpooled=" + formatTime(syncManager.getFinalSaveLastSpoolEnqueuedAt()) +
            ", rejected=" + syncManager.getFinalSaveSpoolRejectedTotal() +
            ", lastRejected=" + formatTime(syncManager.getFinalSaveLastSpoolRejectedAt()));
        sendMessage(sender, YELLOW + "Final-save sync fallback: " + WHITE +
            "total=" + syncManager.getFinalSaveSyncFallbackTotal() +
            ", last=" + formatTime(syncManager.getFinalSaveLastSyncFallbackAt()));
        // Spool disk state
        long spoolPending = syncManager.getFinalSaveSpoolPendingCount();
        long spoolFailed = syncManager.getFinalSaveSpoolFailedCount();
        if (spoolPending > 0 || spoolFailed > 0 || configManager.isFinalSaveSpoolEnabled()) {
            String spoolColor = spoolFailed > 0 ? RED : (spoolPending > 0 ? YELLOW : GREEN);
            sendMessage(sender, YELLOW + "FinalSaveSpool: " + spoolColor +
                "pending=" + spoolPending +
                ", failed=" + spoolFailed +
                ", bytes=" + syncManager.getFinalSaveSpoolBytes());
            long lastReplay = syncManager.getFinalSaveSpoolLastReplayAt();
            if (lastReplay > 0) {
                sendMessage(sender, YELLOW + "  lastReplay=" + WHITE
                    + new java.util.Date(lastReplay));
            }
            String lastError = syncManager.getFinalSaveSpoolLastError();
            if (lastError != null && !lastError.isEmpty()) {
                sendMessage(sender, RED + "  lastError=" + lastError);
            }
            if (spoolFailed > 0) {
                sendMessage(sender, RED + "Spool FAILED entries exist — check "
                    + configManager.getFinalSaveSpoolDir() + "/failed/ for .reason.txt files.");
            }
        }
        // Alerts
        if (syncManager.getFinalSaveSpoolRejectedTotal() > 0) {
            sendMessage(sender, RED + "Final-save CRITICAL: spool rejected/failure occurred. "
                + "Final states may have been lost. Check logs and spool configuration.");
        }
        if (syncManager.getFinalSaveSyncFallbackTotal() > 0) {
            sendMessage(sender, RED + "Final-save CRITICAL: synchronous fallback occurred. "
                + "Game thread may have been blocked. Investigate DB latency / final-save queue sizing.");
        }
        if (syncManager.hasFinalSaveWarning() && !syncManager.hasFinalSaveAlert()) {
            sendMessage(sender, YELLOW + "Final-save WARNING: queue-full events have occurred. "
                + "Spool handled some final saves; monitor queue sizing and replay lag.");
        }
        if (configManager.isOperationLogEnabled()) {
            long dropped = syncManager.getOperationLogDroppedTotal();
            String opLogColor = dropped > 0 ? RED : GREEN;
            sendMessage(sender, YELLOW + "OpLog: " + opLogColor +
                "queue=" + syncManager.getOperationLogQueueSize() + "/" + syncManager.getOperationLogQueueCapacity() +
                ", dropped=" + dropped);
            if (dropped > 0) {
                long lastDropAt = syncManager.getOperationLogLastDropAt();
                sendMessage(sender, RED + "OpLog ALERT: audit entries have been dropped"
                    + (lastDropAt > 0 ? " since " + new java.util.Date(lastDropAt) : "")
                    + ". Treat operation log as incomplete for incident review.");
            }
        } else {
            sendMessage(sender, YELLOW + "OpLog: " + GRAY + "Disabled");
        }
        sendMessage(sender, YELLOW + "Compression: " +
            (configManager.isCompressionEnabled() ? GREEN + "LZ4" : RED + "Disabled"));
        sendMessage(sender, YELLOW + "Debug: " +
            (configManager.isDebug() ? GREEN + "ON" : RED + "OFF"));

        // HikariCP stats
        if (databaseManager.getDataSource() != null) {
            var pool = databaseManager.getDataSource().getHikariPoolMXBean();
            if (pool != null) {
                sendMessage(sender, YELLOW + "DB Pool: " + WHITE +
                    "active=" + pool.getActiveConnections() +
                    ", idle=" + pool.getIdleConnections() +
                    ", total=" + pool.getTotalConnections() +
                    ", waiting=" + pool.getThreadsAwaitingConnection());
            }
        }

        // Latency stats (Dynamo p99.9) — now sent to sender, not just logged
        sendMessage(sender, YELLOW + "Latency: " + GRAY + "(p50/p99/p99.9)");
        for (String line : syncManager.getLatencyStatusLines()) {
            sendMessage(sender, GRAY + "  " + line);
        }

        // Stream stats
        sendMessage(sender, YELLOW + "Streams: " +
            (configManager.isStreamsEnabled() ? GREEN + "Enabled" : RED + "Disabled"));

        // Snapshot stats (round 14b)
        if (syncManager.getSnapshotManager() != null) {
            var sm = syncManager.getSnapshotManager();
            long rejected = sm.getRejectedCount();
            String snapColor = rejected > 0 ? RED : GREEN;
            sendMessage(sender, YELLOW + "Snapshots: " + snapColor +
                "queue=" + sm.getQueueSize() + "/" + sm.getQueueCapacity() +
                ", rejected=" + rejected);
        }

        // Cluster + production mode (round 14b)
        sendMessage(sender, YELLOW + "Cluster: " + WHITE +
            (configManager.getClusterId() != null && !configManager.getClusterId().isBlank()
                ? configManager.getClusterId() : "(none)"));
        if (configManager.isProductionEnabled()) {
            sendMessage(sender, YELLOW + "Production: " + GREEN + "ON" +
                " (redis=" + (configManager.isProductionRequireRedis() ? "required" : "optional") +
                ", sync-fallback=" + (configManager.isFinalSaveAllowSyncFallback() ? "allowed" : "blocked") + ")");
        }
    }

    // ==================== Getters ====================

    public ConfigManager getConfigManager() { return configManager; }
    public DatabaseManager getDatabaseManager() { return databaseManager; }
    public SyncManager getSyncManager() { return syncManager; }
}
