package com.fastsync;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.fastsync.listeners.PlayerListener;
import com.fastsync.log.OperationLog;
import com.fastsync.serialization.ItemStackCompat;
import com.fastsync.sync.SyncManager;
import com.fastsync.util.SchedulerUtil;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
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
import java.util.UUID;
import java.util.logging.Level;

/**
 * FastSync - High-performance cross-server player data synchronization.
 *
 * Design principles (based on community discussion):
 *   1. NBT byte[] serialization - NO base64 string encoding, NO Kryo, NO Gson
 *      Primary: ItemStack.serializeAsBytes() (Paper 1.20.5+)
 *      Fallback: Bukkit object serialization (still byte[], NOT string)
 *   2. LZ4 compression to reduce database storage and network transfer
 *   3. Data loaded during login phase (AsyncPlayerPreLoginEvent) - not after joining
 *      Prevents item duplication bugs from "enter server then load" approach
 *   4. Cross-server lock with proper acknowledgment (Redis pub/sub)
 *      NOT HuskSync's broken "petition" that forces entry after timeout
 *   5. Dedicated thread pool for async operations (NOT ForkJoinPool.commonPool)
 *   6. Version byte prefix for future serialization format migration
 */
public class FastSync extends JavaPlugin implements CommandExecutor, TabCompleter {

    private ConfigManager configManager;
    private DatabaseManager databaseManager;
    private SyncManager syncManager;

    private Object cleanupTask;
    private Object periodicSaveTask;
    private Object heartbeatTask;

    @Override
    public void onEnable() {
        // Check ItemStack serialization compatibility (graceful, not hard fail)
        boolean nativeNbt = ItemStackCompat.isPaperNativeAvailable();
        if (!nativeNbt) {
            getLogger().warning("============================================");
            getLogger().warning(" Paper native NBT API not found (need 1.20.5+).");
            getLogger().warning(" Using Bukkit fallback serialization (still byte[],");
            getLogger().warning(" NOT base64 string). Upgrade Paper for best performance.");
            getLogger().warning("============================================");
        }

        // Initialize config
        saveDefaultConfig();
        configManager = new ConfigManager(this);
        configManager.load();

        // Initialize database
        databaseManager = new DatabaseManager(getLogger(), configManager);
        try {
            databaseManager.initialize();
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "Failed to initialize database! Check your config.yml.", e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        // Initialize sync manager (creates thread pool + optional Redis)
        syncManager = new SyncManager(this, configManager, databaseManager);
        syncManager.initialize();

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
        long heartbeatTicks = configManager.getHeartbeatIntervalSeconds() * 20L;
        heartbeatTask = SchedulerUtil.runAsyncTimer(this, () -> {
            syncManager.heartbeatOnlinePlayers();
        }, heartbeatTicks, heartbeatTicks);
        getLogger().info("Lock heartbeat enabled: every " +
            configManager.getHeartbeatIntervalSeconds() + " seconds (lock-timeout=" +
            configManager.getLockTimeout() + "s).");

        // Start periodic save task (if enabled)
        if (configManager.isPeriodicSave()) {
            long intervalTicks = configManager.getPeriodicSaveIntervalSeconds() * 20L;
            periodicSaveTask = SchedulerUtil.runGlobalTimer(this, () -> {
                // Snapshot online players on the global region thread, then save them
                // in small batches spread across successive ticks to avoid a lag spike
                // when many players are online (process at most 10 players per tick).
                List<Player> players = new ArrayList<>(Bukkit.getOnlinePlayers());
                final int batchSize = configManager.getPeriodicSaveBatchSize();
                for (int i = 0; i < players.size(); i += batchSize) {
                    final int start = i;
                    final int end = Math.min(i + batchSize, players.size());
                    long delayTicks = i / batchSize;
                    SchedulerUtil.runAsyncDelayed(this, () -> {
                        for (int j = start; j < end; j++) {
                            syncManager.savePlayerAsync(players.get(j));
                        }
                    }, delayTicks);
                }
            }, intervalTicks, intervalTicks);
            getLogger().info("Periodic save enabled: every " +
                configManager.getPeriodicSaveIntervalSeconds() + " seconds");
        }

        getLogger().info("FastSync v" + getPluginMeta().getVersion() + " enabled!");
        getLogger().info("Server ID: " + configManager.getServerName());
        getLogger().info("Serialization: " + (nativeNbt ? "Native NBT (Paper)" : "Bukkit fallback"));
        getLogger().info("Compression: " + (configManager.isCompressionEnabled() ? "LZ4" : "Disabled"));
        getLogger().info("Redis: " + (configManager.isRedisEnabled() ? "Enabled" : "Disabled (DB polling)"));
    }

    @Override
    public void onDisable() {
        // Cancel scheduled tasks (Paper/Folia compatible)
        SchedulerUtil.cancel(cleanupTask);
        SchedulerUtil.cancel(periodicSaveTask);
        SchedulerUtil.cancel(heartbeatTask);

        // Save all online players synchronously (release locks — server is stopping)
        if (syncManager != null) {
            getLogger().info("Saving all online players (shutdown)...");
            SyncManager.SaveAllResult result = syncManager.saveAllOnlinePlayers(SyncManager.SaveKind.SHUTDOWN);
            getLogger().info("Shutdown save: " + result.success() + "/" + result.total()
                + " succeeded" + (result.failed() > 0 ? ", " + result.failed() + " failed" : "")
                + (result.incomplete().isEmpty() ? "." : ", incomplete/still-running=" + result.incomplete() + "."));
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
    }

    // ==================== Command Handler ====================

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command,
                             @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("fastsync.admin")) {
            sender.sendMessage(ChatColor.RED + "You don't have permission to use this command.");
            return true;
        }

        if (args.length == 0) {
            sendHelp(sender, label);
            return true;
        }

        switch (args[0].toLowerCase()) {
            case "reload" -> {
                configManager.reload();
                // Refresh SyncManager caches that depend on config (e.g. snapshot trigger set)
                if (syncManager != null) {
                    syncManager.refreshConfigCache();
                }
                sender.sendMessage(ChatColor.GREEN + "[FastSync] Configuration reloaded.");
                sender.sendMessage(ChatColor.GRAY + "Server: " + configManager.getServerName());
                sender.sendMessage(ChatColor.GRAY + "Compression: " +
                    (configManager.isCompressionEnabled() ? "LZ4" : "Disabled"));
                sender.sendMessage(ChatColor.GRAY + "Redis: " +
                    (configManager.isRedisEnabled() ? "Enabled" : "Disabled"));
            }
            case "status" -> sendStatus(sender);
            case "debug" -> {
                boolean newDebug = !configManager.isDebug();
                getConfig().set("debug", newDebug);
                saveConfig();
                configManager.reload();
                sender.sendMessage(ChatColor.GREEN + "[FastSync] Debug mode: " +
                    (newDebug ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF"));
            }
            case "saveall" -> {
                sender.sendMessage(ChatColor.YELLOW + "[FastSync] Saving all online players...");
                SchedulerUtil.runAsync(this, () -> {
                    try {
                        SyncManager.SaveAllResult result = syncManager.saveAllOnlinePlayers(SyncManager.SaveKind.BULK);
                        SchedulerUtil.runGlobal(this, () -> {
                            if (result.allSucceeded()) {
                                sender.sendMessage(ChatColor.GREEN + "[FastSync] All " + result.total() + " players saved!");
                            } else {
                                sender.sendMessage(ChatColor.YELLOW + "[FastSync] Saved " + result.success()
                                    + "/" + result.total() + " players. " + ChatColor.RED + result.failed() + " failed.");
                                if (!result.failures().isEmpty()) {
                                    sender.sendMessage(ChatColor.GRAY + "Failed players:");
                                    result.failures().forEach((uuid, reason) ->
                                        sender.sendMessage(ChatColor.GRAY + "  " + uuid + ": " + ChatColor.RED + reason));
                                }
                            }
                        });
                    } catch (Exception e) {
                        getLogger().log(Level.SEVERE, "Saveall failed", e);
                        SchedulerUtil.runGlobal(this, () ->
                            sender.sendMessage(ChatColor.RED + "[FastSync] Saveall failed: " + e.getMessage())
                        );
                    }
                });
            }
            case "log" -> {
                if (args.length < 2) {
                    sender.sendMessage(ChatColor.RED + "Usage: /" + label + " log <player|uuid> [limit]");
                    return true;
                }
                int limit;
                if (args.length >= 3) {
                    try {
                        limit = Math.min(Integer.parseInt(args[2]), 50);
                    } catch (NumberFormatException e) {
                        sender.sendMessage(ChatColor.RED + "Invalid number: " + args[2]);
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
                        sender.sendMessage(ChatColor.RED + "Player not found or invalid UUID: " + args[1]);
                        return true;
                    }
                }
                final UUID fuuid = targetUuid;
                SchedulerUtil.runAsync(this, () -> {
                    List<OperationLog> logs = syncManager.queryOperationLog(fuuid, limit);
                    // Build all messages on async thread, then send on global thread
                    List<String> messages = new java.util.ArrayList<>();
                    if (logs.isEmpty()) {
                        messages.add(ChatColor.YELLOW + "[FastSync] No operation log entries for " + args[1]);
                    } else {
                        messages.add(ChatColor.GOLD + "===== Operation Log: " + args[1] + " (" + logs.size() + " entries) =====");
                        for (OperationLog log : logs) {
                            ChatColor typeColor = switch (log.type()) {
                                case CONFLICT, CHECKSUM_FAIL, LOCK_EXPIRE -> ChatColor.RED;
                                case SAVE, SNAPSHOT, RESTORE -> ChatColor.GREEN;
                                case LOAD, LOCK_ACQUIRE, LOCK_RELEASE -> ChatColor.AQUA;
                            };
                            messages.add(ChatColor.GRAY + "#" + log.seq() + " " +
                                typeColor + log.type() + ChatColor.GRAY +
                                " | server=" + log.serverName() +
                                " v=" + log.version() + " ft=" + log.fencingToken() +
                                " sz=" + log.dataSize() + "B" +
                                (log.detail() != null ? " | " + ChatColor.WHITE + log.detail() : ""));
                        }
                    }
                    SchedulerUtil.runGlobal(this, () -> {
                        for (String msg : messages) {
                            sender.sendMessage(msg);
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
        sender.sendMessage(ChatColor.GOLD + "===== FastSync =====");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " reload " + ChatColor.GRAY + "- Reload configuration");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " status " + ChatColor.GRAY + "- Show plugin status");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " debug " + ChatColor.GRAY + "- Toggle debug mode");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " saveall " + ChatColor.GRAY + "- Save all online players");
        sender.sendMessage(ChatColor.YELLOW + "/" + label + " log <player> [n] " + ChatColor.GRAY + "- View operation log for a player");
    }

    private void sendStatus(CommandSender sender) {
        sender.sendMessage(ChatColor.GOLD + "===== FastSync Status =====");
        sender.sendMessage(ChatColor.YELLOW + "Server: " + ChatColor.WHITE + configManager.getServerName());
        sender.sendMessage(ChatColor.YELLOW + "Database: " +
            (databaseManager.isHealthy() ? ChatColor.GREEN + "Connected" : ChatColor.RED + "Disconnected"));
        sender.sendMessage(ChatColor.YELLOW + "Redis: " +
            (syncManager.isRedisEnabled() ? ChatColor.GREEN + "Connected" :
             (configManager.isRedisEnabled() ? ChatColor.RED + "Failed" : ChatColor.GRAY + "Disabled")));
        sender.sendMessage(ChatColor.YELLOW + "Serialization: " + ChatColor.WHITE +
            (ItemStackCompat.isPaperNativeAvailable() ? "Native NBT" : "Bukkit fallback"));
        sender.sendMessage(ChatColor.YELLOW + "Active players: " + ChatColor.WHITE + syncManager.getActiveCount());
        sender.sendMessage(ChatColor.YELLOW + "Pending loads: " + ChatColor.WHITE + syncManager.getPendingCount());
        sender.sendMessage(ChatColor.YELLOW + "Pending saves: " + ChatColor.WHITE + syncManager.getPendingSaveCount());
        sender.sendMessage(ChatColor.YELLOW + "Async threads: " + ChatColor.WHITE +
            "active=" + syncManager.getAsyncActiveCount() +
            ", queue=" + syncManager.getAsyncQueueSize());
        sender.sendMessage(ChatColor.YELLOW + "Final-save threads: " + ChatColor.WHITE +
            "active=" + syncManager.getFinalSaveActiveCount() +
            ", queue=" + syncManager.getFinalSaveQueueSize());
        sender.sendMessage(ChatColor.YELLOW + "Fallback counters: " + ChatColor.WHITE +
            "retired_quit_save_total=" + syncManager.getRetiredQuitSaveTotal() +
            ", sync_fallback_save_total=" + syncManager.getSyncFallbackSaveTotal() +
            ", sync_fallback_collect_failed_total=" + syncManager.getSyncFallbackCollectFailedTotal() +
            ", emergency_sync_db_save_total=" + syncManager.getEmergencySyncDbSaveTotal());
        sender.sendMessage(ChatColor.YELLOW + "Compression: " +
            (configManager.isCompressionEnabled() ? ChatColor.GREEN + "LZ4" : ChatColor.RED + "Disabled"));
        sender.sendMessage(ChatColor.YELLOW + "Debug: " +
            (configManager.isDebug() ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF"));

        // HikariCP stats
        if (databaseManager.getDataSource() != null) {
            var pool = databaseManager.getDataSource().getHikariPoolMXBean();
            if (pool != null) {
                sender.sendMessage(ChatColor.YELLOW + "DB Pool: " + ChatColor.WHITE +
                    "active=" + pool.getActiveConnections() +
                    ", idle=" + pool.getIdleConnections() +
                    ", total=" + pool.getTotalConnections() +
                    ", waiting=" + pool.getThreadsAwaitingConnection());
            }
        }

        // Latency stats (Dynamo p99.9)
        sender.sendMessage(ChatColor.YELLOW + "Latency: " + ChatColor.GRAY + "(p50/p99/p99.9)");
        syncManager.logLatencyStats();

        // Stream stats
        sender.sendMessage(ChatColor.YELLOW + "Streams: " +
            (configManager.isStreamsEnabled() ? ChatColor.GREEN + "Enabled" : ChatColor.RED + "Disabled"));
    }

    // ==================== Getters ====================

    public ConfigManager getConfigManager() { return configManager; }
    public DatabaseManager getDatabaseManager() { return databaseManager; }
    public SyncManager getSyncManager() { return syncManager; }
}
