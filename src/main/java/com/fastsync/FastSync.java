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

        // Start periodic save task (if enabled)
        if (configManager.isPeriodicSave()) {
            long intervalTicks = configManager.getPeriodicSaveIntervalSeconds() * 20L;
            periodicSaveTask = SchedulerUtil.runGlobalTimer(this, () -> {
                for (Player player : Bukkit.getOnlinePlayers()) {
                    syncManager.savePlayerAsync(player);
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

        // Save all online players synchronously
        if (syncManager != null) {
            getLogger().info("Saving all online players...");
            syncManager.saveAllOnlinePlayers();
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
                    syncManager.saveAllOnlinePlayers();
                    sender.sendMessage(ChatColor.GREEN + "[FastSync] All players saved!");
                });
            }
            case "log" -> {
                if (args.length < 2) {
                    sender.sendMessage(ChatColor.RED + "Usage: /" + label + " log <player|uuid> [limit]");
                    return true;
                }
                int limit = args.length >= 3 ? Math.min(Integer.parseInt(args[2]), 50) : 20;
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
                    if (logs.isEmpty()) {
                        sender.sendMessage(ChatColor.YELLOW + "[FastSync] No operation log entries for " + args[1]);
                        return;
                    }
                    sender.sendMessage(ChatColor.GOLD + "===== Operation Log: " + args[1] + " (" + logs.size() + " entries) =====");
                    for (OperationLog log : logs) {
                        ChatColor typeColor = switch (log.type()) {
                            case CONFLICT, CHECKSUM_FAIL, LOCK_EXPIRE -> ChatColor.RED;
                            case SAVE, SNAPSHOT, RESTORE -> ChatColor.GREEN;
                            case LOAD, LOCK_ACQUIRE, LOCK_RELEASE -> ChatColor.AQUA;
                        };
                        sender.sendMessage(ChatColor.GRAY + "#" + log.seq() + " " +
                            typeColor + log.type() + ChatColor.GRAY +
                            " | server=" + log.serverName() +
                            " v=" + log.version() + " ft=" + log.fencingToken() +
                            " sz=" + log.dataSize() + "B" +
                            (log.detail() != null ? " | " + ChatColor.WHITE + log.detail() : ""));
                    }
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
