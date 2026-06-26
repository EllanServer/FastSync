package com.fastsync.listeners;

import com.fastsync.config.ConfigManager;
import com.fastsync.sync.SyncManager;
import com.fastsync.util.SchedulerUtil;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.player.PlayerCommandPreprocessEvent;
import org.bukkit.event.world.WorldSaveEvent;
import org.bukkit.plugin.Plugin;

import java.util.UUID;
import java.util.logging.Logger;

import com.fastsync.FastSync;

/**
 * Listener for data-related events: death saves, world-save saves, and
 * locking down commands while a player's data is still being loaded.
 *
 * - On death: optionally save the player's data (save cause: "death").
 * - On world save: optionally save all online players' data (save cause: "world_save").
 * - On command (LOWEST): cancel commands issued by players whose data is not
 *   yet active/loaded, preventing interaction with unloaded state.
 */
public class DataListener implements Listener {

    private final SyncManager syncManager;
    private final ConfigManager config;
    private final Logger logger;
    private final Plugin plugin;

    public DataListener(SyncManager syncManager, ConfigManager config) {
        this.syncManager = syncManager;
        this.config = config;
        this.logger = Bukkit.getLogger();
        this.plugin = Bukkit.getPluginManager().getPlugin("FastSync");
    }

    /**
     * Save a player's data when they die (save cause: "death").
     */
    @EventHandler
    public void onPlayerDeath(PlayerDeathEvent event) {
        if (!config.isSaveOnDeath()) {
            return;
        }

        Player player = event.getEntity();
        syncManager.savePlayerAsync(player, SyncManager.SaveKind.DEATH);

        if (config.isDebug()) {
            logger.info("[FastSync] Saved data for " + player.getUniqueId() + " on death.");
        }
    }

    /**
     * Save all online players' data when the world is saved (save cause: "world_save").
     *
     * <p>Note: save-on-world-save is DISABLED by default. World saves fire frequently
     * (every 5 minutes by default) and can cause unnecessary DB write load. Enable
     * only if you need extra durability against crashes between periodic saves.
     */
    @EventHandler
    public void onWorldSave(WorldSaveEvent event) {
        if (!config.isSaveOnWorldSave()) {
            return;
        }

        // On Folia, WorldSaveEvent fires on a region thread, not the global
        // thread. Bukkit.getOnlinePlayers() must be called from the global/main
        // thread. Snapshot the player list there, then dispatch saves.
        // On Paper, this is already the main thread, so runGlobal() executes inline.
        SchedulerUtil.runGlobal(plugin, () -> {
            for (Player player : Bukkit.getOnlinePlayers()) {
                syncManager.savePlayerAsync(player, SyncManager.SaveKind.WORLD_SAVE);
            }
        });

        if (config.isDebug()) {
            logger.info("[FastSync] Saved data for all online players on world save.");
        }
    }

    /**
     * Cancel commands issued by players whose data is not yet active/loaded.
     * Runs at LOWEST priority so it takes effect before other command handlers.
     */
    @EventHandler(priority = EventPriority.LOWEST)
    public void onCommandPreprocess(PlayerCommandPreprocessEvent event) {
        if (!config.isCancelCommandsWhileLocked()) {
            return;
        }

        UUID uuid = event.getPlayer().getUniqueId();
        if (!syncManager.isPlayerActive(uuid)) {
            event.setCancelled(true);

            if (config.isDebug()) {
                logger.info("[FastSync] Cancelled command from " + uuid +
                    " while player data is loading.");
            }
        }
    }
}
