package com.fastsync.listeners;

import com.fastsync.config.ConfigManager;
import com.fastsync.sync.SyncManager;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.player.PlayerCommandPreprocessEvent;
import org.bukkit.event.world.WorldSaveEvent;

import java.util.UUID;
import java.util.logging.Logger;

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

    public DataListener(SyncManager syncManager, ConfigManager config) {
        this.syncManager = syncManager;
        this.config = config;
        this.logger = Bukkit.getLogger();
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
        // Pass SaveKind.DEATH so that snapshot.save-trigger: "death" works,
        // operation logs record the correct cause, and dirty/component
        // strategies can distinguish death saves from periodic saves.
        syncManager.savePlayerAsync(player, SyncManager.SaveKind.DEATH);

        if (config.isDebug()) {
            logger.info("[FastSync] Saved data for " + player.getUniqueId() + " on death.");
        }
    }

    /**
     * Save all online players' data when the world is saved (save cause: "world_save").
     */
    @EventHandler
    public void onWorldSave(WorldSaveEvent event) {
        if (!config.isSaveOnWorldSave()) {
            return;
        }

        for (Player player : Bukkit.getOnlinePlayers()) {
            // Pass SaveKind.WORLD_SAVE so that snapshot.save-trigger: "world_save"
            // works and operation logs record the correct cause.
            syncManager.savePlayerAsync(player, SyncManager.SaveKind.WORLD_SAVE);
        }

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
