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
import org.bukkit.event.player.PlayerRespawnEvent;
import org.bukkit.event.world.WorldSaveEvent;
import org.bukkit.plugin.Plugin;

import java.util.ArrayList;
import java.util.List;
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
    private final Plugin plugin;

    public DataListener(SyncManager syncManager, ConfigManager config) {
        this.syncManager = syncManager;
        this.config = config;
        this.logger = Bukkit.getLogger();
        this.plugin = org.bukkit.plugin.java.JavaPlugin.getPlugin(com.fastsync.FastSync.class);
    }

    /**
     * Save a player's data when they die (save cause: "death").
     */
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlayerDeath(PlayerDeathEvent event) {
        // Paper's ServerPlayer.reset() applies these event-derived values only
        // when the player respawns. Capture them now so a death-screen quit or
        // cross-server transfer persists the expected post-respawn state.
        syncManager.recordDeathState(event);
        if (!config.isSaveOnDeath()) {
            return;
        }

        Player player = event.getEntity();
        // Pass SaveKind.DEATH so that snapshot.save-trigger: "death" works,
        // operation logs record the correct cause, and dirty/component
        // strategies can distinguish death saves from periodic saves.
        // ServerPlayer clears non-kept inventory only after PlayerDeathEvent
        // returns. Defer collection by one entity tick so the saved snapshot
        // matches the state Minecraft will actually respawn with.
        SchedulerUtil.runAtEntityDelayed(plugin, player,
            () -> syncManager.savePlayerAsync(player, SyncManager.SaveKind.DEATH), 1L);

        if (config.isDebug()) {
            logger.info("[FastSync] Saved data for " + player.getUniqueId() + " on death.");
        }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerRespawn(PlayerRespawnEvent event) {
        syncManager.clearDeathState(event.getPlayer().getUniqueId());
    }

    /**
     * Save all online players' data when the world is saved (save cause: "world_save").
     *
     * <p><b>Folia-safety:</b> WorldSaveEvent fires on the region thread that
     * owns the world being saved. On Folia, that is NOT the global region and
     * is NOT necessarily the region that owns each online player. Calling
     * {@link Bukkit#getOnlinePlayers()} from a world region is unsafe, and
     * calling {@link SyncManager#savePlayerAsync(Player, SyncManager.SaveKind)}
     * touches the Player object (reads getUniqueId, dispatches via entity
     * scheduler) which must happen on the global region.
     *
     * <p>So we dispatch to the global region first, snapshot the player list
     * there, then call savePlayerAsync for each. savePlayerAsync itself
     * dispatches per-player to the entity scheduler for the actual collect,
     * so the global region is only used for the brief list-snapshot + dispatch
     * loop, not for any DB wait.
     */
    @EventHandler
    public void onWorldSave(WorldSaveEvent event) {
        if (!config.isSaveOnWorldSave()) {
            return;
        }

        // Dispatch to global region for the player-list snapshot + per-player
        // save dispatch. This is safe on both Paper (runTask) and Folia
        // (GlobalRegionScheduler.run).
        SchedulerUtil.runGlobal(plugin, () -> {
            List<Player> players = new ArrayList<>(Bukkit.getOnlinePlayers());
            for (Player player : players) {
                // Pass SaveKind.WORLD_SAVE so that snapshot.save-trigger:
                // "world_save" works and operation logs record the correct cause.
                syncManager.savePlayerAsync(player, SyncManager.SaveKind.WORLD_SAVE);
            }

            if (config.isDebug()) {
                logger.info("[FastSync] Saved data for " + players.size()
                    + " online players on world save.");
            }
        });
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
