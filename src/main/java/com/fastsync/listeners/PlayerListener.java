package com.fastsync.listeners;

import com.fastsync.FastSync;
import com.fastsync.sync.SyncManager;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

import java.util.UUID;

/**
 * Player event listener for synchronization.
 *
 * Key design: data is loaded during AsyncPlayerPreLoginEvent (before the player
 * actually joins), preventing the item duplication bugs that occur when data
 * is loaded after the player has already entered the server.
 */
public class PlayerListener implements Listener {

    private static final LegacyComponentSerializer KICK_MESSAGE_SERIALIZER =
        LegacyComponentSerializer.legacyAmpersand();

    private final FastSync plugin;
    private final SyncManager syncManager;

    public PlayerListener(FastSync plugin, SyncManager syncManager) {
        this.plugin = plugin;
        this.syncManager = syncManager;
    }

    /**
     * Load player data during the async pre-login phase.
     * This blocks the login until data is loaded, ensuring data is ready
     * before the player enters the server.
     */
    @EventHandler(priority = EventPriority.LOWEST)
    public void onPreLogin(AsyncPlayerPreLoginEvent event) {
        // Skip if already disallowed by another plugin
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            return;
        }

        // Skip for players with bypass permission (when configured)
        // Note: permissions aren't available at pre-login, so we can't check here

        UUID uuid = event.getUniqueId();

        SyncManager.LoadResult result = syncManager.loadPlayerData(uuid);

        if (!result.isSuccess()) {
            String msg;
            if (result.getStatus() == SyncManager.LoadResult.Status.LOCKED) {
                msg = plugin.getConfigManager().getLockTimeoutKickMessage();
            } else if (result.getStatus() == SyncManager.LoadResult.Status.BUSY) {
                // Login backpressure: too many concurrent loads. Use the
                // dedicated busy-kick-message so players know to retry.
                msg = plugin.getConfigManager().getBusyKickMessage();
            } else {
                plugin.getLogger().warning("Failed to load data for " + uuid + ": " + result.getMessage());
                msg = plugin.getConfigManager().getLoadFailKickMessage();
            }
            event.disallow(
                AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                KICK_MESSAGE_SERIALIZER.deserialize(msg)
            );
        }
    }

    /**
     * Apply loaded player data when the player joins.
     * Must be LOWEST priority so data is applied before other plugins interact.
     */
    @EventHandler(priority = EventPriority.LOWEST)
    public void onJoin(PlayerJoinEvent event) {
        syncManager.applyPlayerData(event.getPlayer());
    }

    /**
     * Collect and save player data when the player quits.
     * Uses HIGHEST priority so other plugins can process quit first.
     */
    @EventHandler(priority = EventPriority.HIGHEST)
    public void onQuit(PlayerQuitEvent event) {
        Player player = event.getPlayer();
        try {
            syncManager.collectAndSavePlayerData(player);
        } finally {
            // A player can leave from the death screen without respawning.
            // Collection is synchronous even though persistence is not, so the
            // event-derived respawn snapshot is no longer needed at this point.
            syncManager.clearDeathState(player.getUniqueId());
        }
    }
}
