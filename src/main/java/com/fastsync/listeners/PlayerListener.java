package com.fastsync.listeners;

import com.fastsync.FastSync;
import com.fastsync.sync.SyncManager;
import com.fastsync.util.SchedulerUtil;
import org.bukkit.ChatColor;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

import java.util.UUID;
import java.util.logging.Level;

/**
 * Player event listener for synchronization.
 *
 * Key design: data is loaded during AsyncPlayerPreLoginEvent (before the player
 * actually joins), preventing the item duplication bugs that occur when data
 * is loaded after the player has already entered the server.
 */
public class PlayerListener implements Listener {

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
        if (result.getStatus() == SyncManager.LoadResult.Status.BUSY) {
            String msg = ChatColor.translateAlternateColorCodes('&',
                plugin.getConfigManager().getBusyKickMessage());
            event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, msg);
            return;
        }
        if (result.getStatus() == SyncManager.LoadResult.Status.PROTECTION) {
            event.disallow(
                AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                ChatColor.RED + "[FastSync] Data protection mode is active. Please try again later.");
            return;
        }
        if (result.getStatus() == SyncManager.LoadResult.Status.LOCKED) {
                String msg = ChatColor.translateAlternateColorCodes('&',
                    plugin.getConfigManager().getLockTimeoutKickMessage());
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, msg);
            } else {
                plugin.getLogger().warning("Failed to load data for " + uuid + ": " + result.getMessage());
                String msg = ChatColor.translateAlternateColorCodes('&',
                    plugin.getConfigManager().getLoadFailKickMessage());
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, msg);
            }
        }
    }

    /**
     * Apply loaded player data when the player joins.
     * Must be LOWEST priority so data is applied before other plugins interact.
     *
     * <p>On Folia, PlayerJoinEvent fires on the region thread that owns the
     * player entity, but we dispatch via entity scheduler to be explicit
     * and safe across all server implementations.
     */
    @EventHandler(priority = EventPriority.LOWEST)
    public void onJoin(PlayerJoinEvent event) {
        var player = event.getPlayer();
        SchedulerUtil.runAtEntity(plugin, player, () ->
            syncManager.applyPlayerData(player)
        , null);
    }

    /**
     * Collect and save player data when the player quits.
     * Uses HIGHEST priority so other plugins can process quit first.
     *
     * <p><b>Folia safety:</b> Bukkit API (inventory, health, stats) must be read
     * on the player's entity region thread. We dispatch via entity scheduler
     * with a {@code retired} callback that handles the case where the entity
     * is no longer valid (e.g., during server shutdown when the entity is
     * retired before our callback runs). In that case, we attempt a
     * synchronous best-effort save as a fallback — losing the final state
     * is unacceptable even if the entity scheduler is unavailable.
     */
    @EventHandler(priority = EventPriority.HIGHEST)
    public void onQuit(PlayerQuitEvent event) {
        var player = event.getPlayer();
        UUID uuid = player.getUniqueId();

        SchedulerUtil.runAtEntity(plugin, player, () -> {
            // Normal case: entity is still valid, collect and save on region thread
            syncManager.collectAndSavePlayerData(player);
        }, () -> {
            // Retired callback: entity no longer valid (player despawned, server shutting down).
            // This can happen on Folia during shutdown when the entity scheduler
            // retires entities before all pending tasks run.
            //
            // We MUST NOT silently lose the final save. Attempt a best-effort
            // synchronous collection. On Paper (non-Folia) the Bukkit API is
            // thread-safe for reads from the main thread during quit; on Folia
            // during shutdown the region threads are being torn down, so this
            // is our last chance to read player state.
            plugin.getLogger().log(Level.WARNING,
                "Entity retired for " + uuid + " during quit — attempting synchronous fallback save");
            try {
                syncManager.collectAndSavePlayerDataSync(player);
            } catch (Exception e) {
                plugin.getLogger().log(Level.SEVERE,
                    "Synchronous fallback save failed for " + uuid, e);
            }
        });
    }
}
