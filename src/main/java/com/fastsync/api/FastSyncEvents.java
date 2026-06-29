package com.fastsync.api;

import com.fastsync.data.PlayerData;
import org.bukkit.entity.Player;
import org.bukkit.event.*;

import java.util.UUID;

/**
 * Developer API events for FastSync.
 *
 * <p>Contains three custom Bukkit events that other plugins can listen to in
 * order to react to, or intercept, FastSync's data synchronization lifecycle:</p>
 * <ul>
 *   <li>{@link FastSyncPreLoadEvent} - fired before loading player data (cancellable)</li>
 *   <li>{@link FastSyncApplyEvent} - fired after applying player data to a player</li>
 *   <li>{@link FastSyncSaveEvent} - fired before saving player data (cancellable)</li>
 * </ul>
 *
 * <p>The events are exposed as public static inner classes and can be used like
 * any other Bukkit event:</p>
 * <pre>
 * &#64;EventHandler
 * public void onSave(FastSyncEvents.FastSyncSaveEvent event) {
 *     if ("death".equals(event.getSaveCause())) {
 *         event.setCancelled(true); // skip saving on death
 *     }
 * }
 * </pre>
 */
public final class FastSyncEvents {

    private FastSyncEvents() {
        throw new UnsupportedOperationException("Utility holder class");
    }

    // ==================== Pre-Load Event ====================

    /**
     * Fired before FastSync loads a player's data.
     *
     * <p>Cancelling this event prevents FastSync from loading the player's saved
     * data (for example, to force a fresh start or to handle the data externally).</p>
     */
    public static class FastSyncPreLoadEvent extends Event implements Cancellable {

        private static final HandlerList handlers = new HandlerList();
        private final UUID playerUuid;
        private boolean cancelled;

        public FastSyncPreLoadEvent(UUID playerUuid) {
            this.playerUuid = playerUuid;
        }

        public FastSyncPreLoadEvent(UUID playerUuid, boolean isAsync) {
            super(isAsync);
            this.playerUuid = playerUuid;
        }

        public UUID getPlayerUuid() {
            return playerUuid;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        @Override
        public HandlerList getHandlers() {
            return handlers;
        }

        public static HandlerList getHandlerList() {
            return handlers;
        }
    }

    // ==================== Apply Event ====================

    /**
     * Fired after FastSync has applied loaded player data to a player.
     *
     * <p>Use this to perform post-sync actions such as updating cosmetics,
     * triggering cooldowns, or logging analytics.</p>
     */
    public static class FastSyncApplyEvent extends Event {

        private static final HandlerList handlers = new HandlerList();
        private final Player player;
        private final PlayerData data;

        public FastSyncApplyEvent(Player player, PlayerData data) {
            this.player = player;
            this.data = data;
        }

        public FastSyncApplyEvent(Player player, PlayerData data, boolean isAsync) {
            super(isAsync);
            this.player = player;
            this.data = data;
        }

        public Player getPlayer() {
            return player;
        }

        public PlayerData getData() {
            return data;
        }

        @Override
        public HandlerList getHandlers() {
            return handlers;
        }

        public static HandlerList getHandlerList() {
            return handlers;
        }
    }

    // ==================== Save Event ====================

    /**
     * Fired before FastSync saves a player's data.
     *
     * <p>Cancelling this event prevents FastSync from persisting the player's
     * data for this save cycle. The exposed {@link PlayerData} may be mutated by
     * listeners before it is serialized. Cancelling a final quit/shutdown save
     * also makes FastSync release its coordination lock; the listener is then
     * responsible for any persistence it requires.</p>
     */
    public static class FastSyncSaveEvent extends Event implements Cancellable {

        private static final HandlerList handlers = new HandlerList();
        private final Player player;
        private final PlayerData data;
        private final String saveCause;
        private boolean cancelled;

        public FastSyncSaveEvent(Player player, PlayerData data, String saveCause) {
            this.player = player;
            this.data = data;
            this.saveCause = saveCause;
        }

        public FastSyncSaveEvent(Player player, PlayerData data, String saveCause, boolean isAsync) {
            super(isAsync);
            this.player = player;
            this.data = data;
            this.saveCause = saveCause;
        }

        public Player getPlayer() {
            return player;
        }

        public PlayerData getData() {
            return data;
        }

        public String getSaveCause() {
            return saveCause;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        @Override
        public HandlerList getHandlers() {
            return handlers;
        }

        public static HandlerList getHandlerList() {
            return handlers;
        }
    }
}
