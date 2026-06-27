package com.fastsync.sync.strategy;

import org.bukkit.entity.Player;

/**
 * Strategy for synchronizing a player's PersistentDataContainer (PDC).
 * 
 * <p>Different strategies trade off safety, compatibility, and granularity:
 * <ul>
 *   <li>{@code off} — no PDC sync at all</li>
 *   <li>{@code safe-all-paper} — use Paper's public serializeToBytes() API (default)</li>
 *   <li>{@code registered-only} — only sync explicitly registered keys (production recommended)</li>
 * </ul>
 */
public interface PdcSyncStrategy {
    
    /** Dump the player's PDC to bytes (null/empty = nothing to sync). */
    byte[] dump(Player player);
    
    /** Restore bytes into the player's PDC. */
    void restore(Player player, byte[] data);
    
    /** Whether this strategy is safe for production use. */
    boolean isSafe();
    
    /** Strategy identifier for /fastsync status. */
    String strategyName();
}
