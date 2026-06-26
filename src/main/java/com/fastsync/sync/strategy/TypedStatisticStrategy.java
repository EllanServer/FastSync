package com.fastsync.sync.strategy;

import org.bukkit.entity.Player;
import org.bukkit.Statistic;
import java.util.*;

/**
 * Strategy for synchronizing typed statistics (ITEM/BLOCK/ENTITY).
 * 
 * <p>Typed statistics require a Material or EntityType parameter and are
 * expensive to collect (full enum iteration). Different strategies trade
 * off completeness vs. performance:
 * <ul>
 *   <li>{@code off} — don't sync typed statistics (default)</li>
 *   <li>{@code whitelist} — only sync whitelisted stat+material/entity combos</li>
 *   <li>{@code full} — sync all typed statistics (expensive: ~1300 Material × N stats)</li>
 * </ul>
 */
public interface TypedStatisticStrategy {
    
    /** Collect typed statistics into the standard category map format. */
    Map<String, Map<String, Integer>> dump(Player player);
    
    /** Restore typed statistics from the category map format. */
    void restore(Player player, Map<String, Map<String, Integer>> data);
    
    /** Strategy identifier for /fastsync status. */
    String strategyName();
}
