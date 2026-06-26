package com.fastsync.sync.strategy;

import org.bukkit.Statistic;
import org.bukkit.entity.Player;
import org.bukkit.Material;
import org.bukkit.entity.EntityType;
import java.util.*;

/**
 * Typed statistic strategy that only syncs whitelisted entries.
 * 
 * <p>Instead of iterating all ~1300 Materials for each ITEM/BLOCK statistic,
 * this strategy only checks the explicitly whitelisted combinations.
 * This is O(whitelist_size) per save instead of O(all_materials).
 */
public class WhitelistTypedStatsStrategy implements TypedStatisticStrategy {
    
    private final List<StatBinding<Material>> itemBindings;
    private final List<StatBinding<Material>> blockBindings;
    private final List<StatBinding<EntityType>> entityBindings;
    
    public WhitelistTypedStatsStrategy(
            List<StatBinding<Material>> itemBindings,
            List<StatBinding<Material>> blockBindings,
            List<StatBinding<EntityType>> entityBindings) {
        this.itemBindings = itemBindings != null ? itemBindings : List.of();
        this.blockBindings = blockBindings != null ? blockBindings : List.of();
        this.entityBindings = entityBindings != null ? entityBindings : List.of();
    }
    
    public record StatBinding<T>(Statistic stat, T target) {}
    
    @Override
    public Map<String, Map<String, Integer>> dump(Player player) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        
        // ITEM statistics
        for (StatBinding<Material> b : itemBindings) {
            try {
                int v = player.getStatistic(b.stat(), b.target());
                if (v != 0) {
                    result.computeIfAbsent("ITEM_" + b.stat().name(), k -> new HashMap<>())
                          .put(b.target().name(), v);
                }
            } catch (Exception ignored) {}
        }
        
        // BLOCK statistics
        for (StatBinding<Material> b : blockBindings) {
            try {
                int v = player.getStatistic(b.stat(), b.target());
                if (v != 0) {
                    result.computeIfAbsent("BLOCK_" + b.stat().name(), k -> new HashMap<>())
                          .put(b.target().name(), v);
                }
            } catch (Exception ignored) {}
        }
        
        // ENTITY statistics
        for (StatBinding<EntityType> b : entityBindings) {
            try {
                int v = player.getStatistic(b.stat(), b.target());
                if (v != 0) {
                    result.computeIfAbsent("ENTITY_" + b.stat().name(), k -> new HashMap<>())
                          .put(b.target().name(), v);
                }
            } catch (Exception ignored) {}
        }
        
        return result;
    }
    
    @Override
    public void restore(Player player, Map<String, Map<String, Integer>> data) {
        for (Map.Entry<String, Map<String, Integer>> cat : data.entrySet()) {
            String category = cat.getKey();
            try {
                if (category.startsWith("ITEM_")) {
                    Statistic stat = Statistic.valueOf(category.substring("ITEM_".length()));
                    for (Map.Entry<String, Integer> e : cat.getValue().entrySet()) {
                        Material mat = Material.matchMaterial(e.getKey());
                        if (mat != null) {
                            try { player.setStatistic(stat, mat, e.getValue()); } catch (Exception ignored) {}
                        }
                    }
                } else if (category.startsWith("BLOCK_")) {
                    Statistic stat = Statistic.valueOf(category.substring("BLOCK_".length()));
                    for (Map.Entry<String, Integer> e : cat.getValue().entrySet()) {
                        Material mat = Material.matchMaterial(e.getKey());
                        if (mat != null) {
                            try { player.setStatistic(stat, mat, e.getValue()); } catch (Exception ignored) {}
                        }
                    }
                } else if (category.startsWith("ENTITY_")) {
                    Statistic stat = Statistic.valueOf(category.substring("ENTITY_".length()));
                    for (Map.Entry<String, Integer> e : cat.getValue().entrySet()) {
                        try {
                            EntityType ent = EntityType.valueOf(e.getKey());
                            player.setStatistic(stat, ent, e.getValue());
                        } catch (Exception ignored) {}
                    }
                }
            } catch (Exception ignored) {}
        }
    }
    
    @Override
    public String strategyName() { return "whitelist"; }
}
