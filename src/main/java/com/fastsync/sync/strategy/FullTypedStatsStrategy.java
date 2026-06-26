package com.fastsync.sync.strategy;

import org.bukkit.Statistic;
import org.bukkit.entity.Player;
import org.bukkit.Material;
import org.bukkit.entity.EntityType;
import java.util.*;

/**
 * Full typed statistic sync — iterates all Materials and EntityTypes.
 * 
 * <p>This is the most complete but most expensive strategy: for each ITEM
 * statistic, it iterates ~1300 Materials; for each ENTITY statistic,
 * ~80 EntityTypes. Use only when you need complete statistic data and
 * have a small player count.
 */
public class FullTypedStatsStrategy implements TypedStatisticStrategy {
    
    private final List<Statistic> itemStats;
    private final List<Statistic> blockStats;
    private final List<Statistic> entityStats;
    private final List<Material> itemMaterials;
    private final List<Material> blockMaterials;
    private final List<EntityType> aliveEntities;
    
    public FullTypedStatsStrategy(
            List<Statistic> itemStats, List<Statistic> blockStats, List<Statistic> entityStats,
            List<Material> itemMaterials, List<Material> blockMaterials,
            List<EntityType> aliveEntities) {
        this.itemStats = itemStats;
        this.blockStats = blockStats;
        this.entityStats = entityStats;
        this.itemMaterials = itemMaterials;
        this.blockMaterials = blockMaterials;
        this.aliveEntities = aliveEntities;
    }
    
    @Override
    public Map<String, Map<String, Integer>> dump(Player player) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        
        for (Statistic stat : itemStats) {
            Map<String, Integer> map = result.computeIfAbsent("ITEM_" + stat.name(), k -> new HashMap<>());
            for (Material mat : itemMaterials) {
                try {
                    int v = player.getStatistic(stat, mat);
                    if (v != 0) map.put(mat.name(), v);
                } catch (Exception ignored) {}
            }
        }
        
        for (Statistic stat : blockStats) {
            Map<String, Integer> map = result.computeIfAbsent("BLOCK_" + stat.name(), k -> new HashMap<>());
            for (Material mat : blockMaterials) {
                try {
                    int v = player.getStatistic(stat, mat);
                    if (v != 0) map.put(mat.name(), v);
                } catch (Exception ignored) {}
            }
        }
        
        for (Statistic stat : entityStats) {
            Map<String, Integer> map = result.computeIfAbsent("ENTITY_" + stat.name(), k -> new HashMap<>());
            for (EntityType ent : aliveEntities) {
                try {
                    int v = player.getStatistic(stat, ent);
                    if (v != 0) map.put(ent.name(), v);
                } catch (Exception ignored) {}
            }
        }
        
        return result;
    }
    
    @Override
    public void restore(Player player, Map<String, Map<String, Integer>> data) {
        // Same restore logic as WhitelistTypedStatsStrategy
        for (Map.Entry<String, Map<String, Integer>> cat : data.entrySet()) {
            String category = cat.getKey();
            try {
                if (category.startsWith("ITEM_") || category.startsWith("BLOCK_")) {
                    String prefix = category.startsWith("ITEM_") ? "ITEM_" : "BLOCK_";
                    Statistic stat = Statistic.valueOf(category.substring(prefix.length()));
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
    public String strategyName() { return "full"; }
}
