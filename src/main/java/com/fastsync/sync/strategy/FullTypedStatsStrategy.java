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

        // P0 (issue #54): write ALL entries, including zeros. Previously
        // `if (v != 0) map.put(...)` skipped zero values to save payload size,
        // but on restore the missing entries were never reset to 0 — leading to
        // silent cross-server stat drift when a stat was reset on the source
        // server (its 0-value was dropped from the payload, leaving the target
        // server's stale non-zero value untouched). MC's ServerStatsCounter.load
        // is a full-replace; we mirror that by capturing the explicit "0" signal.
        for (Statistic stat : itemStats) {
            Map<String, Integer> map = result.computeIfAbsent("ITEM_" + stat.name(), k -> new HashMap<>());
            for (Material mat : itemMaterials) {
                try {
                    int v = player.getStatistic(stat, mat);
                    map.put(mat.name(), v);
                } catch (Exception ignored) {}
            }
        }

        for (Statistic stat : blockStats) {
            Map<String, Integer> map = result.computeIfAbsent("BLOCK_" + stat.name(), k -> new HashMap<>());
            for (Material mat : blockMaterials) {
                try {
                    int v = player.getStatistic(stat, mat);
                    map.put(mat.name(), v);
                } catch (Exception ignored) {}
            }
        }

        for (Statistic stat : entityStats) {
            Map<String, Integer> map = result.computeIfAbsent("ENTITY_" + stat.name(), k -> new HashMap<>());
            for (EntityType ent : aliveEntities) {
                try {
                    int v = player.getStatistic(stat, ent);
                    map.put(ent.name(), v);
                } catch (Exception ignored) {}
            }
        }

        return result;
    }

    @Override
    public void restore(Player player, Map<String, Map<String, Integer>> data) {
        // P0 (issue #54): full-replace semantics. Iterate the strategy's known
        // bindings and set EVERY entry, falling back to 0 if the entry is
        // missing from `data`. This ensures stale non-zero values on the target
        // server are reset to match the source server's snapshot.
        //
        // We still iterate `data` first for forward-compat: if the payload
        // contains a binding we don't know about (e.g. a new Material added in
        // a future MC version), apply it via lookup.
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

        // Now iterate the known bindings and reset any entry that is missing
        // from `data` to 0. This is the "full-replace" half of the operation.
        // We skip categories not present in `data` entirely — those are either
        // (a) from a save that didn't use full strategy, or (b) from a save
        // where the stat category had zero entries (no key written). In case
        // (b) we still want to reset; in case (a) we don't want to clobber a
        // whitelist-strategy payload. Resolve by checking the category prefix
        // against `data` — if the category is present (even with empty map),
        // reset missing bindings to 0; if absent, skip.
        for (Statistic stat : itemStats) {
            String category = "ITEM_" + stat.name();
            Map<String, Integer> saved = data.get(category);
            if (saved == null) continue;
            for (Material mat : itemMaterials) {
                if (!saved.containsKey(mat.name())) {
                    try { player.setStatistic(stat, mat, 0); } catch (Exception ignored) {}
                }
            }
        }
        for (Statistic stat : blockStats) {
            String category = "BLOCK_" + stat.name();
            Map<String, Integer> saved = data.get(category);
            if (saved == null) continue;
            for (Material mat : blockMaterials) {
                if (!saved.containsKey(mat.name())) {
                    try { player.setStatistic(stat, mat, 0); } catch (Exception ignored) {}
                }
            }
        }
        for (Statistic stat : entityStats) {
            String category = "ENTITY_" + stat.name();
            Map<String, Integer> saved = data.get(category);
            if (saved == null) continue;
            for (EntityType ent : aliveEntities) {
                if (!saved.containsKey(ent.name())) {
                    try { player.setStatistic(stat, ent, 0); } catch (Exception ignored) {}
                }
            }
        }
    }
    
    @Override
    public String strategyName() { return "full"; }
}
