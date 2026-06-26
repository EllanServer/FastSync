package com.fastsync.serialization;

import com.fastsync.data.PlayerData;
import net.momirealms.sparrow.nbt.CompoundTag;
import net.momirealms.sparrow.nbt.ListTag;
import net.momirealms.sparrow.nbt.NBT;
import net.momirealms.sparrow.nbt.Tag;
import org.bukkit.GameMode;
import org.bukkit.NamespacedKey;
import org.bukkit.Registry;
import org.bukkit.inventory.ItemStack;
import org.bukkit.potion.PotionEffect;
import org.bukkit.potion.PotionEffectType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serializes PlayerData to/from raw byte[] using sparrow-nbt's CompoundTag.
 *
 * Replaces hand-written NbtBinaryReader/Writer with the professional sparrow-nbt library.
 * sparrow-nbt implements the exact NBT binary format specification:
 *   - Write: type tag byte → name (length-prefixed UTF-8) → payload per type spec
 *   - Read: read type byte → dispatch to correct read method → direct binary
 *
 * NBT.toBytes(CompoundTag) → binary byte[] (no string/base64/JSON)
 * NBT.fromBytes(byte[]) → CompoundTag (direct binary deserialization)
 *
 * The NBT binary format is fundamentally different from string/JSON:
 *   - Reader reads the type byte, then knows EXACTLY how many bytes to read next
 *   - Direct binary copy of primitive values - zero parsing overhead
 *   - No structural character parsing ([], {}, quotes, delimiters)
 */
public class PlayerDataSerializer {

    private PlayerDataSerializer() {}

    /**
     * Serialize PlayerData to NBT binary byte[] using sparrow-nbt.
     */
    public static byte[] serialize(PlayerData data) throws IOException {
        CompoundTag root = NBT.createCompound();

        // Inventory + Armor + Offhand + Ender chest (as byte arrays from ItemStack.serializeAsBytes)
        if (data.getInventory() != null) {
            root.put("inventory", toItemStackList(data.getInventory()));
        }
        if (data.getArmor() != null) {
            root.put("armor", toItemStackList(data.getArmor()));
        }
        if (data.getOffhand() != null) {
            root.putByteArray("offhand", ItemStackCompat.serialize(data.getOffhand()));
        }
        if (data.getEnderChest() != null) {
            root.put("enderChest", toItemStackList(data.getEnderChest()));
        }

        // Vitals
        root.putDouble("health", data.getHealth());
        root.putDouble("maxHealth", data.getMaxHealth());
        root.putInt("foodLevel", data.getFoodLevel());
        root.putFloat("saturation", data.getSaturation());
        root.putFloat("exhaustion", data.getExhaustion());

        // Experience
        root.putInt("expLevel", data.getExpLevel());
        root.putFloat("expProgress", data.getExpProgress());
        root.putInt("totalExperience", data.getTotalExperience());

        // Extra
        root.putString("gameMode", data.getGameMode() != null ? data.getGameMode().name() : "SURVIVAL");
        root.putInt("fireTicks", data.getFireTicks());
        root.putInt("remainingAir", data.getRemainingAir());
        root.putInt("maximumAir", data.getMaximumAir());

        // Flight status
        root.putBoolean("flying", data.isFlying());
        root.putBoolean("allowFlight", data.isAllowFlight());

        // Potion effects (list of compounds)
        if (data.getPotionEffects() != null && !data.getPotionEffects().isEmpty()) {
            ListTag effectList = NBT.createList();
            for (PlayerData.PotionEffectData effect : data.getPotionEffects()) {
                CompoundTag effectTag = NBT.createCompound();
                effectTag.putString("type", effect.getTypeKey());
                effectTag.putInt("duration", effect.getDuration());
                effectTag.putInt("amplifier", effect.getAmplifier());
                byte effectFlags = 0;
                if (effect.isAmbient()) effectFlags |= 0x01;
                if (effect.isParticles()) effectFlags |= 0x02;
                if (effect.isIcon()) effectFlags |= 0x04;
                effectTag.putByte("flags", effectFlags);
                effectList.add(effectTag);
            }
            root.put("potionEffects", effectList);
        }

        // Advancements (compound of compounds: key -> {criterion -> timestamp})
        if (data.getAdvancements() != null && !data.getAdvancements().isEmpty()) {
            CompoundTag advancementsTag = NBT.createCompound();
            for (Map.Entry<String, Map<String, Long>> adv : data.getAdvancements().entrySet()) {
                CompoundTag criteriaTag = NBT.createCompound();
                for (Map.Entry<String, Long> criterion : adv.getValue().entrySet()) {
                    criteriaTag.putLong(criterion.getKey(), criterion.getValue());
                }
                advancementsTag.put(adv.getKey(), criteriaTag);
            }
            root.put("advancements", advancementsTag);
        }

        // Statistics (compound of compounds: category -> {statName -> value})
        if (data.getStatistics() != null && !data.getStatistics().isEmpty()) {
            CompoundTag statsTag = NBT.createCompound();
            for (Map.Entry<String, Map<String, Integer>> cat : data.getStatistics().entrySet()) {
                CompoundTag catTag = NBT.createCompound();
                for (Map.Entry<String, Integer> stat : cat.getValue().entrySet()) {
                    catTag.putInt(stat.getKey(), stat.getValue());
                }
                statsTag.put(cat.getKey(), catTag);
            }
            root.put("statistics", statsTag);
        }

        // Attributes (list of compounds)
        if (data.getAttributes() != null && !data.getAttributes().isEmpty()) {
            ListTag attrList = NBT.createList();
            for (PlayerData.AttributeData attr : data.getAttributes()) {
                CompoundTag attrTag = NBT.createCompound();
                attrTag.putString("key", attr.getAttributeKey());
                attrTag.putDouble("base", attr.getBaseValue());
                if (attr.getModifiers() != null && !attr.getModifiers().isEmpty()) {
                    ListTag modList = NBT.createList();
                    for (PlayerData.ModifierData mod : attr.getModifiers()) {
                        CompoundTag modTag = NBT.createCompound();
                        modTag.putString("uuid", mod.getUuid());
                        modTag.putString("name", mod.getName());
                        modTag.putDouble("amount", mod.getAmount());
                        modTag.putString("operation", mod.getOperation());
                        if (mod.getSerializedData() != null && mod.getSerializedData().length > 0) {
                            modTag.putByteArray("data", mod.getSerializedData());
                        }
                        modList.add(modTag);
                    }
                    attrTag.put("modifiers", modList);
                }
                attrList.add(attrTag);
            }
            root.put("attributes", attrList);
        }

        // Persistent Data Container (compound of key -> byte[])
        if (data.getPersistentDataContainer() != null && !data.getPersistentDataContainer().isEmpty()) {
            CompoundTag pdcTag = NBT.createCompound();
            for (Map.Entry<String, byte[]> entry : data.getPersistentDataContainer().entrySet()) {
                pdcTag.putByteArray(entry.getKey(), entry.getValue());
            }
            root.put("pdc", pdcTag);
        }

        // Location (optional)
        if (data.getWorldName() != null) {
            root.putString("world", data.getWorldName());
            root.putDouble("x", data.getX());
            root.putDouble("y", data.getY());
            root.putDouble("z", data.getZ());
            root.putFloat("yaw", data.getYaw());
            root.putFloat("pitch", data.getPitch());
        }

        // Locked maps (list of byte arrays)
        if (data.getLockedMaps() != null && !data.getLockedMaps().isEmpty()) {
            ListTag mapList = NBT.createList();
            for (byte[] mapData : data.getLockedMaps()) {
                mapList.add(NBT.createByteArray(mapData));
            }
            root.put("lockedMaps", mapList);
        }

        // Metadata
        root.putLong("version", data.getVersion());
        root.putLong("fencingToken", data.getFencingToken());
        root.putLong("timestamp", data.getTimestamp());
        root.putString("saveCause", data.getSaveCause() != null ? data.getSaveCause() : "disconnect");

        // Serialize CompoundTag to binary byte[] (native NBT binary format)
        return NBT.toBytes(root);
    }

    /**
     * Deserialize NBT binary byte[] to PlayerData using sparrow-nbt.
     *
     * NBT.fromBytes reads the type byte, then dispatches to the correct
     * read method - no string/JSON structural character parsing.
     */
    public static PlayerData deserialize(byte[] data) throws IOException {
        CompoundTag root = NBT.fromBytes(data);
        if (root == null) {
            return new PlayerData();
        }

        PlayerData playerData = new PlayerData();

        // Inventory + Armor + Offhand + Ender chest
        if (root.get("inventory") instanceof ListTag invList) {
            playerData.setInventory(fromItemStackList(invList));
        }
        if (root.get("armor") instanceof ListTag armorList) {
            playerData.setArmor(fromItemStackList(armorList));
        }
        Tag offhandTag = root.get("offhand");
        if (offhandTag != null) {
            playerData.setOffhand(ItemStackCompat.deserialize(root.getByteArray("offhand")));
        }
        if (root.get("enderChest") instanceof ListTag ecList) {
            playerData.setEnderChest(fromItemStackList(ecList));
        }

        // Vitals
        playerData.setHealth(root.getDouble("health"));
        playerData.setMaxHealth(root.getDouble("maxHealth"));
        playerData.setFoodLevel(root.getInt("foodLevel"));
        playerData.setSaturation(root.getFloat("saturation"));
        playerData.setExhaustion(root.getFloat("exhaustion"));

        // Experience
        playerData.setExpLevel(root.getInt("expLevel"));
        playerData.setExpProgress(root.getFloat("expProgress"));
        playerData.setTotalExperience(root.getInt("totalExperience"));

        // Extra
        // Try name-based deserialization first (current format: gameMode stored as STRING)
        GameMode gameMode = GameMode.SURVIVAL;
        try {
            String gmName = root.getString("gameMode");
            if (gmName != null && !gmName.isEmpty()) {
                gameMode = GameMode.valueOf(gmName);
            }
        } catch (Exception nameEx) {
            // Fallback: legacy ordinal-based format (for data saved by older versions).
            // The old format stored gameMode as a BYTE (ordinal). We need to check
            // the tag type before reading as byte to avoid exceptions when the tag
            // is actually a STRING (current format).
            try {
                Tag gmTag = root.get("gameMode");
                if (gmTag instanceof net.momirealms.sparrow.nbt.ByteTag byteTag) {
                    int gmOrdinal = byteTag.getAsByte() & 0xFF;
                    GameMode[] gameModes = GameMode.values();
                    if (gmOrdinal >= 0 && gmOrdinal < gameModes.length) {
                        gameMode = gameModes[gmOrdinal];
                    }
                }
            } catch (Exception ordEx) {
                // Keep SURVIVAL default
            }
        }
        playerData.setGameMode(gameMode);
        playerData.setFireTicks(root.getInt("fireTicks"));
        playerData.setRemainingAir(root.getInt("remainingAir"));
        playerData.setMaximumAir(root.getInt("maximumAir"));

        // Flight status
        playerData.setFlying(root.getBoolean("flying"));
        playerData.setAllowFlight(root.getBoolean("allowFlight"));

        // Potion effects
        if (root.get("potionEffects") instanceof ListTag effectList) {
            List<PlayerData.PotionEffectData> effects = new ArrayList<>();
            for (int i = 0; i < effectList.size(); i++) {
                if (effectList.get(i) instanceof CompoundTag effectTag) {
                    String typeKey = effectTag.getString("type");
                    int duration = effectTag.getInt("duration");
                    int amplifier = effectTag.getInt("amplifier");
                    byte flags = effectTag.getByte("flags");
                    effects.add(new PlayerData.PotionEffectData(typeKey, duration, amplifier,
                        (flags & 0x01) != 0, (flags & 0x02) != 0, (flags & 0x04) != 0));
                }
            }
            playerData.setPotionEffects(effects);
        }

        // Advancements
        if (root.get("advancements") instanceof CompoundTag advancementsTag) {
            java.util.Map<String, java.util.Map<String, Long>> advancements = new java.util.HashMap<>();
            for (String advKey : advancementsTag.keySet()) {
                if (advancementsTag.get(advKey) instanceof CompoundTag criteriaTag) {
                    java.util.Map<String, Long> criteria = new java.util.HashMap<>();
                    for (String criterionName : criteriaTag.keySet()) {
                        criteria.put(criterionName, criteriaTag.getLong(criterionName));
                    }
                    advancements.put(advKey, criteria);
                }
            }
            playerData.setAdvancements(advancements);
        }

        // Statistics
        if (root.get("statistics") instanceof CompoundTag statsTag) {
            java.util.Map<String, java.util.Map<String, Integer>> statistics = new java.util.HashMap<>();
            for (String category : statsTag.keySet()) {
                if (statsTag.get(category) instanceof CompoundTag catTag) {
                    java.util.Map<String, Integer> stats = new java.util.HashMap<>();
                    for (String statName : catTag.keySet()) {
                        stats.put(statName, catTag.getInt(statName));
                    }
                    statistics.put(category, stats);
                }
            }
            playerData.setStatistics(statistics);
        }

        // Attributes
        if (root.get("attributes") instanceof ListTag attrList) {
            List<PlayerData.AttributeData> attributes = new ArrayList<>();
            for (int i = 0; i < attrList.size(); i++) {
                if (attrList.get(i) instanceof CompoundTag attrTag) {
                    String key = attrTag.getString("key");
                    double base = attrTag.getDouble("base");
                    List<PlayerData.ModifierData> mods = new ArrayList<>();
                    if (attrTag.get("modifiers") instanceof ListTag modList) {
                        for (int j = 0; j < modList.size(); j++) {
                            if (modList.get(j) instanceof CompoundTag modTag) {
                                byte[] modData = null;
                                if (modTag.get("data") != null) {
                                    modData = modTag.getByteArray("data");
                                }
                                mods.add(new PlayerData.ModifierData(
                                    modTag.getString("uuid"),
                                    modTag.getString("name"),
                                    modTag.getDouble("amount"),
                                    modTag.getString("operation"),
                                    modData
                                ));
                            }
                        }
                    }
                    attributes.add(new PlayerData.AttributeData(key, base, mods));
                }
            }
            playerData.setAttributes(attributes);
        }

        // PDC
        if (root.get("pdc") instanceof CompoundTag pdcTag) {
            java.util.Map<String, byte[]> pdc = new java.util.HashMap<>();
            for (String pdcKey : pdcTag.keySet()) {
                pdc.put(pdcKey, pdcTag.getByteArray(pdcKey));
            }
            playerData.setPersistentDataContainer(pdc);
        }

        // Location
        String worldName = root.getString("world");
        if (worldName != null && !worldName.isEmpty()) {
            playerData.setWorldName(worldName);
            playerData.setX(root.getDouble("x"));
            playerData.setY(root.getDouble("y"));
            playerData.setZ(root.getDouble("z"));
            playerData.setYaw(root.getFloat("yaw"));
            playerData.setPitch(root.getFloat("pitch"));
        }

        // Locked maps
        if (root.get("lockedMaps") instanceof ListTag mapList) {
            List<byte[]> maps = new ArrayList<>();
            for (int i = 0; i < mapList.size(); i++) {
                Tag mapTag = mapList.get(i);
                if (mapTag instanceof net.momirealms.sparrow.nbt.ByteArrayTag baTag) {
                    maps.add(baTag.getAsByteArray());
                }
            }
            playerData.setLockedMaps(maps);
        }

        // Metadata
        playerData.setVersion(root.getLong("version"));
        playerData.setFencingToken(root.getLong("fencingToken"));
        playerData.setTimestamp(root.getLong("timestamp"));
        String saveCause = root.getString("saveCause");
        playerData.setSaveCause(saveCause != null && !saveCause.isEmpty() ? saveCause : "disconnect");

        return playerData;
    }

    // ==================== Item Stack List Helpers ====================

    /**
     * Convert ItemStack[] to ListTag of ByteArrayTag (each item's NBT bytes).
     */
    private static ListTag toItemStackList(ItemStack[] items) {
        ListTag list = NBT.createList();
        for (ItemStack item : items) {
            list.add(NBT.createByteArray(ItemStackCompat.serialize(item)));
        }
        return list;
    }

    /**
     * Convert ListTag of ByteArrayTag back to ItemStack[].
     */
    private static ItemStack[] fromItemStackList(ListTag list) {
        ItemStack[] items = new ItemStack[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Tag element = list.get(i);
            if (element instanceof net.momirealms.sparrow.nbt.ByteArrayTag baTag) {
                try {
                    items[i] = ItemStackCompat.deserialize(baTag.getAsByteArray());
                } catch (Exception e) {
                    items[i] = null;
                }
            }
        }
        return items;
    }

    // ==================== Helpers ====================

    public static PlayerData.PotionEffectData toPotionEffectData(PotionEffect effect) {
        return new PlayerData.PotionEffectData(effect.getType().getKey().toString(),
            effect.getDuration(), effect.getAmplifier(), effect.isAmbient(),
            effect.hasParticles(), effect.hasIcon());
    }

    public static PotionEffect toPotionEffect(PlayerData.PotionEffectData data) {
        try {
            NamespacedKey key = NamespacedKey.fromString(data.getTypeKey());
            if (key == null) return null;
            PotionEffectType type = Registry.EFFECT.get(key);
            if (type == null) return null;
            return new PotionEffect(type, data.getDuration(), data.getAmplifier(),
                data.isAmbient(), data.isParticles(), data.isIcon());
        } catch (Exception e) { return null; }
    }
}
