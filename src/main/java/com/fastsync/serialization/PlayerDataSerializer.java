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
        // CRITICAL: write a presence flag AND always write the list tag, even
        // when the list is empty. The apply path distinguishes:
        //   null           = "not collected / unknown" → do not touch target
        //   empty list     = "explicitly no effects" → clear all target effects
        // If we only write the tag when non-empty (old behavior), a player who
        // cleared all effects on server A produces no tag in the Blob; server B
        // sees no tag, leaves data.potionEffects = null, and the apply path
        // (correctly) does NOT clear target effects — so the old effects
        // silently persist on server B.
        //
        // potionEffectsPresent=true  → data.potionEffects is meaningful
        //                               (may be empty list = "no effects")
        // potionEffectsPresent=false → data.potionEffects was null at serialize
        //                               time (not collected)
        // The flag is ALWAYS written, so its absence on read is treated as an
        // unsupported/legacy payload (deserialize throws) rather than silently
        // preserving a stale default.
        if (data.getPotionEffects() != null) {
            root.putBoolean("potionEffectsPresent", true);
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
        } else {
            root.putBoolean("potionEffectsPresent", false);
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
        // gameMode is stored as a STRING (e.g. "SURVIVAL", "CREATIVE"). The
        // legacy ordinal-byte format written by very old builds was removed
        // when the project dropped old-version compatibility; data still
        // carrying a byte tag here is treated as corrupt rather than silently
        // reinterpreted, so a future migration tool can find it.
        GameMode gameMode = GameMode.SURVIVAL;
        String gmName = root.getString("gameMode");
        if (gmName != null && !gmName.isEmpty()) {
            try {
                gameMode = GameMode.valueOf(gmName);
            } catch (IllegalArgumentException e) {
                throw new IOException("Unknown gameMode value '" + gmName
                    + "' (expected a GameMode name)", e);
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
        // The presence flag is ALWAYS written by the current serializer (true
        // or false), so an absent flag means the Blob was not produced by the
        // current code path. Clean-slate: treat that as an unsupported/corrupt
        // payload rather than silently preserving a legacy default, which could
        // mask a stale effect set on the target server.
        //   potionEffectsPresent=true  → set effects (may be empty list = "no effects")
        //   potionEffectsPresent=false → set null (not collected)
        if (root.get("potionEffectsPresent") == null) {
            throw new IOException("Missing 'potionEffectsPresent' flag — unsupported/legacy payload");
        }
        if (root.getBoolean("potionEffectsPresent")) {
            List<PlayerData.PotionEffectData> effects = new ArrayList<>();
            if (root.get("potionEffects") instanceof ListTag effectList) {
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
            }
            // Explicitly set, even if effects is empty — empty means
            // "clear all target effects" on apply.
            playerData.setPotionEffects(effects);
        } else {
            // potionEffectsPresent=false: data was null at serialize time
            playerData.setPotionEffects(null);
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
     *
     * <p>A deserialization failure for a non-empty slot is propagated as an
     * {@link ItemSerializationException} rather than silently turned into a
     * {@code null} slot. Silently dropping a corrupted item would delete the
     * player's item on the next save; surfacing the failure lets the caller
     * (load path) refuse the whole Blob / record a conflict snapshot.
     */
    private static ItemStack[] fromItemStackList(ListTag list) {
        ItemStack[] items = new ItemStack[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Tag element = list.get(i);
            if (element instanceof net.momirealms.sparrow.nbt.ByteArrayTag baTag) {
                // ItemStackCompat.deserialize returns null for an empty payload
                // (a real air slot) and throws ItemSerializationException on
                // genuine corruption. Both behaviors are correct here.
                items[i] = ItemStackCompat.deserialize(baTag.getAsByteArray());
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

    // ==================== Phase 2: Per-Component Serialization ====================

    /**
     * Serialize a single component to a standalone NBT byte[].
     *
     * <p>Each component is wrapped in a CompoundTag with a single key
     * (the component name) whose value is the component's NBT subtree.
     * This keeps the per-component blob self-describing — readers can
     * identify which component a blob represents without external metadata.
     *
     * <p>Components not yet migrated to per-component storage continue to
     * live in the legacy single-Blob {@link #serialize(PlayerData)} format.
     * The SyncManager decides which path to use based on the dirty mask
     * and the player's {@code component_bitmap}.
     *
     * @param componentName one of {@link com.fastsync.sync.dirty.ComponentDirtyMask.Component#name()}
     * @param data          the full PlayerData (only the named component is read)
     * @return NBT-encoded byte[] for the single component, or null if the
     *         component is null/empty and should not be stored.
     */
    public static byte[] serializeComponent(String componentName, PlayerData data) throws IOException {
        CompoundTag root = NBT.createCompound();
        CompoundTag componentRoot = serializeComponentFields(componentName, data);
        if (componentRoot == null) return null;
        root.put(componentName, componentRoot);
        return NBT.toBytes(root);
    }

    /**
     * Deserialize a single component's NBT byte[] and merge it into the
     * provided PlayerData. Other fields in {@code data} are left untouched.
     */
    public static void deserializeComponent(String componentName, byte[] bytes, PlayerData data) throws IOException {
        if (bytes == null || bytes.length == 0) return;
        CompoundTag root = NBT.fromBytes(bytes);
        if (root == null) return;
        Tag tag = root.get(componentName);
        if (!(tag instanceof CompoundTag componentRoot)) return;
        deserializeComponentFields(componentName, componentRoot, data);
    }

    /**
     * Write a single component's fields into a fresh CompoundTag.
     * Returns null if the component has no data (e.g. PDC is empty).
     */
    private static CompoundTag serializeComponentFields(String name, PlayerData data) throws IOException {
        CompoundTag c = NBT.createCompound();
        switch (name) {
            case "INVENTORY" -> {
                if (data.getInventory() != null) c.put("inventory", toItemStackList(data.getInventory()));
                if (data.getArmor() != null) c.put("armor", toItemStackList(data.getArmor()));
                // CRITICAL: offhand must have an explicit present flag.
                //
                // If we only write the offhand field when getOffhand() != null
                // (the old behavior), then a player who clears their offhand
                // produces a component payload with NO offhand field. On the
                // next load, deserializeComponentFields() sees no offhand field
                // and leaves data.offhand untouched — which means the BASELINE
                // Blob's old offhand item silently persists in the player's
                // offhand slot. The empty-state signal is lost.
                //
                // offhandPresent=true  → offhand field is meaningful (may be null/empty)
                // offhandPresent=false → offhand was explicitly cleared to null
                // The flag is ALWAYS written; an absent flag on read is treated
                // as a corrupt/unsupported component (deserializeComponentFields
                // throws) rather than silently preserving a stale baseline offhand.
                if (data.getOffhand() != null) {
                    c.putBoolean("offhandPresent", true);
                    c.putByteArray("offhand", ItemStackCompat.serialize(data.getOffhand()));
                } else {
                    c.putBoolean("offhandPresent", false);
                }
            }
            case "ENDER_CHEST" -> {
                if (data.getEnderChest() != null) c.put("enderChest", toItemStackList(data.getEnderChest()));
            }
            case "VITALS" -> {
                c.putDouble("health", data.getHealth());
                c.putDouble("maxHealth", data.getMaxHealth());
            }
            case "FOOD" -> {
                c.putInt("foodLevel", data.getFoodLevel());
                c.putFloat("saturation", data.getSaturation());
                c.putFloat("exhaustion", data.getExhaustion());
            }
            case "EXPERIENCE" -> {
                c.putInt("expLevel", data.getExpLevel());
                c.putFloat("expProgress", data.getExpProgress());
                c.putInt("totalExperience", data.getTotalExperience());
            }
            case "POTION_EFFECTS" -> {
                if (data.getPotionEffects() != null && !data.getPotionEffects().isEmpty()) {
                    ListTag effectList = NBT.createList();
                    for (PlayerData.PotionEffectData effect : data.getPotionEffects()) {
                        CompoundTag e = NBT.createCompound();
                        e.putString("type", effect.getTypeKey());
                        e.putInt("duration", effect.getDuration());
                        e.putInt("amplifier", effect.getAmplifier());
                        byte flags = 0;
                        if (effect.isAmbient()) flags |= 0x01;
                        if (effect.isParticles()) flags |= 0x02;
                        if (effect.isIcon()) flags |= 0x04;
                        e.putByte("flags", flags);
                        effectList.add(e);
                    }
                    c.put("potionEffects", effectList);
                }
            }
            case "GAME_MODE" -> c.putString("gameMode",
                data.getGameMode() != null ? data.getGameMode().name() : "SURVIVAL");
            case "FIRE_TICKS" -> c.putInt("fireTicks", data.getFireTicks());
            case "AIR" -> {
                c.putInt("remainingAir", data.getRemainingAir());
                c.putInt("maximumAir", data.getMaximumAir());
            }
            case "FLIGHT" -> {
                c.putBoolean("flying", data.isFlying());
                c.putBoolean("allowFlight", data.isAllowFlight());
            }
            case "ADVANCEMENTS" -> {
                if (data.getAdvancements() != null && !data.getAdvancements().isEmpty()) {
                    CompoundTag a = NBT.createCompound();
                    for (var entry : data.getAdvancements().entrySet()) {
                        CompoundTag criteria = NBT.createCompound();
                        for (var crit : entry.getValue().entrySet()) {
                            criteria.putLong(crit.getKey(), crit.getValue());
                        }
                        a.put(entry.getKey(), criteria);
                    }
                    c.put("advancements", a);
                }
            }
            case "STATISTICS" -> {
                if (data.getStatistics() != null && !data.getStatistics().isEmpty()) {
                    CompoundTag s = NBT.createCompound();
                    for (var cat : data.getStatistics().entrySet()) {
                        CompoundTag catTag = NBT.createCompound();
                        for (var stat : cat.getValue().entrySet()) {
                            catTag.putInt(stat.getKey(), stat.getValue());
                        }
                        s.put(cat.getKey(), catTag);
                    }
                    c.put("statistics", s);
                }
            }
            case "ATTRIBUTES" -> {
                if (data.getAttributes() != null && !data.getAttributes().isEmpty()) {
                    ListTag attrList = NBT.createList();
                    for (PlayerData.AttributeData attr : data.getAttributes()) {
                        CompoundTag a = NBT.createCompound();
                        a.putString("key", attr.getAttributeKey());
                        a.putDouble("base", attr.getBaseValue());
                        if (attr.getModifiers() != null && !attr.getModifiers().isEmpty()) {
                            ListTag mods = NBT.createList();
                            for (PlayerData.ModifierData mod : attr.getModifiers()) {
                                CompoundTag m = NBT.createCompound();
                                m.putString("uuid", mod.getUuid());
                                m.putString("name", mod.getName());
                                m.putDouble("amount", mod.getAmount());
                                m.putString("operation", mod.getOperation());
                                if (mod.getSerializedData() != null && mod.getSerializedData().length > 0) {
                                    m.putByteArray("data", mod.getSerializedData());
                                }
                                mods.add(m);
                            }
                            a.put("modifiers", mods);
                        }
                        attrList.add(a);
                    }
                    c.put("attributes", attrList);
                }
            }
            case "PDC" -> {
                if (data.getPersistentDataContainer() != null && !data.getPersistentDataContainer().isEmpty()) {
                    CompoundTag pdcTag = NBT.createCompound();
                    for (var entry : data.getPersistentDataContainer().entrySet()) {
                        pdcTag.putByteArray(entry.getKey(), entry.getValue());
                    }
                    c.put("pdc", pdcTag);
                }
            }
            case "LOCATION" -> {
                if (data.getWorldName() != null) {
                    c.putString("world", data.getWorldName());
                    c.putDouble("x", data.getX());
                    c.putDouble("y", data.getY());
                    c.putDouble("z", data.getZ());
                    c.putFloat("yaw", data.getYaw());
                    c.putFloat("pitch", data.getPitch());
                }
            }
            default -> {
                // Unknown component — return null so caller skips it.
                return null;
            }
        }
        // Mark this component as present (collected) even if empty.
        // This distinguishes "component was collected but is empty" (e.g. no
        // potion effects) from "component was not collected at all" (null).
        // On deserialize, _present=true with empty fields means "clear target".
        c.putBoolean("_present", true);
        return c;
    }

    /**
     * Read a single component's fields from its CompoundTag into PlayerData.
     */
    private static void deserializeComponentFields(String name, CompoundTag c, PlayerData data) throws IOException {
        switch (name) {
            case "INVENTORY" -> {
                if (c.get("inventory") instanceof ListTag inv) data.setInventory(fromItemStackList(inv));
                if (c.get("armor") instanceof ListTag arm) data.setArmor(fromItemStackList(arm));
                // offhandPresent gates whether the offhand field is meaningful.
                // See serializeComponentFields() for the rationale: without this
                // flag, a player who cleared their offhand would have no offhand
                // field written, and the baseline Blob's stale offhand item
                // would silently persist on next load.
                //
                // The current serializer ALWAYS writes offhandPresent (true or
                // false), so an absent flag means the component was not produced
                // by the current code path. Clean-slate: treat that as a corrupt
                // component rather than silently preserving the baseline Blob's
                // offhand (which could re-apply a stale item the source cleared).
                if (c.get("offhandPresent") == null) {
                    throw new IOException("INVENTORY component missing 'offhandPresent' flag "
                        + "— unsupported/legacy component payload");
                }
                if (c.getBoolean("offhandPresent")) {
                    data.setOffhand(ItemStackCompat.deserialize(c.getByteArray("offhand")));
                } else {
                    // Explicit empty state — clear the offhand slot.
                    data.setOffhand(null);
                }
            }
            case "ENDER_CHEST" -> {
                if (c.get("enderChest") instanceof ListTag ec) data.setEnderChest(fromItemStackList(ec));
            }
            case "VITALS" -> {
                data.setHealth(c.getDouble("health"));
                data.setMaxHealth(c.getDouble("maxHealth"));
            }
            case "FOOD" -> {
                data.setFoodLevel(c.getInt("foodLevel"));
                data.setSaturation(c.getFloat("saturation"));
                data.setExhaustion(c.getFloat("exhaustion"));
            }
            case "EXPERIENCE" -> {
                data.setExpLevel(c.getInt("expLevel"));
                data.setExpProgress(c.getFloat("expProgress"));
                data.setTotalExperience(c.getInt("totalExperience"));
            }
            case "POTION_EFFECTS" -> {
                // If _present=true, always set effects (empty list = clear).
                // This handles the case where a player's potion effects were
                // removed — the empty component payload must clear the old state.
                java.util.List<PlayerData.PotionEffectData> effects = new java.util.ArrayList<>();
                if (c.get("potionEffects") instanceof ListTag list) {
                    for (int i = 0; i < list.size(); i++) {
                        if (list.get(i) instanceof CompoundTag e) {
                            byte flags = e.getByte("flags");
                            effects.add(new PlayerData.PotionEffectData(
                                e.getString("type"), e.getInt("duration"), e.getInt("amplifier"),
                                (flags & 0x01) != 0, (flags & 0x02) != 0, (flags & 0x04) != 0));
                        }
                    }
                }
                data.setPotionEffects(effects);
            }
            case "GAME_MODE" -> {
                // Stored as a GameMode name string. Matches the full-Blob path
                // (both write String now); the legacy ordinal-byte form is no
                // longer read on the hot path. An unknown value is surfaced as
                // an exception so a corrupt component is rejected rather than
                // silently falling back to SURVIVAL.
                String gmName = c.getString("gameMode");
                if (gmName == null || gmName.isEmpty()) {
                    data.setGameMode(GameMode.SURVIVAL);
                } else {
                    try {
                        data.setGameMode(GameMode.valueOf(gmName));
                    } catch (IllegalArgumentException e) {
                        throw new IOException("Unknown gameMode value '" + gmName
                            + "' in GAME_MODE component (expected a GameMode name)", e);
                    }
                }
            }
            case "FIRE_TICKS" -> data.setFireTicks(c.getInt("fireTicks"));
            case "AIR" -> {
                data.setRemainingAir(c.getInt("remainingAir"));
                data.setMaximumAir(c.getInt("maximumAir"));
            }
            case "FLIGHT" -> {
                data.setFlying(c.getBoolean("flying"));
                data.setAllowFlight(c.getBoolean("allowFlight"));
            }
            case "ADVANCEMENTS" -> {
                // _present=true means always set (empty map = clear)
                java.util.Map<String, java.util.Map<String, Long>> advs = new java.util.HashMap<>();
                if (c.get("advancements") instanceof CompoundTag a) {
                    for (String key : a.keySet()) {
                        if (a.get(key) instanceof CompoundTag crit) {
                            java.util.Map<String, Long> m = new java.util.HashMap<>();
                            for (String cn : crit.keySet()) m.put(cn, crit.getLong(cn));
                            advs.put(key, m);
                        }
                    }
                }
                data.setAdvancements(advs);
            }
            case "STATISTICS" -> {
                java.util.Map<String, java.util.Map<String, Integer>> stats = new java.util.HashMap<>();
                if (c.get("statistics") instanceof CompoundTag s) {
                    for (String cat : s.keySet()) {
                        if (s.get(cat) instanceof CompoundTag catTag) {
                            java.util.Map<String, Integer> m = new java.util.HashMap<>();
                            for (String sn : catTag.keySet()) m.put(sn, catTag.getInt(sn));
                            stats.put(cat, m);
                        }
                    }
                }
                data.setStatistics(stats);
            }
            case "ATTRIBUTES" -> {
                java.util.List<PlayerData.AttributeData> attrs = new java.util.ArrayList<>();
                if (c.get("attributes") instanceof ListTag list) {
                    for (int i = 0; i < list.size(); i++) {
                        if (list.get(i) instanceof CompoundTag a) {
                            java.util.List<PlayerData.ModifierData> mods = new java.util.ArrayList<>();
                            if (a.get("modifiers") instanceof ListTag ml) {
                                for (int j = 0; j < ml.size(); j++) {
                                    if (ml.get(j) instanceof CompoundTag m) {
                                        byte[] md = m.get("data") != null ? m.getByteArray("data") : null;
                                        mods.add(new PlayerData.ModifierData(
                                            m.getString("uuid"), m.getString("name"),
                                            m.getDouble("amount"), m.getString("operation"), md));
                                    }
                                }
                            }
                            attrs.add(new PlayerData.AttributeData(a.getString("key"), a.getDouble("base"), mods));
                        }
                    }
                }
                data.setAttributes(attrs);
            }
            case "PDC" -> {
                java.util.Map<String, byte[]> pdc = new java.util.HashMap<>();
                if (c.get("pdc") instanceof CompoundTag pdcTag) {
                    for (String key : pdcTag.keySet()) pdc.put(key, pdcTag.getByteArray(key));
                }
                data.setPersistentDataContainer(pdc);
            }
            case "LOCATION" -> {
                if (c.getString("world") != null && !c.getString("world").isEmpty()) {
                    data.setWorldName(c.getString("world"));
                    data.setX(c.getDouble("x"));
                    data.setY(c.getDouble("y"));
                    data.setZ(c.getDouble("z"));
                    data.setYaw(c.getFloat("yaw"));
                    data.setPitch(c.getFloat("pitch"));
                }
            }
            default -> { /* unknown component: skip */ }
        }
    }
}
