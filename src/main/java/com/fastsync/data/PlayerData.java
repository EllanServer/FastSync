package com.fastsync.data;

import org.bukkit.GameMode;
import org.bukkit.inventory.ItemStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds all synchronizable player data.
 * Designed for direct NBT byte[] serialization - no string/base64 encoding.
 *
 * Full feature parity with HuskSync, using NBT binary format throughout.
 */
public class PlayerData {

    private ItemStack[] inventory;
    private ItemStack[] armor;
    private ItemStack offhand;
    private ItemStack[] enderChest;

    private double health;
    private double maxHealth;
    private int foodLevel;
    private float saturation;
    private float exhaustion;

    private int expLevel;
    private float expProgress;
    private int totalExperience;

    private GameMode gameMode;
    private int fireTicks;
    private int remainingAir;
    private int maximumAir;

    private List<PotionEffectData> potionEffects;

    // --- HuskSync feature parity ---

    // Advancements: map of advancement key -> serialized criterion data
    // Each entry: key (NamespacedKey string), criteria map (criterion -> granted timestamp)
    private Map<String, Map<String, Long>> advancements;

    // Statistics: map of stat category -> map of stat name -> value
    // Uses Bukkit Statistic enum names
    private Map<String, Map<String, Integer>> statistics;

    // Attribute base values. Runtime modifiers are intentionally not copied:
    // Paper's public API does not expose permanent vs transient membership.
    private List<AttributeData> attributes;

    // Flight status
    private boolean flying;
    private boolean allowFlight;

    // Persistent Data Container: map of key -> byte[] (raw PDC values)
    private Map<String, byte[]> persistentDataContainer;

    // Location (optional, for mirror worlds)
    private String worldName;
    private String worldUuid;
    private double x, y, z;
    private float yaw, pitch;

    /**
     * Wall-clock timestamp when this data was collected.
     *
     * <p><b>Spanner note:</b> For display/debugging only. Do NOT use this for
     * ordering or "which data is newer" decisions — use {@link #version} or
     * {@link #fencingToken} instead, which are monotonic and immune to clock skew.
     */
    private long timestamp;
    private long version;
    private long fencingToken; // Kleppmann fencing token for stale-write defence
    private String saveCause; // disconnect, death, world_save, shutdown, api, etc.
    // Runtime-only marker: this instance contains only a dirty component
    // subset and must never be serialized as the authoritative full Blob.
    private boolean componentSubset;
    // Runtime-only mask of components actually collected into a subset. This
    // freezes the entity-thread config decision across the async persistence
    // boundary, where a config reload may otherwise enable an uncollected field.
    private long collectedComponentMask;

    public PlayerData() {
        this(true);
    }

    private PlayerData(boolean initializeCollections) {
        if (initializeCollections) {
            this.potionEffects = new ArrayList<>();
            this.advancements = new HashMap<>();
            this.statistics = new HashMap<>();
            this.attributes = new ArrayList<>();
            this.persistentDataContainer = new HashMap<>();
        }
        this.timestamp = System.currentTimeMillis();
        this.version = 0;
        this.fencingToken = 0;
        this.saveCause = "disconnect";
        this.componentSubset = !initializeCollections;
    }

    /** Allocation-minimal carrier for a component-only save. */
    public static PlayerData forComponentSubset() {
        return new PlayerData(false);
    }

    // ==================== Getters & Setters ====================

    public ItemStack[] getInventory() { return inventory; }
    public void setInventory(ItemStack[] inventory) { this.inventory = inventory; }

    public ItemStack[] getArmor() { return armor; }
    public void setArmor(ItemStack[] armor) { this.armor = armor; }

    public ItemStack getOffhand() { return offhand; }
    public void setOffhand(ItemStack offhand) { this.offhand = offhand; }

    public ItemStack[] getEnderChest() { return enderChest; }
    public void setEnderChest(ItemStack[] enderChest) { this.enderChest = enderChest; }

    public double getHealth() { return health; }
    public void setHealth(double health) { this.health = health; }

    public double getMaxHealth() { return maxHealth; }
    public void setMaxHealth(double maxHealth) { this.maxHealth = maxHealth; }

    public int getFoodLevel() { return foodLevel; }
    public void setFoodLevel(int foodLevel) { this.foodLevel = foodLevel; }

    public float getSaturation() { return saturation; }
    public void setSaturation(float saturation) { this.saturation = saturation; }

    public float getExhaustion() { return exhaustion; }
    public void setExhaustion(float exhaustion) { this.exhaustion = exhaustion; }

    public int getExpLevel() { return expLevel; }
    public void setExpLevel(int expLevel) { this.expLevel = expLevel; }

    public float getExpProgress() { return expProgress; }
    public void setExpProgress(float expProgress) { this.expProgress = expProgress; }

    public int getTotalExperience() { return totalExperience; }
    public void setTotalExperience(int totalExperience) { this.totalExperience = totalExperience; }

    public GameMode getGameMode() { return gameMode; }
    public void setGameMode(GameMode gameMode) { this.gameMode = gameMode; }

    public int getFireTicks() { return fireTicks; }
    public void setFireTicks(int fireTicks) { this.fireTicks = fireTicks; }

    public int getRemainingAir() { return remainingAir; }
    public void setRemainingAir(int remainingAir) { this.remainingAir = remainingAir; }

    public int getMaximumAir() { return maximumAir; }
    public void setMaximumAir(int maximumAir) { this.maximumAir = maximumAir; }

    public List<PotionEffectData> getPotionEffects() { return potionEffects; }
    public void setPotionEffects(List<PotionEffectData> potionEffects) { this.potionEffects = potionEffects; }

    public Map<String, Map<String, Long>> getAdvancements() { return advancements; }
    public void setAdvancements(Map<String, Map<String, Long>> advancements) { this.advancements = advancements; }

    public Map<String, Map<String, Integer>> getStatistics() { return statistics; }
    public void setStatistics(Map<String, Map<String, Integer>> statistics) { this.statistics = statistics; }

    public List<AttributeData> getAttributes() { return attributes; }
    public void setAttributes(List<AttributeData> attributes) { this.attributes = attributes; }

    public boolean isFlying() { return flying; }
    public void setFlying(boolean flying) { this.flying = flying; }

    public boolean isAllowFlight() { return allowFlight; }
    public void setAllowFlight(boolean allowFlight) { this.allowFlight = allowFlight; }

    public Map<String, byte[]> getPersistentDataContainer() { return persistentDataContainer; }
    public void setPersistentDataContainer(Map<String, byte[]> persistentDataContainer) { this.persistentDataContainer = persistentDataContainer; }

    public String getWorldName() { return worldName; }
    public void setWorldName(String worldName) { this.worldName = worldName; }
    public String getWorldUuid() { return worldUuid; }
    public void setWorldUuid(String worldUuid) { this.worldUuid = worldUuid; }

    public double getX() { return x; }
    public void setX(double x) { this.x = x; }

    public double getY() { return y; }
    public void setY(double y) { this.y = y; }

    public double getZ() { return z; }
    public void setZ(double z) { this.z = z; }

    public float getYaw() { return yaw; }
    public void setYaw(float yaw) { this.yaw = yaw; }

    public float getPitch() { return pitch; }
    public void setPitch(float pitch) { this.pitch = pitch; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }

    public long getFencingToken() { return fencingToken; }
    public void setFencingToken(long fencingToken) { this.fencingToken = fencingToken; }

    public String getSaveCause() { return saveCause; }
    public void setSaveCause(String saveCause) { this.saveCause = saveCause; }

    public boolean isComponentSubset() { return componentSubset; }
    public void setComponentSubset(boolean componentSubset) { this.componentSubset = componentSubset; }
    public void markComponentCollected(long storageMask) { collectedComponentMask |= storageMask; }
    public boolean isComponentCollected(long storageMask) {
        return (collectedComponentMask & storageMask) != 0L;
    }

    // ==================== Inner Data Classes ====================

    public static class PotionEffectData {
        private final String typeKey;
        private final int duration;
        private final int amplifier;
        private final boolean ambient;
        private final boolean particles;
        private final boolean icon;

        public PotionEffectData(String typeKey, int duration, int amplifier,
                                boolean ambient, boolean particles, boolean icon) {
            this.typeKey = typeKey;
            this.duration = duration;
            this.amplifier = amplifier;
            this.ambient = ambient;
            this.particles = particles;
            this.icon = icon;
        }

        public String getTypeKey() { return typeKey; }
        public int getDuration() { return duration; }
        public int getAmplifier() { return amplifier; }
        public boolean isAmbient() { return ambient; }
        public boolean isParticles() { return particles; }
        public boolean isIcon() { return icon; }
    }

    public static class AttributeData {
        private final String attributeKey;
        private final double baseValue;

        public AttributeData(String attributeKey, double baseValue) {
            this.attributeKey = attributeKey;
            this.baseValue = baseValue;
        }

        public String getAttributeKey() { return attributeKey; }
        public double getBaseValue() { return baseValue; }
    }
}
