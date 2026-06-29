package com.fastsync.serialization;

import com.fastsync.data.PlayerData;
import org.bukkit.GameMode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the per-component serialize/deserialize API added in
 * phase 2. Verifies that each component can be serialized to its own
 * NBT byte[] and round-tripped back into a PlayerData without affecting
 * other components' fields.
 *
 * <p>Note: ItemStack-related components (INVENTORY, ENDER_CHEST) can't
 * be fully tested here because ItemStackCompat.serialize requires the
 * Paper runtime. We test the non-ItemStack components which exercise
 * the core serialization logic.
 */
class PlayerDataSerializerComponentTest {

    @Test
    void testVitalsRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        original.setHealth(15.5);
        original.setMaxHealth(20.0);

        byte[] bytes = PlayerDataSerializer.serializeComponent("VITALS", original);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        PlayerData target = new PlayerData();
        target.setHealth(1.0);
        target.setMaxHealth(1.0);

        PlayerDataSerializer.deserializeComponent("VITALS", bytes, target);

        assertEquals(15.5, target.getHealth(), 0.001);
        assertEquals(20.0, target.getMaxHealth(), 0.001);
    }

    @Test
    void testFoodRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        original.setFoodLevel(18);
        original.setSaturation(5.0f);
        original.setExhaustion(0.5f);

        byte[] bytes = PlayerDataSerializer.serializeComponent("FOOD", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("FOOD", bytes, target);

        assertEquals(18, target.getFoodLevel());
        assertEquals(5.0f, target.getSaturation(), 0.001f);
        assertEquals(0.5f, target.getExhaustion(), 0.001f);
    }

    @Test
    void testExperienceRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        original.setExpLevel(42);
        original.setExpProgress(0.75f);
        original.setTotalExperience(1200);

        byte[] bytes = PlayerDataSerializer.serializeComponent("EXPERIENCE", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("EXPERIENCE", bytes, target);

        assertEquals(42, target.getExpLevel());
        assertEquals(0.75f, target.getExpProgress(), 0.001f);
        assertEquals(1200, target.getTotalExperience());
    }

    @Test
    void testGameModeRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        original.setGameMode(GameMode.CREATIVE);

        byte[] bytes = PlayerDataSerializer.serializeComponent("GAME_MODE", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("GAME_MODE", bytes, target);

        assertEquals(GameMode.CREATIVE, target.getGameMode());
    }

    @Test
    void testGameModeComponentStoredAsStringNotOrdinalByte() throws IOException {
        // The component path previously wrote gameMode as a BYTE (ordinal). It
        // now writes a STRING (GameMode.name()) so the full-Blob and component
        // paths use the same representation. Verify the on-disk tag type.
        PlayerData original = new PlayerData();
        original.setGameMode(GameMode.SPECTATOR);

        byte[] bytes = PlayerDataSerializer.serializeComponent("GAME_MODE", original);
        assertNotNull(bytes);

        net.momirealms.sparrow.nbt.CompoundTag root =
            net.momirealms.sparrow.nbt.NBT.fromBytes(bytes);
        net.momirealms.sparrow.nbt.Tag gmTag = root.get("GAME_MODE");
        assertNotNull(gmTag, "Component root should contain the GAME_MODE subtree");
        net.momirealms.sparrow.nbt.Tag inner =
            ((net.momirealms.sparrow.nbt.CompoundTag) gmTag).get("gameMode");
        assertTrue(inner instanceof net.momirealms.sparrow.nbt.StringTag,
            "gameMode must be a StringTag, not a ByteTag (was: "
                + (inner == null ? "null" : inner.getClass().getSimpleName()) + ")");
        // Cross-check the value via the typed getter.
        assertEquals("SPECTATOR",
            ((net.momirealms.sparrow.nbt.CompoundTag) gmTag).getString("gameMode"));
    }

    @Test
    void testGameModeComponentRoundTripsAllModes() throws IOException {
        for (GameMode mode : GameMode.values()) {
            PlayerData original = new PlayerData();
            original.setGameMode(mode);
            byte[] bytes = PlayerDataSerializer.serializeComponent("GAME_MODE", original);
            PlayerData target = new PlayerData();
            PlayerDataSerializer.deserializeComponent("GAME_MODE", bytes, target);
            assertEquals(mode, target.getGameMode(),
                "Round-trip failed for " + mode);
        }
    }

    @Test
    void testFullBlobRejectsUnknownGameModeString() throws IOException {
        // Build a full Blob whose gameMode is a string but not a valid
        // GameMode name. The legacy ordinal fallback was removed; an unknown
        // value must surface as an IOException rather than silently falling
        // back to SURVIVAL.
        net.momirealms.sparrow.nbt.CompoundTag root = net.momirealms.sparrow.nbt.NBT.createCompound();
        root.putString("gameMode", "NOT_A_REAL_GAMEMODE");
        root.putLong("version", 1L);
        byte[] bytes = net.momirealms.sparrow.nbt.NBT.toBytes(root);

        assertThrows(IOException.class, () -> PlayerDataSerializer.deserialize(bytes));
    }

    @Test
    void testGameModeComponentRejectsUnknownGameModeString() throws IOException {
        net.momirealms.sparrow.nbt.CompoundTag wrapper = net.momirealms.sparrow.nbt.NBT.createCompound();
        net.momirealms.sparrow.nbt.CompoundTag inner = net.momirealms.sparrow.nbt.NBT.createCompound();
        inner.putString("gameMode", "BOGUS");
        inner.putBoolean("_present", true);
        wrapper.put("GAME_MODE", inner);
        byte[] bytes = net.momirealms.sparrow.nbt.NBT.toBytes(wrapper);

        PlayerData target = new PlayerData();
        assertThrows(IOException.class,
            () -> PlayerDataSerializer.deserializeComponent("GAME_MODE", bytes, target));
    }

    @Test
    void testFireTicksRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        original.setFireTicks(100);

        byte[] bytes = PlayerDataSerializer.serializeComponent("FIRE_TICKS", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("FIRE_TICKS", bytes, target);

        assertEquals(100, target.getFireTicks());
    }

    @Test
    void testAirRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        original.setRemainingAir(150);
        original.setMaximumAir(300);

        byte[] bytes = PlayerDataSerializer.serializeComponent("AIR", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("AIR", bytes, target);

        assertEquals(150, target.getRemainingAir());
        assertEquals(300, target.getMaximumAir());
    }

    @Test
    void testFlightRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        original.setFlying(true);
        original.setAllowFlight(true);

        byte[] bytes = PlayerDataSerializer.serializeComponent("FLIGHT", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        target.setFlying(false);
        target.setAllowFlight(false);

        PlayerDataSerializer.deserializeComponent("FLIGHT", bytes, target);

        assertTrue(target.isFlying());
        assertTrue(target.isAllowFlight());
    }

    @Test
    void testLocationRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        original.setWorldName("world");
        original.setWorldUuid("123e4567-e89b-12d3-a456-426614174000");
        original.setX(123.45);
        original.setY(64.0);
        original.setZ(-78.9);
        original.setYaw(45.5f);
        original.setPitch(-30.0f);

        byte[] bytes = PlayerDataSerializer.serializeComponent("LOCATION", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("LOCATION", bytes, target);

        assertEquals("world", target.getWorldName());
        assertEquals("123e4567-e89b-12d3-a456-426614174000", target.getWorldUuid());
        assertEquals(123.45, target.getX(), 0.001);
        assertEquals(64.0, target.getY(), 0.001);
        assertEquals(-78.9, target.getZ(), 0.001);
        assertEquals(45.5f, target.getYaw(), 0.001f);
        assertEquals(-30.0f, target.getPitch(), 0.001f);
    }

    @Test
    void testEmptyComponentProducesPresentPayload() throws IOException {
        // Phase 3: empty components must produce a valid payload with _present=true,
        // NOT null. This ensures that "player has no potion effects" is distinguishable
        // from "potion effects component was not collected". Without this, removing
        // all potion effects would not clear the old state on the target server.
        PlayerData empty = new PlayerData();
        byte[] bytes = PlayerDataSerializer.serializeComponent("PDC", empty);
        assertNotNull(bytes, "Empty PDC should produce a _present payload, not null");
        assertTrue(bytes.length > 0, "Empty PDC payload should be non-empty (contains _present marker)");

        // Deserialize should clear the target PDC (set to empty map)
        PlayerData target = new PlayerData();
        java.util.Map<String, byte[]> oldPdc = new java.util.HashMap<>();
        oldPdc.put("old:key", new byte[]{1, 2, 3});
        target.setPersistentDataContainer(oldPdc);

        PlayerDataSerializer.deserializeComponent("PDC", bytes, target);
        assertNotNull(target.getPersistentDataContainer());
        assertTrue(target.getPersistentDataContainer().isEmpty(),
            "Deserializing empty PDC payload should clear the target PDC");
    }

    @Test
    void testUnknownComponentReturnsNull() throws IOException {
        PlayerData data = new PlayerData();
        byte[] bytes = PlayerDataSerializer.serializeComponent("NONEXISTENT", data);
        assertNull(bytes, "Unknown component should serialize to null");
    }

    @Test
    void testDeserializeEmptyBytesFailsClosed() {
        PlayerData target = new PlayerData();
        target.setHealth(10.0);

        assertThrows(IOException.class,
            () -> PlayerDataSerializer.deserializeComponent("VITALS", new byte[0], target));
        assertEquals(10.0, target.getHealth(), 0.001);
    }

    @Test
    void testDeserializeMissingComponentRootFailsClosed() throws IOException {
        net.momirealms.sparrow.nbt.CompoundTag wrapper =
            net.momirealms.sparrow.nbt.NBT.createCompound();
        wrapper.put("FOOD", net.momirealms.sparrow.nbt.NBT.createCompound());
        byte[] bytes = net.momirealms.sparrow.nbt.NBT.toBytes(wrapper);

        assertThrows(IOException.class,
            () -> PlayerDataSerializer.deserializeComponent("VITALS", bytes, new PlayerData()));
    }

    @Test
    void testDeserializeMissingPresenceMarkerFailsClosed() throws IOException {
        net.momirealms.sparrow.nbt.CompoundTag wrapper =
            net.momirealms.sparrow.nbt.NBT.createCompound();
        net.momirealms.sparrow.nbt.CompoundTag inner =
            net.momirealms.sparrow.nbt.NBT.createCompound();
        inner.putDouble("health", 10.0);
        inner.putDouble("maxHealth", 20.0);
        wrapper.put("VITALS", inner);
        byte[] bytes = net.momirealms.sparrow.nbt.NBT.toBytes(wrapper);

        assertThrows(IOException.class,
            () -> PlayerDataSerializer.deserializeComponent("VITALS", bytes, new PlayerData()));
    }

    @Test
    void testComponentIsolation() throws IOException {
        PlayerData original = new PlayerData();
        original.setHealth(15.0);
        original.setFoodLevel(20);

        byte[] vitalsBytes = PlayerDataSerializer.serializeComponent("VITALS", original);
        byte[] foodBytes = PlayerDataSerializer.serializeComponent("FOOD", original);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("VITALS", vitalsBytes, target);

        assertEquals(15.0, target.getHealth(), 0.001);
        assertEquals(0, target.getFoodLevel(), "Food should not be set by VITALS deserialization");

        PlayerDataSerializer.deserializeComponent("FOOD", foodBytes, target);
        assertEquals(20, target.getFoodLevel());
        assertEquals(15.0, target.getHealth(), 0.001);
    }

    @Test
    void testStatisticsRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        Map<String, Map<String, Integer>> stats = new HashMap<>();
        Map<String, Integer> unt = new HashMap<>();
        unt.put("JUMP", 42);
        unt.put("WALK_ONE_CM", 12345);
        stats.put("UNtyped", unt);
        original.setStatistics(stats);

        byte[] bytes = PlayerDataSerializer.serializeComponent("STATISTICS", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("STATISTICS", bytes, target);

        assertNotNull(target.getStatistics());
        assertEquals(42, target.getStatistics().get("UNtyped").get("JUMP"));
        assertEquals(12345, target.getStatistics().get("UNtyped").get("WALK_ONE_CM"));
    }

    @Test
    void testAdvancementsRoundTrip() throws IOException {
        PlayerData original = new PlayerData();
        Map<String, Map<String, Long>> advs = new HashMap<>();
        Map<String, Long> crit = new HashMap<>();
        crit.put("got_diamond", 1700000000000L);
        advs.put("minecraft:story/mine_diamond", crit);
        original.setAdvancements(advs);

        byte[] bytes = PlayerDataSerializer.serializeComponent("ADVANCEMENTS", original);
        assertNotNull(bytes);

        PlayerData target = new PlayerData();
        PlayerDataSerializer.deserializeComponent("ADVANCEMENTS", bytes, target);

        assertNotNull(target.getAdvancements());
        assertEquals(1700000000000L, target.getAdvancements().get("minecraft:story/mine_diamond").get("got_diamond"));
    }

    @Test
    void testEmptyPotionEffectsClearsTarget() throws IOException {
        // Phase 3: empty POTION_EFFECTS payload must clear target effects.
        // This prevents a player who lost all potion effects from getting
        // the old effects back on next login.
        PlayerData empty = new PlayerData();
        // Ensure potion effects is empty (default)
        byte[] bytes = PlayerDataSerializer.serializeComponent("POTION_EFFECTS", empty);
        assertNotNull(bytes, "Empty POTION_EFFECTS should produce a _present payload");

        // Set up a target with old effects
        PlayerData target = new PlayerData();
        java.util.List<PlayerData.PotionEffectData> oldEffects = new java.util.ArrayList<>();
        oldEffects.add(new PlayerData.PotionEffectData("minecraft:speed", 200, 1, false, true, true));
        target.setPotionEffects(oldEffects);

        // Deserialize the empty payload — should clear the old effects
        PlayerDataSerializer.deserializeComponent("POTION_EFFECTS", bytes, target);
        assertNotNull(target.getPotionEffects());
        assertTrue(target.getPotionEffects().isEmpty(),
            "Empty POTION_EFFECTS payload should clear old effects");
    }
}
