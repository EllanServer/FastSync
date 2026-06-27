package com.fastsync.serialization;

import com.fastsync.data.PlayerData;
import net.momirealms.sparrow.nbt.CompoundTag;
import net.momirealms.sparrow.nbt.NBT;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clean-slate regression tests: payloads missing the presence flags that the
 * current serializer ALWAYS writes must be rejected as
 * unsupported/legacy/corrupt, rather than silently preserving a stale
 * baseline default.
 *
 * <p>Covers two directives from the round-3 production-hardening review:
 * <ul>
 *   <li>Old full-Blob payload missing {@code potionEffectsPresent} → rejected.</li>
 *   <li>Old INVENTORY component missing {@code offhandPresent} → rejected.</li>
 * </ul>
 *
 * <p>These are pure serialization tests — no Paper runtime or DB required. The
 * crafted payloads skip every ItemStack branch (no {@code inventory}/{@code armor}
 * list tags), so they exercise only the presence-flag guards.
 */
class PlayerDataSerializerLegacyRejectionTest {

    /**
     * A full-Blob payload produced without the {@code potionEffectsPresent} flag
     * (e.g. by an older serializer that only wrote the tag when effects existed)
     * must be rejected on deserialize. Silent acceptance would leave
     * {@code data.potionEffects == null} ("not collected"), causing the apply
     * path to skip clearing target effects — a stale-effect rollback.
     */
    @Test
    void deserializeRejectsPayloadMissingPotionEffectsPresent() {
        // Empty root compound: every field defaults, no ItemStack tags, and
        // critically no potionEffectsPresent flag.
        CompoundTag root = NBT.createCompound();
        byte[] bytes = NBT.toBytes(root);

        IOException ex = assertThrows(IOException.class,
            () -> PlayerDataSerializer.deserialize(bytes),
            "Missing potionEffectsPresent flag must be rejected as an "
                + "unsupported/legacy payload");
        assertTrue(ex.getMessage().contains("potionEffectsPresent"),
            "Exception should name the missing flag: " + ex.getMessage());
    }

    /**
     * An INVENTORY component payload missing the {@code offhandPresent} flag
     * must be rejected. Silent acceptance would leave {@code data.offhand}
     * untouched, so the baseline Blob's stale offhand item would persist on
     * the target server even though the source cleared it.
     *
     * <p>The crafted component has no {@code inventory}/{@code armor} list, so
     * no ItemStack deserialization runs — only the offhandPresent guard fires.
     */
    @Test
    void deserializeComponentRejectsInventoryMissingOffhandPresent() {
        // Root: { "INVENTORY": { } } — empty component with no offhandPresent.
        CompoundTag root = NBT.createCompound();
        CompoundTag inventoryComponent = NBT.createCompound();
        root.put("INVENTORY", inventoryComponent);
        byte[] bytes = NBT.toBytes(root);

        IOException ex = assertThrows(IOException.class,
            () -> PlayerDataSerializer.deserializeComponent(
                "INVENTORY", bytes, new PlayerData()),
            "Missing offhandPresent flag must be rejected as an "
                + "unsupported/legacy component payload");
        assertTrue(ex.getMessage().contains("offhandPresent"),
            "Exception should name the missing flag: " + ex.getMessage());
    }
}
