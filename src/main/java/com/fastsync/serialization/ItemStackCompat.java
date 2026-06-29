package com.fastsync.serialization;

import org.bukkit.inventory.ItemStack;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ItemStack serialization using Paper 1.21.11-26.2 native NBT byte[] API.
 *
 * <p>Uses {@code ItemStack.serializeAsBytes()} / {@code ItemStack.deserializeBytes()}
 * directly — no reflection, no Bukkit object serialization fallback, no NMS detection.
 * Runtime range: Paper 1.21.11 through 26.2 (api-version stays 1.21 so the
 * same Java 21 bytecode JAR loads at both ends).
 *
 * <p><b>Failure handling:</b> serialization / deserialization failures do
 * <em>not</em> return an empty array or {@code null}. Doing so would silently
 * turn an unserializable item into an air slot — a slot that fails to save
 * would be loaded back as empty on the next server, silently deleting the
 * player's item. Instead these methods throw {@link ItemSerializationException}
 * so the caller can fail the whole save or record a conflict snapshot.
 *
 * <p>The only legitimate "empty" result is for an actual air / null item, which
 * is a real inventory state and must round-trip as a zero-length byte array.
 */
public class ItemStackCompat {

    private static final Logger logger = Logger.getLogger("FastSync");

    private ItemStackCompat() {}

    /**
     * Serialize an ItemStack to byte[].
     *
     * @param item the ItemStack to serialize ({@code null} or air returns an
     *             empty array — a real, round-trippable air-slot representation)
     * @return serialized byte[] (empty for air / null)
     * @throws ItemSerializationException if {@code serializeAsBytes()} throws.
     *         The exception is propagated instead of being swallowed so the
     *         caller can fail the save rather than silently storing an air slot.
     */
    public static byte[] serialize(ItemStack item) {
        if (item == null || item.getType().isAir()) {
            return new byte[0];
        }
        try {
            return item.serializeAsBytes();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ItemStackCompat] serializeAsBytes failed for "
                + item.getType() + ": " + e.getMessage(), e);
            throw new ItemSerializationException(
                "Failed to serialize ItemStack (" + item.getType() + "): " + e.getMessage(), e);
        }
    }

    /**
     * Deserialize an ItemStack from byte[].
     *
     * @param bytes serialized bytes (empty / null returns {@code null} — an
     *              air slot; this is a real round-tripped state, not an error)
     * @return the deserialized ItemStack, or {@code null} for an empty payload
     * @throws ItemSerializationException if {@code deserializeBytes()} throws.
     *         Propagated so the caller can refuse to apply the corrupted
     *         component instead of silently dropping the item.
     */
    public static ItemStack deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            return ItemStack.deserializeBytes(bytes);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ItemStackCompat] deserializeBytes failed: " + e.getMessage(), e);
            throw new ItemSerializationException(
                "Failed to deserialize ItemStack: " + e.getMessage(), e);
        }
    }
}
