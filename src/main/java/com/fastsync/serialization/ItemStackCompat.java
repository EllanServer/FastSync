package com.fastsync.serialization;

import org.bukkit.inventory.ItemStack;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ItemStack serialization using Paper 1.21.11+ native NBT byte[] API.
 *
 * <p>Uses {@code ItemStack.serializeAsBytes()} / {@code ItemStack.deserializeBytes()}
 * directly — no reflection, no Bukkit object serialization fallback, no NMS detection.
 * Target: Paper 1.21.11 (api-version in plugin.yml).
 */
public class ItemStackCompat {

    private static final Logger logger = Logger.getLogger("FastSync");

    private ItemStackCompat() {}

    /**
     * Check if the Paper native NBT byte[] serialization API is available.
     * On Paper 1.21.11+ this is always true.
     */
    public static boolean isPaperNativeAvailable() {
        return true;
    }

    /**
     * Serialize an ItemStack to byte[].
     *
     * @param item the ItemStack to serialize (null returns empty array)
     * @return serialized byte[]
     */
    public static byte[] serialize(ItemStack item) {
        if (item == null || item.getType().isAir()) {
            return new byte[0];
        }
        try {
            return item.serializeAsBytes();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ItemStackCompat] serializeAsBytes failed: " + e.getMessage(), e);
            return new byte[0];
        }
    }

    /**
     * Deserialize an ItemStack from byte[].
     */
    public static ItemStack deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            return ItemStack.deserializeBytes(bytes);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ItemStackCompat] deserializeBytes failed: " + e.getMessage(), e);
            return null;
        }
    }
}
