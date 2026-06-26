package com.fastsync.serialization;

import org.bukkit.Bukkit;
import org.bukkit.inventory.ItemStack;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ItemStack serialization compatibility layer.
 *
 * Primary path (Paper 1.20.5+):
 *   Uses ItemStack.serializeAsBytes() / deserializeBytes() - native NBT byte[].
 *
 * Fallback path (Paper < 1.20.5):
 *   Uses Bukkit's serialization via BukkitObjectOutputStream/ObjectInputStream,
 *   but wraps the result so it's still stored as byte[] (NOT string/base64).
 *   This avoids the performance-killing base64 string encoding that plagues
 *   other sync plugins, even on lower versions.
 *
 * Key principle from community discussion:
 *   "低版本也可以走nbt序列化啊，只是别变成string" (Low versions can also use NBT
 *   serialization, just don't convert to string)
 */
public class ItemStackCompat {

    private static final Logger logger = Logger.getLogger("FastSync");

    // Format header bytes — prepended to every serialized ItemStack payload.
    // This eliminates exception-based format detection on deserialization.
    // Legacy data (without header) is handled by checking if the first byte
    // matches a known format marker; if not, both paths are tried.
    public static final byte FORMAT_PAPER_NATIVE = 1;
    public static final byte FORMAT_BUKKIT_OBJECT = 2;

    // Reflection cache for Paper API methods
    private static Boolean paperNativeAvailable = null;
    private static Method serializeAsBytesMethod = null;
    private static Method deserializeBytesMethod = null;

    // Reflection cache for NMS-based NBT serialization (fallback)
    private static Boolean nmsNbtAvailable = null;

    private ItemStackCompat() {}

    /**
     * Check if the Paper native NBT byte[] serialization API is available.
     */
    public static boolean isPaperNativeAvailable() {
        if (paperNativeAvailable != null) {
            return paperNativeAvailable;
        }
        try {
            serializeAsBytesMethod = ItemStack.class.getMethod("serializeAsBytes");
            deserializeBytesMethod = ItemStack.class.getMethod("deserializeBytes", byte[].class);
            paperNativeAvailable = true;
            logger.info("[ItemStackCompat] Paper native NBT serialization available (serializeAsBytes).");
        } catch (NoSuchMethodException e) {
            paperNativeAvailable = false;
            logger.warning("[ItemStackCompat] Paper native API not found, using fallback serialization.");
            logger.warning("[ItemStackCompat] For best performance, upgrade to Paper 1.20.5+.");
        }
        return paperNativeAvailable;
    }

    /**
     * Serialize an ItemStack to byte[].
     *
     * Uses Paper's native NBT serialization when available.
     * Falls back to Bukkit object serialization (still byte[], NOT string).
     *
     * Format: [1 byte: format marker][payload bytes]
     *   FORMAT_PAPER_NATIVE (1)  → Paper serializeAsBytes() output
     *   FORMAT_BUKKIT_OBJECT (2) → BukkitObjectOutputStream output
     *
     * @param item the ItemStack to serialize (null returns empty array)
     * @return serialized byte[] with format header
     */
    public static byte[] serialize(ItemStack item) {
        if (item == null || item.getType().isAir()) {
            return new byte[0];
        }

        if (isPaperNativeAvailable()) {
            try {
                byte[] raw = (byte[]) serializeAsBytesMethod.invoke(item);
                return prependHeader(FORMAT_PAPER_NATIVE, raw);
            } catch (Exception e) {
                logger.log(Level.WARNING, "[ItemStackCompat] Native serialize failed, falling back", e);
            }
        }

        // Fallback: Bukkit object serialization (byte[], not string)
        byte[] raw = serializeBukkit(item);
        return prependHeader(FORMAT_BUKKIT_OBJECT, raw);
    }

    /**
     * Deserialize an ItemStack from byte[].
     *
     * Reads the format header byte to determine the serialization format,
     * then dispatches to the correct deserializer — no exception-based guessing.
     *
     * For legacy data without a format header (first byte is not 1 or 2),
     * falls back to trying both paths.
     *
     * @param bytes the serialized data (empty array returns null)
     * @return deserialized ItemStack, or null on failure
     */
    public static ItemStack deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        byte firstByte = bytes[0];

        if (firstByte == FORMAT_PAPER_NATIVE) {
            // Paper native format — strip header and deserialize
            byte[] raw = Arrays.copyOfRange(bytes, 1, bytes.length);
            return deserializePaperNative(raw);
        } else if (firstByte == FORMAT_BUKKIT_OBJECT) {
            // Bukkit object format — strip header and deserialize
            byte[] raw = Arrays.copyOfRange(bytes, 1, bytes.length);
            return deserializeBukkit(raw);
        } else {
            // Legacy data without format header — try both paths.
            // Paper's NBT binary starts with 0x0A (compound tag);
            // Bukkit's ObjectOutputStream starts with 0xAC (-84).
            // Neither starts with 1 or 2, so false positives are impossible.
            if (isPaperNativeAvailable()) {
                try {
                    return (ItemStack) deserializeBytesMethod.invoke(null, (Object) bytes);
                } catch (Exception e) {
                    logger.log(Level.FINE, "[ItemStackCompat] Legacy native deserialize failed, trying Bukkit", e);
                }
            }
            return deserializeBukkit(bytes);
        }
    }

    /** Prepend a 1-byte format header to the payload. */
    private static byte[] prependHeader(byte format, byte[] payload) {
        byte[] result = new byte[1 + payload.length];
        result[0] = format;
        System.arraycopy(payload, 0, result, 1, payload.length);
        return result;
    }

    /** Deserialize using Paper's native API (no fallback). */
    private static ItemStack deserializePaperNative(byte[] raw) {
        if (isPaperNativeAvailable()) {
            try {
                return (ItemStack) deserializeBytesMethod.invoke(null, (Object) raw);
            } catch (Exception e) {
                logger.log(Level.WARNING, "[ItemStackCompat] Paper native deserialize failed", e);
            }
        }
        return null;
    }

    /**
     * Check if the NMS-based NBT serialization is available (for potential
     * even-lower-level fallback that still writes NBT binary, not string).
     */
    public static boolean isNmsNbtAvailable() {
        if (nmsNbtAvailable != null) {
            return nmsNbtAvailable;
        }
        try {
            // Try to find the NMS ItemStack save method via reflection
            // This is version-dependent, so we just check if the class exists
            String nmsVersion = getNmsVersion();
            if (nmsVersion != null) {
                Class<?> nmsItemStackClass = Class.forName(
                    "net.minecraft.world.item.ItemStack");
                Class<?> nbtTagCompoundClass = Class.forName(
                    "net.minecraft.nbt.NBTTagCompound");
                nmsNbtAvailable = true;
                logger.info("[ItemStackCompat] NMS NBT classes detected (version: " + nmsVersion + ").");
            } else {
                nmsNbtAvailable = false;
            }
        } catch (Exception e) {
            nmsNbtAvailable = false;
        }
        return nmsNbtAvailable;
    }

    /**
     * Get the NMS version string (e.g., "v1_20_R3") for reflection.
     */
    private static String getNmsVersion() {
        try {
            String pkg = Bukkit.getServer().getClass().getPackage().getName();
            return pkg.substring(pkg.lastIndexOf('.') + 1);
        } catch (Exception e) {
            return null;
        }
    }

    // ==================== Bukkit Fallback Serialization ====================

    /**
     * Serialize using Bukkit's object serialization.
     *
     * This still produces byte[] (via ObjectOutputStream), NOT string.
     * The result is larger than NBT but still avoids base64 overhead.
     */
    private static byte[] serializeBukkit(ItemStack item) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            org.bukkit.util.io.BukkitObjectOutputStream oos =
                new org.bukkit.util.io.BukkitObjectOutputStream(baos);
            oos.writeObject(item);
            oos.flush();
            oos.close();
            return baos.toByteArray();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ItemStackCompat] Bukkit serialize failed", e);
            return new byte[0];
        }
    }

    /**
     * Deserialize using Bukkit's object serialization.
     */
    private static ItemStack deserializeBukkit(byte[] bytes) {
        try {
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(bytes);
            org.bukkit.util.io.BukkitObjectInputStream ois =
                new org.bukkit.util.io.BukkitObjectInputStream(bais);
            ItemStack item = (ItemStack) ois.readObject();
            ois.close();
            return item;
        } catch (Exception e) {
            logger.log(Level.WARNING, "[ItemStackCompat] Bukkit deserialize failed", e);
            return null;
        }
    }
}
