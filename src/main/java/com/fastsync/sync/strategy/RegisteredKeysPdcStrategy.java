package com.fastsync.sync.strategy;

import org.bukkit.NamespacedKey;
import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataContainer;
import org.bukkit.persistence.PersistentDataType;
import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * PDC sync for explicitly registered keys only.
 *
 * <p>Production-recommended: only keys that are explicitly registered in
 * config will be synced. This avoids accidentally syncing plugin-internal
 * data that shouldn't cross servers.
 *
 * <p>Serialization format is pure binary — NO Base64, NO String.valueOf,
 * NO intermediate string conversion. Each value is written using the
 * DataOutputStream method matching its PersistentDataType:
 * <ul>
 *   <li>BYTE → writeByte</li>
 *   <li>SHORT → writeShort</li>
 *   <li>INTEGER → writeInt</li>
 *   <li>LONG → writeLong</li>
 *   <li>FLOAT → writeFloat</li>
 *   <li>DOUBLE → writeDouble</li>
 *   <li>BOOLEAN → writeBoolean</li>
 *   <li>STRING → writeUTF</li>
 *   <li>BYTE_ARRAY → writeInt(length) + write(raw bytes)</li>
 * </ul>
 *
 * <pre>
 * Format:
 *   [count(int)]
 *   for each entry:
 *     [keyUTF]
 *     [typeId(byte)]
 *     [value (type-specific binary)]
 * </pre>
 */
public class RegisteredKeysPdcStrategy implements PdcSyncStrategy {

    private final Map<NamespacedKey, PersistentDataType<?, ?>> registeredKeys;
    private final Logger logger;
    private final boolean debug;

    // Type IDs for self-describing binary format
    private static final byte TYPE_BYTE = 1;
    private static final byte TYPE_SHORT = 2;
    private static final byte TYPE_INTEGER = 3;
    private static final byte TYPE_LONG = 4;
    private static final byte TYPE_FLOAT = 5;
    private static final byte TYPE_DOUBLE = 6;
    private static final byte TYPE_BOOLEAN = 7;
    private static final byte TYPE_STRING = 8;
    private static final byte TYPE_BYTE_ARRAY = 9;

    /** Max allowed BYTE_ARRAY length in PDC values — guards against corrupted data. */
    private static final int MAX_PDC_VALUE_BYTES = 1024 * 1024; // 1MB

    /**
     * Upper bound on the entry count read from the restore payload. A corrupted
     * payload could declare a huge count and make the loop spin reading until
     * EOF; bounding it turns that into a deterministic rejection (the count is
     * also bounded by the number of registered keys, so this is a safety cap
     * rather than a functional limit).
     */
    private static final int MAX_PDC_ENTRIES = 256;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public RegisteredKeysPdcStrategy(List<KeyBinding> keys, Logger logger, boolean debug) {
        this.logger = logger;
        this.debug = debug;
        this.registeredKeys = new HashMap<>();
        for (KeyBinding b : keys) {
            this.registeredKeys.put(b.key, b.type);
        }
    }

    public record KeyBinding(NamespacedKey key, PersistentDataType<?, ?> type) {}

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public byte[] dump(Player player) {
        PersistentDataContainer pdc = player.getPersistentDataContainer();

        try (var baos = new ByteArrayOutputStream();
             var out = new DataOutputStream(baos)) {
            // Collect non-null entries first (need count before writing)
            var entries = new ArrayList<Map.Entry<NamespacedKey, Object>>();
            if (pdc != null) {
                for (var entry : registeredKeys.entrySet()) {
                    if (pdc.has(entry.getKey(), (PersistentDataType) entry.getValue())) {
                        Object val = pdc.get(entry.getKey(), (PersistentDataType) entry.getValue());
                        if (val != null) {
                            entries.add(Map.entry(entry.getKey(), val));
                        }
                    }
                }
            }
            // IMPORTANT: Even when entries is empty, return a valid "0 entries"
            // payload (not null). This ensures restore() is called, which clears
            // all registered keys on the target server — preventing ghost keys
            // that were deleted on the source server from persisting.
            out.writeInt(entries.size());
            for (var entry : entries) {
                out.writeUTF(entry.getKey().toString());
                PersistentDataType type = registeredKeys.get(entry.getKey());
                byte typeId = typeIdOf(type);
                out.writeByte(typeId);
                writeValue(out, entry.getValue(), typeId);
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            logger.log(debug ? Level.WARNING : Level.SEVERE,
                "[PDC] registered dump failed; refusing to save stale PDC state", e);
            throw new IllegalStateException("Failed to serialize registered PDC keys", e);
        }
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void restore(Player player, byte[] data) {
        if (data == null) return;
        if (data.length == 0) {
            throw new IllegalArgumentException("Registered PDC payload is empty");
        }

        // Decode and validate the complete payload before mutating the player's
        // container. A truncated entry or type mismatch must not clear valid
        // target data and leave only a partially restored prefix.
        Map<NamespacedKey, DecodedValue> decoded = new HashMap<>();
        try (var bais = new ByteArrayInputStream(data);
             var in = new DataInputStream(bais)) {
            int count = in.readInt();
            if (count < 0 || count > MAX_PDC_ENTRIES || count > registeredKeys.size()) {
                throw new IOException("entry count " + count + " out of bounds");
            }
            for (int i = 0; i < count; i++) {
                String keyStr = in.readUTF();
                byte typeId = in.readByte();
                Object value = readValue(in, typeId);
                if (value == null) {
                    throw new IOException("unknown type id " + typeId + " for " + keyStr);
                }

                NamespacedKey key = NamespacedKey.fromString(keyStr);
                PersistentDataType type = key != null ? registeredKeys.get(key) : null;
                if (type == null) {
                    throw new IOException("unregistered PDC key " + keyStr);
                }
                if (typeIdOf(type) != typeId) {
                    throw new IOException("type mismatch for " + keyStr);
                }
                if (decoded.put(key, new DecodedValue(type, value)) != null) {
                    throw new IOException("duplicate PDC key " + keyStr);
                }
            }
            if (in.available() != 0) {
                throw new IOException("trailing bytes after PDC payload");
            }
        } catch (IOException e) {
            logger.log(debug ? Level.WARNING : Level.SEVERE,
                "[PDC] registered restore rejected before mutation: " + e.getMessage(), e);
            throw new IllegalArgumentException("Invalid registered PDC payload", e);
        }

        // The complete payload is valid; apply it with full-replace semantics.
        PersistentDataContainer pdc = player.getPersistentDataContainer();
        Map<NamespacedKey, DecodedValue> previous = new HashMap<>();
        for (Map.Entry<NamespacedKey, PersistentDataType<?, ?>> entry : registeredKeys.entrySet()) {
            PersistentDataType type = entry.getValue();
            Object value = pdc.get(entry.getKey(), type);
            if (value != null) {
                previous.put(entry.getKey(), new DecodedValue(type, value));
            }
        }
        try {
            for (NamespacedKey registeredKey : registeredKeys.keySet()) {
                pdc.remove(registeredKey);
            }
            for (Map.Entry<NamespacedKey, DecodedValue> entry : decoded.entrySet()) {
                DecodedValue value = entry.getValue();
                pdc.set(entry.getKey(), value.type(), value.value());
            }
        } catch (RuntimeException applyFailure) {
            // Best-effort rollback: a custom PersistentDataType implementation
            // must not leave a half-applied registered-key set.
            try {
                for (NamespacedKey registeredKey : registeredKeys.keySet()) {
                    pdc.remove(registeredKey);
                }
                for (Map.Entry<NamespacedKey, DecodedValue> entry : previous.entrySet()) {
                    DecodedValue value = entry.getValue();
                    pdc.set(entry.getKey(), value.type(), value.value());
                }
            } catch (RuntimeException rollbackFailure) {
                applyFailure.addSuppressed(rollbackFailure);
            }
            throw applyFailure;
        }
    }

    @SuppressWarnings("rawtypes")
    private record DecodedValue(PersistentDataType type, Object value) {}

    // ==================== Binary Type Dispatch ====================

    private static byte typeIdOf(PersistentDataType<?, ?> type) {
        if (type == PersistentDataType.BYTE) return TYPE_BYTE;
        if (type == PersistentDataType.SHORT) return TYPE_SHORT;
        if (type == PersistentDataType.INTEGER) return TYPE_INTEGER;
        if (type == PersistentDataType.LONG) return TYPE_LONG;
        if (type == PersistentDataType.FLOAT) return TYPE_FLOAT;
        if (type == PersistentDataType.DOUBLE) return TYPE_DOUBLE;
        if (type == PersistentDataType.BOOLEAN) return TYPE_BOOLEAN;
        if (type == PersistentDataType.STRING) return TYPE_STRING;
        if (type == PersistentDataType.BYTE_ARRAY) return TYPE_BYTE_ARRAY;
        return 0; // unknown
    }

    private static void writeValue(DataOutputStream out, Object value, byte typeId) throws IOException {
        switch (typeId) {
            case TYPE_BYTE -> out.writeByte((Byte) value);
            case TYPE_SHORT -> out.writeShort((Short) value);
            case TYPE_INTEGER -> out.writeInt((Integer) value);
            case TYPE_LONG -> out.writeLong((Long) value);
            case TYPE_FLOAT -> out.writeFloat((Float) value);
            case TYPE_DOUBLE -> out.writeDouble((Double) value);
            case TYPE_BOOLEAN -> out.writeBoolean((Boolean) value);
            case TYPE_STRING -> out.writeUTF((String) value);
            case TYPE_BYTE_ARRAY -> {
                byte[] bytes = (byte[]) value;
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            default -> throw new IOException("Unsupported type id: " + typeId);
        }
    }

    private static Object readValue(DataInputStream in, byte typeId) throws IOException {
        return switch (typeId) {
            case TYPE_BYTE -> in.readByte();
            case TYPE_SHORT -> in.readShort();
            case TYPE_INTEGER -> in.readInt();
            case TYPE_LONG -> in.readLong();
            case TYPE_FLOAT -> in.readFloat();
            case TYPE_DOUBLE -> in.readDouble();
            case TYPE_BOOLEAN -> in.readBoolean();
            case TYPE_STRING -> in.readUTF();
            case TYPE_BYTE_ARRAY -> {
                int len = in.readInt();
                if (len < 0 || len > MAX_PDC_VALUE_BYTES) {
                    throw new IOException("BYTE_ARRAY length " + len + " out of bounds (max " + MAX_PDC_VALUE_BYTES + ")");
                }
                byte[] bytes = new byte[len];
                in.readFully(bytes);
                yield bytes;
            }
            default -> null; // unknown type, skip
        };
    }

    @Override public boolean isSafe() { return true; }
    @Override public String strategyName() { return "registered-only"; }
}
