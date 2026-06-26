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
 */
public class RegisteredKeysPdcStrategy implements PdcSyncStrategy {
    
    private final Map<NamespacedKey, PersistentDataType<?, ?>> registeredKeys;
    private final Logger logger;
    private final boolean debug;
    
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
        if (pdc == null || pdc.isEmpty()) return null;
        
        try (var baos = new ByteArrayOutputStream();
             var out = new DataOutputStream(baos)) {
            int count = 0;
            // First pass: count non-null entries
            var entries = new ArrayList<Map.Entry<NamespacedKey, Object>>();
            for (var entry : registeredKeys.entrySet()) {
                if (pdc.has(entry.getKey(), (PersistentDataType) entry.getValue())) {
                    Object val = pdc.get(entry.getKey(), (PersistentDataType) entry.getValue());
                    if (val != null) {
                        entries.add(Map.entry(entry.getKey(), val));
                        count++;
                    }
                }
            }
            if (count == 0) return null;
            
            out.writeInt(count);
            for (var entry : entries) {
                String keyStr = entry.getKey().toString();
                out.writeUTF(keyStr);
                String valStr = String.valueOf(entry.getValue());
                out.writeUTF(valStr);
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            if (debug) logger.log(Level.FINE, "[PDC] registered dump failed: " + e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void restore(Player player, byte[] data) {
        if (data == null || data.length == 0) return;
        PersistentDataContainer pdc = player.getPersistentDataContainer();
        
        try (var bais = new ByteArrayInputStream(data);
             var in = new DataInputStream(bais)) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                String keyStr = in.readUTF();
                String valStr = in.readUTF();
                NamespacedKey key = NamespacedKey.fromString(keyStr);
                if (key == null) continue;
                PersistentDataType type = registeredKeys.get(key);
                if (type == null) continue;
                
                try {
                    Object val = deserializeValue(valStr, type);
                    if (val != null) {
                        pdc.set(key, type, val);
                    }
                } catch (Exception ignored) {}
            }
        } catch (IOException e) {
            if (debug) logger.log(Level.FINE, "[PDC] registered restore failed: " + e.getMessage(), e);
        }
    }
    
    @SuppressWarnings("unchecked")
    private Object deserializeValue(String valStr, PersistentDataType type) {
        String name = type.getComplexType().getSimpleName();
        return switch (name) {
            case "Integer" -> Integer.parseInt(valStr);
            case "String" -> valStr;
            case "Double" -> Double.parseDouble(valStr);
            case "Float" -> Float.parseFloat(valStr);
            case "Long" -> Long.parseLong(valStr);
            case "Short" -> Short.parseShort(valStr);
            case "Byte" -> Byte.parseByte(valStr);
            case "Boolean" -> Boolean.parseBoolean(valStr);
            default -> null; // Unsupported type
        };
    }
    
    @Override public boolean isSafe() { return true; }
    @Override public String strategyName() { return "registered-only"; }
}
