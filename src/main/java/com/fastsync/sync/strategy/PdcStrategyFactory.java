package com.fastsync.sync.strategy;

import com.fastsync.config.ConfigManager;
import org.bukkit.NamespacedKey;
import org.bukkit.persistence.PersistentDataType;
import java.util.*;
import java.util.logging.Logger;

/**
 * Factory that creates the appropriate PdcSyncStrategy based on config.
 */
public class PdcStrategyFactory {
    
    public static PdcSyncStrategy create(ConfigManager config, Logger logger) {
        String mode = config.getPdcMode();
        boolean debug = config.isDebug();
        
        if (mode == null || mode.isBlank() || "off".equalsIgnoreCase(mode)) {
            return new NoopPdcStrategy();
        }
        
        if ("safe-all-paper".equalsIgnoreCase(mode)) {
            boolean clearBeforeRestore = config.isPdcClearBeforeRestore();
            return new PaperPdcBytesStrategy(logger, debug, clearBeforeRestore);
        }
        
        if ("registered-only".equalsIgnoreCase(mode)) {
            List<RegisteredKeysPdcStrategy.KeyBinding> keys = parseRegisteredKeys(config, logger);
            // P2 (round 15): registered-only mode with an empty key list is
            // technically safe (NoopPdcStrategy would be the equivalent), but
            // operationally misleading — an operator who toggled sync-pdc=true
            // expects PDC to actually flow, and a silent no-op looks like a
            // broken feature. Warn loudly so the missing config is obvious.
            if (keys.isEmpty()) {
                logger.warning("[PDC] sync-pdc is enabled but pdc.registered-keys is empty; "
                    + "no PDC keys will be synchronized. Populate pdc.registered-keys "
                    + "with 'namespace:key=TYPE' entries, or set pdc.mode to 'off'.");
            }
            return new RegisteredKeysPdcStrategy(keys, logger, debug);
        }

        logger.warning("[PDC] Unknown mode '" + mode + "', falling back to off.");
        return new NoopPdcStrategy();
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static List<RegisteredKeysPdcStrategy.KeyBinding> parseRegisteredKeys(
            ConfigManager config, Logger logger) {
        List<String> rawKeys = config.getRegisteredPdcKeys();
        List<RegisteredKeysPdcStrategy.KeyBinding> result = new ArrayList<>();
        
        if (rawKeys == null || rawKeys.isEmpty()) return result;
        
        for (String entry : rawKeys) {
            // Format: "namespace:key=TYPE" e.g. "myplugin:progress=INTEGER"
            String[] parts = entry.split("=", 2);
            if (parts.length != 2) {
                logger.warning("[PDC] Invalid registered key format: '" + entry + "'. Expected 'namespace:key=TYPE'.");
                continue;
            }
            String keyStr = parts[0].trim();
            String typeStr = parts[1].trim().toUpperCase();
            
            NamespacedKey key = NamespacedKey.fromString(keyStr);
            if (key == null) {
                logger.warning("[PDC] Invalid NamespacedKey: '" + keyStr + "'");
                continue;
            }
            
            PersistentDataType type = lookupPersistentDataType(typeStr);
            if (type == null) {
                logger.warning("[PDC] Unknown PersistentDataType: '" + typeStr + "' for key " + keyStr);
                continue;
            }
            
            result.add(new RegisteredKeysPdcStrategy.KeyBinding(key, type));
        }
        
        return result;
    }
    
    @SuppressWarnings("rawtypes")
    private static PersistentDataType lookupPersistentDataType(String name) {
        return switch (name) {
            case "STRING" -> PersistentDataType.STRING;
            case "INTEGER" -> PersistentDataType.INTEGER;
            case "DOUBLE" -> PersistentDataType.DOUBLE;
            case "FLOAT" -> PersistentDataType.FLOAT;
            case "LONG" -> PersistentDataType.LONG;
            case "SHORT" -> PersistentDataType.SHORT;
            case "BYTE" -> PersistentDataType.BYTE;
            case "BOOLEAN" -> PersistentDataType.BOOLEAN;
            case "BYTE_ARRAY" -> PersistentDataType.BYTE_ARRAY;
            // List-typed PersistentDataTypes are intentionally not exposed
            // here: the registered-keys path targets stable scalar/array
            // types only. Add new scalar types explicitly when needed.
            default -> null;
        };
    }
}
