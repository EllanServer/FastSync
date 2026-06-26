package com.fastsync.sync.strategy;

import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataContainer;
import java.lang.reflect.Method;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * PDC sync via reflection — UNSAFE fallback for older Paper versions
 * where serializeToBytes() is not a public API.
 * 
 * <p>This strategy requires explicit confirmation in config
 * (pdc.unsafe-reflection.enabled: true) to prevent accidental use.
 */
public class ReflectionNmsPdcStrategy implements PdcSyncStrategy {
    
    private final Logger logger;
    private final boolean debug;
    
    public ReflectionNmsPdcStrategy(Logger logger, boolean debug) {
        this.logger = logger;
        this.debug = debug;
    }
    
    @Override
    public byte[] dump(Player player) {
        try {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            if (pdc == null || pdc.isEmpty()) return null;
            Method serialize = findMethod(pdc.getClass(), "serializeToBytes");
            if (serialize == null) {
                if (debug) logger.fine("[PDC] serializeToBytes not found on " + pdc.getClass().getName());
                return null;
            }
            serialize.setAccessible(true);
            return (byte[]) serialize.invoke(pdc);
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] reflection dump failed: " + e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    public void restore(Player player, byte[] data) {
        if (data == null || data.length == 0) return;
        try {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            Method deserialize = findMethod(pdc.getClass(), "deserializeBytes", byte[].class);
            if (deserialize == null) {
                if (debug) logger.fine("[PDC] deserializeBytes not found on " + pdc.getClass().getName());
                return;
            }
            deserialize.setAccessible(true);
            deserialize.invoke(pdc, data);
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] reflection restore failed: " + e.getMessage(), e);
        }
    }
    
    private static Method findMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
        Class<?> c = clazz;
        while (c != null && c != Object.class) {
            try {
                return c.getDeclaredMethod(name, paramTypes);
            } catch (NoSuchMethodException ignored) {
                c = c.getSuperclass();
            }
        }
        return null;
    }
    
    @Override public boolean isSafe() { return false; }
    @Override public String strategyName() { return "unsafe-reflection"; }
}
