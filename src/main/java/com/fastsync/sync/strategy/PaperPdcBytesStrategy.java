package com.fastsync.sync.strategy;

import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataContainer;
import java.lang.reflect.Method;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * PDC sync via Paper's public serializeToBytes() / readFromBytes() API.
 *
 * <p>These methods were promoted from package-private to public via the
 * {@code PersistentDataContainerView} interface in Paper 1.21.11.
 *
 * <p>This strategy uses reflection so it compiles against older Paper APIs
 * (e.g., 1.21.8) while still calling the fast, official public API when
 * running on a Paper version that exposes it. If the methods are not found
 * at runtime, the strategy silently returns {@code null} / no-ops (graceful
 * degradation — the plugin still works, PDC is just not synced in that mode).
 */
public class PaperPdcBytesStrategy implements PdcSyncStrategy {

    private final Logger logger;
    private final boolean debug;

    // Cached reflected methods — resolved once at construction time.
    // Null means the method was not found on this server version.
    private final Method serializeMethod;
    private final Method readFromBytesMethod; // New public API: readFromBytes(byte[], boolean)
    private final Method deserializeBytesMethod; // Legacy fallback: deserializeBytes(byte[])

    public PaperPdcBytesStrategy(Logger logger, boolean debug) {
        this.logger = logger;
        this.debug = debug;

        Method ser = null;
        Method readNew = null;
        Method readOld = null;
        // Tracks whether methods were resolved via getDeclaredMethod (fallback path).
        // Only those need setAccessible(true); methods from getMethod() are public.
        boolean usedDeclaredFallback = false;
        try {
            // Try PersistentDataContainerView first (public API in 1.21.11+)
            Class<?> viewClass = null;
            try {
                viewClass = Class.forName("io.papermc.paper.persistence.PersistentDataContainerView");
            } catch (ClassNotFoundException ignored) {
                // Older Paper — interface doesn't exist yet
            }

            if (viewClass != null) {
                // Public API: serializeToBytes() on the view interface
                ser = viewClass.getMethod("serializeToBytes");
                // Public API: readFromBytes(byte[], boolean clear)
                readNew = viewClass.getMethod("readFromBytes", byte[].class, boolean.class);
            }

            // Fallback: look for package-private methods on the implementation class.
            if (ser == null) {
                Class<?> craftPdcClass = null;
                try {
                    craftPdcClass = Class.forName("org.bukkit.craftbukkit.persistence.CraftPersistentDataContainer");
                } catch (ClassNotFoundException ignored) {
                    // Could not locate implementation class
                }
                if (craftPdcClass != null) {
                    usedDeclaredFallback = true;
                    ser = findDeclaredMethod(craftPdcClass, "serializeToBytes");
                    readOld = findDeclaredMethod(craftPdcClass, "deserializeBytes", byte[].class);
                    if (readOld == null) {
                        readOld = findDeclaredMethod(craftPdcClass, "readFromBytes", byte[].class, boolean.class);
                        if (readOld != null) {
                            readNew = readOld;
                            readOld = null;
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] Failed to resolve Paper PDC bytes methods", e);
        }

        this.serializeMethod = ser;
        this.readFromBytesMethod = readNew;
        this.deserializeBytesMethod = readOld;

        // Only call setAccessible on methods resolved via getDeclaredMethod (the
        // fallback path). Methods from getMethod() are already public, so
        // setAccessible is unnecessary and triggers unnecessary security checks.
        if (usedDeclaredFallback) {
            if (ser != null) {
                ser.setAccessible(true);
            }
            if (readNew != null) {
                readNew.setAccessible(true);
            }
            if (readOld != null) {
                readOld.setAccessible(true);
            }
        }

        if (debug) {
            if (ser != null && (readNew != null || readOld != null)) {
                logger.fine("[PDC] Paper bytes strategy ready (serialize=" + ser.getName()
                    + ", read=" + (readNew != null ? "readFromBytes" : "deserializeBytes") + ")");
            } else {
                logger.fine("[PDC] Paper bytes methods not found on this server version; PDC sync will be skipped in safe-all-paper mode.");
            }
        }
    }

    @Override
    public byte[] dump(Player player) {
        if (serializeMethod == null) return null;
        try {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            if (pdc == null || pdc.isEmpty()) return null;
            byte[] bytes = (byte[]) serializeMethod.invoke(pdc);
            return (bytes != null && bytes.length > 0) ? bytes : null;
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] serializeToBytes failed: " + e.getMessage(), e);
            return null;
        }
    }

    @Override
    public void restore(Player player, byte[] data) {
        if (data == null || data.length == 0) return;
        Method readMethod = readFromBytesMethod != null ? readFromBytesMethod : deserializeBytesMethod;
        if (readMethod == null) return;
        try {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            if (readFromBytesMethod != null) {
                // New public API: readFromBytes(byte[], boolean clear)
                // clear=false means append/overwrite existing keys without wiping the container.
                readMethod.invoke(pdc, data, false);
            } else {
                // Legacy: deserializeBytes(byte[]) — overwrites existing keys (no clear param)
                readMethod.invoke(pdc, data);
            }
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] readFromBytes/deserializeBytes failed: " + e.getMessage(), e);
        }
    }

    /**
     * Walk the class hierarchy (including superclasses) to find a declared
     * method by name, even if it is package-private or protected.
     */
    private static Method findDeclaredMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
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

    @Override public boolean isSafe() { return true; }
    @Override public String strategyName() { return "safe-all-paper"; }
}
