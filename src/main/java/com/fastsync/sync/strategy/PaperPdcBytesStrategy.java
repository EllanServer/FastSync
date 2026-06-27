package com.fastsync.sync.strategy;

import io.papermc.paper.persistence.PersistentDataContainerView;
import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataContainer;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * PDC sync via Paper 1.21.11+ public serializeToBytes() / readFromBytes() API.
 *
 * <p>These methods are on the {@link PersistentDataContainerView} interface,
 * promoted to public in Paper 1.21.11. No reflection, no CraftBukkit internals.
 * If running on an older Paper that does not have these methods, the plugin
 * will fail at class-load time with a NoSuchMethodError — by design, since the
 * target version is Paper 1.21.11.
 */
public class PaperPdcBytesStrategy implements PdcSyncStrategy {

    private final Logger logger;
    private final boolean debug;
    private final boolean clearBeforeRestore;

    public PaperPdcBytesStrategy(Logger logger, boolean debug, boolean clearBeforeRestore) {
        this.logger = logger;
        this.debug = debug;
        this.clearBeforeRestore = clearBeforeRestore;
    }

    @Override
    public byte[] dump(Player player) {
        try {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            if (pdc == null) return null;
            // Don't skip empty PDC — serializeToBytes() returns a valid empty
            // container representation that, when restored with clear=true,
            // wipes the target container. This prevents ghost keys.
            // Even if bytes.length == 0, return it so the caller knows PDC
            // sync was attempted and should call restore() to clear the target.
            return pdc.serializeToBytes();
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] serializeToBytes failed: " + e.getMessage(), e);
            return null;
        }
    }

    @Override
    public void restore(Player player, byte[] data) {
        if (data == null) return;
        PersistentDataContainer pdc = player.getPersistentDataContainer();

        // Empty payload means "source PDC is empty" — clear the target container
        // to remove ghost keys that were deleted on the source server.
        if (data.length == 0) {
            if (clearBeforeRestore) {
                clearContainer(pdc);
            }
            return;
        }

        try {
            // Public API: readFromBytes(byte[], boolean clear)
            // clear=true wipes the container before reading (full sync).
            // clear=false appends/overwrites existing keys (merge mode).
            pdc.readFromBytes(data, clearBeforeRestore);
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] readFromBytes failed: " + e.getMessage(), e);
        }
    }

    /**
     * Remove all keys from a PersistentDataContainer. Used when the source
     * server's PDC is empty and clearBeforeRestore is enabled — ensures
     * ghost keys don't persist on the target server.
     */
    private static void clearContainer(PersistentDataContainer pdc) {
        if (pdc == null) return;
        for (org.bukkit.NamespacedKey key : new java.util.HashSet<>(pdc.getKeys())) {
            pdc.remove(key);
        }
    }

    @Override public boolean isSafe() { return true; }
    @Override public String strategyName() { return "safe-all-paper"; }
}
