package com.fastsync.sync.strategy;

import io.papermc.paper.persistence.PersistentDataContainerView;
import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataContainer;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * PDC sync via Paper 1.21.11-26.2 public serializeToBytes() / readFromBytes() API.
 *
 * <p>These methods are on the {@link PersistentDataContainerView} interface,
 * promoted to public in Paper 1.21.11. No reflection, no CraftBukkit internals.
 * If running on an older Paper that does not have these methods, the plugin
 * will fail at class-load time with a NoSuchMethodError — by design, since the
 * minimum target version is Paper 1.21.11.
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
            logger.log(debug ? Level.WARNING : Level.SEVERE,
                "[PDC] serializeToBytes failed; refusing to save a stale/empty PDC", e);
            throw new IllegalStateException("Failed to serialize player PDC", e);
        }
    }

    @Override
    public void restore(Player player, byte[] data) {
        if (data == null) return;
        PersistentDataContainer pdc = player.getPersistentDataContainer();

        // serializeToBytes() writes a valid NBT compound even for an empty PDC;
        // zero bytes can therefore only be malformed input.
        if (data.length == 0) {
            throw new IllegalArgumentException("PDC payload is empty");
        }

        try {
            // CraftPersistentDataContainer clears itself BEFORE parsing when
            // readFromBytes(data, true) is used. Decode into a temporary public-
            // API container first so malformed NBT cannot wipe live player data.
            PersistentDataContainer decoded =
                pdc.getAdapterContext().newPersistentDataContainer();
            decoded.readFromBytes(data, true);
            if (clearBeforeRestore) {
                clearContainer(pdc);
            }
            decoded.copyTo(pdc, true);
        } catch (Exception e) {
            logger.log(debug ? Level.WARNING : Level.SEVERE,
                "[PDC] readFromBytes failed before live-container mutation", e);
            throw new IllegalStateException("Failed to restore player PDC", e);
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
