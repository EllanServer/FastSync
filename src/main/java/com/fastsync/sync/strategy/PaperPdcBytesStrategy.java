package com.fastsync.sync.strategy;

import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataContainer;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * PDC sync via Paper's public serializeToBytes() / readFromBytes() API.
 * 
 * <p>Available since Paper 1.21.11 (the method was promoted from
 * package-private to public via PersistentDataContainerView interface).
 * This is the default strategy — SAFE, no reflection, official API.
 */
public class PaperPdcBytesStrategy implements PdcSyncStrategy {
    
    private final Logger logger;
    private final boolean debug;
    
    public PaperPdcBytesStrategy(Logger logger, boolean debug) {
        this.logger = logger;
        this.debug = debug;
    }
    
    @Override
    public byte[] dump(Player player) {
        try {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            if (pdc == null || pdc.isEmpty()) return null;
            // Paper 1.21.11+: serializeToBytes() is a public method on
            // io.papermc.paper.persistence.PersistentDataContainerView
            return pdc.serializeToBytes();
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] serializeToBytes failed: " + e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    public void restore(Player player, byte[] data) {
        if (data == null || data.length == 0) return;
        try {
            PersistentDataContainer pdc = player.getPersistentDataContainer();
            // readFromBytes(byte[], boolean clear): clear=false means append/overwrite
            // existing keys without wiping the container first.
            pdc.readFromBytes(data, false);
        } catch (Exception e) {
            if (debug) logger.log(Level.FINE, "[PDC] readFromBytes failed: " + e.getMessage(), e);
        }
    }
    
    @Override public boolean isSafe() { return true; }
    @Override public String strategyName() { return "safe-all-paper"; }
}
