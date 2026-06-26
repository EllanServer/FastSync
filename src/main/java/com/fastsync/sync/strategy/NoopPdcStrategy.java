package com.fastsync.sync.strategy;

import org.bukkit.entity.Player;

/** PDC sync disabled — no-op. */
public class NoopPdcStrategy implements PdcSyncStrategy {
    @Override public byte[] dump(Player p) { return null; }
    @Override public void restore(Player p, byte[] d) { }
    @Override public boolean isSafe() { return true; }
    @Override public String strategyName() { return "off"; }
}
