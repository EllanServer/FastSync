package com.fastsync.api;

import com.fastsync.FastSync;
import com.fastsync.sync.dirty.ComponentDirtyMask.Component;

import java.util.UUID;

/**
 * Public API for third-party plugins to mark player data as dirty.
 *
 * <p>Bukkit events do not cover all state mutations (e.g. direct API calls
 * like {@code player.getInventory().setItem()} may not fire an event).
 * Plugins that modify player state via API should call {@link #markDirty}
 * to ensure FastSync persists the change on the next periodic save.
 */
public final class FastSyncApi {

    private FastSyncApi() {}

    public static void markDirty(UUID uuid, Component component) {
        FastSync plugin = FastSync.getInstance();
        if (plugin == null || plugin.getSyncManager() == null) return;
        plugin.getSyncManager().markPlayerDirty(uuid, component);
    }

    public static void markDirty(UUID uuid, String componentName) {
        FastSync plugin = FastSync.getInstance();
        if (plugin == null || plugin.getSyncManager() == null) return;
        plugin.getSyncManager().markPlayerDirty(uuid, componentName);
    }

    public static void markInventoryDirty(UUID uuid) {
        markDirty(uuid, Component.INVENTORY);
    }

    public static void markEnderChestDirty(UUID uuid) {
        markDirty(uuid, Component.ENDER_CHEST);
    }

    public static void markVitalsDirty(UUID uuid) {
        markDirty(uuid, Component.VITALS);
    }

    public static void markFoodDirty(UUID uuid) {
        markDirty(uuid, Component.FOOD);
    }

    public static void markExperienceDirty(UUID uuid) {
        markDirty(uuid, Component.EXPERIENCE);
    }

    public static void markPdcDirty(UUID uuid) {
        markDirty(uuid, Component.PDC);
    }

    public static void markStatisticsDirty(UUID uuid) {
        markDirty(uuid, Component.STATISTICS);
    }
}
