package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.PlayerInventory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SyncManagerInventoryStorageTest {

    @Test
    void inventoryStoragePayloadIsAppliedWithoutCopy() throws Exception {
        SyncManager manager = manager(mock(ConfigManager.class));
        PlayerInventory inventory = mock(PlayerInventory.class);

        ItemStack[] contents = new ItemStack[36];
        invokeSetInventoryContents(manager, inventory, contents);

        verify(inventory).setStorageContents(same(contents));
        verify(inventory, never()).getContents();
        verify(inventory, never()).getStorageContents();
    }

    @Test
    void enderChestStoragePayloadIsAppliedWithoutCopy() throws Exception {
        SyncManager manager = manager(mock(ConfigManager.class));
        Inventory enderChest = mock(Inventory.class);

        ItemStack[] contents = new ItemStack[27];
        Method method = SyncManager.class.getDeclaredMethod(
            "setEnderChestContents", Inventory.class, ItemStack[].class);
        method.setAccessible(true);
        method.invoke(manager, enderChest, contents);

        verify(enderChest).setStorageContents(same(contents));
        verify(enderChest, never()).getContents();
        verify(enderChest, never()).getStorageContents();
    }

    @Test
    void collectionReadsStorageSlotsInsteadOfAllInventorySlots() throws Exception {
        ConfigManager config = mock(ConfigManager.class);
        when(config.isSyncInventory()).thenReturn(true);
        when(config.isSyncEnderChest()).thenReturn(true);
        SyncManager manager = manager(config);

        Player player = mock(Player.class);
        PlayerInventory inventory = mock(PlayerInventory.class);
        Inventory enderChest = mock(Inventory.class);
        ItemStack[] storage = new ItemStack[36];
        ItemStack[] enderStorage = new ItemStack[27];
        when(player.getUniqueId()).thenReturn(UUID.randomUUID());
        when(player.getInventory()).thenReturn(inventory);
        when(player.getEnderChest()).thenReturn(enderChest);
        when(inventory.getStorageContents()).thenReturn(storage);
        when(enderChest.getStorageContents()).thenReturn(enderStorage);

        Method method = SyncManager.class.getDeclaredMethod("collectPlayerData", Player.class);
        method.setAccessible(true);
        PlayerData data = (PlayerData) method.invoke(manager, player);

        assertSame(storage, data.getInventory());
        assertSame(enderStorage, data.getEnderChest());
        verify(inventory).getStorageContents();
        verify(inventory, never()).getContents();
        verify(enderChest).getStorageContents();
        verify(enderChest, never()).getContents();
    }

    private static SyncManager manager(ConfigManager config) {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("inventory-storage-test"));
        return new SyncManager(plugin, config, mock(DatabaseManager.class));
    }

    private static void invokeSetInventoryContents(
            SyncManager manager, PlayerInventory inventory, ItemStack[] contents) throws Exception {
        Method method = SyncManager.class.getDeclaredMethod(
            "setInventoryContents", PlayerInventory.class, ItemStack[].class);
        method.setAccessible(true);
        method.invoke(manager, inventory, contents);
    }
}
