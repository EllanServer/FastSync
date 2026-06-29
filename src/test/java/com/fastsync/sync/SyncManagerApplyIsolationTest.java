package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.PlayerInventory;
import org.bukkit.plugin.PluginManager;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class SyncManagerApplyIsolationTest {

    @Test
    void clearBeforeApplyDoesNotClearDisabledComponents() throws Exception {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("apply-isolation-test"));
        ConfigManager config = mock(ConfigManager.class);
        when(config.isClearBeforeApply()).thenReturn(true);
        when(config.isSyncInventory()).thenReturn(false);
        when(config.isSyncEnderChest()).thenReturn(false);
        when(config.isSyncPotionEffects()).thenReturn(false);

        SyncManager manager = new SyncManager(plugin, config, mock(DatabaseManager.class));
        Player player = mock(Player.class);
        UUID uuid = UUID.randomUUID();
        when(player.getUniqueId()).thenReturn(uuid);

        PlayerData data = new PlayerData();
        data.setVersion(4);
        data.setFencingToken(8);
        pendingData(manager).put(uuid, data);

        PluginManager pluginManager = mock(PluginManager.class);
        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(Bukkit::getPluginManager).thenReturn(pluginManager);
            manager.applyPlayerData(player);
        }

        verify(player, never()).getInventory();
        verify(player, never()).getEnderChest();
        verify(player, never()).getActivePotionEffects();
        verify(pluginManager).callEvent(any());
        assertTrue(manager.isPlayerActive(uuid));
    }

    @Test
    void completeGreenfieldPayloadIsNotPreCleared() throws Exception {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("apply-no-double-clear-test"));
        ConfigManager config = mock(ConfigManager.class);
        when(config.isClearBeforeApply()).thenReturn(true);
        when(config.isSyncInventory()).thenReturn(true);
        when(config.isSyncEnderChest()).thenReturn(true);
        when(config.isSyncPotionEffects()).thenReturn(true);

        SyncManager manager = new SyncManager(plugin, config, mock(DatabaseManager.class));
        Player player = mock(Player.class);
        PlayerInventory inventory = mock(PlayerInventory.class);
        Inventory enderChest = mock(Inventory.class);
        UUID uuid = UUID.randomUUID();
        when(player.getUniqueId()).thenReturn(uuid);
        when(player.getInventory()).thenReturn(inventory);
        when(player.getEnderChest()).thenReturn(enderChest);
        when(player.getActivePotionEffects()).thenReturn(java.util.Set.of());

        ItemStack[] storage = new ItemStack[36];
        ItemStack[] armor = new ItemStack[4];
        ItemStack[] ender = new ItemStack[27];
        PlayerData data = new PlayerData();
        data.setInventory(storage);
        data.setArmor(armor);
        data.setEnderChest(ender);
        data.setVersion(4);
        data.setFencingToken(8);
        pendingData(manager).put(uuid, data);

        PluginManager pluginManager = mock(PluginManager.class);
        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(Bukkit::getPluginManager).thenReturn(pluginManager);
            manager.applyPlayerData(player);
        }

        verify(inventory, never()).clear();
        verify(enderChest, never()).clear();
        verify(inventory).setStorageContents(same(storage));
        verify(inventory).setArmorContents(same(armor));
        verify(inventory).setItemInOffHand(null);
        verify(enderChest).setStorageContents(same(ender));
        verify(player, times(1)).getActivePotionEffects();
        assertTrue(manager.isPlayerActive(uuid));
    }

    @Test
    void airAndFlightAreFullyReplaced() throws Exception {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("apply-air-flight-test"));
        ConfigManager config = mock(ConfigManager.class);
        when(config.isSyncAir()).thenReturn(true);
        when(config.isSyncFlight()).thenReturn(true);

        SyncManager manager = new SyncManager(plugin, config, mock(DatabaseManager.class));
        Player player = mock(Player.class);
        UUID uuid = UUID.randomUUID();
        when(player.getUniqueId()).thenReturn(uuid);

        PlayerData data = new PlayerData();
        data.setMaximumAir(480);
        data.setRemainingAir(123);
        data.setAllowFlight(true);
        data.setFlying(false);
        data.setVersion(4);
        data.setFencingToken(8);
        pendingData(manager).put(uuid, data);

        PluginManager pluginManager = mock(PluginManager.class);
        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(Bukkit::getPluginManager).thenReturn(pluginManager);
            manager.applyPlayerData(player);
        }

        InOrder order = inOrder(player);
        order.verify(player).setMaximumAir(480);
        order.verify(player).setRemainingAir(123);
        order.verify(player).setAllowFlight(true);
        order.verify(player).setFlying(false);
        assertTrue(manager.isPlayerActive(uuid));
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<UUID, PlayerData> pendingData(SyncManager manager) throws Exception {
        Field field = SyncManager.class.getDeclaredField("pendingData");
        field.setAccessible(true);
        return (ConcurrentHashMap<UUID, PlayerData>) field.get(manager);
    }
}
