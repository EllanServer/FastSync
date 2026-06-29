package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.api.FastSyncEvents;
import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.fastsync.sync.strategy.PdcSyncStrategy;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.Event;
import org.bukkit.plugin.PluginManager;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SyncManagerApiEventTest {

    @Test
    void cancelledPreLoadBypassesDatabaseApplyAndSave() {
        DatabaseManager database = mock(DatabaseManager.class);
        SyncManager manager = manager(mock(ConfigManager.class), database);
        PluginManager pluginManager = mock(PluginManager.class);
        doAnswer(invocation -> {
            Event event = invocation.getArgument(0);
            if (event instanceof FastSyncEvents.FastSyncPreLoadEvent preLoad) {
                preLoad.setCancelled(true);
            }
            return null;
        }).when(pluginManager).callEvent(any(Event.class));

        UUID uuid = UUID.randomUUID();
        Player player = mock(Player.class);
        when(player.getUniqueId()).thenReturn(uuid);
        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(Bukkit::getPluginManager).thenReturn(pluginManager);
            SyncManager.LoadResult result = manager.loadPlayerData(uuid);
            assertEquals(SyncManager.LoadResult.Status.BYPASSED, result.getStatus());
            assertTrue(result.isSuccess());
            manager.applyPlayerData(player);
        }

        assertTrue(manager.isPlayerActive(uuid), "bypassed player is ready for normal gameplay");
        manager.collectAndSavePlayerData(player);
        assertFalse(manager.isPlayerActive(uuid));
        verifyNoInteractions(database);
    }

    @Test
    void cancelledFinalSaveReleasesOwnedLockWithoutPersisting() throws Exception {
        ConfigManager config = mock(ConfigManager.class);
        when(config.getServerName()).thenReturn("server-a");
        DatabaseManager database = mock(DatabaseManager.class);
        when(database.releaseLock(any(), anyString(), anyLong(), anyString())).thenReturn(true);
        SyncManager manager = manager(config, database);
        UUID uuid = UUID.randomUUID();
        Player player = mock(Player.class);
        when(player.getUniqueId()).thenReturn(uuid);
        map(manager, "activePlayers").put(uuid, true);
        map(manager, "playerVersions").put(uuid, 4L);
        map(manager, "playerFencingTokens").put(uuid, 9L);
        map(manager, "playerLockSessions").put(uuid, "session-a");

        PluginManager pluginManager = mock(PluginManager.class);
        doAnswer(invocation -> {
            Event event = invocation.getArgument(0);
            if (event instanceof FastSyncEvents.FastSyncSaveEvent save) {
                save.setCancelled(true);
            }
            return null;
        }).when(pluginManager).callEvent(any(Event.class));
        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(Bukkit::getPluginManager).thenReturn(pluginManager);
            manager.collectAndSavePlayerData(player);
        }

        verify(database).releaseLock(uuid, "server-a", 9L, "session-a");
        verify(database, never()).saveDataAndReleaseLockClearComponents(
            any(), any(), anyLong(), anyLong(), anyLong(), anyString(), anyString());
        assertFalse(manager.isPlayerActive(uuid));
    }

    @Test
    void finalCollectionFailureDoesNotReleaseOrHeartbeatLock() throws Exception {
        ConfigManager config = mock(ConfigManager.class);
        when(config.getLockTimeout()).thenReturn(30);
        DatabaseManager database = mock(DatabaseManager.class);
        SyncManager manager = manager(config, database);
        UUID uuid = UUID.randomUUID();
        Player player = mock(Player.class);
        when(player.getUniqueId()).thenReturn(uuid);
        map(manager, "activePlayers").put(uuid, true);
        map(manager, "playerVersions").put(uuid, 4L);
        map(manager, "playerFencingTokens").put(uuid, 9L);
        map(manager, "playerLockSessions").put(uuid, "session-a");

        PdcSyncStrategy pdc = mock(PdcSyncStrategy.class);
        when(pdc.strategyName()).thenReturn("safe-all-paper");
        when(pdc.dump(player)).thenThrow(new IllegalStateException("broken PDC"));
        setField(manager, "pdcStrategy", pdc);

        manager.collectAndSavePlayerData(player);

        verifyNoInteractions(database);
        assertFalse(manager.isPlayerActive(uuid));
        assertFalse(map(manager, "playerLockSessions").containsKey(uuid));
    }

    private static SyncManager manager(ConfigManager config, DatabaseManager database) {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("api-event-test"));
        return new SyncManager(plugin, config, database);
    }

    @SuppressWarnings("unchecked")
    private static <V> ConcurrentHashMap<UUID, V> map(SyncManager manager, String name) throws Exception {
        Field field = SyncManager.class.getDeclaredField(name);
        field.setAccessible(true);
        return (ConcurrentHashMap<UUID, V>) field.get(manager);
    }

    private static void setField(SyncManager manager, String name, Object value) throws Exception {
        Field field = SyncManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(manager, value);
    }
}
