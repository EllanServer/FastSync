package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SyncManagerHeartbeatFailureTest {

    @Test
    void connectionFailureSkipsPerPlayerFallback() throws Exception {
        DatabaseManager database = mock(DatabaseManager.class);
        doThrow(new SQLTransientConnectionException("pool timeout"))
            .when(database).refreshLockBatch(anyMap(), anyMap(), eq("server-a"), anySet());

        SyncManager manager = manager(database);
        UUID uuid = trackPlayer(manager);

        manager.heartbeatOnlinePlayers();

        verify(database, never()).refreshLock(any(), anyString(), anyLong(), anyString());
        assertEquals(1, getField(manager, "heartbeatFailureCount", AtomicInteger.class).get());
        assertTrue(getField(manager, "activePlayers", ConcurrentHashMap.class).containsKey(uuid));
    }

    @Test
    void statementFailureFallsBackToPerPlayerRefresh() throws Exception {
        DatabaseManager database = mock(DatabaseManager.class);
        doThrow(new SQLException("batch statement rejected", "42000"))
            .when(database).refreshLockBatch(anyMap(), anyMap(), eq("server-a"), anySet());
        when(database.refreshLock(any(), eq("server-a"), anyLong(), anyString())).thenReturn(true);

        SyncManager manager = manager(database);
        UUID uuid = trackPlayer(manager);

        manager.heartbeatOnlinePlayers();

        verify(database).refreshLock(eq(uuid), eq("server-a"), eq(7L), eq("session-a"));
        assertEquals(0, getField(manager, "heartbeatFailureCount", AtomicInteger.class).get());
    }

    @Test
    void sqlStateClass08IsRecognizedAsConnectionFailure() {
        assertTrue(SyncManager.isConnectionFailure(new SQLException("connection lost", "08006")));
        assertFalse(SyncManager.isConnectionFailure(new SQLException("deadlock", "40001")));
    }

    private SyncManager manager(DatabaseManager database) {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("heartbeat-failure-test"));
        ConfigManager config = mock(ConfigManager.class);
        when(config.getServerName()).thenReturn("server-a");
        return new SyncManager(plugin, config, database);
    }

    @SuppressWarnings("unchecked")
    private UUID trackPlayer(SyncManager manager) throws Exception {
        UUID uuid = UUID.randomUUID();
        getField(manager, "activePlayers", ConcurrentHashMap.class).put(uuid, true);
        getField(manager, "playerFencingTokens", ConcurrentHashMap.class).put(uuid, 7L);
        getField(manager, "playerLockSessions", ConcurrentHashMap.class).put(uuid, "session-a");
        return uuid;
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String name, Class<T> type) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(target);
    }
}
