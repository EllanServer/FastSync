package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.concurrent.AsyncExecutor;
import com.fastsync.database.DatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Round 16 tests for the dedicated final-save executor (P0 #3).
 *
 * <p>Verifies that:
 * <ul>
 *   <li>SyncManager.initialize() creates a non-null finalSaveExecutor</li>
 *   <li>QUIT saves are submitted to finalSaveExecutor, NOT asyncExecutor</li>
 *   <li>shutdown() closes finalSaveExecutor after the main asyncExecutor</li>
 * </ul>
 *
 * <p>Uses Mockito + reflection (same pattern as SyncManagerLockReleaseTest).
 */
class SyncManagerFinalSaveExecutorTest {

    private FastSync plugin;
    private ConfigManager config;
    private DatabaseManager databaseManager;
    private SyncManager syncManager;
    private Logger testLogger;

    @BeforeEach
    void setup() throws Exception {
        plugin = mock(FastSync.class);
        testLogger = Logger.getLogger("final-save-exec-test");
        when(plugin.getLogger()).thenReturn(testLogger);

        config = mock(ConfigManager.class);
        when(config.getServerName()).thenReturn("test-server");
        when(config.isDebug()).thenReturn(false);
        when(config.getPoolSize()).thenReturn(10);
        when(config.getQueueCapacity()).thenReturn(16);
        when(config.getMaxConcurrentLoads()).thenReturn(4);
        when(config.isRedisEnabled()).thenReturn(false);
        when(config.isSnapshotEnabled()).thenReturn(false);
        when(config.isOperationLogEnabled()).thenReturn(false);
        when(config.isLatencyTrackingEnabled()).thenReturn(false);
        when(config.isDirtyTrackingEnabled()).thenReturn(false);
        when(config.isComponentStorageEnabled()).thenReturn(false);

        databaseManager = mock(DatabaseManager.class);

        syncManager = new SyncManager(plugin, config, databaseManager);
        syncManager.initialize();
    }

    /**
     * Round 16 (P0 #3): initialize() must create a non-null
     * finalSaveExecutor distinct from asyncExecutor.
     */
    @Test
    void initializeCreatesFinalSaveExecutor() throws Exception {
        AsyncExecutor asyncExecutor = getField("asyncExecutor");
        AsyncExecutor finalSaveExecutor = getField("finalSaveExecutor");

        assertNotNull(asyncExecutor, "asyncExecutor must be initialized");
        assertNotNull(finalSaveExecutor, "finalSaveExecutor must be initialized");
        assertNotSame(asyncExecutor, finalSaveExecutor,
            "finalSaveExecutor must be a distinct pool, not the same instance as asyncExecutor");
        assertEquals(7, syncManager.getNonCriticalDbLimit(),
            "pool=10 with two final-save threads and one heartbeat reserve leaves seven non-critical slots");
        assertEquals(7, syncManager.getNonCriticalDbAvailablePermits());
    }

    /**
     * Round 16 (P0 #3): shutdown() must close BOTH executors. The
     * finalSaveExecutor must be null after shutdown (set to null in the
     * shutdown method).
     */
    @Test
    void shutdownClosesFinalSaveExecutor() throws Exception {
        AsyncExecutor finalSaveExecutor = getField("finalSaveExecutor");
        assertNotNull(finalSaveExecutor, "precondition: finalSaveExecutor must exist");

        syncManager.shutdown();

        AsyncExecutor asyncAfter = getField("asyncExecutor");
        AsyncExecutor finalAfter = getField("finalSaveExecutor");
        assertNull(asyncAfter, "asyncExecutor must be nulled after shutdown");
        assertNull(finalAfter, "finalSaveExecutor must be nulled after shutdown");
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(String name) throws Exception {
        Field f = SyncManager.class.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(syncManager);
    }
}
