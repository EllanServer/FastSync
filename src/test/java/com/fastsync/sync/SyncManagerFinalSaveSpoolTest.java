package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import com.fastsync.spool.FinalSaveSpool;
import com.fastsync.spool.FinalSaveSpoolRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SyncManagerFinalSaveSpoolTest {

    @TempDir
    Path tempDir;

    @Test
    void retryableFinalSaveFailureIsSpooledWithCurrentCasMetadata() throws Exception {
        ConfigManager config = mock(ConfigManager.class);
        when(config.getClusterId()).thenReturn("cluster-a");
        when(config.getServerName()).thenReturn("server-a");
        when(config.getCompressionMinSize()).thenReturn(0);
        when(config.getLockTimeout()).thenReturn(60);

        SyncManager manager = new SyncManager(plugin(), config, mock(DatabaseManager.class));
        FinalSaveSpool spool = new FinalSaveSpool(
            Logger.getLogger("final-save-spool-test"), tempDir, false, 10, 1_000_000, 7);
        setField(manager, "finalSaveSpool", spool);

        UUID uuid = UUID.randomUUID();
        PlayerData data = new PlayerData();
        data.setVersion(12);
        data.setFencingToken(34);

        boolean spooled = manager.spoolRetryableFinalSave(
            uuid, data, SyncManager.SaveKind.QUIT,
            SyncManager.SaveResult.error("db down", SyncManager.SaveFailureReason.DB_UNAVAILABLE),
            "session-a", "test DB failure");

        assertTrue(spooled);
        assertEquals(1, spool.getPendingCount());
        FinalSaveSpoolRecord record = spool.read(spool.listPending(1).getFirst());
        assertEquals(uuid, record.uuid());
        assertEquals("cluster-a", record.clusterId());
        assertEquals("server-a", record.serverName());
        assertEquals("session-a", record.lockSessionId());
        assertEquals(12, record.expectedVersion());
        assertEquals(34, record.fencingToken());
        assertEquals("QUIT", record.saveKind());
    }

    @Test
    void fencingConflictIsNeverSpooled() throws Exception {
        ConfigManager config = mock(ConfigManager.class);
        SyncManager manager = new SyncManager(plugin(), config, mock(DatabaseManager.class));
        FinalSaveSpool spool = new FinalSaveSpool(
            Logger.getLogger("final-save-spool-test"), tempDir, false, 10, 1_000_000, 7);
        setField(manager, "finalSaveSpool", spool);

        boolean spooled = manager.spoolRetryableFinalSave(
            UUID.randomUUID(), new PlayerData(), SyncManager.SaveKind.QUIT,
            SyncManager.SaveResult.conflict(1, 2, 10, SyncManager.SaveFailureReason.FENCING_MISMATCH),
            "session-a", "test conflict");

        assertFalse(spooled);
        assertEquals(0, spool.getPendingCount());
    }

    @Test
    void productionStartupFailsClosedWhenSpoolCannotInitialize() throws Exception {
        Path dataFile = tempDir.resolve("not-a-directory");
        Files.writeString(dataFile, "block directory creation");

        FastSync plugin = plugin();
        when(plugin.getDataFolder()).thenReturn(dataFile.toFile());

        ConfigManager config = mock(ConfigManager.class);
        when(config.getPoolSize()).thenReturn(10);
        when(config.getQueueCapacity()).thenReturn(16);
        when(config.getFinalSaveThreads()).thenReturn(2);
        when(config.getFinalSaveQueueCapacity()).thenReturn(1024);
        when(config.isFinalSaveSpoolEnabled()).thenReturn(true);
        when(config.getFinalSaveSpoolDir()).thenReturn("spool");
        when(config.getFinalSaveSpoolMaxFiles()).thenReturn(10);
        when(config.getFinalSaveSpoolMaxBytes()).thenReturn(1_000_000L);
        when(config.getFinalSaveSpoolRetainFailedDays()).thenReturn(7);
        when(config.isFinalSaveSpoolFsync()).thenReturn(false);
        when(config.isProductionEnabled()).thenReturn(true);

        SyncManager manager = new SyncManager(plugin, config, mock(DatabaseManager.class));

        IllegalStateException error = assertThrows(IllegalStateException.class, manager::initialize);
        assertTrue(error.getMessage().contains("Refusing to start"));
        assertNull(getField(manager, "asyncExecutor"));
        assertNull(getField(manager, "finalSaveExecutor"));
    }

    @Test
    void saveFailureClassificationOnlyRetriesDatabaseErrors() {
        assertEquals(SyncManager.SaveFailureReason.DB_UNAVAILABLE,
            SyncManager.classifySaveException(
                new RuntimeException("wrapped", new SQLException("db down"))));
        assertEquals(SyncManager.SaveFailureReason.SERIALIZATION_ERROR,
            SyncManager.classifySaveException(new IOException("cannot encode")));
        assertEquals(SyncManager.SaveFailureReason.UNKNOWN,
            SyncManager.classifySaveException(new IllegalStateException("unexpected")));
    }

    private FastSync plugin() {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("final-save-spool-test"));
        return plugin;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
