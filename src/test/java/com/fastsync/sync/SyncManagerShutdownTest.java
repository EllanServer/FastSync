package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import org.bukkit.entity.Player;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SyncManagerShutdownTest {

    private TestSyncManager syncManager;

    @BeforeEach
    void setup() {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("shutdown-test"));

        ConfigManager config = mock(ConfigManager.class);
        DatabaseManager databaseManager = mock(DatabaseManager.class);

        syncManager = new TestSyncManager(plugin, config, databaseManager);
    }

    @Test
    void savePlayersSnapshot_shutdownSetsShuttingDownFlag() throws Exception {
        syncManager.savePlayersSnapshot(List.of(), SyncManager.SaveKind.SHUTDOWN);

        assertTrue(getShuttingDown(syncManager));
    }

    @Test
    void savePlayersSnapshot_bulkDoesNotSetShuttingDownFlag() throws Exception {
        syncManager.savePlayersSnapshot(List.of(), SyncManager.SaveKind.BULK);

        assertFalse(getShuttingDown(syncManager));
    }

    private static boolean getShuttingDown(SyncManager syncManager) throws Exception {
        Field field = SyncManager.class.getDeclaredField("shuttingDown");
        field.setAccessible(true);
        return field.getBoolean(syncManager);
    }

    private static final class TestSyncManager extends SyncManager {

        private TestSyncManager(FastSync plugin, ConfigManager config, DatabaseManager databaseManager) {
            super(plugin, config, databaseManager);
        }

        @Override
        public List<Map.Entry<UUID, CompletableFuture<SaveResult>>> dispatchPlayerSaves(
                List<Player> players, SaveKind kind) {
            return List.of();
        }

        @Override
        public SaveAllResult waitForPlayerSaves(
                List<Map.Entry<UUID, CompletableFuture<SaveResult>>> futures, SaveKind kind) {
            return new SaveAllResult(futures.size(), futures.size(), 0, Map.of());
        }
    }
}
