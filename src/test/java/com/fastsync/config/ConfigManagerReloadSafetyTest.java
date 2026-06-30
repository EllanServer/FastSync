package com.fastsync.config;

import com.fastsync.testutil.TestConfigBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigManagerReloadSafetyTest {

    @Test
    void detectsIdentityAndConnectionChangesThatNeedRestart() throws Exception {
        ConfigManager active = config();
        ConfigManager candidate = config();
        set(candidate, "serverName", "other-server");
        set(candidate, "clusterId", "other-cluster");
        set(candidate, "dbHost", "db.internal");
        set(candidate, "redisEnabled", true);
        set(candidate, "productionEnabled", true);

        List<String> changes = ConfigManager.restartRequiredChanges(active, candidate);

        assertTrue(changes.contains("server-name"));
        assertTrue(changes.contains("cluster-id"));
        assertTrue(changes.contains("database.*"));
        assertTrue(changes.contains("redis connection/stream settings"));
        assertTrue(changes.contains("production.*"));
    }

    @Test
    void permitsSettingsWhoseConsumersRefreshLive() throws Exception {
        ConfigManager active = config();
        ConfigManager candidate = config();
        set(candidate, "syncInventory", false);
        set(candidate, "periodicSave", true);
        set(candidate, "periodicSaveIntervalSeconds", 60);
        set(candidate, "periodicSaveBatchSize", 5);
        set(candidate, "heartbeatIntervalSeconds", 3);
        set(candidate, "compressionMinSize", 512);
        set(candidate, "operationLogRetention", 250);
        set(candidate, "debug", true);

        assertEquals(List.of(), ConfigManager.restartRequiredChanges(active, candidate));
    }

    private static ConfigManager config() throws Exception {
        ConfigManager config = new TestConfigBuilder().defaults().build();
        // Structural fields not populated by TestConfigBuilder must match a
        // realistic initialized instance on both sides.
        set(config, "finalSaveSpoolDir", "final-save-spool");
        return config;
    }

    private static void set(ConfigManager config, String fieldName, Object value) throws Exception {
        Field field = ConfigManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(config, value);
    }
}
