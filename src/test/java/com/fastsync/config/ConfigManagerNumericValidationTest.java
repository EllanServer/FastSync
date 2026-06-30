package com.fastsync.config;

import com.fastsync.testutil.TestConfigBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigManagerNumericValidationTest {

    @Test
    void clampsSchedulerAndRetryValuesThatWouldFailAtRuntime() throws Exception {
        ConfigManager config = new TestConfigBuilder().defaults().build();
        set(config, "poolSize", 4);
        set(config, "lockRetryIntervalMs", 0L);
        set(config, "lockMaxRetries", 0);
        set(config, "periodicSaveIntervalSeconds", 0);
        set(config, "periodicSaveBatchSize", 0);
        set(config, "maxConcurrentLoads", 99);

        config.validateNumericRanges();

        assertEquals(1L, config.getLockRetryIntervalMs());
        assertEquals(1, config.getLockMaxRetries());
        assertEquals(1, config.getPeriodicSaveIntervalSeconds());
        assertEquals(1, config.getPeriodicSaveBatchSize());
        assertEquals(2, config.getMaxConcurrentLoads(),
            "two DB connections must remain available for save/heartbeat work");
    }

    @Test
    void clampsConnectionAndAllocationLimits() throws Exception {
        ConfigManager config = new TestConfigBuilder().defaults().build();
        set(config, "dbPort", 70_000);
        set(config, "redisPort", 0);
        set(config, "poolSize", Integer.MAX_VALUE);
        set(config, "queueCapacity", Integer.MAX_VALUE);
        set(config, "finalSaveThreads", Integer.MAX_VALUE);
        set(config, "finalSaveQueueCapacity", Integer.MAX_VALUE);
        set(config, "connectionTimeout", 1L);
        set(config, "idleTimeout", 1L);
        set(config, "maxLifetime", 1L);
        set(config, "leakDetectionThreshold", 1L);
        set(config, "redisTimeout", 1);
        set(config, "serializationMaxRawBytes", Integer.MAX_VALUE);
        set(config, "serializationMaxWrappedBytes", Integer.MAX_VALUE);
        set(config, "latencyWindowSize", Integer.MAX_VALUE);
        set(config, "operationLogRetention", -1);

        config.validateNumericRanges();

        assertEquals(3306, config.getDbPort());
        assertEquals(6379, config.getRedisPort());
        assertEquals(10, config.getPoolSize());
        assertEquals(256, config.getQueueCapacity());
        assertEquals(4, config.getFinalSaveThreads());
        assertEquals(1_024, config.getFinalSaveQueueCapacity());
        assertEquals(250L, config.getConnectionTimeout());
        assertEquals(10_000L, config.getIdleTimeout());
        assertEquals(30_000L, config.getMaxLifetime());
        assertEquals(2_000L, config.getLeakDetectionThreshold());
        assertEquals(100, config.getRedisTimeout());
        assertEquals(1 << 20, config.getSerializationMaxRawBytes());
        assertEquals(5 * (1 << 19), config.getSerializationMaxWrappedBytes());
        assertEquals(1_000, config.getLatencyWindowSize());
        assertEquals(100, config.getOperationLogRetention());
    }

    private static void set(ConfigManager config, String fieldName, Object value) throws Exception {
        Field field = ConfigManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(config, value);
    }
}
