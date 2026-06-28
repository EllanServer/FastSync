package com.fastsync.testutil;

import com.fastsync.config.ConfigManager;

import java.lang.reflect.Field;

/**
 * Builds a {@link ConfigManager} using reflection for unit/integration tests.
 *
 * <p>Uses the package-private test constructor of ConfigManager that bypasses
 * the JavaPlugin reference. Tests set fields directly via reflection, so the
 * Sparrow-yaml config file is never read.</p>
 */
public class TestConfigBuilder {

    private final ConfigManager config;

    public TestConfigBuilder() {
        this.config = new ConfigManager(true);
    }

    public TestConfigBuilder defaults() throws ReflectiveOperationException {
        set("serverName", "test-server");
        set("clusterId", "test-cluster");
        set("tablePrefix", "fastsync_test_");
        set("lockTimeout", 30);
        set("lockRetryIntervalMs", 500L);
        set("lockMaxRetries", 5);
        set("debug", true);
        set("operationLogEnabled", false);
        set("operationLogRetention", 100);
        set("streamsEnabled", false);
        set("compressionMinSize", 0);
        set("maxSnapshots", 10);
        set("verifyChecksum", false);
        set("redisEnabled", false);
        set("redisHost", "localhost");
        set("redisPort", 6379);
        set("redisPassword", "");
        set("redisDatabase", 0);
        set("redisSsl", false);
        set("dbType", "mysql");
        set("dbHost", "localhost");
        set("dbPort", 3306);
        set("dbDatabase", "fastsync");
        set("dbUsername", "root");
        set("dbPassword", "root");
        set("dbParameters", "");
        set("poolSize", 10);
        set("connectionTimeout", 30000L);
        set("idleTimeout", 600000L);
        set("maxLifetime", 1800000L);
        set("leakDetectionThreshold", 60000L);
        set("saveOnDeath", false);
        set("saveOnWorldSave", false);
        set("periodicSave", false);
        set("snapshotEnabled", false);
        return this;
    }

    public TestConfigBuilder withDatabase(String host, int port, String name,
                                          String user, String password) throws ReflectiveOperationException {
        set("dbHost", host);
        set("dbPort", port);
        set("dbDatabase", name);
        set("dbUsername", user);
        set("dbPassword", password);
        return this;
    }

    public TestConfigBuilder operationLogEnabled(boolean enabled) throws ReflectiveOperationException {
        set("operationLogEnabled", enabled);
        return this;
    }

    public ConfigManager build() {
        return config;
    }

    private void set(String fieldName, Object value) throws ReflectiveOperationException {
        Field field = ConfigManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(config, value);
    }
}
