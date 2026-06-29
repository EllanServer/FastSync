package com.fastsync.config;

import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConfigMigratorTest {

    @TempDir
    Path tempDir;

    @Test
    void migratesV2FormatMetadataToV3() throws Exception {
        Path config = tempDir.resolve("config.yml");
        Files.writeString(config, """
            config_version: 2
            server-name: keep-me
            serialization:
              format-version: 1
            """);
        JavaPlugin plugin = mock(JavaPlugin.class);
        when(plugin.getLogger()).thenReturn(Logger.getAnonymousLogger());

        int version = new ConfigMigrator(plugin).checkAndMigrate(config.toFile());
        YamlConfiguration migrated = YamlConfiguration.loadConfiguration(config.toFile());

        assertEquals(3, version);
        assertEquals(3, migrated.getInt("config_version"));
        assertEquals(2, migrated.getInt("serialization.format-version"));
        assertEquals("keep-me", migrated.getString("server-name"));
    }
}
