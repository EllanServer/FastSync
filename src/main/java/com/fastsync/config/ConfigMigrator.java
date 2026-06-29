package com.fastsync.config;

import org.bukkit.configuration.file.YamlConfiguration;

import java.io.File;
import java.io.IOException;

/**
 * Config migration helper using Bukkit's YamlConfiguration API.
 *
 * <p>This is separate from ConfigManager because sparrow-yaml's
 * {@code YamlDocument} does not expose a simple {@code set(String, Object)}
 * method — its path-based API is complex and version-specific. Bukkit's
 * {@code YamlConfiguration} provides straightforward {@code set/get} access,
 * which is all we need for migration (adding/renaming keys).
 *
 * <p>The migration process:
 * <ol>
 *   <li>Load the config file with Bukkit YAML</li>
 *   <li>Check and add missing keys</li>
 *   <li>Update config_version</li>
 *   <li>Save back to disk</li>
 * </ol>
 * After migration, ConfigManager loads the file with sparrow-yaml as usual.
 */
public class ConfigMigrator {

    private final org.bukkit.plugin.java.JavaPlugin plugin;

    public ConfigMigrator(org.bukkit.plugin.java.JavaPlugin plugin) {
        this.plugin = plugin;
    }

    private static final int CURRENT_CONFIG_VERSION = 3;

    /**
     * Check and migrate the config file if needed.
     *
     * @param configFile the config.yml file
     * @return the config version after migration
     */
    public int checkAndMigrate(File configFile) {
        if (!configFile.exists()) {
            return CURRENT_CONFIG_VERSION;
        }

        YamlConfiguration yaml = YamlConfiguration.loadConfiguration(configFile);
        int currentVersion = yaml.getInt("config_version", 1);

        if (currentVersion >= CURRENT_CONFIG_VERSION) {
            return currentVersion;
        }

        plugin.getLogger().info("[Config] Migrating config from v" + currentVersion
            + " to v" + CURRENT_CONFIG_VERSION);

        boolean changed = false;

        // v1 → v2: Added config_version, language and multi-codec compression.
        if (currentVersion < 2) {
            if (!yaml.contains("language")) {
                yaml.set("language", "en");
                changed = true;
            }
            if (!yaml.contains("compression.zstd-level")) {
                yaml.set("compression.zstd-level", 3);
                changed = true;
            }
            // Ensure compression.type exists
            if (!yaml.contains("compression.type")) {
                yaml.set("compression.type", "LZ4");
                changed = true;
            }
        }

        // v2 -> v3: correct the advertised wrapper format. Runtime v2 remains
        // backward-readable with all v1 LZ4 blobs.
        if (currentVersion < 3) {
            yaml.set("serialization.format-version", 2);
            changed = true;
        }

        // Update config version
        yaml.set("config_version", CURRENT_CONFIG_VERSION);
        changed = true;

        if (changed) {
            try {
                yaml.save(configFile);
                plugin.getLogger().info("[Config] Migration to v" + CURRENT_CONFIG_VERSION
                    + " completed successfully.");
            } catch (IOException e) {
                plugin.getLogger().warning("[Config] Failed to save migrated config: "
                    + e.getMessage() + " — using in-memory values.");
            }
        }

        return CURRENT_CONFIG_VERSION;
    }
}
