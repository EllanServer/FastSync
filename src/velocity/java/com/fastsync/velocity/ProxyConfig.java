package com.fastsync.velocity;

import org.slf4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Loads and manages the proxy configuration file (proxy-config.yml).
 */
public class ProxyConfig {

    private final Path dataDirectory;
    private final Logger logger;

    // Status
    private long statusQueryTimeoutMs = 3000;

    // Debug
    private boolean debug = false;

    public ProxyConfig(Path dataDirectory, Logger logger) {
        this.dataDirectory = dataDirectory;
        this.logger = logger;
    }

    public void load() {
        Path configPath = dataDirectory.resolve("proxy-config.yml");

        // Copy default config if not present
        if (!Files.exists(configPath)) {
            try (InputStream in = getClass().getResourceAsStream("/proxy-config.yml")) {
                if (in != null) {
                    Files.createDirectories(dataDirectory);
                    Files.copy(in, configPath);
                    logger.info("Created default proxy-config.yml at {}", configPath);
                }
            } catch (IOException e) {
                logger.warn("Failed to copy default proxy-config.yml", e);
            }
        }

        if (!Files.exists(configPath)) {
            logger.warn("proxy-config.yml not found, using defaults");
            return;
        }

        try (InputStream in = Files.newInputStream(configPath)) {
            Yaml yaml = new Yaml();
            Map<String, Object> root = yaml.load(in);

            if (root == null) return;

            Map<String, Object> status = getMap(root, "status");
            if (status != null) {
                statusQueryTimeoutMs = getLong(status, "query-timeout-ms", 3000);
            }

            debug = getBool(root, "debug", false);

            validateRanges();

            logger.info("Proxy config loaded: status-timeout={}ms, debug={}",
                statusQueryTimeoutMs, debug);

        } catch (Exception e) {
            logger.warn("Failed to load proxy-config.yml, using defaults", e);
        }
    }

    /** Keep the status-query scheduler delay bounded. */
    void validateRanges() {
        long configuredStatus = statusQueryTimeoutMs;
        statusQueryTimeoutMs = Math.max(100L, Math.min(statusQueryTimeoutMs, 60_000L));
        if (statusQueryTimeoutMs != configuredStatus) {
            logger.warn("status.query-timeout-ms must be 100-60000; using {}", statusQueryTimeoutMs);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMap(Map<String, Object> parent, String key) {
        Object val = parent.get(key);
        return val instanceof Map ? (Map<String, Object>) val : null;
    }

    private boolean getBool(Map<String, Object> map, String key, boolean def) {
        Object val = map.get(key);
        return val instanceof Boolean ? (Boolean) val : def;
    }

    private long getLong(Map<String, Object> map, String key, long def) {
        Object val = map.get(key);
        if (val instanceof Number) return ((Number) val).longValue();
        return def;
    }

    // Getters
    public long getStatusQueryTimeoutMs() { return statusQueryTimeoutMs; }
    public boolean isDebug() { return debug; }
    public void setDebug(boolean debug) { this.debug = debug; }
}
