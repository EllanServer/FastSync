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

    // Smart handoff
    private boolean smartHandoffEnabled = true;
    private long waitTimeoutMs = 5000;
    private long pollIntervalMs = 500;
    private String waitingMessage = "&e[FastSync] Waiting for your data to be saved...";
    private String timeoutMessage = "&c[FastSync] Data save timed out, transferring anyway.";

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

            Map<String, Object> handoff = getMap(root, "smart-handoff");
            if (handoff != null) {
                smartHandoffEnabled = getBool(handoff, "enabled", true);
                waitTimeoutMs = getLong(handoff, "wait-timeout-ms", 5000);
                pollIntervalMs = getLong(handoff, "poll-interval-ms", 500);
                waitingMessage = getString(handoff, "waiting-message", waitingMessage);
                timeoutMessage = getString(handoff, "timeout-message", timeoutMessage);
            }

            Map<String, Object> status = getMap(root, "status");
            if (status != null) {
                statusQueryTimeoutMs = getLong(status, "query-timeout-ms", 3000);
            }

            debug = getBool(root, "debug", false);

            logger.info("Proxy config loaded: smart-handoff={}, wait-timeout={}ms, debug={}",
                smartHandoffEnabled, waitTimeoutMs, debug);

        } catch (Exception e) {
            logger.warn("Failed to load proxy-config.yml, using defaults", e);
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

    private String getString(Map<String, Object> map, String key, String def) {
        Object val = map.get(key);
        return val instanceof String ? (String) val : def;
    }

    // Getters
    public boolean isSmartHandoffEnabled() { return smartHandoffEnabled; }
    public long getWaitTimeoutMs() { return waitTimeoutMs; }
    public long getPollIntervalMs() { return pollIntervalMs; }
    public String getWaitingMessage() { return waitingMessage; }
    public String getTimeoutMessage() { return timeoutMessage; }
    public long getStatusQueryTimeoutMs() { return statusQueryTimeoutMs; }
    public boolean isDebug() { return debug; }
}
