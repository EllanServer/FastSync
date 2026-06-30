package com.fastsync.config;

import com.fastsync.testutil.TestConfigBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies {@link ConfigManager#validateProductionSafety()} refuses to let the
 * plugin start with the insecure bundled sample values, per the round-3
 * production-hardening directive:
 *
 * <ul>
 *   <li>root/password sample credentials → reject.</li>
 *   <li>non-localhost host + sslMode=DISABLED (or useSSL=false) → reject.</li>
 *   <li>explicit {@code allow-insecure-remote: true} downgrades the TLS check
 *       to a warning (no throw).</li>
 *   <li>localhost + sslMode=DISABLED → allowed (same-machine DB).</li>
 *   <li>non-localhost + sslMode=REQUIRED → allowed (encrypted).</li>
 * </ul>
 *
 * <p>Uses the test-only {@code ConfigManager(boolean)} constructor and
 * reflection to set fields directly (no config file, no JavaPlugin).
 */
class ConfigManagerProductionSafetyTest {

    @Test
    void rejectsSampleRootPasswordCredentials() throws Exception {
        ConfigManager config = base();
        set(config, "dbUsername", "root");
        set(config, "dbPassword", "password");
        assertThrows(RuntimeException.class,
            config::validateProductionSafety,
            "root/password sample credentials must refuse startup");
    }

    @Test
    void rejectsNonLocalhostWithSslModeDisabled() throws Exception {
        ConfigManager config = base();
        set(config, "dbUsername", "appuser");   // real creds, not the sample
        set(config, "dbPassword", "realpass");
        set(config, "dbHost", "db.example.com");
        set(config, "dbParameters", "sslMode=DISABLED&useUnicode=true&characterEncoding=UTF-8");
        assertThrows(RuntimeException.class,
            config::validateProductionSafety,
            "sslMode=DISABLED against a non-localhost host must refuse startup");
    }

    @Test
    void rejectsNonLocalhostWithDeprecatedUseSslFalse() throws Exception {
        ConfigManager config = base();
        set(config, "dbUsername", "appuser");
        set(config, "dbPassword", "realpass");
        set(config, "dbHost", "10.0.0.5");
        // useSSL=false is the deprecated alias of sslMode=DISABLED and is
        // equally insecure on the network — the guard must catch it too.
        set(config, "dbParameters", "useSSL=false&useUnicode=true");
        assertThrows(RuntimeException.class,
            config::validateProductionSafety,
            "useSSL=false against a non-localhost host must refuse startup");
    }

    @Test
    void allowsInsecureRemoteWhenExplicitlyOptedIn() throws Exception {
        ConfigManager config = base();
        set(config, "dbUsername", "appuser");
        set(config, "dbPassword", "realpass");
        set(config, "dbHost", "db.example.com");
        set(config, "dbParameters", "sslMode=DISABLED&useUnicode=true");
        set(config, "allowInsecureRemote", true);
        assertDoesNotThrow(config::validateProductionSafety,
            "allow-insecure-remote=true must downgrade the TLS check to a warning");
    }

    @Test
    void allowsLocalhostWithSslModeDisabled() throws Exception {
        ConfigManager config = base();
        set(config, "dbUsername", "appuser");
        set(config, "dbPassword", "realpass");
        set(config, "dbHost", "localhost");
        set(config, "dbParameters", "sslMode=DISABLED&useUnicode=true");
        assertDoesNotThrow(config::validateProductionSafety,
            "sslMode=DISABLED to a loopback host is allowed");
    }

    @Test
    void allowsNonLocalhostWithSslModeRequired() throws Exception {
        ConfigManager config = base();
        set(config, "dbUsername", "appuser");
        set(config, "dbPassword", "realpass");
        set(config, "dbHost", "db.example.com");
        set(config, "dbParameters", "sslMode=REQUIRED&useUnicode=true");
        assertDoesNotThrow(config::validateProductionSafety,
            "sslMode=REQUIRED to a remote host is allowed");
    }

    // ==================== Round 16: cluster-id DB isolation ====================

    /** The v2 composite primary key isolates clusters even with the default prefix. */
    @Test
    void allowsClusterIdWithDefaultTablePrefix() throws Exception {
        ConfigManager config = base();
        set(config, "clusterId", "production");
        set(config, "tablePrefix", "fastsync_");
        assertDoesNotThrow(config::validateProductionSafety,
            "v2 schema isolates by cluster_id in PK, so default table-prefix is safe");
    }

    /** A distinct table prefix remains a valid optional isolation layer. */
    @Test
    void allowsClusterIdWithDistinctTablePrefix() throws Exception {
        ConfigManager config = base();
        set(config, "clusterId", "production");
        set(config, "tablePrefix", "prod_");  // distinct, not the default
        assertDoesNotThrow(config::validateProductionSafety,
            "cluster-id + distinct table-prefix is allowed");
    }

    /** Cluster identity is mandatory even for a single logical cluster. */
    @Test
    void rejectsEmptyClusterId() throws Exception {
        ConfigManager config = base();
        set(config, "clusterId", "");  // single cluster
        set(config, "tablePrefix", "fastsync_");  // default is fine for single cluster
        assertThrows(RuntimeException.class, config::validateProductionSafety,
            "cluster-id is part of every DB key and must always be explicit");
    }

    /** Whitespace-only identity is equivalent to missing identity. */
    @Test
    void rejectsBlankClusterId() throws Exception {
        ConfigManager config = base();
        set(config, "clusterId", "   ");
        set(config, "tablePrefix", "fastsync_");
        assertThrows(RuntimeException.class, config::validateProductionSafety,
            "whitespace-only cluster-id must be rejected");
    }

    // ==================== Production spool requirement ====================

    /**
     * Production mode MUST require the final-save spool. Without it, a
     * saturated final-save executor silently drops the player's final state.
     */
    @Test
    void rejectsProductionModeWithoutSpool() throws Exception {
        ConfigManager config = base();
        set(config, "dbUsername", "appuser");
        set(config, "dbPassword", "realpass");
        set(config, "productionEnabled", true);
        set(config, "productionRequireRedis", false); // isolate the spool check
        set(config, "finalSaveSpoolEnabled", false);
        assertThrows(RuntimeException.class,
            config::validateProductionSafety,
            "production mode without spool must refuse startup");
    }

    /**
     * Production mode with spool enabled, Redis enabled, and a non-empty
     * cluster-id passes validation.
     */
    @Test
    void allowsProductionModeWithSpoolAndRedis() throws Exception {
        ConfigManager config = base();
        set(config, "dbUsername", "appuser");
        set(config, "dbPassword", "realpass");
        set(config, "productionEnabled", true);
        set(config, "productionRequireRedis", true);
        set(config, "redisEnabled", true);
        set(config, "clusterId", "prod-cluster");
        set(config, "finalSaveSpoolEnabled", true);
        assertDoesNotThrow(config::validateProductionSafety,
            "production mode with spool + redis + cluster-id is allowed");
    }

    /** Start from TestConfigBuilder defaults (which pass validation) and override per-test. */
    private static ConfigManager base() throws Exception {
        return new TestConfigBuilder().defaults().build();
    }

    private static void set(ConfigManager config, String fieldName, Object value) throws Exception {
        Field f = ConfigManager.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(config, value);
    }
}
