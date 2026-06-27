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

    /**
     * Round 16 (P0 #1): a non-empty cluster-id paired with the default
     * table-prefix must refuse startup. cluster-id only isolates Redis
     * messaging, NOT database rows — two clusters sharing the same MySQL
     * database + table-prefix would silently overwrite each other's data.
     */
    @Test
    void rejectsClusterIdWithDefaultTablePrefix() throws Exception {
        ConfigManager config = base();
        set(config, "clusterId", "production");
        set(config, "tablePrefix", "fastsync_");  // the default
        assertThrows(RuntimeException.class,
            config::validateProductionSafety,
            "cluster-id + default table-prefix must refuse startup (DB row collision risk)");
    }

    /**
     * Round 16 (P0 #1): a non-empty cluster-id with a DISTINCT table-prefix
     * is allowed — the operator has explicitly isolated DB rows per cluster.
     */
    @Test
    void allowsClusterIdWithDistinctTablePrefix() throws Exception {
        ConfigManager config = base();
        set(config, "clusterId", "production");
        set(config, "tablePrefix", "prod_");  // distinct, not the default
        assertDoesNotThrow(config::validateProductionSafety,
            "cluster-id + distinct table-prefix is allowed");
    }

    /**
     * Round 16 (P0 #1): empty cluster-id (single-cluster deploy) with the
     * default table-prefix is allowed — no multi-cluster collision risk.
     */
    @Test
    void allowsEmptyClusterIdWithDefaultTablePrefix() throws Exception {
        ConfigManager config = base();
        set(config, "clusterId", "");  // single cluster
        set(config, "tablePrefix", "fastsync_");  // default is fine for single cluster
        assertDoesNotThrow(config::validateProductionSafety,
            "empty cluster-id + default table-prefix is allowed for single-cluster deploys");
    }

    /**
     * Round 16 (P0 #1): blank cluster-id (whitespace only) is treated as
     * empty — must NOT trigger the rejection.
     */
    @Test
    void allowsBlankClusterIdWithDefaultTablePrefix() throws Exception {
        ConfigManager config = base();
        set(config, "clusterId", "   ");
        set(config, "tablePrefix", "fastsync_");
        assertDoesNotThrow(config::validateProductionSafety,
            "blank cluster-id must be treated as empty (no multi-cluster intent)");
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
