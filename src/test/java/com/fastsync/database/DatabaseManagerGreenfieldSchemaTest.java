package com.fastsync.database;

import com.fastsync.config.ConfigManager;
import com.fastsync.testutil.TestConfigBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.SQLException;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for greenfield schema self-check (Round 15, P1).
 *
 * <p>{@link DatabaseManager#validateGreenfieldSchemaStrict()} is a read-only
 * validation that verifies all required columns exist after
 * {@code createTables()}. This test suite checks:
 * <ul>
 *   <li>greenfieldCreateTablesHasAllRequiredColumns — happy path, all columns
 *       present, no SQLException thrown</li>
 * </ul>
 *
 * <p>Future tests could inject a malformed table (e.g. via direct DDL before
 * initialize()) to verify rejection, but the current test focuses on the
 * baseline case.
 */
class DatabaseManagerGreenfieldSchemaTest {

    private static final Logger LOGGER = Logger.getLogger(DatabaseManagerGreenfieldSchemaTest.class.getName());

    private static boolean dockerAvailable;
    private static MySQLContainer mysql;

    private DatabaseManager databaseManager;
    private ConfigManager config;

    @BeforeAll
    static void setupClass() {
        dockerAvailable = isDockerAvailable();
        if (!dockerAvailable) {
            LOGGER.warning("Docker not available. Skipping MySQL integration tests.");
            return;
        }

        LOGGER.info("Starting MySQL container for greenfield schema tests...");
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("fastsync_greenfield_test")
                .withUsername("test")
                .withPassword("test");
        mysql.start();
    }

    @AfterAll
    static void tearDownClass() {
        if (mysql != null && mysql.isRunning()) {
            mysql.stop();
        }
    }

    @BeforeEach
    void setup() throws Exception {
        Assumptions.assumeTrue(dockerAvailable, "Docker is required for MySQL integration tests");

        config = new TestConfigBuilder()
                .defaults()
                .withDatabase(
                        mysql.getHost(),
                        mysql.getMappedPort(3306),
                        mysql.getDatabaseName(),
                        mysql.getUsername(),
                        mysql.getPassword()
                )
                .build();

        databaseManager = new DatabaseManager(LOGGER, config);
        // initialize() calls createTables() + validateGreenfieldSchemaStrict()
        // — the test asserts that this completes without SQLException.
    }

    @AfterEach
    void tearDown() {
        if (databaseManager != null) {
            databaseManager.close();
            databaseManager = null;
        }
    }

    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Exception | Error e) {
            return false;
        }
    }

    /**
     * Round 15 test: greenfieldCreateTablesHasAllRequiredColumns.
     *
     * <p>Verifies that a fresh database (no existing tables) passes the
     * strict schema validation after createTables(). The required column set
     * matches the DDL in {@link DatabaseManager#createTables()}:
     * <ul>
     *   <li>player_data: uuid, data, version, checksum, fencing_token,
     *       locked_by, locked_at, lock_session_id, last_server, last_updated,
     *       component_bitmap, component_generation</li>
     *   <li>player_component: uuid, component, generation, data, version,
     *       checksum, updated_at</li>
     * </ul>
     *
     * <p>Any missing column would cause {@link DatabaseManager#initialize()}
     * to throw {@link SQLException}, failing the test.
     */
    @Test
    void greenfieldCreateTablesHasAllRequiredColumns() throws SQLException {
        // The assertion is implicit: initialize() succeeds without throwing.
        databaseManager.initialize();

        // Explicit sanity check: both tables exist.
        try (var conn = databaseManager.getDataSource().getConnection();
             var stmt = conn.createStatement()) {
            // Player_data table
            var rs1 = stmt.executeQuery(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '"
                + config.getTablePrefix() + "player_data'");
            rs1.next();
            int playerDataCols = rs1.getInt(1);
            assertTrue(playerDataCols >= 12,
                "player_data must have at least 12 columns (found " + playerDataCols + ")");

            // Player_component table
            var rs2 = stmt.executeQuery(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '"
                + config.getTablePrefix() + "player_component'");
            rs2.next();
            int componentCols = rs2.getInt(1);
            assertTrue(componentCols >= 7,
                "player_component must have at least 7 columns (found " + componentCols + ")");
        }
    }
}
