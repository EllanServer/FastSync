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

import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseLockSessionTest {

    private static final Logger LOGGER = Logger.getLogger(DatabaseLockSessionTest.class.getName());

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

        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
            .withDatabaseName("fastsync_test")
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
        databaseManager.initialize();

        try (var conn = databaseManager.getDataSource().getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + config.getTablePrefix() + "player_data");
        }
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

    @Test
    void acquireLockRejectsNullSession() {
        UUID uuid = UUID.randomUUID();

        assertThrows(IllegalArgumentException.class,
            () -> databaseManager.acquireLock(uuid, "server-a", null));
    }

    @Test
    void acquireLockRejectsBlankSession() {
        UUID uuid = UUID.randomUUID();

        assertThrows(IllegalArgumentException.class,
            () -> databaseManager.acquireLock(uuid, "server-a", "   "));
    }

    @Test
    void sameServerNameDifferentSessionCannotAcquireWhileOldLockAlive() throws Exception {
        UUID uuid = UUID.randomUUID();

        LockResult first = databaseManager.acquireLock(uuid, "server-a", "session-a");
        assertTrue(first.acquired());

        LockResult second = databaseManager.acquireLock(uuid, "server-a", "session-b");
        assertFalse(second.acquired());
    }

    @Test
    void staleSessionCannotReleaseCurrentSession() throws Exception {
        UUID uuid = UUID.randomUUID();

        LockResult first = databaseManager.acquireLock(uuid, "server-a", "session-a");
        assertTrue(first.acquired());
        assertTrue(databaseManager.releaseLock(
            uuid, "server-a", first.fencingToken(), "session-a"));

        LockResult current = databaseManager.acquireLock(uuid, "server-a", "session-b");
        assertTrue(current.acquired());

        assertFalse(databaseManager.releaseLock(
            uuid, "server-a", first.fencingToken(), "session-a"));
    }
}
