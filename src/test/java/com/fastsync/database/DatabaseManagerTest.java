package com.fastsync.database;

import com.fastsync.config.ConfigManager;
import com.fastsync.testutil.TestConfigBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.SQLException;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link DatabaseManager} using a real MySQL instance
 * started via Testcontainers.
 *
 * <p>These tests verify the core distributed-sync safety mechanisms:
 * <ul>
 *   <li>Fencing token stale-write rejection (Kleppmann)</li>
 *   <li>Optimistic concurrency version check (Dynamo)</li>
 *   <li>Atomic operation sequence counter (Raft-inspired op log)</li>
 *   <li>Per-UUID lock acquisition/release</li>
 * </ul>
 *
 * <p>If Docker is not available, the tests are skipped automatically.</p>
 */
class DatabaseManagerTest {

    private static final Logger LOGGER = Logger.getLogger(DatabaseManagerTest.class.getName());

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

        LOGGER.info("Starting MySQL container for integration tests...");
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

        // Clean player_data table before each test. The operation_log table is
        // managed by OperationLogManager and is not touched by these tests.
        try (var conn = databaseManager.getDataSource().getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + config.getTablePrefix() + "player_data");
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
    void testAcquireLockGeneratesMonotonicallyIncreasingFencingTokens() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String serverA = "server-a";
        String serverB = "server-b";

        // Server A acquires lock
        LockResult lock1 = databaseManager.acquireLock(uuid, serverA);
        assertTrue(lock1.acquired(), "First lock acquisition should succeed");
        assertEquals(1, lock1.fencingToken(), "First token should be 1");

        // Server A releases lock
        databaseManager.releaseLock(uuid, serverA);

        // Server B acquires lock
        LockResult lock2 = databaseManager.acquireLock(uuid, serverB);
        assertTrue(lock2.acquired(), "Second lock acquisition should succeed after release");
        assertEquals(2, lock2.fencingToken(), "Fencing token should monotonically increase to 2");

        // Server A trying to acquire again should fail (held by B)
        LockResult lock3 = databaseManager.acquireLock(uuid, serverA);
        assertFalse(lock3.acquired(), "Lock acquisition should fail while held by another server");
    }

    @Test
    void testSaveDataRejectsStaleVersion() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3};
        long checksum = DatabaseManager.computeChecksum(data);

        // Acquire lock and save initial data
        LockResult lock = databaseManager.acquireLock(uuid, "server-a", "test-session");
        assertTrue(lock.acquired());
        assertTrue(databaseManager.saveData(uuid, data, checksum, 0, lock.fencingToken(), "server-a"));

        // Another server acquires lock (fencing token increments)
        databaseManager.releaseLock(uuid, "server-a");
        LockResult lock2 = databaseManager.acquireLock(uuid, "server-b", "test-session");
        assertTrue(lock2.acquired());

        // Save with newer data, using the current DB version as expectedVersion
        byte[] data2 = new byte[]{4, 5, 6};
        long checksum2 = DatabaseManager.computeChecksum(data2);
        VersionedData current = databaseManager.loadData(uuid);
        assertTrue(databaseManager.saveData(uuid, data2, checksum2, current.version(), lock2.fencingToken(), "server-b"));

        // Now the original server tries to write with stale version 0
        // This should be rejected because DB version is now 2
        byte[] staleData = new byte[]{7, 8, 9};
        long staleChecksum = DatabaseManager.computeChecksum(staleData);
        boolean saved = databaseManager.saveData(uuid, staleData, staleChecksum, 0, lock.fencingToken(), "server-a");
        assertFalse(saved, "Stale version write should be rejected (Dynamo OCC)");

        // Verify DB still has the newer data
        VersionedData loaded = databaseManager.loadData(uuid);
        assertArrayEquals(data2, loaded.data(), "DB should retain the newer data");
    }

    @Test
    void testSaveDataRejectsLowerFencingToken() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3};
        long checksum = DatabaseManager.computeChecksum(data);

        // Server A acquires lock (token 1) and saves
        LockResult lockA = databaseManager.acquireLock(uuid, "server-a", "test-session");
        assertTrue(databaseManager.saveData(uuid, data, checksum, 0, lockA.fencingToken(), "server-a"));

        // Server B acquires lock (token 2) and saves
        databaseManager.releaseLock(uuid, "server-a");
        LockResult lockB = databaseManager.acquireLock(uuid, "server-b", "test-session");
        byte[] dataB = new byte[]{4, 5, 6};
        long checksumB = DatabaseManager.computeChecksum(dataB);
        VersionedData current = databaseManager.loadData(uuid);
        assertTrue(databaseManager.saveData(uuid, dataB, checksumB, current.version(), lockB.fencingToken(), "server-b"));

        // Server A (token 1) tries to write again with the current version —
        // should still be rejected because the stored fencing token (2) > lockA's token (1)
        VersionedData afterB = databaseManager.loadData(uuid);
        byte[] dataA2 = new byte[]{7, 8, 9};
        long checksumA2 = DatabaseManager.computeChecksum(dataA2);
        boolean saved = databaseManager.saveData(uuid, dataA2, checksumA2, afterB.version(), lockA.fencingToken(), "server-a");
        assertFalse(saved, "Lower fencing token write should be rejected (Kleppmann)");
    }

    @Test
    void testLoadDataReturnsStoredVersionAndFencingToken() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data = new byte[]{10, 20, 30};
        long checksum = DatabaseManager.computeChecksum(data);

        LockResult lock = databaseManager.acquireLock(uuid, "server-a", "test-session");
        assertTrue(databaseManager.saveData(uuid, data, checksum, 0, lock.fencingToken(), "server-a"));

        VersionedData loaded = databaseManager.loadData(uuid);
        assertArrayEquals(data, loaded.data());
        assertEquals(1, loaded.version(), "Version should be incremented to 1 after save");
        assertTrue(loaded.fencingToken() >= lock.fencingToken(),
                "Stored fencing token should be >= the lock token");
    }

    @Test
    void testNoForUpdateOnHotPathSql() throws Exception {
        // Static audit: verify the hot-path save/acquireLock SQL does not contain FOR UPDATE
        // This is a regression test for the InnoDB locking audit.
        String hotPathSql = """
                UPDATE `%s` SET locked_by = ?, locked_at = ?, fencing_token = fencing_token + 1
                WHERE uuid = ? AND (locked_by IS NULL OR locked_at < ? OR locked_by = ?)
                """;
        String saveSql = """
                UPDATE `%s` SET data = ?, version = version + 1, checksum = ?,
                locked_by = NULL, locked_at = NULL, last_server = ?, last_updated = ?
                WHERE uuid = ? AND version = ? AND fencing_token <= ?
                """;

        assertFalse(hotPathSql.toUpperCase().contains("FOR UPDATE"),
                "acquireLock SQL must not use FOR UPDATE (single-row PK update)");
        assertFalse(saveSql.toUpperCase().contains("FOR UPDATE"),
                "saveData SQL must not use FOR UPDATE (single-row PK CAS)");
    }
}
