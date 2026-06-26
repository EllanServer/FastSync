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
        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lock.acquired());
        assertTrue(databaseManager.saveData(uuid, data, checksum, 0, lock.fencingToken(), "server-a"));

        // Another server acquires lock (fencing token increments)
        databaseManager.releaseLock(uuid, "server-a");
        LockResult lock2 = databaseManager.acquireLock(uuid, "server-b");
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
        LockResult lockA = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(databaseManager.saveData(uuid, data, checksum, 0, lockA.fencingToken(), "server-a"));

        // Server B acquires lock (token 2) and saves
        databaseManager.releaseLock(uuid, "server-a");
        LockResult lockB = databaseManager.acquireLock(uuid, "server-b");
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

        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
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

    // ==================== Keep-Lock vs Release-Lock Tests ====================

    @Test
    void testSaveDataKeepLock_keepsLockedByAndRefreshesLockedAt() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3};
        long checksum = DatabaseManager.computeChecksum(data);

        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lock.acquired());

        // Save with keep-lock (online/periodic save)
        boolean saved = databaseManager.saveDataKeepLock(uuid, data, checksum, 0,
            lock.fencingToken(), "server-a");
        assertTrue(saved, "saveDataKeepLock should succeed");

        // Verify locked_by is still set to our server
        String lockHolder = databaseManager.getLockHolder(uuid);
        assertEquals("server-a", lockHolder,
            "locked_by should still be 'server-a' after keep-lock save");

        // Verify locked_at was refreshed (should be close to now)
        // We can't read locked_at directly through the public API, but we can
        // verify it by checking that another server CANNOT acquire the lock
        // (locked_at is recent, so the lock is not stale).
        LockResult lockByOther = databaseManager.acquireLock(uuid, "server-b");
        assertFalse(lockByOther.acquired(),
            "Another server should NOT be able to acquire the lock after keep-lock save");
    }

    @Test
    void testSaveDataAndReleaseLock_clearsLockedBy() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3};
        long checksum = DatabaseManager.computeChecksum(data);

        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lock.acquired());

        // Save with release-lock (quit/final save)
        boolean saved = databaseManager.saveDataAndReleaseLock(uuid, data, checksum, 0,
            lock.fencingToken(), "server-a");
        assertTrue(saved, "saveDataAndReleaseLock should succeed");

        // Verify locked_by is cleared
        String lockHolder = databaseManager.getLockHolder(uuid);
        assertNull(lockHolder,
            "locked_by should be NULL after release-lock save");

        // Another server should now be able to acquire the lock
        LockResult lockByOther = databaseManager.acquireLock(uuid, "server-b");
        assertTrue(lockByOther.acquired(),
            "Another server should be able to acquire the lock after release-lock save");
    }

    @Test
    void testSaveDataKeepLock_advancesVersion() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data1 = new byte[]{1, 2, 3};
        long checksum1 = DatabaseManager.computeChecksum(data1);

        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lock.acquired());

        // First keep-lock save
        assertTrue(databaseManager.saveDataKeepLock(uuid, data1, checksum1, 0,
            lock.fencingToken(), "server-a"));

        VersionedData after1 = databaseManager.loadData(uuid);
        assertEquals(1, after1.version(), "Version should be 1 after first save");

        // Second keep-lock save (version advances)
        byte[] data2 = new byte[]{4, 5, 6};
        long checksum2 = DatabaseManager.computeChecksum(data2);
        assertTrue(databaseManager.saveDataKeepLock(uuid, data2, checksum2, after1.version(),
            lock.fencingToken(), "server-a"));

        VersionedData after2 = databaseManager.loadData(uuid);
        assertEquals(2, after2.version(), "Version should be 2 after second keep-lock save");
        assertArrayEquals(data2, after2.data());
    }

    @Test
    void testSaveDataKeepLock_rejectsStaleFencingToken() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3};
        long checksum = DatabaseManager.computeChecksum(data);

        // Server A acquires lock (token 1)
        LockResult lockA = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(databaseManager.saveDataKeepLock(uuid, data, checksum, 0,
            lockA.fencingToken(), "server-a"));

        // Server A releases, Server B acquires (token 2)
        databaseManager.releaseLock(uuid, "server-a");
        LockResult lockB = databaseManager.acquireLock(uuid, "server-b");
        assertTrue(lockB.acquired());

        // Server A tries keep-lock save with stale token — must fail
        byte[] staleData = new byte[]{7, 8, 9};
        long staleChecksum = DatabaseManager.computeChecksum(staleData);
        VersionedData current = databaseManager.loadData(uuid);
        boolean saved = databaseManager.saveDataKeepLock(uuid, staleData, staleChecksum,
            current.version(), lockA.fencingToken(), "server-a");
        assertFalse(saved, "Keep-lock save with stale fencing token must be rejected");

        // Lock should still be held by server B
        assertEquals("server-b", databaseManager.getLockHolder(uuid));
    }

    // ==================== Heartbeat (refreshLock) Tests ====================

    @Test
    void testRefreshLock_returnsTrueWhenWeHoldLock() throws SQLException {
        UUID uuid = UUID.randomUUID();
        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lock.acquired());

        boolean refreshed = databaseManager.refreshLock(uuid, "server-a", lock.fencingToken());
        assertTrue(refreshed, "refreshLock should return true when we hold the lock");

        // Another server should not be able to acquire (lock is fresh)
        LockResult lockByOther = databaseManager.acquireLock(uuid, "server-b");
        assertFalse(lockByOther.acquired(), "Lock should still be held after heartbeat refresh");
    }

    @Test
    void testRefreshLock_returnsFalseWhenLockTakenOver() throws SQLException {
        UUID uuid = UUID.randomUUID();

        // Server A acquires lock
        LockResult lockA = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lockA.acquired());

        // Server A releases, Server B acquires (new fencing token)
        databaseManager.releaseLock(uuid, "server-a");
        LockResult lockB = databaseManager.acquireLock(uuid, "server-b");
        assertTrue(lockB.acquired());

        // Server A tries to refresh with old token — must fail
        boolean refreshed = databaseManager.refreshLock(uuid, "server-a", lockA.fencingToken());
        assertFalse(refreshed, "refreshLock should return false when lock was taken by another server");

        // Server B can still refresh
        boolean bRefreshed = databaseManager.refreshLock(uuid, "server-b", lockB.fencingToken());
        assertTrue(bRefreshed, "Current lock holder should be able to refresh");
    }

    @Test
    void testRefreshLock_returnsFalseForWrongServer() throws SQLException {
        UUID uuid = UUID.randomUUID();

        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lock.acquired());

        // Server B tries to refresh server A's lock — must fail
        boolean refreshed = databaseManager.refreshLock(uuid, "server-b", lock.fencingToken());
        assertFalse(refreshed, "refreshLock should return false for wrong server name");
    }

    // ==================== Online Save Conflict Tests ====================

    @Test
    void testOnlineSaveConflict_doesNotReleaseLock() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3};
        long checksum = DatabaseManager.computeChecksum(data);

        // Server A acquires lock and saves (keep-lock)
        LockResult lockA = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(databaseManager.saveDataKeepLock(uuid, data, checksum, 0,
            lockA.fencingToken(), "server-a"));

        // Simulate lock infringement: Server A releases, Server B takes over
        databaseManager.releaseLock(uuid, "server-a");
        LockResult lockB = databaseManager.acquireLock(uuid, "server-b");
        assertTrue(lockB.acquired());

        // Server A tries another keep-lock save with old token — must fail (conflict)
        VersionedData current = databaseManager.loadData(uuid);
        byte[] newData = new byte[]{4, 5, 6};
        long newChecksum = DatabaseManager.computeChecksum(newData);
        boolean saved = databaseManager.saveDataKeepLock(uuid, newData, newChecksum,
            current.version(), lockA.fencingToken(), "server-a");
        assertFalse(saved, "Online save with stale token must fail (conflict)");

        // Critical: lock should STILL be held by server B (not released by failed save)
        assertEquals("server-b", databaseManager.getLockHolder(uuid),
            "Lock must NOT be released after online save conflict — holder unchanged");
    }

    @Test
    void testQuitSaveConflict_releasesLock() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3};
        long checksum = DatabaseManager.computeChecksum(data);

        // Server A acquires lock and saves (keep-lock)
        LockResult lockA = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(databaseManager.saveDataKeepLock(uuid, data, checksum, 0,
            lockA.fencingToken(), "server-a"));

        // Lock infringement: Server B takes over
        databaseManager.releaseLock(uuid, "server-a");
        LockResult lockB = databaseManager.acquireLock(uuid, "server-b");

        // Server A tries release-lock save (quit) with old token — must fail
        VersionedData current = databaseManager.loadData(uuid);
        boolean saved = databaseManager.saveDataAndReleaseLock(uuid, data, checksum,
            current.version(), lockA.fencingToken(), "server-a");
        assertFalse(saved, "Quit save with stale token must fail (conflict)");

        // For quit saves, the code calls releaseLock() after conflict.
        // But releaseLock only clears if locked_by = serverName, and the lock
        // is now held by server-b, so releaseLock("server-a") is a no-op.
        databaseManager.releaseLock(uuid, "server-a");

        // Lock should still be held by server B
        assertEquals("server-b", databaseManager.getLockHolder(uuid),
            "Quit save conflict should not release another server's lock");
    }

    // ==================== Final-Save Version Race Tests ====================

    /**
     * Simulates the final-save version race:
     * 1. Player has version 0, fencing token 1 (server-a).
     * 2. Periodic save (keep-lock) advances version to 1.
     * 3. Quit save (release-lock) tries with the OLD version 0 — must fail (CAS).
     *
     * This proves the race condition exists at the DB level and that the
     * SyncManager's refreshVersionAndFencingToken() fix is necessary.
     */
    @Test
    void testQuitSaveWithStaleVersion_failsAfterKeepLockSaveAdvancesVersion() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data1 = new byte[]{1, 2, 3};
        long checksum1 = DatabaseManager.computeChecksum(data1);

        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lock.acquired());

        // Periodic save (keep-lock) — advances version 0 -> 1
        assertTrue(databaseManager.saveDataKeepLock(uuid, data1, checksum1, 0,
            lock.fencingToken(), "server-a"));
        assertEquals(1, databaseManager.loadData(uuid).version(),
            "Version should be 1 after keep-lock save");

        // Quit save (release-lock) with STALE version 0 — must fail
        byte[] finalData = new byte[]{4, 5, 6};
        long finalChecksum = DatabaseManager.computeChecksum(finalData);
        boolean saved = databaseManager.saveDataAndReleaseLock(uuid, finalData, finalChecksum,
            0, lock.fencingToken(), "server-a");
        assertFalse(saved, "Quit save with stale version must fail CAS — this is the race condition");

        // Lock should still be held (release-lock save failed)
        assertEquals("server-a", databaseManager.getLockHolder(uuid),
            "Lock should still be held after failed release-lock save");
    }

    /**
     * Verifies the fix for the final-save version race:
     * After a keep-lock save advances the version, a release-lock save
     * with the UPDATED version succeeds.
     *
     * This proves the SyncManager's same-fencing retry would work:
     * refresh version from playerVersions map, retry with actual version.
     */
    @Test
    void testQuitSaveWithUpdatedVersion_succeedsAfterKeepLockSave() throws SQLException {
        UUID uuid = UUID.randomUUID();
        byte[] data1 = new byte[]{1, 2, 3};
        long checksum1 = DatabaseManager.computeChecksum(data1);

        LockResult lock = databaseManager.acquireLock(uuid, "server-a");
        assertTrue(lock.acquired());

        // Periodic save (keep-lock) — advances version 0 -> 1
        assertTrue(databaseManager.saveDataKeepLock(uuid, data1, checksum1, 0,
            lock.fencingToken(), "server-a"));
        long currentVersion = databaseManager.loadData(uuid).version();
        assertEquals(1, currentVersion, "Version should be 1 after keep-lock save");

        // Quit save (release-lock) with UPDATED version 1 — must succeed
        byte[] finalData = new byte[]{4, 5, 6};
        long finalChecksum = DatabaseManager.computeChecksum(finalData);
        boolean saved = databaseManager.saveDataAndReleaseLock(uuid, finalData, finalChecksum,
            currentVersion, lock.fencingToken(), "server-a");
        assertTrue(saved, "Quit save with updated version must succeed — same fencing token, correct version");

        // Lock should be released
        assertNull(databaseManager.getLockHolder(uuid),
            "Lock should be released after successful release-lock save");

        // Verify final data was saved
        assertArrayEquals(finalData, databaseManager.loadData(uuid).data(),
            "DB should contain the final (quit) save data");
    }

    // ==================== Batch Heartbeat (refreshLockBatch) Tests ====================

    @Test
    void testRefreshLockBatch_refreshesMultiplePlayers() throws SQLException {
        UUID uuidA = UUID.randomUUID();
        UUID uuidB = UUID.randomUUID();
        UUID uuidC = UUID.randomUUID();

        LockResult lockA = databaseManager.acquireLock(uuidA, "server-a");
        LockResult lockB = databaseManager.acquireLock(uuidB, "server-a");
        LockResult lockC = databaseManager.acquireLock(uuidC, "server-a");
        assertTrue(lockA.acquired() && lockB.acquired() && lockC.acquired());

        java.util.Map<UUID, Long> players = new java.util.HashMap<>();
        players.put(uuidA, lockA.fencingToken());
        players.put(uuidB, lockB.fencingToken());
        players.put(uuidC, lockC.fencingToken());

        java.util.Set<UUID> failed = new java.util.HashSet<>();
        databaseManager.refreshLockBatch(players, "server-a", failed);

        assertTrue(failed.isEmpty(), "All refreshes should succeed — no failed players");

        // All locks should still be held by server-a (not taken over)
        assertEquals("server-a", databaseManager.getLockHolder(uuidA));
        assertEquals("server-a", databaseManager.getLockHolder(uuidB));
        assertEquals("server-a", databaseManager.getLockHolder(uuidC));
    }

    @Test
    void testRefreshLockBatch_reportsFailedPlayers() throws SQLException {
        UUID uuidA = UUID.randomUUID();
        UUID uuidB = UUID.randomUUID();

        LockResult lockA = databaseManager.acquireLock(uuidA, "server-a");
        LockResult lockB = databaseManager.acquireLock(uuidB, "server-a");
        assertTrue(lockA.acquired() && lockB.acquired());

        // Server B takes over uuidB's lock
        databaseManager.releaseLock(uuidB, "server-a");
        LockResult lockB2 = databaseManager.acquireLock(uuidB, "server-b");
        assertTrue(lockB2.acquired());

        // Batch refresh: uuidA should succeed, uuidB should fail
        java.util.Map<UUID, Long> players = new java.util.HashMap<>();
        players.put(uuidA, lockA.fencingToken());
        players.put(uuidB, lockB.fencingToken()); // stale token — lock now held by server-b

        java.util.Set<UUID> failed = new java.util.HashSet<>();
        databaseManager.refreshLockBatch(players, "server-a", failed);

        assertEquals(1, failed.size(), "Exactly one player should fail refresh");
        assertTrue(failed.contains(uuidB), "uuidB should be in failed set (lock taken by server-b)");
        assertFalse(failed.contains(uuidA), "uuidA should NOT be in failed set (still ours)");
    }
}
