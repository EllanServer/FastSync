package com.fastsync.chaos;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.fastsync.serialization.CompressionUtil;
import com.fastsync.serialization.CorruptDataException;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that corrupt component rows are detectable by the same mechanisms
 * the SyncManager load path relies on for its fail-closed behaviour.
 *
 * <p>Round 3 directive #2: when a component checksum mismatches, unwrap fails,
 * or deserialize fails, the entire load must fail (release lock, reject login,
 * no baseline save). The SyncManager load path itself requires a Paper runtime,
 * but the detection primitives it calls — {@link CompressionUtil#unwrap} and
 * {@link DatabaseManager#verifyChecksum} — are pure and can be tested directly.
 *
 * <p>This test stores corrupt data via the real DB (Testcontainers MySQL 8.4)
 * and asserts that:
 * <ol>
 *   <li>Garbage bytes that fail {@link CompressionUtil#unwrap} throw
 *       {@link CorruptDataException} — the SyncManager load block catches this
 *       and aborts the login rather than applying a stale baseline.</li>
 *   <li>Validly-wrapped bytes paired with a wrong checksum are rejected by
 *       {@link DatabaseManager#verifyChecksum} — the SyncManager load block
 *       treats this as corruption and aborts.</li>
 *   <li>{@link DatabaseManager#loadComponentsWithGeneration} returns the raw
 *       stored bytes, so the upper layer can run its detection without the DB
 *       layer silently swallowing corruption.</li>
 * </ol>
 *
 * <p>If Docker is not available, tests are skipped.
 */
class CorruptComponentRejectionTest {

    private static final Logger LOGGER = Logger.getLogger(CorruptComponentRejectionTest.class.getName());

    private static boolean dockerAvailable;
    private static MySQLContainer mysql;
    private DatabaseManager databaseManager;
    private ConfigManager config;

    @BeforeAll
    static void setupClass() {
        dockerAvailable = isDockerAvailable();
        if (!dockerAvailable) {
            LOGGER.warning("Docker not available. Skipping corrupt component tests.");
            return;
        }
        LOGGER.info("Starting MySQL container for corrupt component tests...");
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("fastsync_corrupt")
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
        Assumptions.assumeTrue(dockerAvailable, "Docker is required for corrupt component tests");
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
        databaseManager = new DatabaseManager(Logger.getLogger("corrupt-test"), config);
        databaseManager.initialize();
    }

    /**
     * Garbage bytes stored as a component row must be rejected by
     * {@link CompressionUtil#unwrap}. This is the exact exception the
     * SyncManager load path catches to abort the login (release lock, kick,
     * no baseline save) instead of silently falling back to the stale Blob.
     */
    @Test
    void corruptComponentBytesRejectedByUnwrap() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";
        String session = "corrupt-unwrap-session";

        var lock = databaseManager.acquireLock(uuid, server, session);
        assertTrue(lock.acquired(), "Lock acquisition should succeed");
        long fencingToken = lock.fencingToken();

        // Write a baseline full Blob so a naive loader would have something
        // to "fall back to" — the corruption must NOT let that happen.
        byte[] baselineBlob = CompressionUtil.wrap(new byte[]{10, 20, 30, 40}, 0);
        long baselineChecksum = DatabaseManager.computeChecksum(baselineBlob);
        boolean saved = databaseManager.saveDataKeepLockClearComponents(
            uuid, baselineBlob, baselineChecksum, 0, fencingToken, server, session);
        assertTrue(saved, "Baseline Blob save should succeed");

        // Store garbage bytes as the INVENTORY component. These are NOT valid
        // wrapped data — unwrap must reject them.
        byte[] garbage = new byte[]{(byte) 0xFF, (byte) 0xEE, (byte) 0xDD, (byte) 0xCC};
        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", garbage);
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", DatabaseManager.computeChecksum(garbage));

        var result = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, fencingToken, session, -1, 1L);
        assertTrue(result.success(), "Component save should succeed at DB level (raw bytes)");

        // Read the raw stored bytes back — the DB layer stores them as-is.
        long gen = databaseManager.getComponentGeneration(uuid);
        Map<String, DatabaseManager.ComponentData> loaded =
            databaseManager.loadComponentsWithGeneration(uuid, Set.of("INVENTORY"), gen);
        assertEquals(1, loaded.size(), "Component row should be retrievable");
        byte[] storedBytes = loaded.get("INVENTORY").data();
        assertArrayEquals(garbage, storedBytes, "DB should return raw stored bytes");

        // The detection: unwrap MUST throw CorruptDataException on garbage.
        // SyncManager's load block catches this and aborts the login.
        CorruptDataException ex = assertThrows(CorruptDataException.class,
            () -> CompressionUtil.unwrap(storedBytes),
            "Corrupt component bytes must be rejected by unwrap — this is the "
                + "detection the SyncManager load path relies on to abort login");
        assertNotNull(ex.getMessage(), "CorruptDataException must carry a message");
    }

    /**
     * Validly-wrapped component bytes paired with a WRONG checksum must be
     * rejected by {@link DatabaseManager#verifyChecksum}. The SyncManager load
     * path treats a checksum mismatch as corruption and aborts the login.
     */
    @Test
    void validBytesWithWrongChecksumRejected() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";
        String session = "corrupt-checksum-session";

        var lock = databaseManager.acquireLock(uuid, server, session);
        assertTrue(lock.acquired());
        long fencingToken = lock.fencingToken();

        // Write a baseline Blob.
        byte[] baselineBlob = CompressionUtil.wrap(new byte[]{1, 2, 3}, 0);
        long baselineChecksum = DatabaseManager.computeChecksum(baselineBlob);
        assertTrue(databaseManager.saveDataKeepLockClearComponents(
            uuid, baselineBlob, baselineChecksum, 0, fencingToken, server, session));

        // Store a validly-wrapped INVENTORY component but with a DELIBERATELY
        // WRONG checksum. The bytes unwrap fine, but verifyChecksum must fail.
        byte[] validWrapped = CompressionUtil.wrap(new byte[]{7, 8, 9, 10, 11}, 0);
        long wrongChecksum = 99999L; // not the real checksum
        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", validWrapped);
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", wrongChecksum);

        var result = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, fencingToken, session, -1, 1L);
        assertTrue(result.success());

        // Read back — the stored checksum is the wrong one we supplied.
        long gen = databaseManager.getComponentGeneration(uuid);
        Map<String, DatabaseManager.ComponentData> loaded =
            databaseManager.loadComponentsWithGeneration(uuid, Set.of("INVENTORY"), gen);
        DatabaseManager.ComponentData comp = loaded.get("INVENTORY");
        assertNotNull(comp, "Component row should exist");

        // The bytes are valid (unwrap succeeds)...
        byte[] unwrapped = CompressionUtil.unwrap(comp.data());
        assertArrayEquals(new byte[]{7, 8, 9, 10, 11}, unwrapped,
            "Valid wrapped bytes should unwrap correctly");

        // ...but the checksum does NOT match — verifyChecksum must return false.
        // SyncManager's load block treats this as corruption and aborts login.
        assertFalse(DatabaseManager.verifyChecksum(comp.data(), comp.checksum()),
            "Wrong checksum must be rejected by verifyChecksum — this is the "
                + "detection the SyncManager load path relies on to abort login");
        assertEquals(wrongChecksum, comp.checksum(),
            "Stored checksum should be the wrong value we supplied");
    }

    private static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }
}
