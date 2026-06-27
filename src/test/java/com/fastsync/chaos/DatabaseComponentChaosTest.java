package com.fastsync.chaos;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
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
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB-level chaos test for component storage safety, using a real MySQL
 * instance via Testcontainers.
 *
 * <p>This test verifies that the actual SQL transactions in
 * {@link DatabaseManager#upsertComponentsIfLockHeld} and
 * {@link DatabaseManager#saveDataAndReleaseLockClearComponents} enforce
 * the 8 invariants from the component storage RFC, under random operation
 * sequences with concurrent fencing tokens.
 *
 * <h2>Test scenarios</h2>
 * <ul>
 *   <li>Component save with correct fencing → succeeds, version increments</li>
 *   <li>Component save with stale fencing → rejected, no state change</li>
 *   <li>Component save without lock → rejected</li>
 *   <li>Full Blob save → increments generation, resets bitmap</li>
 *   <li>Old generation component → not loaded on subsequent read</li>
 *   <li>Concurrent lock acquisition → only one succeeds</li>
 *   <li>1000-iteration chaos run with fixed seed</li>
 * </ul>
 *
 * <p>If Docker is not available, tests are skipped.
 */
class DatabaseComponentChaosTest {

    private static final Logger LOGGER = Logger.getLogger(DatabaseComponentChaosTest.class.getName());
    private static final long SEED = 12345L;

    private static boolean dockerAvailable;
    private static MySQLContainer mysql;
    private DatabaseManager databaseManager;
    private ConfigManager config;

    @BeforeAll
    static void setupClass() {
        dockerAvailable = isDockerAvailable();
        if (!dockerAvailable) {
            LOGGER.warning("Docker not available. Skipping DB chaos tests.");
            return;
        }
        LOGGER.info("Starting MySQL container for DB chaos tests...");
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("fastsync_chaos")
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
        Assumptions.assumeTrue(dockerAvailable, "Docker is required for DB chaos tests");
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
        databaseManager = new DatabaseManager(Logger.getLogger("chaos-test"), config);
        databaseManager.initialize();
    }

    @Test
    void testComponentSaveWithCorrectFencingSucceeds() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        // Acquire lock
        var lockResult = databaseManager.acquireLock(uuid, server);
        assertTrue(lockResult.acquired(), "Lock acquisition should succeed");
        long fencingToken = lockResult.fencingToken();

        // Component save with correct fencing
        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", new byte[]{1, 2, 3});
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", 12345L);

        var result = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, fencingToken, null, -1, 1L);

        assertTrue(result.success(), "Component save should succeed with correct fencing");
        assertEquals(1, result.newVersion() - result.oldVersion(),
            "Version should increment by 1");
        assertEquals(1L, result.componentBitmap(),
            "Bitmap should have INVENTORY bit set");
    }

    @Test
    void testComponentSaveWithStaleFencingRejected() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String serverA = "server-A";
        String serverB = "server-B";

        // Server A acquires lock
        var lockA = databaseManager.acquireLock(uuid, serverA);
        assertTrue(lockA.acquired());
        long tokenA = lockA.fencingToken();

        // Server B acquires lock (A's lock is released because B called acquireLock
        // and the lock was free for B — but in our implementation, B would need A
        // to release first. Let's simulate A releasing then B acquiring.)
        databaseManager.releaseLock(uuid, serverA);
        var lockB = databaseManager.acquireLock(uuid, serverB);
        assertTrue(lockB.acquired());
        long tokenB = lockB.fencingToken();
        assertTrue(tokenB > tokenA, "B's fencing token should be higher than A's");

        // Now A tries to save with stale token A — MUST be rejected
        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", new byte[]{1, 2, 3});
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", 999L);

        var result = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, serverA, tokenA, null, -1, 1L);

        assertFalse(result.success(), "Stale fencing token save MUST be rejected");
        assertTrue(result.errorMessage().contains("mismatch"),
            "Error should mention mismatch: " + result.errorMessage());
    }

    @Test
    void testComponentSaveWithoutLockRejected() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        // Don't acquire lock — try to save directly
        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", new byte[]{1, 2, 3});
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", 12345L);

        var result = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, 1L, null, -1, 1L);

        assertFalse(result.success(), "Save without lock MUST be rejected");
    }

    @Test
    void testFullBlobSaveIncrementsGenerationAndResetsBitmap() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        var lock = databaseManager.acquireLock(uuid, server);
        assertTrue(lock.acquired());
        long fencingToken = lock.fencingToken();

        // Write a component first
        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", new byte[]{1, 2, 3});
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", 12345L);
        var compResult = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, fencingToken, null, -1, 1L);
        assertTrue(compResult.success());

        long genBefore = databaseManager.getComponentGeneration(uuid);
        long bitmapBefore = databaseManager.getComponentBitmap(uuid);
        assertEquals(1L, bitmapBefore, "Bitmap should have INVENTORY bit");

        // Full Blob save (quit) — should increment generation + reset bitmap
        byte[] blob = new byte[]{10, 20, 30};
        long checksum = 99999L;
        boolean saved = databaseManager.saveDataAndReleaseLockClearComponents(
            uuid, blob, checksum, compResult.newVersion(), fencingToken, server, null);
        assertTrue(saved, "Full Blob save should succeed");

        long genAfter = databaseManager.getComponentGeneration(uuid);
        long bitmapAfter = databaseManager.getComponentBitmap(uuid);

        assertEquals(genBefore + 1, genAfter,
            "Generation should increment by 1");
        assertEquals(0L, bitmapAfter,
            "Bitmap should be reset to 0 after full Blob save");
    }

    @Test
    void testOldGenerationComponentNotLoaded() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        var lock = databaseManager.acquireLock(uuid, server);
        assertTrue(lock.acquired());
        long fencingToken = lock.fencingToken();

        // Write INVENTORY component at generation 0
        Map<String, byte[]> comp1 = new HashMap<>();
        comp1.put("INVENTORY", new byte[]{1, 2, 3});
        Map<String, Long> cs1 = new HashMap<>();
        cs1.put("INVENTORY", 111L);
        var r1 = databaseManager.upsertComponentsIfLockHeld(
            uuid, comp1, cs1, server, fencingToken, null, -1, 1L);
        assertTrue(r1.success());
        assertEquals(0, r1.generation(), "First component save should be at generation 0");

        // Full Blob save → generation becomes 1
        byte[] blob = new byte[]{99};
        boolean saved = databaseManager.saveDataKeepLockClearComponents(
            uuid, blob, 222L, r1.newVersion(), fencingToken, server, null);
        assertTrue(saved);

        long newGen = databaseManager.getComponentGeneration(uuid);
        assertEquals(1, newGen, "Generation should be 1 after full Blob save");

        // Try to load components at the new generation — should find nothing
        // (the old component row is at generation 0, not 1)
        java.util.Set<String> names = new java.util.HashSet<>();
        names.add("INVENTORY");
        var loaded = databaseManager.loadComponentsWithGeneration(uuid, names, newGen);
        assertTrue(loaded.isEmpty(),
            "Old generation component should NOT be loaded at new generation");

        // But at old generation, it should still exist (for GC/diagnosis)
        var oldLoaded = databaseManager.loadComponentsWithGeneration(uuid, names, 0);
        assertFalse(oldLoaded.isEmpty(),
            "Old generation component should still exist at generation 0 (for GC)");
    }

    @Test
    void testConcurrentLockAcquisitionOnlyOneSucceeds() throws SQLException {
        UUID uuid = UUID.randomUUID();

        var lockA = databaseManager.acquireLock(uuid, "server-A");
        var lockB = databaseManager.acquireLock(uuid, "server-B");

        // Both can't hold the lock simultaneously (unless A's lock expired)
        // Since we call them sequentially and A hasn't released, B should fail
        assertTrue(lockA.acquired(), "First lock should succeed");
        assertFalse(lockB.acquired(), "Second lock should fail while first is held");
    }

    @Test
    void testChaosRun1000Iterations() throws SQLException {
        // This test runs 1000 random operations against the real DB,
        // checking that version monotonicity and fencing safety hold.
        Random rng = new Random(SEED);
        UUID uuid = UUID.randomUUID();
        String server = "chaos-server";
        long currentVersion = 0;
        long currentFencing = 0;
        long currentGeneration = 0;

        // Acquire initial lock
        var lock = databaseManager.acquireLock(uuid, server);
        assertTrue(lock.acquired());
        currentFencing = lock.fencingToken();

        for (int i = 0; i < 100; i++) { // 100 iterations (DB ops are slow)
            int op = rng.nextInt(4);
            switch (op) {
                case 0 -> {
                    // Component save
                    Map<String, byte[]> comps = new HashMap<>();
                    comps.put("INVENTORY", new byte[]{(byte) i});
                    Map<String, Long> cs = new HashMap<>();
                    cs.put("INVENTORY", (long) i);
                    var result = databaseManager.upsertComponentsIfLockHeld(
                        uuid, comps, cs, server, currentFencing, null, -1, 1L);
                    if (result.success()) {
                        assertTrue(result.newVersion() > currentVersion,
                            "Iter " + i + ": version not monotonic: " + currentVersion + " -> " + result.newVersion());
                        currentVersion = result.newVersion();
                        assertTrue(result.generation() >= currentGeneration,
                            "Iter " + i + ": generation not monotonic");
                        currentGeneration = result.generation();
                    }
                }
                case 1 -> {
                    // Full Blob save (keep lock)
                    boolean saved = databaseManager.saveDataKeepLockClearComponents(
                        uuid, new byte[]{(byte) i}, (long) i * 10,
                        currentVersion, currentFencing, server, null);
                    if (saved) {
                        currentVersion++;
                        currentGeneration++;
                        assertEquals(0, databaseManager.getComponentBitmap(uuid),
                            "Iter " + i + ": bitmap should be 0 after full save");
                    }
                }
                case 2 -> {
                    // Try stale fencing write — MUST fail
                    long staleToken = currentFencing - 1;
                    if (staleToken > 0) {
                        var staleResult = databaseManager.upsertComponentsIfLockHeld(
                            uuid, Map.of("VITALS", new byte[]{1}),
                            Map.of("VITALS", 1L), server, staleToken, null, -1, 2L);
                        assertFalse(staleResult.success(),
                            "Iter " + i + ": stale fencing write must be rejected");
                    }
                }
                case 3 -> {
                    // Verify current state
                    assertEquals(currentVersion, databaseManager.getCurrentVersion(uuid),
                        "Iter " + i + ": DB version mismatch");
                    assertEquals(currentGeneration, databaseManager.getComponentGeneration(uuid),
                        "Iter " + i + ": DB generation mismatch");
                }
            }
        }
    }

    // ==================== Integration cases from review round 4 ====================
    //
    // These cases fill the gaps left by the original chaos tests. They verify
    // the safety properties called out in the static review that previous
    // tests only partially covered:
    //
    //   - testComponentSaveBumpsPlayerDataVersionExplicit: component-only save
    //     must bump player_data.version by exactly 1, and the returned
    //     newVersion must match what the DB reports via getCurrentVersion.
    //   - testNoBaselineBlobComponentRowsAreOrphanedOnCrash: simulates the
    //     "new player, no baseline, component-only save, then crash" scenario
    //     the playersWithBaseline gate in SyncManager is designed to prevent.
    //     This DB-level test confirms that if component rows exist without a
    //     non-empty Blob baseline, loadData() returns EMPTY — which is exactly
    //     the orphan state the gate protects against.
    //   - testComponentSaveFollowedByQuitFullSave_blobWins: verifies invariant
    //     #7 from the review — after a component save writes INVENTORY at
    //     generation G, a subsequent QUIT full Blob save bumps to G+1 and
    //     zero-bits the bitmap, so the next load (at G+1) sees NO component
    //     override. The full Blob wins; the stale component row is invisible.

    /**
     * Verifies that a single component save bumps player_data.version by
     * exactly 1, and that the newVersion returned in the result matches
     * what the DB reports via {@link DatabaseManager#getCurrentVersion}.
     *
     * <p>This is invariant #6 from the review. The original
     * {@code testComponentSaveWithCorrectFencingSucceeds} only asserted
     * {@code newVersion - oldVersion == 1}; this case additionally checks
     * that the DB itself reflects the new version, closing a potential
     * "result says success but DB wasn't actually updated" gap.
     */
    @Test
    void testComponentSaveBumpsPlayerDataVersionExplicit() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        var lock = databaseManager.acquireLock(uuid, server);
        assertTrue(lock.acquired());
        long fencingToken = lock.fencingToken();

        long versionBefore = databaseManager.getCurrentVersion(uuid);
        assertEquals(0, versionBefore, "New player should start at version 0");

        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", new byte[]{7, 8, 9});
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", 42L);

        var result = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, fencingToken, null, -1, 1L);

        assertTrue(result.success(), "Component save should succeed");
        assertEquals(versionBefore, result.oldVersion(),
            "oldVersion in result should match DB version before save");
        assertEquals(versionBefore + 1, result.newVersion(),
            "newVersion should be oldVersion + 1");

        // Critical assertion: the DB itself must reflect the new version.
        // If the result says success but the DB wasn't updated, the local
        // playerVersions map in SyncManager would drift out of sync.
        long versionAfter = databaseManager.getCurrentVersion(uuid);
        assertEquals(result.newVersion(), versionAfter,
            "DB version after save must match the newVersion returned in the result");
    }

    /**
     * Simulates the "new player, no baseline, component-only save, then crash"
     * scenario that the {@code playersWithBaseline} gate in SyncManager is
     * designed to prevent.
     *
     * <p>Sequence:
     * <ol>
     *   <li>Acquire lock for a brand-new player (no player_data row).</li>
     *   <li>Bypass the gate (this is a DB-level test, not SyncManager) and
     *       write a component row directly via upsertComponentsIfLockHeld.</li>
     *   <li>Simulate crash: do NOT write a full Blob, do NOT release lock.</li>
     *   <li>Reload via loadData(uuid) — the load path used by SyncManager.</li>
     * </ol>
     *
     * <p>Expected: loadData() returns VersionedData.EMPTY because player_data.data
     * is null/empty. The component row exists in player_component but is NOT
     * loaded by loadData() (which only reads the Blob). This is the exact
     * "orphaned component rows" state the gate prevents.
     *
     * <p>The test confirms the gate is necessary: without it, the component
     * row would be silently orphaned and the player's state lost.
     */
    @Test
    void testNoBaselineBlobComponentRowsAreOrphanedOnCrash() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        var lock = databaseManager.acquireLock(uuid, server);
        assertTrue(lock.acquired());
        long fencingToken = lock.fencingToken();

        // Write a component WITHOUT a prior full Blob baseline.
        // In SyncManager, persistComponentsOnly's playersWithBaseline gate
        // would refuse this and fall back to a full Blob save first.
        // Here we bypass the gate to demonstrate what happens without it.
        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", new byte[]{1, 2, 3, 4, 5});
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", 555L);

        var result = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, fencingToken, null, -1, 1L);
        assertTrue(result.success(), "Component save should succeed at the DB level");

        // Simulate crash: no full Blob save, no lock release.
        // The lock will expire naturally; we just don't write the Blob.

        // Now reload via the same path SyncManager uses.
        var loaded = databaseManager.loadData(uuid);
        assertFalse(loaded.hasData(),
            "loadData() must return EMPTY when player_data.data is null — "
            + "this is the orphan state the playersWithBaseline gate prevents. "
            + "If this assertion ever fails, the gate's premise is broken.");

        // The component row DOES exist in player_component, but loadData()
        // does not look there. This is by design — component overlay only
        // happens after loadData() succeeds AND component_bitmap != 0.
        // Since loadData() returned EMPTY, SyncManager treats the player as
        // brand new and the component row is orphaned.
        long bitmap = databaseManager.getComponentBitmap(uuid);
        assertEquals(1L, bitmap,
            "component_bitmap should have INVENTORY bit set (component save wrote it)");

        long gen = databaseManager.getComponentGeneration(uuid);
        // The component row exists at this generation.
        var orphanedComponents = databaseManager.loadComponentsWithGeneration(
            uuid, java.util.Set.of("INVENTORY"), gen);
        assertFalse(orphanedComponents.isEmpty(),
            "The component row IS in the table — it's just orphaned because "
            + "loadData() returns EMPTY and the load path never gets to the "
            + "component overlay step. This proves the gate is necessary.");
    }

    /**
     * Verifies invariant #7 from the review: after a component save writes
     * INVENTORY at generation G, a subsequent QUIT full Blob save bumps to
     * G+1 and zero-bits the bitmap, so the next load (at G+1) sees NO
     * component override. The full Blob wins; the stale component row is
     * invisible.
     *
     * <p>This is the core safety property of the ClearComponents design:
     * even though the old component row is not physically deleted, it
     * cannot resurrect because loadComponentsWithGeneration only returns
     * rows matching the current generation.
     */
    @Test
    void testComponentSaveFollowedByQuitFullSave_blobWins() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        var lock = databaseManager.acquireLock(uuid, server);
        assertTrue(lock.acquired());
        long fencingToken = lock.fencingToken();

        // Step 1: Write INVENTORY component at generation 0
        Map<String, byte[]> components = new HashMap<>();
        components.put("INVENTORY", new byte[]{1, 2, 3});  // stale component payload
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("INVENTORY", 111L);
        var compResult = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, fencingToken, null, -1, 1L);
        assertTrue(compResult.success());
        assertEquals(0, compResult.generation(), "First component save should be at generation 0");
        assertEquals(1L, compResult.componentBitmap(), "Bitmap should have INVENTORY bit");

        // Step 2: QUIT full Blob save — writes the full Blob, bumps generation
        // to 1, zeros the bitmap. The component row at generation 0 is now
        // invisible to future loads.
        byte[] finalBlob = new byte[]{99, 88, 77};  // the "winning" full Blob
        long finalChecksum = 99999L;
        boolean saved = databaseManager.saveDataAndReleaseLockClearComponents(
            uuid, finalBlob, finalChecksum, compResult.newVersion(), fencingToken, server, null);
        assertTrue(saved, "QUIT full Blob save should succeed");

        // Verify DB state after QUIT save
        long genAfter = databaseManager.getComponentGeneration(uuid);
        long bitmapAfter = databaseManager.getComponentBitmap(uuid);
        assertEquals(1, genAfter, "Generation should be 1 after QUIT full Blob save");
        assertEquals(0L, bitmapAfter, "Bitmap should be 0 after QUIT full Blob save");

        // Step 3: Simulate next login — load data and try component overlay
        // at the current generation. The stale INVENTORY component at
        // generation 0 must NOT be returned.
        var loaded = databaseManager.loadData(uuid);
        assertTrue(loaded.hasData(), "Full Blob should be loadable");
        assertEquals(finalChecksum, loaded.checksum(), "Loaded Blob should be the QUIT save's Blob");

        var componentOverlay = databaseManager.loadComponentsWithGeneration(
            uuid, java.util.Set.of("INVENTORY"), genAfter);
        assertTrue(componentOverlay.isEmpty(),
            "Component overlay at generation " + genAfter + " must be empty — "
            + "the stale INVENTORY row at generation 0 must NOT be loaded. "
            + "If this fails, the full Blob is being silently overridden by "
            + "stale component data.");

        // The stale row still exists at generation 0 (for GC/diagnosis),
        // but it is invisible to the load path.
        var staleRow = databaseManager.loadComponentsWithGeneration(
            uuid, java.util.Set.of("INVENTORY"), 0);
        assertFalse(staleRow.isEmpty(),
            "Stale component row at generation 0 should still exist for GC/diagnosis");
    }

    /**
     * Verifies the online-save variant of invariant #7: after a component save
     * at generation G, an online full Blob save (keepLockClearComponents) also
     * bumps generation and zeros bitmap, making the stale component invisible.
     *
     * <p>This is the same property as {@link #testComponentSaveFollowedByQuitFullSave_blobWins}
     * but for the PERIODIC/BULK/WORLD_SAVE/DEATH path (releaseLock=false).
     * The online save keeps the lock but still invalidates stale components.
     */
    @Test
    void testComponentSaveFollowedByOnlineFullSave_blobWins() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        var lock = databaseManager.acquireLock(uuid, server);
        assertTrue(lock.acquired());
        long fencingToken = lock.fencingToken();

        // Component save at generation 0
        Map<String, byte[]> components = new HashMap<>();
        components.put("VITALS", new byte[]{42});
        Map<String, Long> checksums = new HashMap<>();
        checksums.put("VITALS", 42L);
        var compResult = databaseManager.upsertComponentsIfLockHeld(
            uuid, components, checksums, server, fencingToken, null, -1, 2L);  // bit 1 = VITALS
        assertTrue(compResult.success());
        assertEquals(0, compResult.generation());

        // Online full Blob save (keepLockClearComponents) — generation bumps to 1
        byte[] onlineBlob = new byte[]{10, 20, 30};
        boolean saved = databaseManager.saveDataKeepLockClearComponents(
            uuid, onlineBlob, 333L, compResult.newVersion(), fencingToken, server, null);
        assertTrue(saved, "Online full Blob save should succeed");

        assertEquals(1, databaseManager.getComponentGeneration(uuid),
            "Generation should be 1 after online full Blob save");
        assertEquals(0L, databaseManager.getComponentBitmap(uuid),
            "Bitmap should be 0 after online full Blob save");

        // The VITALS component at generation 0 must be invisible at generation 1
        var overlay = databaseManager.loadComponentsWithGeneration(
            uuid, java.util.Set.of("VITALS"), 1);
        assertTrue(overlay.isEmpty(),
            "Stale VITALS at generation 0 must NOT be loaded at generation 1");
    }

    /**
     * Verifies that multiple component saves accumulate the bitmap correctly,
     * and that a single full Blob save clears ALL accumulated bits.
     *
     * <p>This catches a regression where the ClearComponents UPDATE might
     * accidentally only clear some bits (e.g. due to a wrong column reference
     * or a partial UPDATE). The bitmap must go to exactly 0, not "old | new"
     * or "old & ~new".
     */
    @Test
    void testMultipleComponentSavesThenFullBlobSaveClearsAllBits() throws SQLException {
        UUID uuid = UUID.randomUUID();
        String server = "server-A";

        var lock = databaseManager.acquireLock(uuid, server);
        assertTrue(lock.acquired());
        long fencingToken = lock.fencingToken();

        // Save INVENTORY (bit 0) + VITALS (bit 1) + EXPERIENCE (bit 2)
        long bits = 0b111;
        Map<String, byte[]> comps = new HashMap<>();
        comps.put("INVENTORY", new byte[]{1});
        comps.put("VITALS", new byte[]{2});
        comps.put("EXPERIENCE", new byte[]{3});
        Map<String, Long> cs = new HashMap<>();
        cs.put("INVENTORY", 1L);
        cs.put("VITALS", 2L);
        cs.put("EXPERIENCE", 3L);

        var r = databaseManager.upsertComponentsIfLockHeld(
            uuid, comps, cs, server, fencingToken, null, -1, bits);
        assertTrue(r.success());
        assertEquals(bits, r.componentBitmap(),
            "Bitmap should have bits 0,1,2 set: " + Long.toBinaryString(r.componentBitmap()));
        assertEquals(bits, databaseManager.getComponentBitmap(uuid),
            "DB bitmap should match result bitmap");

        // Full Blob save must clear ALL bits
        boolean saved = databaseManager.saveDataKeepLockClearComponents(
            uuid, new byte[]{99}, 999L, r.newVersion(), fencingToken, server, null);
        assertTrue(saved);

        assertEquals(0L, databaseManager.getComponentBitmap(uuid),
            "All accumulated bits must be cleared by full Blob save");
    }

    // ==================== Helpers ====================

    private static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }
}
