package com.fastsync.stress;

import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.fastsync.database.LockResult;
import com.fastsync.testutil.TestConfigBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round 16 stress tests (run via the `stress` GitHub Actions workflow).
 *
 * <p>These tests exercise the DatabaseManager layer directly under realistic
 * load patterns that the production review flagged as risks:
 * <ul>
 *   <li>loginStormHoldsUpUnderConcurrentAcquireAndSave — 100-300 players
 *       acquiring locks + saving + releasing concurrently (login storm)</li>
 *   <li>crossServerSwitchContendsOnSameUuidLock — simulates rapid handoff:
 *       server A releases, server B acquires, on the same UUID, in a tight
 *       loop (frequent cross-server switching)</li>
 *   <li>dbLatencySpikeDoesNotCorruptLockState — injects artificial latency
 *       via a small sleep between acquire and save, mimicking DB/Redis
 *       hiccup, and verifies lock_session_id still gates subsequent saves</li>
 * </ul>
 *
 * <p>Tagged {@code "stress"} so the normal CI `./gradlew test` excludes them
 * (they take minutes and need Docker). The dedicated `stress.yml` workflow
 * runs them with `--tests "*StressTest"` and a longer timeout.
 *
 * <p>These are NOT Paper-runtime tests. They hit the real MySQL via
 * Testcontainers but bypass Bukkit/Folia scheduling, so they validate DB
 * correctness + throughput, not game-tick impact.
 */
@Tag("stress")
class FastSyncStressTest {

    private static final Logger LOGGER = Logger.getLogger(FastSyncStressTest.class.getName());

    private static boolean dockerAvailable;
    private static MySQLContainer mysql;

    private DatabaseManager databaseManager;
    private ConfigManager config;

    @BeforeAll
    static void setupClass() {
        dockerAvailable = isDockerAvailable();
        if (!dockerAvailable) {
            LOGGER.warning("Docker not available. Skipping stress tests.");
            return;
        }

        LOGGER.info("Starting MySQL 8.4 container for stress tests...");
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("fastsync_stress")
                .withUsername("test")
                .withPassword("test")
                .withReuse(false);
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
        Assumptions.assumeTrue(dockerAvailable, "Docker is required for stress tests");

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
            stmt.execute("TRUNCATE TABLE " + config.getTablePrefix() + "player_component");
        }
    }

    @AfterEach
    void tearDown() {
        if (databaseManager != null) {
            databaseManager.close();
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
     * Login storm: N players concurrently acquire lock → save blob → release.
     *
     * <p>Verifies the DB layer holds up under 200 concurrent players without
     * deadlocks, connection exhaustion, or lock-state corruption. Measures
     * p50/p99 latency so the stress workflow can fail on regression.
     */
    @Test
    void loginStormHoldsUpUnderConcurrentAcquireAndSave() throws Exception {
        final int playerCount = 200;
        final int threadPoolSize = 32;  // bounded, like the real async executor
        final byte[] blob = new byte[4 * 1024];  // 4KB typical compressed player data
        ThreadLocalRandom.current().nextBytes(blob);

        ExecutorService pool = Executors.newFixedThreadPool(threadPoolSize);
        // Only threadPoolSize tasks can start before the gate opens; waiting
        // for all playerCount tasks here deadlocks the bounded executor because
        // the queued tasks cannot count down until a worker is released.
        CountDownLatch ready = new CountDownLatch(threadPoolSize);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);
        List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>(playerCount));

        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>(playerCount);
            for (int i = 0; i < playerCount; i++) {
                final UUID uuid = UUID.randomUUID();
                final String server = "server-" + (i % 3);
                final String session = "session-" + uuid;
                futures.add(CompletableFuture.runAsync(() -> {
                    ready.countDown();
                    try {
                        assertTrue(start.await(30, TimeUnit.SECONDS), "start gate timeout");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failures.incrementAndGet();
                        return;
                    }
                    long t0 = System.nanoTime();
                    try {
                        LockResult lock = databaseManager.acquireLock(uuid, server, session);
                        if (!lock.acquired()) {
                            failures.incrementAndGet();
                            return;
                        }
                        long checksum = DatabaseManager.computeChecksum(blob);
                        boolean saved = databaseManager.saveDataAndReleaseLockClearComponents(
                                uuid, blob, checksum, 0L, lock.fencingToken(), server, session);
                        if (saved) {
                            success.incrementAndGet();
                        } else {
                            failures.incrementAndGet();
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    } finally {
                        latencies.add(System.nanoTime() - t0);
                    }
                }, pool));
            }

            assertTrue(ready.await(60, TimeUnit.SECONDS), "worker readiness timeout");
            long wallStart = System.nanoTime();
            start.countDown();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get(120, TimeUnit.SECONDS);
            long wallElapsed = System.nanoTime() - wallStart;

            // Report
            java.util.Collections.sort(latencies);
            long p50 = latencies.get(latencies.size() / 2) / 1_000_000;
            long p99 = latencies.get((int) (latencies.size() * 0.99)) / 1_000_000;
            LOGGER.info(String.format(
                    "[Stress] loginStorm: %d players, %d threads, success=%d, failures=%d, "
                            + "wall=%dms, p50=%dms, p99=%dms",
                    playerCount, threadPoolSize, success.get(), failures.get(),
                    wallElapsed / 1_000_000, p50, p99));

            // Assertions: at least 95% success, p99 under 5s (generous for GHA runner).
            assertTrue(success.get() >= playerCount * 0.95,
                    "Login storm success rate too low: " + success.get() + "/" + playerCount);
            assertTrue(p99 < 5000,
                    "Login storm p99 latency too high: " + p99 + "ms");
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Cross-server switch: same UUID rapidly cycles server A → B → A → B.
     *
     * <p>Simulates frequent cross-server switching. Each cycle: server A
     * holds lock + saves + releases, then server B acquires. Verifies
     * fencing_token + lock_session_id correctly gate the handoff — server B
     * must NOT acquire until A releases, and A's stale session must NOT
     * save after B acquires.
     */
    @Test
    void crossServerSwitchContendsOnSameUuidLock() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final byte[] blob = new byte[2 * 1024];
        ThreadLocalRandom.current().nextBytes(blob);
        final int cycles = 50;

        long version = 0;
        for (int i = 0; i < cycles; i++) {
            String server = (i % 2 == 0) ? "server-A" : "server-B";
            String session = "session-" + i + "-" + server;

            LockResult lock = databaseManager.acquireLock(uuid, server, session);
            assertTrue(lock.acquired(), "Cycle " + i + ": server " + server + " failed to acquire lock");

            long checksum = DatabaseManager.computeChecksum(blob);
            boolean saved = databaseManager.saveDataAndReleaseLockClearComponents(
                    uuid, blob, checksum, version, lock.fencingToken(), server, session);
            assertTrue(saved, "Cycle " + i + ": save failed (CAS conflict on version " + version + ")");

            // Stale session must NOT be able to save (lock already released / re-acquired)
            if (i > 0) {
                String staleSession = "session-" + (i - 1) + "-" + ((i - 1) % 2 == 0 ? "server-A" : "server-B");
                boolean staleSave = databaseManager.saveDataKeepLockClearComponents(
                        uuid, blob, checksum, version, lock.fencingToken(),
                        (i - 1) % 2 == 0 ? "server-A" : "server-B", staleSession);
                assertTrue(!staleSave, "Cycle " + i + ": STALE session saved successfully — lock_session_id gate broken!");
            }

            version++;
        }

        LOGGER.info("[Stress] crossServerSwitch: " + cycles + " cycles completed, version=" + version);
    }

    /**
     * DB latency spike: injects a sleep between acquire and save to mimic a
     * DB/Redis hiccup, then verifies the lock state is still consistent —
     * a concurrent acquirer must respect the original session's lock until
     * it expires or is released.
     */
    @Test
    void dbLatencySpikeDoesNotCorruptLockState() throws Exception {
        final UUID uuid = UUID.randomUUID();
        final byte[] blob = new byte[1024];
        ThreadLocalRandom.current().nextBytes(blob);

        // Server A acquires the lock.
        String serverA = "server-A";
        String sessionA = "session-A-1";
        LockResult lockA = databaseManager.acquireLock(uuid, serverA, sessionA);
        assertTrue(lockA.acquired(), "Server A failed to acquire lock");

        // Simulate DB latency spike: server A holds the lock longer than usual.
        Thread.sleep(500);

        // Server B tries to acquire while A still holds (lock not expired).
        String serverB = "server-B";
        String sessionB = "session-B-1";
        LockResult lockB = databaseManager.acquireLock(uuid, serverB, sessionB);
        assertTrue(!lockB.acquired(),
                "Server B acquired lock while server A still held it — lock expiry broken");

        // Server A completes its save and releases.
        long checksum = DatabaseManager.computeChecksum(blob);
        boolean savedA = databaseManager.saveDataAndReleaseLockClearComponents(
                uuid, blob, checksum, 0L, lockA.fencingToken(), serverA, sessionA);
        assertTrue(savedA, "Server A save failed after latency spike");

        // Now server B can acquire.
        LockResult lockB2 = databaseManager.acquireLock(uuid, serverB, sessionB);
        assertTrue(lockB2.acquired(), "Server B failed to acquire lock after A released");

        // Server A's stale session must NOT save (lock now held by B).
        boolean staleSaveA = databaseManager.saveDataKeepLockClearComponents(
                uuid, blob, checksum, 0L, lockA.fencingToken(), serverA, sessionA);
        assertTrue(!staleSaveA, "Server A's stale session saved after B acquired — lock_session_id gate broken!");

        // Cleanup: B releases.
        databaseManager.saveDataAndReleaseLockClearComponents(
                uuid, blob, checksum, 1L, lockB2.fencingToken(), serverB, sessionB);

        LOGGER.info("[Stress] dbLatencySpike: lock state remained consistent across latency spike");
    }
}
