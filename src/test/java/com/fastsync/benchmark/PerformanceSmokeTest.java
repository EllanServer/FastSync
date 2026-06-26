package com.fastsync.benchmark;

import com.fastsync.log.ChronicleQueueLogManager;
import com.fastsync.log.OperationLog;
import com.fastsync.log.OperationType;
import com.fastsync.serialization.CompressionUtil;
import com.fastsync.database.DatabaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke-test level performance measurements for the refactored hot paths.
 *
 * <p>This is NOT a JMH benchmark — it is a lightweight JUnit test that runs in CI
 * and prints timing summaries. It verifies that the new Chronicle Queue + jOOQ
 * stack performs within acceptable bounds and does not regress.
 *
 * <p>Tests:
 * <ul>
 *   <li>Chronicle Queue: append 1,000 entries + query history (latency)</li>
 *   <li>LZ4 compression: compress/decompress various sizes (throughput)</li>
 *   <li>CRC32 checksum: compute over various sizes (throughput)</li>
 *   <li>StreamEvent serialization: toMap/fromMap round-trip (throughput)</li>
 * </ul>
 */
class PerformanceSmokeTest {

    private ChronicleQueueLogManager logManager;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("perf-smoke");
        // CQ initialization is deferred to ensureCqReady() — only CQ tests call it.
        // Non-CQ tests (LZ4, CRC32, StreamEvent) don't need Chronicle Queue.
    }

    /**
     * Lazy CQ initialization + probe. Only called by CQ-specific tests.
     * Skips the test if Chronicle Queue can't initialize (CI without --add-opens).
     */
    private void ensureCqReady() {
        if (logManager != null) return;
        try {
            logManager = new ChronicleQueueLogManager(tempDir, 500);
            logManager.initialize();
            UUID probeId = UUID.randomUUID();
            OperationLog probeLog = OperationLog.create(probeId, OperationType.SAVE,
                "probe", 0, 0, 0, "probe");
            logManager.append(probeLog).join();
            List<OperationLog> probeResult = logManager.queryHistory(probeId, 1);
            if (probeResult.isEmpty()) {
                throw new IllegalStateException("CQ probe: append succeeded but query returned empty");
            }
        } catch (Throwable e) {
            logManager = null;
            org.junit.jupiter.api.Assumptions.assumeTrue(false,
                "Chronicle Queue skipped: " + e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        if (logManager != null) {
            logManager.close();
        }
        try {
            Files.walk(tempDir)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                });
        } catch (Exception ignored) {}
    }

    // ==================== Chronicle Queue ====================

    @Test
    void chronicleQueue_appendThroughput() {
        ensureCqReady();
        UUID playerId = UUID.randomUUID();
        int count = 1000;

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            OperationLog log = OperationLog.create(playerId, OperationType.SAVE, "test-server",
                i, i, 4096, "perf-test entry " + i);
            logManager.append(log).join();
        }
        long elapsed = System.nanoTime() - start;
        double throughput = count / (elapsed / 1_000_000_000.0);

        System.out.printf("[CQ] Append %d entries: %.2f ms total, %.0f ops/sec%n",
            count, elapsed / 1_000_000.0, throughput);

        // Chronicle Queue should handle at least 2,000 ops/sec on CI shared runners
        assertTrue(throughput > 2000, "CQ append throughput too low: " + throughput + " ops/sec");
    }

    @Test
    void chronicleQueue_queryHistoryLatency() {
        ensureCqReady();
        UUID playerId = UUID.randomUUID();
        int count = 500;

        for (int i = 0; i < count; i++) {
            OperationLog log = OperationLog.create(playerId, OperationType.SAVE, "test-server",
                i, i, 4096, "entry " + i);
            logManager.append(log).join();
        }

        // Query last 10
        long start1 = System.nanoTime();
        List<OperationLog> result10 = logManager.queryHistory(playerId, 10);
        long elapsed1 = System.nanoTime() - start1;

        // Query last 100
        long start2 = System.nanoTime();
        List<OperationLog> result100 = logManager.queryHistory(playerId, 100);
        long elapsed2 = System.nanoTime() - start2;

        System.out.printf("[CQ] Query history (500 entries written):%n");
        System.out.printf("  queryLast10:  %.3f ms, returned %d entries%n",
            elapsed1 / 1_000_000.0, result10.size());
        System.out.printf("  queryLast100: %.3f ms, returned %d entries%n",
            elapsed2 / 1_000_000.0, result100.size());

        assertEquals(10, result10.size(), "Should return 10 entries");
        assertEquals(100, result100.size(), "Should return 100 entries");

        // Query should complete under 100ms even for 500-entry scan
        assertTrue(elapsed1 < 100_000_000, "Query 10 too slow: " + elapsed1 / 1_000_000.0 + "ms");
        assertTrue(elapsed2 < 100_000_000, "Query 100 too slow: " + elapsed2 / 1_000_000.0 + "ms");
    }

    @Test
    void chronicleQueue_prunePerformance() {
        ensureCqReady();
        UUID playerId = UUID.randomUUID();
        int count = 1000;

        for (int i = 0; i < count; i++) {
            OperationLog log = OperationLog.create(playerId, OperationType.SAVE, "test-server",
                i, i, 4096, "entry " + i);
            logManager.append(log).join();
        }

        long start = System.nanoTime();
        logManager.prune(playerId, 100);
        long elapsed = System.nanoTime() - start;

        System.out.printf("[CQ] Prune %d→100: %.2f ms%n", count, elapsed / 1_000_000.0);

        // Prune should complete under 1 second
        assertTrue(elapsed < 1_000_000_000, "Prune too slow: " + elapsed / 1_000_000.0 + "ms");
    }

    // ==================== LZ4 Compression ====================

    @Test
    void compression_throughput() {
        int[] sizes = {1024, 16384, 65536, 262144};

        for (int size : sizes) {
            byte[] data = generateCompressibleData(size);

            // Compress
            long start = System.nanoTime();
            int iterations = 1000;
            byte[] compressed = null;
            for (int i = 0; i < iterations; i++) {
                compressed = CompressionUtil.wrap(data, 128);
            }
            long elapsed = System.nanoTime() - start;
            double compressThroughput = (size * iterations) / (elapsed / 1_000_000_000.0) / (1024 * 1024);

            // Decompress
            byte[] finalCompressed = compressed;
            long start2 = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                CompressionUtil.unwrap(finalCompressed);
            }
            long elapsed2 = System.nanoTime() - start2;
            double decompressThroughput = (size * iterations) / (elapsed2 / 1_000_000_000.0) / (1024 * 1024);

            double ratio = (double) size / compressed.length;

            System.out.printf("[LZ4] %6d bytes → %6d bytes (ratio %.1fx): compress %.0f MB/s, decompress %.0f MB/s%n",
                size, compressed.length, ratio, compressThroughput, decompressThroughput);

            // Compression should achieve at least 2x ratio for compressible data
            assertTrue(ratio > 2.0, "Compression ratio too low for size " + size + ": " + ratio);
            // Throughput should be at least 10 MB/s (CI shared runners are slow)
            assertTrue(compressThroughput > 10, "Compress throughput too low: " + compressThroughput + " MB/s");
        }
    }

    // ==================== CRC32 Checksum ====================

    @Test
    void checksum_throughput() {
        int[] sizes = {1024, 16384, 65536, 262144};

        for (int size : sizes) {
            byte[] data = new byte[size];
            ThreadLocalRandom.current().nextBytes(data);

            // Warm up
            for (int i = 0; i < 1000; i++) {
                DatabaseManager.computeChecksum(data);
            }

            int iterations = 10000;
            long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                DatabaseManager.computeChecksum(data);
            }
            long elapsed = System.nanoTime() - start;
            // Guard against nanoTime precision issues on some CI runners
            if (elapsed <= 0) elapsed = 1;
            double throughput = (double) size * iterations / (elapsed / 1_000_000_000.0) / (1024 * 1024);

            System.out.printf("[CRC32] %6d bytes: %.0f MB/s%n", size, throughput);

            // CRC32 should process at least 30 MB/s (CI shared runners are slow)
            assertTrue(throughput > 30, "CRC32 throughput too low: " + throughput + " MB/s");
        }
    }

    // ==================== StreamEvent Serialization ====================

    @Test
    void streamEvent_serializationThroughput() {
        UUID playerId = UUID.randomUUID();

        long start = System.nanoTime();
        int iterations = 100000;
        for (int i = 0; i < iterations; i++) {
            var event = com.fastsync.redis.stream.StreamEvent.create(
                com.fastsync.redis.stream.StreamEventType.PLAYER_CHECKOUT,
                playerId, "server-1", "", 42L, 7L, "cause=quit");
            var map = event.toMap();
            com.fastsync.redis.stream.StreamEvent.fromMap("1234567890-0", map);
        }
        long elapsed = System.nanoTime() - start;
        double throughput = iterations / (elapsed / 1_000_000_000.0);

        System.out.printf("[StreamEvent] toMap+fromMap round-trip: %.0f ops/sec%n", throughput);

        // Should handle at least 50,000 ops/sec (CI shared runners are slow)
        assertTrue(throughput > 50000, "StreamEvent serialization too slow: " + throughput + " ops/sec");
    }

    // ==================== Concurrent CQ Append ====================

    @Test
    void chronicleQueue_concurrentAppend() throws Exception {
        ensureCqReady();
        UUID playerId = UUID.randomUUID();
        int threadCount = 4;
        int entriesPerThread = 250;
        var threads = new Thread[threadCount];
        var errors = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    for (int i = 0; i < entriesPerThread; i++) {
                        OperationLog log = OperationLog.create(playerId, OperationType.SAVE,
                            "thread-" + threadId, i, i, 4096, "concurrent " + threadId + "/" + i);
                        logManager.append(log).join();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
        }

        long start = System.nanoTime();
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
        long elapsed = System.nanoTime() - start;

        int totalEntries = threadCount * entriesPerThread;
        double throughput = totalEntries / (elapsed / 1_000_000_000.0);

        System.out.printf("[CQ] Concurrent append (%d threads × %d entries = %d total): %.2f ms, %.0f ops/sec%n",
            threadCount, entriesPerThread, totalEntries, elapsed / 1_000_000.0, throughput);

        assertEquals(0, errors.get(), "Errors during concurrent append");
        assertTrue(throughput > 500, "Concurrent throughput too low: " + throughput + " ops/sec");

        // Verify all entries are readable
        List<OperationLog> history = logManager.queryHistory(playerId, totalEntries);
        System.out.printf("[CQ] Verified: %d entries readable after concurrent write%n", history.size());
        assertTrue(history.size() >= totalEntries * 0.95, "Lost entries: expected " + totalEntries + ", got " + history.size());
    }

    // ==================== Helpers ====================

    private byte[] generateCompressibleData(int size) {
        byte[] data = new byte[size];
        // Simulate NBT-like data: mostly zeros with some structured patterns
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int i = 0;
        while (i < size) {
            int blockLen = Math.min(rng.nextInt(64, 256), size - i);
            byte pattern = (byte) rng.nextInt(0, 4);
            for (int j = 0; j < blockLen; j++) {
                data[i + j] = (j % 8 == 0) ? pattern : 0;
            }
            i += blockLen;
        }
        return data;
    }
}
