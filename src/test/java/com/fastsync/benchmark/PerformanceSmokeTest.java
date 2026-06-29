package com.fastsync.benchmark;

import com.fastsync.log.FileOperationLogManager;
import com.fastsync.log.OperationLog;
import com.fastsync.log.OperationType;
import com.fastsync.serialization.CompressionUtil;
import com.fastsync.database.DatabaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke-test level performance measurements for the hot paths.
 *
 * <p>This is NOT a JMH benchmark — it is a lightweight JUnit test that runs in CI
 * and prints timing summaries. It verifies that the file-based operation log,
 * LZ4/ZSTD compression, CRC32 checksum, and StreamEvent serialization perform within
 * acceptable bounds.
 *
 * <p>All tests use only public Java APIs — no {@code --add-opens} required.
 */
class PerformanceSmokeTest {

    private FileOperationLogManager logManager;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        CompressionUtil.setEnabled(true);
        CompressionUtil.setAlgorithm(CompressionUtil.CompressionAlgorithm.LZ4);
        tempDir = Files.createTempDirectory("perf-smoke");
        logManager = new FileOperationLogManager(tempDir, 500);
        logManager.initialize();
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

    // ==================== File Operation Log ====================

    @Test
    void fileLog_appendThroughput() {
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

        System.out.printf("[FileLog] Append %d entries: %.2f ms total, %.0f ops/sec%n",
            count, elapsed / 1_000_000.0, throughput);

        // File-based log should handle at least 5,000 ops/sec
        assertTrue(throughput > 5000, "FileLog append throughput too low: " + throughput + " ops/sec");
    }

    @Test
    void fileLog_queryHistoryLatency() {
        UUID playerId = UUID.randomUUID();
        int count = 500;

        for (int i = 0; i < count; i++) {
            OperationLog log = OperationLog.create(playerId, OperationType.SAVE, "test-server",
                i, i, 4096, "entry " + i);
            logManager.append(log).join();
        }

        long start1 = System.nanoTime();
        List<OperationLog> result10 = logManager.queryHistory(playerId, 10);
        long elapsed1 = System.nanoTime() - start1;

        long start2 = System.nanoTime();
        List<OperationLog> result100 = logManager.queryHistory(playerId, 100);
        long elapsed2 = System.nanoTime() - start2;

        System.out.printf("[FileLog] Query history (500 entries written):%n");
        System.out.printf("  queryLast10:  %.3f ms, returned %d entries%n",
            elapsed1 / 1_000_000.0, result10.size());
        System.out.printf("  queryLast100: %.3f ms, returned %d entries%n",
            elapsed2 / 1_000_000.0, result100.size());

        assertEquals(10, result10.size(), "Should return 10 entries");
        assertEquals(100, result100.size(), "Should return 100 entries");

        assertTrue(elapsed1 < 100_000_000, "Query 10 too slow: " + elapsed1 / 1_000_000.0 + "ms");
        assertTrue(elapsed2 < 100_000_000, "Query 100 too slow: " + elapsed2 / 1_000_000.0 + "ms");
    }

    @Test
    void fileLog_prunePerformance() {
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

        System.out.printf("[FileLog] Prune %d→100: %.2f ms%n", count, elapsed / 1_000_000.0);

        assertTrue(elapsed < 1_000_000_000, "Prune too slow: " + elapsed / 1_000_000.0 + "ms");
    }

    @Test
    void fileLog_concurrentAppend() throws Exception {
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

        System.out.printf("[FileLog] Concurrent append (%d threads × %d entries = %d total): %.2f ms, %.0f ops/sec%n",
            threadCount, entriesPerThread, totalEntries, elapsed / 1_000_000.0, throughput);

        assertEquals(0, errors.get(), "Errors during concurrent append");
        assertTrue(throughput > 1000, "Concurrent throughput too low: " + throughput + " ops/sec");

        List<OperationLog> history = logManager.queryHistory(playerId, totalEntries);
        System.out.printf("[FileLog] Verified: %d entries readable after concurrent write%n", history.size());
        assertTrue(history.size() >= totalEntries * 0.95, "Lost entries: expected " + totalEntries + ", got " + history.size());
    }

    // ==================== LZ4 Compression ====================

    @Test
    void compression_throughput() {
        int[] sizes = {1024, 16384, 65536, 262144};

        for (CompressionUtil.CompressionAlgorithm algorithm
                : CompressionUtil.CompressionAlgorithm.values()) {
            CompressionUtil.setAlgorithm(algorithm);
            for (int size : sizes) {
                byte[] data = generateCompressibleData(size);

                long start = System.nanoTime();
                int iterations = 1000;
                byte[] compressed = null;
                for (int i = 0; i < iterations; i++) {
                    compressed = CompressionUtil.wrap(data, 128);
                }
                long elapsed = System.nanoTime() - start;
                double compressThroughput = (size * iterations)
                    / (elapsed / 1_000_000_000.0) / (1024 * 1024);

                byte[] finalCompressed = compressed;
                long start2 = System.nanoTime();
                for (int i = 0; i < iterations; i++) {
                    CompressionUtil.unwrap(finalCompressed);
                }
                long elapsed2 = System.nanoTime() - start2;
                double decompressThroughput = (size * iterations)
                    / (elapsed2 / 1_000_000_000.0) / (1024 * 1024);

                double ratio = (double) size / compressed.length;

                System.out.printf("[%s] %6d bytes → %6d bytes (ratio %.1fx): compress %.0f MB/s, decompress %.0f MB/s%n",
                    algorithm, size, compressed.length, ratio,
                    compressThroughput, decompressThroughput);

                assertTrue(ratio > 2.0,
                    algorithm + " ratio too low for size " + size + ": " + ratio);
                assertTrue(compressThroughput > 10,
                    algorithm + " throughput too low: " + compressThroughput + " MB/s");
            }
        }
    }

    // ==================== CRC32 Checksum ====================

    @Test
    void checksum_throughput() {
        int[] sizes = {1024, 16384, 65536, 262144};

        for (int size : sizes) {
            byte[] data = new byte[size];
            ThreadLocalRandom.current().nextBytes(data);

            for (int i = 0; i < 1000; i++) {
                DatabaseManager.computeChecksum(data);
            }

            int iterations = 10000;
            long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                DatabaseManager.computeChecksum(data);
            }
            long elapsed = System.nanoTime() - start;
            if (elapsed <= 0) elapsed = 1;
            double throughput = (double) size * iterations / (elapsed / 1_000_000_000.0) / (1024 * 1024);

            System.out.printf("[CRC32] %6d bytes: %.0f MB/s%n", size, throughput);

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

        assertTrue(throughput > 50000, "StreamEvent serialization too slow: " + throughput + " ops/sec");
    }

    // ==================== Helpers ====================

    private byte[] generateCompressibleData(int size) {
        byte[] data = new byte[size];
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
