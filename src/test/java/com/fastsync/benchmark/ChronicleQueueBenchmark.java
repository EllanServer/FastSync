package com.fastsync.benchmark;

import com.fastsync.log.ChronicleQueueLogManager;
import com.fastsync.log.OperationLog;
import com.fastsync.log.OperationType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for Chronicle Queue append-only journal performance.
 *
 * <p>Measures write throughput and query latency of the new local journal
 * that replaced the SQL-backed operation_log table.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class ChronicleQueueBenchmark {

    private ChronicleQueueLogManager logManager;
    private UUID playerId;
    private OperationLog sampleLog;
    private Path tempDir;

    @Setup
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory("cq-bench");
        try {
            logManager = new ChronicleQueueLogManager(tempDir, 1000);
            logManager.initialize();
        } catch (Throwable e) {
            // Skip if Chronicle Queue can't initialize (CI without --add-opens)
            throw new IllegalStateException("CQ benchmark skipped: " + e.getMessage(), e);
        }
        playerId = UUID.randomUUID();
        sampleLog = OperationLog.create(playerId, OperationType.SAVE, "bench-server",
            42L, 10L, 4096, "benchmark save");
    }

    @TearDown
    public void tearDown() {
        if (logManager != null) {
            logManager.close();
        }
        // Clean up temp dir
        try {
            Files.walk(tempDir)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                });
        } catch (Exception ignored) {}
    }

    @Benchmark
    public void appendSync(Blackhole bh) {
        logManager.append(sampleLog).join();
        bh.consume(sampleLog);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public List<OperationLog> queryHistory10() {
        return logManager.queryHistory(playerId, 10);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public List<OperationLog> queryHistory100() {
        return logManager.queryHistory(playerId, 100);
    }
}
