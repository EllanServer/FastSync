package com.fastsync.concurrent;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tracks operation latencies and computes percentile metrics (p50, p99, p99.9).
 *
 * Inspired by Dynamo's focus on p99.9 SLA rather than average/median.
 * The paper explicitly states: "SLAs are expressed and measured at the 99.9
 * percentile of the distribution" because averages mask tail latency.
 *
 * Uses a sliding window of the last N samples to keep memory bounded.
 *
 * <p><b>Performance note:</b> Samples are stored in a preallocated ring
 * buffer backed by an {@link AtomicLongArray} with an {@link AtomicInteger}
 * head pointer. This avoids boxing every {@code long} into a {@code Long}
 * (allocation pressure) and keeps {@code record()} amortized O(1) regardless
 * of window size. {@link AtomicLongArray} provides atomic reads/writes, so
 * {@code snapshot()} can safely read while {@code record()} is writing on
 * another thread — no torn 64-bit reads on 32-bit JVMs, no data races.
 * Percentile computation snapshots the buffer into a fresh {@code long[]}
 * and sorts it once.
 */
public class LatencyTracker {

    private final String operationName;
    private final Logger logger;
    private final int windowSize;
    private final AtomicLongArray buffer;
    private final AtomicInteger head = new AtomicInteger(0);
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    public LatencyTracker(String operationName, Logger logger, int windowSize) {
        this.operationName = operationName;
        this.logger = logger;
        this.windowSize = windowSize;
        this.buffer = new AtomicLongArray(windowSize);
    }

    /**
     * Record a latency sample in milliseconds.
     *
     * <p>Amortized O(1): writes straight into a preallocated {@link AtomicLongArray}
     * ring buffer at the index returned by the head pointer, avoiding both
     * boxing and eviction bookkeeping. {@code count} is atomically capped at
     * {@code windowSize}; once the buffer is full the head pointer simply
     * wraps around and overwrites the oldest sample.
     */
    public void record(long latencyMs) {
        int idx = head.getAndIncrement();
        buffer.set(idx % windowSize, latencyMs);
        // Track how many slots hold valid data, capped at windowSize.
        count.getAndUpdate(c -> Math.min(c + 1, windowSize));
        totalOperations.incrementAndGet();
    }

    /**
     * Record an error (operation failed).
     */
    public void recordError() {
        totalErrors.incrementAndGet();
    }

    /**
     * Compute a percentile from the current samples.
     * @param percentile 0-100 (e.g., 99.9 for p99.9)
     * @return the latency at that percentile, or -1 if no samples
     */
    public double getPercentile(double percentile) {
        long[] arr = snapshot();
        if (arr.length == 0) return -1;

        Arrays.sort(arr);
        return percentileFromSorted(arr, percentile);
    }

    /**
     * Snapshot the current ring-buffer contents into a fresh {@code long[]}.
     * The returned array is unsorted and safe for the caller to mutate.
     */
    private long[] snapshot() {
        int n = Math.min(count.get(), windowSize);
        long[] arr = new long[n];
        for (int i = 0; i < n; i++) {
            arr[i] = buffer.get(i);
        }
        return arr;
    }

    /**
     * Compute a percentile from a <em>sorted</em> array.
     */
    private static double percentileFromSorted(long[] sorted, double percentile) {
        int index = (int) Math.ceil((percentile / 100.0) * sorted.length) - 1;
        index = Math.max(0, Math.min(index, sorted.length - 1));
        return sorted[index];
    }

    /**
     * Log current latency statistics.
     *
     * <p>Snapshots and sorts the ring buffer <em>once</em>, then derives
     * p50/p99/p99.9/min/max/avg from that single sorted array instead of
     * re-snapshotting and re-sorting for each percentile.
     */
    public void logStats() {
        long[] arr = snapshot();
        if (arr.length == 0) {
            logger.info(String.format("[Latency] %s: no samples yet (total ops: %d, errors: %d)",
                operationName, totalOperations.get(), totalErrors.get()));
            return;
        }

        Arrays.sort(arr); // single sort for all derived metrics

        long sum = 0;
        for (long v : arr) sum += v;
        double avg = (double) sum / arr.length;
        double p50 = percentileFromSorted(arr, 50);
        double p99 = percentileFromSorted(arr, 99);
        double p999 = percentileFromSorted(arr, 99.9);
        long min = arr[0];
        long max = arr[arr.length - 1];

        logger.info(String.format(
            "[Latency] %s: samples=%d, avg=%.1fms, p50=%.1fms, p99=%.1fms, p99.9=%.1fms, min=%dms, max=%dms | total_ops=%d, errors=%d",
            operationName, arr.length, avg, p50, p99, p999, min, max,
            totalOperations.get(), totalErrors.get()));
    }

    public long getTotalOperations() { return totalOperations.get(); }
    public long getTotalErrors() { return totalErrors.get(); }
    public int getSampleCount() { return count.get(); }
}
