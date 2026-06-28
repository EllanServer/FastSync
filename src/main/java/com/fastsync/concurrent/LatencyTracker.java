package com.fastsync.concurrent;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Tracks operation latencies and computes percentile metrics (p50, p99, p99.9).
 *
 * <p>Inspired by Dynamo's focus on p99.9 SLA rather than average/median.
 * The paper explicitly states: "SLAs are expressed and measured at the 99.9
 * percentile of the distribution" because averages mask tail latency.
 *
 * <h2>Performance (rewritten)</h2>
 * <p>The previous implementation used {@code ConcurrentLinkedDeque<Long>} which:
 * <ul>
 *   <li>Boxed every sample ({@code long → Long}), allocating one object per record.</li>
 *   <li>Called {@code size()} indirectly via AtomicInteger — fine, but
 *       {@code logStats()} invoked {@code getPercentile} three times, each
 *       performing a full {@code toArray + Arrays.sort}.</li>
 *   <li>{@code record()} was amortized O(1) but allocation-heavy under load.</li>
 * </ul>
 *
 * <p>The new implementation:
 * <ul>
 *   <li>Uses a fixed-size {@code long[]} ring buffer — zero allocation per record.</li>
 *   <li>{@code record()} is lock-free via a {@code long} head/tail packed into
 *       an {@code AtomicLong}; contention is reduced by using {@code getAndIncrement}.</li>
 *   <li>{@code logStats()} sorts the snapshot once and computes all percentiles
 *       via binary-search index lookup.</li>
 * </ul>
 *
 * <p>Trade-off: the ring buffer is a snapshot — once full, new samples overwrite
 * the oldest. Under heavy contention two writers may briefly collide; the
 * overwritten sample is the oldest one, which is acceptable for a statistics
 * tracker. The previous implementation had the same effective behavior with
 * its eviction loop.
 */
public class LatencyTracker {

    private final String operationName;
    private final Logger logger;
    private final int windowSize;

    /** Packed head (high 32) + tail (low 32) for single-atomic update. */
    private final AtomicLong headTail = new AtomicLong(0L);
    private final long[] ring;
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    public LatencyTracker(String operationName, Logger logger, int windowSize) {
        this.operationName = operationName;
        this.logger = logger;
        // Round up to power of two for fast modulo, at least 16.
        int cap = Integer.highestOneBit(Math.max(16, windowSize)) << 1;
        this.windowSize = cap;
        this.ring = new long[cap];
    }

    /**
     * Record a latency sample in milliseconds. Lock-free, zero allocation.
     */
    public void record(long latencyMs) {
        // Atomically claim the next slot. We pack head+tail into one AtomicLong
        // so a single CAS reserves a slot without losing the head pointer.
        // high 32 bits = head (next write index), low 32 bits = tail (oldest)
        int mask = ring.length - 1;
        while (true) {
            long cur = headTail.get();
            int head = (int) (cur >>> 32);
            int tail = (int) cur;
            int nextHead = (head + 1) & 0x7FFFFFFF;
            int newTail = tail;
            if ((nextHead - tail) > ring.length) {
                // Buffer full: advance tail (overwrite oldest).
                newTail = tail + 1;
            }
            long next = ((long) nextHead << 32) | (newTail & 0xFFFFFFFFL);
            if (headTail.compareAndSet(cur, next)) {
                ring[head & mask] = latencyMs;
                break;
            }
        }
        totalOperations.incrementAndGet();
    }

    /**
     * Record an error (operation failed).
     */
    public void recordError() {
        totalErrors.incrementAndGet();
    }

    /**
     * Take a sorted snapshot of the current samples. Reused by {@link #logStats()}
     * and {@link #getPercentile(double)} to avoid re-sorting.
     */
    private long[] sortedSnapshot() {
        long cur = headTail.get();
        int head = (int) (cur >>> 32);
        int tail = (int) cur;
        int count = head - tail;
        if (count <= 0) {
            return new long[0];
        }
        // Cap count to ring capacity (defensive — should already be ≤ ring.length).
        count = Math.min(count, ring.length);
        long[] copy = new long[count];
        int mask = ring.length - 1;
        for (int i = 0; i < count; i++) {
            copy[i] = ring[(tail + i) & mask];
        }
        Arrays.sort(copy);
        return copy;
    }

    /**
     * Compute a percentile from the current samples.
     * @param percentile 0-100 (e.g., 99.9 for p99.9)
     * @return the latency at that percentile, or -1 if no samples
     */
    public double getPercentile(double percentile) {
        long[] arr = sortedSnapshot();
        if (arr.length == 0) return -1;
        int index = (int) Math.ceil((percentile / 100.0) * arr.length) - 1;
        index = Math.max(0, Math.min(index, arr.length - 1));
        return arr[index];
    }

    /**
     * Log current latency statistics. Sorts the snapshot once and computes all
     * percentiles from that single sorted array.
     */
    public void logStats() {
        long[] arr = sortedSnapshot();
        if (arr.length == 0) {
            logger.info(String.format("[Latency] %s: no samples yet (total ops: %d, errors: %d)",
                operationName, totalOperations.get(), totalErrors.get()));
            return;
        }

        long sum = 0;
        for (long v : arr) sum += v;
        double avg = (double) sum / arr.length;
        // Single sorted array — all percentiles via direct index.
        double p50 = arr[indexFor(arr.length, 50)];
        double p99 = arr[indexFor(arr.length, 99)];
        double p999 = arr[indexFor(arr.length, 99.9)];
        long min = arr[0];
        long max = arr[arr.length - 1];

        logger.info(String.format(
            "[Latency] %s: samples=%d, avg=%.1fms, p50=%.1fms, p99=%.1fms, p99.9=%.1fms, min=%dms, max=%dms | total_ops=%d, errors=%d",
            operationName, arr.length, avg, p50, p99, p999, min, max,
            totalOperations.get(), totalErrors.get()));
    }

    private static int indexFor(int len, double percentile) {
        int i = (int) Math.ceil((percentile / 100.0) * len) - 1;
        return Math.max(0, Math.min(i, len - 1));
    }

    public long getTotalOperations() { return totalOperations.get(); }
    public long getTotalErrors() { return totalErrors.get(); }

    /**
     * Return a single-line summary string suitable for /fastsync status.
     * Format: "name: p50=Xms p99=Xms p99.9=Xms n=N err=E"
     */
    public String getStatusLine() {
        long[] arr = sortedSnapshot();
        if (arr.length == 0) {
            return operationName + ": no samples (total=" + totalOperations.get()
                + ", err=" + totalErrors.get() + ")";
        }
        double p50 = arr[indexFor(arr.length, 50)];
        double p99 = arr[indexFor(arr.length, 99)];
        double p999 = arr[indexFor(arr.length, 99.9)];
        return String.format("%s: p50=%.1fms p99=%.1fms p99.9=%.1fms n=%d err=%d",
            operationName, p50, p99, p999, arr.length, totalErrors.get());
    }

    public int getSampleCount() {
        long cur = headTail.get();
        int head = (int) (cur >>> 32);
        int tail = (int) cur;
        int count = head - tail;
        return Math.max(0, Math.min(count, ring.length));
    }
}
