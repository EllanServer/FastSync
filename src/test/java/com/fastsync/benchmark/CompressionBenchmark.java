package com.fastsync.benchmark;

import com.fastsync.serialization.CompressionUtil;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for LZ4/ZSTD compression used in player data serialization.
 *
 * <p>Player NBT data typically has lots of zeros and repeated patterns, making it
 * highly compressible. This benchmark measures throughput and compression ratio
 * for realistic data sizes.</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class CompressionBenchmark {

    @Param({"1024", "16384", "65536", "262144"})
    private int dataSize;

    @Param({"0", "16", "128"})
    private int entropy; // bytes of randomness injected per block

    @Param({"LZ4", "ZSTD"})
    private String algorithm;

    private byte[] rawData;
    private byte[] wrappedData;

    @Setup
    public void setup() {
        CompressionUtil.setEnabled(true);
        CompressionUtil.setAlgorithm(
            CompressionUtil.CompressionAlgorithm.valueOf(algorithm));
        rawData = new byte[dataSize];
        ThreadLocalRandom.current().nextBytes(rawData);

        // Zero out most bytes to simulate compressible NBT-like data
        for (int i = 0; i < dataSize; i += Math.max(1, 256 - entropy)) {
            for (int j = i; j < Math.min(i + 128, dataSize); j++) {
                rawData[j] = 0;
            }
        }

        wrappedData = CompressionUtil.wrap(rawData, 256);
    }

    @Benchmark
    public byte[] compress() {
        return CompressionUtil.wrap(rawData, 256);
    }

    @Benchmark
    public byte[] decompress() {
        return CompressionUtil.unwrap(wrappedData);
    }

}
