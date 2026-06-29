package com.fastsync.serialization;

import java.util.Arrays;

/** Verifies native codecs from the final shaded artifact, not Gradle's classpath. */
public final class ShadowRuntimeProbe {

    private ShadowRuntimeProbe() {
    }

    public static void main(String[] args) {
        byte[] raw = new byte[64 * 1024];
        Arrays.fill(raw, (byte) 42);
        CompressionUtil.setEnabled(true);
        CompressionUtil.setAlgorithm(CompressionUtil.CompressionAlgorithm.ZSTD);
        byte[] wrapped = CompressionUtil.wrap(raw, 0);
        if (!Arrays.equals(raw, CompressionUtil.unwrap(wrapped))) {
            throw new AssertionError("ZSTD round-trip from shadow JAR failed");
        }
    }
}
