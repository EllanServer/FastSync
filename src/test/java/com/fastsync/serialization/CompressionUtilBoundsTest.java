package com.fastsync.serialization;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bounds-checking tests for {@link CompressionUtil#unwrap(byte[])}.
 *
 * <p>The decompression path reads the declared original length straight out
 * of the blob header. These tests verify that a corrupted / poisoned header
 * cannot cause an OOM or {@code NegativeArraySizeException}, and that LZ4
 * decompression failures are surfaced as {@link CorruptDataException} rather
 * than propagated as random exceptions.
 *
 * <p>Pure-Java (LZ4 only, no Paper runtime), so this runs in the standard
 * unit-test suite without Testcontainers.
 */
class CompressionUtilBoundsTest {

    @AfterEach
    void resetLimits() {
        // Restore defaults so one test's configureLimits() call can't leak
        // into another (the limits are static/volatile).
        CompressionUtil.configureLimits(
            CompressionUtil.DEFAULT_MAX_RAW_BYTES,
            CompressionUtil.DEFAULT_MAX_WRAPPED_BYTES);
    }

    @Test
    void unwrapRejectsOversizedDeclaredOriginalLength() {
        // Build a compressed-frame header that declares a 2 GiB original
        // length, far above the default 1 MiB cap. The payload body is
        // irrelevant — the cap must fire before any allocation.
        byte[] wrapped = new byte[6];
        wrapped[0] = CompressionUtil.FORMAT_VERSION;
        wrapped[1] = CompressionUtil.FLAG_COMPRESSED;
        // 0x7FFFFFFF == Integer.MAX_VALUE as big-endian int
        wrapped[2] = 0x7F;
        wrapped[3] = (byte) 0xFF;
        wrapped[4] = (byte) 0xFF;
        wrapped[5] = (byte) 0xFF;

        CorruptDataException ex = assertThrows(CorruptDataException.class,
            () -> CompressionUtil.unwrap(wrapped));
        assertTrue(ex.getMessage().contains("exceeds limit"),
            "Message should mention the limit: " + ex.getMessage());
    }

    @Test
    void unwrapRejectsNegativeOriginalLength() {
        // 0x80000000 == Integer.MIN_VALUE as big-endian int → negative after sign extension.
        byte[] wrapped = new byte[6];
        wrapped[0] = CompressionUtil.FORMAT_VERSION;
        wrapped[1] = CompressionUtil.FLAG_COMPRESSED;
        wrapped[2] = (byte) 0x80;
        wrapped[3] = 0x00;
        wrapped[4] = 0x00;
        wrapped[5] = 0x00;

        assertThrows(CorruptDataException.class, () -> CompressionUtil.unwrap(wrapped));
    }

    @Test
    void unwrapRejectsZeroOriginalLength() {
        byte[] wrapped = new byte[6];
        wrapped[0] = CompressionUtil.FORMAT_VERSION;
        wrapped[1] = CompressionUtil.FLAG_COMPRESSED;
        // originalLength = 0
        assertThrows(CorruptDataException.class, () -> CompressionUtil.unwrap(wrapped));
    }

    @Test
    void unwrapRejectsOversizedWrappedPayload() {
        // Lower the wrapped cap so we can build a small payload that exceeds it
        // without allocating a huge array.
        CompressionUtil.configureLimits(1 << 20, 16);
        byte[] wrapped = new byte[32];
        wrapped[0] = CompressionUtil.FORMAT_VERSION;
        wrapped[1] = 0; // uncompressed
        CorruptDataException ex = assertThrows(CorruptDataException.class,
            () -> CompressionUtil.unwrap(wrapped));
        assertTrue(ex.getMessage().contains("Wrapped payload exceeds limit"),
            "Message should mention wrapped payload limit: " + ex.getMessage());
    }

    @Test
    void unwrapRejectsTooShortCompressedFrame() {
        // Compressed flag set but fewer than 6 bytes total → no room for the
        // 4-byte original-length field.
        byte[] wrapped = {CompressionUtil.FORMAT_VERSION, CompressionUtil.FLAG_COMPRESSED, 0x00, 0x01};
        assertThrows(CorruptDataException.class, () -> CompressionUtil.unwrap(wrapped));
    }

    @Test
    void unwrapRejectsUnsupportedVersion() {
        byte[] wrapped = {99, 0, 0, 0};
        CorruptDataException ex = assertThrows(CorruptDataException.class,
            () -> CompressionUtil.unwrap(wrapped));
        assertTrue(ex.getMessage().contains("Unsupported format version"));
    }

    @Test
    void unwrapRejectsNullAndTooShort() {
        assertThrows(CorruptDataException.class, () -> CompressionUtil.unwrap(null));
        assertThrows(CorruptDataException.class, () -> CompressionUtil.unwrap(new byte[]{1}));
    }

    @Test
    void unwrapSurfacesLz4FailureAsCorruptDataException() {
        // Declare a plausible original length but provide garbage compressed
        // bytes. LZ4 must reject it; the rejection must be wrapped as
        // CorruptDataException (not a raw LZ4Exception).
        byte[] wrapped = new byte[10];
        wrapped[0] = CompressionUtil.FORMAT_VERSION;
        wrapped[1] = CompressionUtil.FLAG_COMPRESSED;
        // originalLength = 4 (small, under the cap)
        wrapped[2] = 0; wrapped[3] = 0; wrapped[4] = 0; wrapped[5] = 4;
        // garbage "compressed" body
        wrapped[6] = (byte) 0xDE; wrapped[7] = (byte) 0xAD;
        wrapped[8] = (byte) 0xBE; wrapped[9] = (byte) 0xEF;

        assertThrows(CorruptDataException.class, () -> CompressionUtil.unwrap(wrapped));
    }

    @Test
    void roundTripStillWorksAfterBoundsChecks() {
        // Sanity: legitimate data must still round-trip through wrap/unwrap.
        byte[] raw = new byte[2048];
        for (int i = 0; i < raw.length; i++) raw[i] = (byte) (i % 7);
        byte[] wrapped = CompressionUtil.wrap(raw, 128);
        byte[] restored = CompressionUtil.unwrap(wrapped);
        assertArrayEquals(raw, restored);
    }

    @Test
    void roundTripUncompressedStillWorks() {
        // Below the min-size threshold → uncompressed path.
        byte[] raw = {1, 2, 3, 4, 5};
        byte[] wrapped = CompressionUtil.wrap(raw, 128);
        byte[] restored = CompressionUtil.unwrap(wrapped);
        assertArrayEquals(raw, restored);
    }
}
