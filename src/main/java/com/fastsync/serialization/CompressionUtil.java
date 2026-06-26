package com.fastsync.serialization;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.util.Arrays;

/**
 * LZ4 compression utility with format version and flags header.
 *
 * Binary format:
 *   [1 byte: FORMAT_VERSION]
 *   [1 byte: FLAGS (bit 0: compressed)]
 *   if compressed:
 *     [4 bytes: original length (big-endian int)]
 *     [... compressed data ...]
 *   else:
 *     [... raw data ...]
 *
 * This avoids the base64 string encoding overhead that plagues other sync plugins.
 * LZ4 provides ~3-5x compression on NBT data with extremely fast decompression.
 */
public class CompressionUtil {

    public static final byte FORMAT_VERSION = 1;
    public static final byte FLAG_COMPRESSED = 0x01;

    /**
     * Maximum allowed decompressed size. Prevents OOM from corrupted/malicious
     * blobs with a forged original-length header (e.g. 0x7FFFFFFF).
     * 8MB is well above any realistic player data payload (~50-200KB typical).
     */
    private static final int MAX_DECOMPRESSED_SIZE = 8 * 1024 * 1024;

    private static final LZ4Factory factory = LZ4Factory.fastestInstance();
    private static final LZ4Compressor compressor = factory.fastCompressor();
    private static final LZ4FastDecompressor decompressor = factory.fastDecompressor();

    private CompressionUtil() {}

    /**
     * Wraps raw data with header and optionally compresses with LZ4.
     *
     * @param data      raw serialized data
     * @param minSize   minimum data size to trigger compression (bytes)
     * @return wrapped data with header (and optionally compressed)
     */
    public static byte[] wrap(byte[] data, int minSize) {
        return wrap(data, minSize, true);
    }

    /**
     * Wraps raw data with header and optionally compresses with LZ4.
     *
     * @param data              raw serialized data
     * @param minSize           minimum data size to trigger compression (bytes)
     * @param compressionEnabled if false, data is stored uncompressed (header only)
     * @return wrapped data with header (and optionally compressed)
     */
    public static byte[] wrap(byte[] data, int minSize, boolean compressionEnabled) {
        int effectiveMinSize = compressionEnabled ? minSize : Integer.MAX_VALUE;

        if (data == null || data.length == 0) {
            byte[] result = new byte[2];
            result[0] = FORMAT_VERSION;
            result[1] = 0; // not compressed, empty
            return result;
        }

        boolean shouldCompress = data.length >= effectiveMinSize;

        if (shouldCompress) {
            int maxCompressedLen = compressor.maxCompressedLength(data.length);
            // Allocate the final result array directly: header + worst-case compressed length,
            // then compress straight into result[6..] and trim to the exact compressed length.
            // This avoids allocating a separate maxCompressedLen temp buffer + a System.arraycopy.
            byte[] result = new byte[6 + maxCompressedLen];
            result[0] = FORMAT_VERSION;
            result[1] = FLAG_COMPRESSED;
            result[2] = (byte) (data.length >>> 24);
            result[3] = (byte) (data.length >>> 16);
            result[4] = (byte) (data.length >>> 8);
            result[5] = (byte) (data.length);
            int compressedLen = compressor.compress(data, 0, data.length, result, 6, maxCompressedLen);

            // Only use compression if it actually reduces size
            // Account for 4 extra bytes (original length header)
            if (compressedLen + 6 < data.length + 2) {
                // Trim the oversized buffer down to the exact compressed length.
                return Arrays.copyOf(result, 6 + compressedLen);
            }
        }

        // Store uncompressed
        byte[] result = new byte[2 + data.length];
        result[0] = FORMAT_VERSION;
        result[1] = 0; // not compressed
        System.arraycopy(data, 0, result, 2, data.length);
        return result;
    }

    /**
     * Unwraps and optionally decompresses data.
     *
     * @param wrappedData data produced by {@link #wrap}
     * @return raw serialized data
     */
    public static byte[] unwrap(byte[] wrappedData) {
        if (wrappedData == null || wrappedData.length < 2) {
            return new byte[0];
        }

        byte version = wrappedData[0];
        byte flags = wrappedData[1];

        // Future: handle version migration here
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException(
                "Unsupported format version: " + version + " (expected " + FORMAT_VERSION + ")");
        }

        boolean compressed = (flags & FLAG_COMPRESSED) != 0;

        if (compressed) {
            if (wrappedData.length < 6) {
                throw new IllegalArgumentException("Compressed data too short");
            }
            int originalLength = ((wrappedData[2] & 0xFF) << 24)
                               | ((wrappedData[3] & 0xFF) << 16)
                               | ((wrappedData[4] & 0xFF) << 8)
                               | (wrappedData[5] & 0xFF);

            // Guard against corrupted/malicious blobs with inflated or negative
        // original-length. A negative value would cause NegativeArraySizeException
        // in new byte[originalLength], which is caught by the caller but produces
        // a confusing error message.
        if (originalLength < 0 || originalLength > MAX_DECOMPRESSED_SIZE) {
            throw new IllegalArgumentException(
                "Invalid decompressed size " + originalLength
                + " (max " + MAX_DECOMPRESSED_SIZE + ") — possible data corruption");
        }

            int compressedLen = wrappedData.length - 6;
            byte[] restored = new byte[originalLength];
            decompressor.decompress(wrappedData, 6, restored, 0, originalLength);
            return restored;
        } else {
            return Arrays.copyOfRange(wrappedData, 2, wrappedData.length);
        }
    }

    /**
     * Returns the format version byte.
     */
    public static byte getFormatVersion() {
        return FORMAT_VERSION;
    }
}
