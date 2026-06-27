package com.fastsync.serialization;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * LZ4 compression utility with format version and flags header.
 *
 * <p>Binary format:
 * <pre>
 *   [1 byte: FORMAT_VERSION]
 *   [1 byte: FLAGS (bit 0: compressed)]
 *   if compressed:
 *     [4 bytes: original length (big-endian int)]
 *     [... compressed data ...]
 *   else:
 *     [... raw data ...]
 * </pre>
 *
 * <p>This avoids the base64 string encoding overhead that plagues other sync plugins.
 * LZ4 provides ~3-5x compression on NBT data with extremely fast decompression.
 *
 * <h2>Bounds checking</h2>
 * The decompression path reads {@code originalLength} straight out of the blob
 * header. A corrupted / poisoned DB row could therefore declare a huge length
 * and trigger an OOM (or a {@code NegativeArraySizeException} for negative
 * values) on the login thread. {@link #unwrap(byte[])} enforces upper bounds
 * on both the wrapped payload and the declared original length before any
 * allocation, and converts any LZ4 failure into a {@link CorruptDataException}
 * so the load path fails closed (releases the lock, refuses the payload)
 * instead of propagating a random exception.
 *
 * <p>Limits are configurable via {@link #configureLimits(int, int)} and default
 * to conservative values (1 MiB raw / 2.5 MiB wrapped). The plugin's
 * {@code ConfigManager} applies the configured values on startup.
 */
public class CompressionUtil {

    public static final byte FORMAT_VERSION = 1;
    public static final byte FLAG_COMPRESSED = 0x01;

    private static final LZ4Factory factory = LZ4Factory.fastestInstance();
    private static final LZ4Compressor compressor = factory.fastCompressor();
    private static final LZ4FastDecompressor decompressor = factory.fastDecompressor();

    /**
     * Default cap on the declared decompressed (raw) length, in bytes.
     * A player data Blob should never legitimately approach this; the cap
     * exists purely to bound memory in the face of corrupted data.
     */
    static final int DEFAULT_MAX_RAW_BYTES = 1 << 20;            // 1 MiB
    /**
     * Default cap on the wrapped (on-disk) payload length, in bytes.
     * Slightly larger than the raw cap so legitimately-compressed data still
     * fits, but still bounded.
     */
    static final int DEFAULT_MAX_WRAPPED_BYTES = 5 * (1 << 19);  // 2.5 MiB

    private static volatile int maxRawBytes = DEFAULT_MAX_RAW_BYTES;
    private static volatile int maxWrappedBytes = DEFAULT_MAX_WRAPPED_BYTES;

    private CompressionUtil() {}

    /**
     * Apply configured decompression bounds. Called once from {@code ConfigManager}
     * at load time. Values {@code <= 0} are ignored (keeps the prior limit) so a
     * misconfigured file can't disable the guard.
     */
    public static void configureLimits(int maxRawBytes, int maxWrappedBytes) {
        if (maxRawBytes > 0) CompressionUtil.maxRawBytes = maxRawBytes;
        if (maxWrappedBytes > 0) CompressionUtil.maxWrappedBytes = maxWrappedBytes;
    }

    /** Current cap on declared raw length (for tests / diagnostics). */
    public static int getMaxRawBytes() { return maxRawBytes; }
    /** Current cap on wrapped payload length (for tests / diagnostics). */
    public static int getMaxWrappedBytes() { return maxWrappedBytes; }

    /**
     * Wraps raw data with header and optionally compresses with LZ4.
     *
     * <p><b>Allocation note (kept honest):</b> this method performs two
     * allocations on the compressed path — a scratch buffer sized to LZ4's
     * worst-case bound, then a trimmed result buffer into which the compressed
     * bytes are copied. The class-level doc previously claimed a single-buffer
     * {@code ByteArrayOutputStream} implementation; that was inaccurate, so the
     * claim has been removed rather than left as misleading documentation. The
     * extra copy is small relative to a DB round-trip and is not on a hot
     * enough path to justify a ThreadLocal scratch buffer yet.
     *
     * @param data      raw serialized data
     * @param minSize   minimum data size to trigger compression (bytes)
     * @return wrapped data with header (and optionally compressed)
     */
    public static byte[] wrap(byte[] data, int minSize) {
        if (data == null || data.length == 0) {
            return new byte[] { FORMAT_VERSION, 0 };
        }

        boolean shouldCompress = data.length >= minSize;
        if (shouldCompress) {
            int maxCompressedLen = compressor.maxCompressedLength(data.length);
            // Worst case: compression makes data larger (rare for tiny inputs).
            // Allocate a scratch buffer at the worst-case bound, compress into
            // it, then copy into a right-sized result. Two allocations, but the
            // scratch buffer is reused only within this call (no thread-local).
            byte[] tmp = new byte[maxCompressedLen];
            int compressedLen = compressor.compress(data, 0, data.length,
                tmp, 0, maxCompressedLen);

            // Only use compression if it actually reduces size
            // (account for 4-byte original-length header).
            if (compressedLen + 6 < data.length + 2) {
                byte[] result = new byte[6 + compressedLen];
                result[0] = FORMAT_VERSION;
                result[1] = FLAG_COMPRESSED;
                result[2] = (byte) (data.length >>> 24);
                result[3] = (byte) (data.length >>> 16);
                result[4] = (byte) (data.length >>> 8);
                result[5] = (byte) (data.length);
                System.arraycopy(tmp, 0, result, 6, compressedLen);
                return result;
            }
        }

        // Store uncompressed — single allocation.
        byte[] result = new byte[2 + data.length];
        result[0] = FORMAT_VERSION;
        result[1] = 0; // not compressed
        System.arraycopy(data, 0, result, 2, data.length);
        return result;
    }

    /**
     * Unwraps and optionally decompresses data.
     *
     * <p>Zero-copy on the compressed path: passes the wrapped buffer directly
     * to the LZ4 decompressor with the correct offset, avoiding a
     * {@code Arrays.copyOfRange}.
     *
     * <p>Bounds-checked: the wrapped payload length and the declared original
     * length are validated <em>before</em> the destination array is allocated,
     * so a corrupted header cannot cause an OOM or {@code NegativeArraySize}.
     * Any LZ4 decompression failure is wrapped in a {@link CorruptDataException}.
     *
     * @param wrappedData data produced by {@link #wrap}
     * @return raw serialized data
     * @throws CorruptDataException if the payload is too short, the declared
     *         length is out of bounds, or LZ4 decompression fails
     */
    public static byte[] unwrap(byte[] wrappedData) {
        if (wrappedData == null || wrappedData.length < 2) {
            throw new CorruptDataException("Wrapped data too short: "
                + (wrappedData == null ? "null" : wrappedData.length + " bytes"));
        }

        if (wrappedData.length > maxWrappedBytes) {
            throw new CorruptDataException("Wrapped payload exceeds limit: "
                + wrappedData.length + " > " + maxWrappedBytes + " bytes");
        }

        byte version = wrappedData[0];
        byte flags = wrappedData[1];

        if (version != FORMAT_VERSION) {
            throw new CorruptDataException("Unsupported format version: " + version
                + " (expected " + FORMAT_VERSION + ")");
        }

        boolean compressed = (flags & FLAG_COMPRESSED) != 0;

        if (compressed) {
            if (wrappedData.length < 6) {
                throw new CorruptDataException("Compressed data too short: "
                    + wrappedData.length + " bytes (need >= 6)");
            }
            int originalLength = ((wrappedData[2] & 0xFF) << 24)
                               | ((wrappedData[3] & 0xFF) << 16)
                               | ((wrappedData[4] & 0xFF) << 8)
                               | (wrappedData[5] & 0xFF);

            if (originalLength <= 0) {
                throw new CorruptDataException("Invalid original length: " + originalLength);
            }
            if (originalLength > maxRawBytes) {
                throw new CorruptDataException("Declared original length exceeds limit: "
                    + originalLength + " > " + maxRawBytes + " bytes");
            }

            byte[] restored = new byte[originalLength];
            // Decompress directly from wrappedData[6..] — no intermediate copy.
            try {
                decompressor.decompress(wrappedData, 6, restored, 0, originalLength);
            } catch (LZ4Exception e) {
                throw new CorruptDataException(
                    "LZ4 decompression failed (declared length=" + originalLength
                        + ", available=" + (wrappedData.length - 6) + "): " + e.getMessage(), e);
            } catch (ArrayIndexOutOfBoundsException e) {
                // LZ4 fast decompressor reads exactly originalLength; a
                // truncated/corrupt source can still throw here.
                throw new CorruptDataException(
                    "LZ4 source truncated (declared length=" + originalLength
                        + ", available=" + (wrappedData.length - 6) + "): " + e.getMessage(), e);
            }
            return restored;
        } else {
            // Uncompressed path: still need a copy because callers may mutate
            // the returned array, but it's a single allocation.
            byte[] result = new byte[wrappedData.length - 2];
            System.arraycopy(wrappedData, 2, result, 0, result.length);
            return result;
        }
    }

    /**
     * Returns the format version byte.
     */
    public static byte getFormatVersion() {
        return FORMAT_VERSION;
    }
}
