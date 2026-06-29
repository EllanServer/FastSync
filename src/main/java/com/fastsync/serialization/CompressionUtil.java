package com.fastsync.serialization;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.util.Objects;

/**
 * Compression utility with format version and algorithm-selection header.
 *
 * <p>Binary format:
 * <pre>
 *   [1 byte: FORMAT_VERSION]
 *   [1 byte: FLAGS (bit 0: compressed, bits 1-2: algorithm)]
 *   if compressed:
 *     [4 bytes: original length (big-endian int)]
 *     [... compressed data ...]
 *   else:
 *     [... raw data ...]
 * </pre>
 *
 * <p>Algorithm encoding in FLAGS bits 1-2:
 * <ul>
 *   <li>0b00 = LZ4 (fast, default for hot path)</li>
 *   <li>0b01 = ZSTD (high ratio, for spool/snapshot)</li>
 * </ul>
 *
 * <p>The algorithm is selected via {@link #setAlgorithm(CompressionAlgorithm)}
 * and defaults to LZ4 for backward compatibility. ZSTD provides ~30-50% better
 * compression ratio on NBT data at slightly higher CPU cost, making it ideal
 * for disk-bound paths (FinalSaveSpool, SnapshotManager).
 *
 * <h2>Bounds checking</h2>
 * The decompression path reads {@code originalLength} straight out of the blob
 * header. A corrupted / poisoned DB row could therefore declare a huge length
 * and trigger an OOM. {@link #unwrap(byte[])} enforces upper bounds on both
 * the wrapped payload and the declared original length before any allocation.
 */
public class CompressionUtil {

    public static final byte FORMAT_VERSION = 2;
    public static final byte FLAG_COMPRESSED = 0x01;
    public static final byte FLAG_ALGORITHM_MASK = 0x06; // bits 1-2
    public static final byte FLAG_ALGORITHM_LZ4 = 0x00;
    public static final byte FLAG_ALGORITHM_ZSTD = 0x02;

    // Legacy format version 1 (LZ4 only) — still readable by unwrap()
    public static final byte FORMAT_VERSION_LEGACY = 1;

    private static final LZ4Factory factory = LZ4Factory.fastestInstance();
    private static final LZ4Compressor lz4Compressor = factory.fastCompressor();
    private static final LZ4FastDecompressor lz4Decompressor = factory.fastDecompressor();

    // Per-thread scratch buffers avoid both the worst-case temporary allocation
    // and a native ZSTD context allocation on every player save.
    private static final ThreadLocal<byte[]> lz4Scratch =
        ThreadLocal.withInitial(() -> new byte[8192]);
    private static final ThreadLocal<byte[]> zstdScratch =
        ThreadLocal.withInitial(() -> new byte[8192]);

    static final int DEFAULT_MAX_RAW_BYTES = 1 << 20;            // 1 MiB
    static final int DEFAULT_MAX_WRAPPED_BYTES = 5 * (1 << 19);  // 2.5 MiB

    private static volatile int maxRawBytes = DEFAULT_MAX_RAW_BYTES;
    private static volatile int maxWrappedBytes = DEFAULT_MAX_WRAPPED_BYTES;
    private static volatile boolean enabled = true;

    // Default algorithm — LZ4 for backward compatibility and hot-path speed
    private static volatile CompressionAlgorithm algorithm = CompressionAlgorithm.LZ4;

    // ZSTD compression level (1-22, higher = better ratio but slower)
    private static volatile int zstdLevel = 3;

    public enum CompressionAlgorithm {
        LZ4,
        ZSTD
    }

    private CompressionUtil() {}

    /**
     * Set the compression algorithm used by {@link #wrap(byte[], int)}.
     *
     * @param algorithm the algorithm to use (LZ4 or ZSTD)
     */
    public static void setAlgorithm(CompressionAlgorithm algorithm) {
        CompressionUtil.algorithm = Objects.requireNonNull(algorithm, "algorithm");
    }

    /**
     * Get the currently configured compression algorithm.
     */
    public static CompressionAlgorithm getAlgorithm() {
        return algorithm;
    }

    public static void setEnabled(boolean enabled) {
        CompressionUtil.enabled = enabled;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    /** Fail during plugin startup instead of on the first player save. */
    public static void verifyConfiguredCodec() {
        if (!enabled || algorithm != CompressionAlgorithm.ZSTD) return;
        try {
            byte[] source = new byte[64];
            long bound = Zstd.compressBound(source.length);
            if (bound <= 0 || bound > Integer.MAX_VALUE) {
                throw new IllegalStateException("Invalid ZSTD compression bound: " + bound);
            }
            byte[] destination = new byte[(int) bound];
            long result = Zstd.compressByteArray(destination, 0, destination.length,
                source, 0, source.length, zstdLevel);
            if (Zstd.isError(result)) {
                throw new ZstdException(result, "ZSTD startup probe failed");
            }
        } catch (LinkageError | RuntimeException e) {
            throw new IllegalStateException(
                "ZSTD is configured but its native codec is unavailable", e);
        }
    }

    /**
     * Set the ZSTD compression level (1-22).
     * Level 3 is the default (good balance of ratio and speed).
     *
     * @param level ZSTD compression level
     */
    public static void setZstdLevel(int level) {
        zstdLevel = Math.max(1, Math.min(22, level));
    }

    public static void configureLimits(int maxRawBytes, int maxWrappedBytes) {
        if (maxRawBytes > 0) CompressionUtil.maxRawBytes = maxRawBytes;
        if (maxWrappedBytes > 0) CompressionUtil.maxWrappedBytes = maxWrappedBytes;
    }

    public static int getMaxRawBytes() { return maxRawBytes; }
    public static int getMaxWrappedBytes() { return maxWrappedBytes; }

    /**
     * Wraps raw data with header and optionally compresses.
     *
     * @param data      raw serialized data
     * @param minSize   minimum data size to trigger compression (bytes)
     * @return wrapped data with header (and optionally compressed)
     */
    public static byte[] wrap(byte[] data, int minSize) {
        if (data == null || data.length == 0) {
            return new byte[] { FORMAT_VERSION, 0 };
        }
        if (data.length > maxRawBytes) {
            throw new CorruptDataException("Raw payload exceeds configured limit: "
                + data.length + " > " + maxRawBytes + " bytes");
        }

        boolean shouldCompress = enabled && data.length >= Math.max(0, minSize);
        if (shouldCompress) {
            if (algorithm == CompressionAlgorithm.ZSTD) {
                return wrapZstd(data);
            } else {
                return wrapLz4(data);
            }
        }

        // Store uncompressed — single allocation.
        byte[] result = new byte[2 + data.length];
        result[0] = FORMAT_VERSION;
        result[1] = 0; // not compressed
        System.arraycopy(data, 0, result, 2, data.length);
        if (result.length > maxWrappedBytes) {
            throw new CorruptDataException("Wrapped payload exceeds configured limit: "
                + result.length + " > " + maxWrappedBytes + " bytes");
        }
        return result;
    }

    private static byte[] wrapLz4(byte[] data) {
        int maxCompressedLen = lz4Compressor.maxCompressedLength(data.length);
        byte[] tmp = lz4Scratch.get();
        if (tmp.length < maxCompressedLen) {
            tmp = new byte[maxCompressedLen];
            lz4Scratch.set(tmp);
        }
        int compressedLen = lz4Compressor.compress(data, 0, data.length,
            tmp, 0, maxCompressedLen);

        if (compressedLen + 6 < data.length + 2) {
            byte[] result = new byte[6 + compressedLen];
            result[0] = FORMAT_VERSION;
            result[1] = (byte) (FLAG_COMPRESSED | FLAG_ALGORITHM_LZ4);
            result[2] = (byte) (data.length >>> 24);
            result[3] = (byte) (data.length >>> 16);
            result[4] = (byte) (data.length >>> 8);
            result[5] = (byte) (data.length);
            System.arraycopy(tmp, 0, result, 6, compressedLen);
            if (result.length > maxWrappedBytes) {
                throw new CorruptDataException("Wrapped payload exceeds configured limit: "
                    + result.length + " > " + maxWrappedBytes + " bytes");
            }
            return result;
        }
        // Compression didn't help — fall through to uncompressed
        return uncompressed(data);
    }

    private static byte[] wrapZstd(byte[] data) {
        long bound = Zstd.compressBound(data.length);
        if (bound <= 0 || bound > Integer.MAX_VALUE) {
            throw new CorruptDataException("Invalid ZSTD compression bound: " + bound);
        }
        int maxCompressedLen = (int) bound;
        byte[] tmp = zstdScratch.get();
        if (tmp.length < maxCompressedLen) {
            tmp = new byte[maxCompressedLen];
            zstdScratch.set(tmp);
        }

        long compressedResult = Zstd.compressByteArray(
            tmp, 0, maxCompressedLen, data, 0, data.length, zstdLevel);
        if (Zstd.isError(compressedResult)) {
            throw new ZstdException(compressedResult, "Failed to compress player data");
        }
        int compressedLength = Math.toIntExact(compressedResult);

        if (compressedLength + 6 < data.length + 2) {
            byte[] result = new byte[6 + compressedLength];
            result[0] = FORMAT_VERSION;
            result[1] = (byte) (FLAG_COMPRESSED | FLAG_ALGORITHM_ZSTD);
            result[2] = (byte) (data.length >>> 24);
            result[3] = (byte) (data.length >>> 16);
            result[4] = (byte) (data.length >>> 8);
            result[5] = (byte) (data.length);
            System.arraycopy(tmp, 0, result, 6, compressedLength);
            if (result.length > maxWrappedBytes) {
                throw new CorruptDataException("Wrapped payload exceeds configured limit: "
                    + result.length + " > " + maxWrappedBytes + " bytes");
            }
            return result;
        }
        // Compression didn't help — fall through to uncompressed
        return uncompressed(data);
    }

    private static byte[] uncompressed(byte[] data) {
        byte[] result = new byte[2 + data.length];
        result[0] = FORMAT_VERSION;
        result[1] = 0;
        System.arraycopy(data, 0, result, 2, data.length);
        if (result.length > maxWrappedBytes) {
            throw new CorruptDataException("Wrapped payload exceeds configured limit: "
                + result.length + " > " + maxWrappedBytes + " bytes");
        }
        return result;
    }

    /**
     * Unwraps and optionally decompresses data.
     *
     * <p>Supports both format version 1 (legacy LZ4-only) and version 2
     * (multi-algorithm). This ensures backward compatibility with data
     * written by older plugin versions.
     *
     * @param wrappedData data produced by {@link #wrap}
     * @return raw serialized data
     * @throws CorruptDataException if the payload is corrupted
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

        // Support legacy format version 1 (LZ4 only, no algorithm bits)
        if (version != FORMAT_VERSION && version != FORMAT_VERSION_LEGACY) {
            throw new CorruptDataException("Unsupported format version: " + version
                + " (expected " + FORMAT_VERSION + " or " + FORMAT_VERSION_LEGACY + ")");
        }

        // Validate that only known flag bits are set.
        // v2 allows: FLAG_COMPRESSED (0x01) + FLAG_ALGORITHM_MASK (0x06) = 0x07
        // v1 (legacy) only allows FLAG_COMPRESSED (0x01)
        byte allowedFlags = (version == FORMAT_VERSION_LEGACY)
            ? FLAG_COMPRESSED
            : (byte) (FLAG_COMPRESSED | FLAG_ALGORITHM_MASK);
        if ((flags & ~allowedFlags) != 0) {
            throw new CorruptDataException("Unsupported compression flags: 0x"
                + Integer.toHexString(flags & 0xFF));
        }

        boolean compressed = (flags & FLAG_COMPRESSED) != 0;
        byte algoBits = version == FORMAT_VERSION_LEGACY
            ? FLAG_ALGORITHM_LZ4
            : (byte) (flags & FLAG_ALGORITHM_MASK);

        if (!compressed && algoBits != FLAG_ALGORITHM_LZ4) {
            throw new CorruptDataException("Compression algorithm set on uncompressed payload: 0x"
                + Integer.toHexString(algoBits & 0xFF));
        }
        if (compressed && algoBits != FLAG_ALGORITHM_LZ4
                && algoBits != FLAG_ALGORITHM_ZSTD) {
            throw new CorruptDataException("Unsupported compression algorithm: 0x"
                + Integer.toHexString(algoBits & 0xFF));
        }

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

            if (algoBits == FLAG_ALGORITHM_ZSTD) {
                try {
                    long decompressed = Zstd.decompressByteArray(restored, 0, originalLength, wrappedData, 6, wrappedData.length - 6);
                    if (Zstd.isError(decompressed)) {
                        throw new ZstdException(decompressed);
                    }
                    if (decompressed != originalLength) {
                        throw new CorruptDataException(
                            "ZSTD decompression size mismatch (expected=" + originalLength
                                + ", actual=" + decompressed + ")");
                    }
                } catch (ZstdException e) {
                    throw new CorruptDataException(
                        "ZSTD decompression failed (declared length=" + originalLength
                            + ", available=" + (wrappedData.length - 6) + "): " + e.getMessage(), e);
                }
            } else {
                // LZ4 (default)
                try {
                    lz4Decompressor.decompress(wrappedData, 6, restored, 0, originalLength);
                } catch (LZ4Exception e) {
                    throw new CorruptDataException(
                        "LZ4 decompression failed (declared length=" + originalLength
                            + ", available=" + (wrappedData.length - 6) + "): " + e.getMessage(), e);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new CorruptDataException(
                        "LZ4 source truncated (declared length=" + originalLength
                            + ", available=" + (wrappedData.length - 6) + "): " + e.getMessage(), e);
                }
            }
            return restored;
        } else {
            int rawLength = wrappedData.length - 2;
            if (rawLength > maxRawBytes) {
                throw new CorruptDataException("Raw payload exceeds limit: "
                    + rawLength + " > " + maxRawBytes + " bytes");
            }
            byte[] result = new byte[wrappedData.length - 2];
            System.arraycopy(wrappedData, 2, result, 0, result.length);
            return result;
        }
    }

    /**
     * Returns the current format version byte.
     */
    public static byte getFormatVersion() {
        return FORMAT_VERSION;
    }
}
