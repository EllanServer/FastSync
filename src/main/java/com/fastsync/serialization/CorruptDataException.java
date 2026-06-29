package com.fastsync.serialization;

/**
 * Thrown when a wrapped (optionally LZ4/ZSTD-compressed) byte[] cannot be safely
 * unwrapped — for example because the declared original length is missing,
 * non-positive, exceeds the configured cap, or because codec decompression
 * itself failed.
 *
 * <p>This is a data-corruption signal, not a normal control-flow path. The
 * load/save paths catch it, release the player's lock, and refuse to apply
 * the corrupted payload rather than silently producing a partial / empty
 * player data blob.
 */
public class CorruptDataException extends RuntimeException {
    public CorruptDataException(String message) {
        super(message);
    }

    public CorruptDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
