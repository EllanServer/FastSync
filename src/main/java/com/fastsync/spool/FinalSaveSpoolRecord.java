package com.fastsync.spool;

import java.util.UUID;

/**
 * Immutable record representing a final-save that was spooled to disk because
 * the final-save executor queue was full.
 *
 * <p>Stores the compressed Blob (not PlayerData) so replay does not need
 * Bukkit API access and is not affected by serialization format changes.
 */
public record FinalSaveSpoolRecord(
    int formatVersion,
    UUID uuid,
    String clusterId,
    String serverName,
    String lockSessionId,
    long expectedVersion,
    long fencingToken,
    long checksum,
    byte[] compressedBlob,
    String saveKind,
    long createdAt,
    int attempts,
    String lastError
) {
    public static final int CURRENT_FORMAT = 1;
}
