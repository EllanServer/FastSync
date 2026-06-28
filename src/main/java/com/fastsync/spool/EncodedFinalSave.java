package com.fastsync.spool;

import java.util.UUID;

/**
 * Encoded final-save data ready for DB CAS or spool.
 * Contains the compressed blob + all CAS parameters.
 */
public record EncodedFinalSave(
    UUID uuid,
    String clusterId,
    String serverName,
    String lockSessionId,
    long expectedVersion,
    long fencingToken,
    long checksum,
    byte[] compressedBlob,
    String saveKind,
    long createdAt
) {}
