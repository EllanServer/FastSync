package com.fastsync.spool;

import com.fastsync.database.DatabaseManager;
import com.fastsync.data.PlayerData;
import com.fastsync.serialization.CompressionUtil;
import com.fastsync.serialization.PlayerDataSerializer;
import com.fastsync.sync.SyncManager;

import java.util.UUID;

/**
 * Encodes a PlayerData into the compressed blob + CAS parameters needed
 * for both DB save and disk spool. Extracted from persistCollectedData()
 * so the spool path can encode without duplicating serialization logic.
 */
public final class FinalSaveEncoder {

    private FinalSaveEncoder() {}

    public static EncodedFinalSave encode(
            UUID uuid,
            PlayerData data,
            SyncManager.SaveKind kind,
            String clusterId,
            String serverName,
            String lockSessionId,
            int compressionMinSize
    ) throws Exception {
        data.setSaveCause(kind.causeName);
        byte[] serialized = PlayerDataSerializer.serialize(data);
        byte[] compressed = CompressionUtil.wrap(serialized, compressionMinSize);
        long checksum = DatabaseManager.computeChecksum(serialized);
        return new EncodedFinalSave(
            uuid,
            clusterId,
            serverName,
            lockSessionId,
            data.getVersion(),
            data.getFencingToken(),
            checksum,
            compressed,
            kind.name(),
            System.currentTimeMillis()
        );
    }
}
