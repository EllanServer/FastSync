package com.fastsync.conflict;

import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import com.fastsync.serialization.CompressionUtil;
import com.fastsync.serialization.PlayerDataSerializer;
import com.fastsync.snapshot.SnapshotManager;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles write conflicts detected by optimistic concurrency control.
 *
 * Inspired by Dynamo's conflict detection and resolution:
 * - When a stale write is detected (version mismatch), the data being saved
 *   is based on an outdated snapshot.
 * - The manager decides what to do with the stale data based on config:
 *   "snapshot": Save the stale data as a conflict snapshot for manual recovery
 *   "discard": Simply discard the stale data (the newer DB version wins)
 *   "overwrite": Force overwrite (dangerous, only for specific scenarios)
 *
 * In all cases, the lock is released so other servers can proceed.
 */
public class ConflictManager {

    public enum RecoveryStrategy {
        SNAPSHOT,   // Save stale data as a conflict snapshot
        DISCARD,    // Discard stale data (newer DB version wins)
        OVERWRITE;  // Force overwrite (dangerous)

        /**
         * Parse a config string ("snapshot", "discard", "overwrite") into a
         * {@link RecoveryStrategy}. Unknown/null values fall back to SNAPSHOT.
         */
        public static RecoveryStrategy fromConfig(String value) {
            if (value == null) return SNAPSHOT;
            return switch (value.toLowerCase(java.util.Locale.ROOT)) {
                case "discard" -> DISCARD;
                case "overwrite" -> OVERWRITE;
                default -> SNAPSHOT;
            };
        }
    }

    private final Logger logger;
    private final ConfigManager config;
    private final SnapshotManager snapshotManager;

    public ConflictManager(Logger logger, ConfigManager config, SnapshotManager snapshotManager) {
        this.logger = logger;
        this.config = config;
        this.snapshotManager = snapshotManager;
    }

    /**
     * Handle a write conflict for a player.
     *
     * @param uuid player UUID
     * @param staleData the PlayerData that failed to save (based on old version)
     * @param expectedVersion the version we expected (stale)
     * @param actualVersion the actual version in the DB (newer)
     */
    public void handleConflict(UUID uuid, PlayerData staleData, long expectedVersion, long actualVersion) {
        logger.warning(String.format(
            "[Conflict] Write conflict for %s: expected version %d but DB has version %d. " +
            "Another server wrote newer data. Applying recovery strategy: %s",
            uuid, expectedVersion, actualVersion, config.getConflictRecoveryStrategy()));

        RecoveryStrategy strategy = RecoveryStrategy.fromConfig(config.getConflictRecoveryStrategy());

        switch (strategy) {
            case SNAPSHOT -> saveConflictSnapshot(uuid, staleData, expectedVersion, actualVersion);
            case DISCARD -> logger.info("[Conflict] Stale data for " + uuid + " discarded. DB version " + actualVersion + " preserved.");
            case OVERWRITE -> {
                // This is handled by the caller using forceSaveData
                logger.warning("[Conflict] OVERWRITE strategy requested for " + uuid + " - caller must force-save.");
            }
        }
    }

    /**
     * Save the stale data as a conflict snapshot so it can be manually recovered.
     */
    private void saveConflictSnapshot(UUID uuid, PlayerData staleData, long expectedVersion, long actualVersion) {
        if (snapshotManager == null) {
            logger.warning("[Conflict] Cannot save conflict snapshot for " + uuid + " - snapshots disabled. Data discarded.");
            return;
        }

        try {
            byte[] serialized = PlayerDataSerializer.serialize(staleData);
            byte[] compressed = CompressionUtil.wrap(serialized, config.getCompressionMinSize(), config.isCompressionEnabled());
            String cause = String.format("CONFLICT_expected_v%d_actual_v%d", expectedVersion, actualVersion);

            snapshotManager.createSnapshot(uuid, compressed, cause)
                .thenRun(() -> {
                    logger.info("[Conflict] Stale data saved as conflict snapshot for " + uuid +
                        " (cause: " + cause + ")");
                    // Prune old snapshots so conflict snapshots cannot accumulate
                    // without bound. This mirrors the prune call used on the success
                    // path in SyncManager. Pinned snapshots are preserved by pruneSnapshots.
                    snapshotManager.pruneSnapshots(uuid, config.getMaxSnapshots())
                        .exceptionally(pruneEx -> {
                            logger.log(Level.WARNING, "[Conflict] Failed to prune snapshots for " + uuid, pruneEx);
                            return null;
                        });
                })
                .exceptionally(e -> {
                    logger.log(Level.WARNING, "[Conflict] Failed to save conflict snapshot for " + uuid, e);
                    return null;
                });
        } catch (Exception e) {
            logger.log(Level.WARNING, "[Conflict] Failed to serialize stale data for " + uuid, e);
        }
    }
}
