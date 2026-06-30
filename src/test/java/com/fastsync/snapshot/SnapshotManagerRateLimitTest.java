package com.fastsync.snapshot;

import com.fastsync.config.ConfigManager;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotManagerRateLimitTest {

    @Test
    void reservesOnceWithinConfiguredIntervalAndCanReleaseFailure() {
        SnapshotManager manager = new SnapshotManager(
            Logger.getLogger("SnapshotManagerRateLimitTest"), new ConfigManager(true));
        UUID uuid = UUID.randomUUID();

        long first = manager.reserveSnapshot(uuid, 60_000L);
        assertTrue(first > 0);
        assertEquals(-1L, manager.reserveSnapshot(uuid, 60_000L));

        manager.releaseSnapshotReservation(uuid, first);
        assertTrue(manager.reserveSnapshot(uuid, 60_000L) > 0,
            "a failed async snapshot must not consume the whole backup interval");
    }

    @Test
    void zeroIntervalAllowsEveryTriggeredSnapshot() {
        SnapshotManager manager = new SnapshotManager(
            Logger.getLogger("SnapshotManagerRateLimitTest"), new ConfigManager(true));
        UUID uuid = UUID.randomUUID();

        assertTrue(manager.reserveSnapshot(uuid, 0L) > 0);
        assertTrue(manager.reserveSnapshot(uuid, 0L) > 0);
    }

    @Test
    void staleFailureCannotReleaseNewerReservation() {
        SnapshotManager manager = new SnapshotManager(
            Logger.getLogger("SnapshotManagerRateLimitTest"), new ConfigManager(true));
        UUID uuid = UUID.randomUUID();

        long staleToken = manager.reserveSnapshot(uuid, 0L);
        long currentToken = manager.reserveSnapshot(uuid, 0L);
        assertTrue(currentToken > staleToken);

        manager.releaseSnapshotReservation(uuid, staleToken);
        assertEquals(-1L, manager.reserveSnapshot(uuid, 60_000L),
            "an older failure callback must not remove the current reservation");
    }

    @Test
    void freshReservationSurvivesAgeBasedCleanup() {
        SnapshotManager manager = new SnapshotManager(
            Logger.getLogger("SnapshotManagerRateLimitTest"), new ConfigManager(true));
        UUID uuid = UUID.randomUUID();

        assertTrue(manager.reserveSnapshot(uuid, 60_000L) > 0);
        manager.cleanupSnapshotReservations(60_000L);
        assertEquals(-1L, manager.reserveSnapshot(uuid, 60_000L));
    }
}
