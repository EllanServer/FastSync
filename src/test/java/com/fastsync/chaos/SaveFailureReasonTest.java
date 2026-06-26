package com.fastsync.chaos;

import com.fastsync.sync.SyncManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SyncManager.SaveFailureReason} and the extended
 * {@link SyncManager.SaveResult} record.
 */
class SaveFailureReasonTest {

    @Test
    void testSuccessHasNoneReason() {
        var result = SyncManager.SaveResult.success(10, 100);
        assertTrue(result.success());
        assertEquals(SyncManager.SaveFailureReason.NONE, result.failureReason());
        assertFalse(result.isRetryable(), "Success should not be retryable");
    }

    @Test
    void testSuccessWithOldAndNewVersion() {
        var result = SyncManager.SaveResult.success(10, 11, 100);
        assertTrue(result.success());
        assertEquals(10, result.expectedVersion());
        assertEquals(11, result.actualVersion());
        assertEquals(SyncManager.SaveFailureReason.NONE, result.failureReason());
    }

    @Test
    void testVersionConflictNotRetryable() {
        var result = SyncManager.SaveResult.conflict(10, 12, 100,
            SyncManager.SaveFailureReason.VERSION_CONFLICT);
        assertFalse(result.success());
        assertEquals(SyncManager.SaveFailureReason.VERSION_CONFLICT, result.failureReason());
        assertFalse(result.isRetryable(), "VERSION_CONFLICT should not be retryable");
    }

    @Test
    void testFencingMismatchNotRetryable() {
        var result = SyncManager.SaveResult.conflict(10, 12, 100,
            SyncManager.SaveFailureReason.FENCING_MISMATCH);
        assertFalse(result.success());
        assertEquals(SyncManager.SaveFailureReason.FENCING_MISMATCH, result.failureReason());
        assertFalse(result.isRetryable(), "FENCING_MISMATCH should not be retryable");
    }

    @Test
    void testSameFencingSelfConflictRetryable() {
        var result = SyncManager.SaveResult.conflict(10, 11, 100,
            SyncManager.SaveFailureReason.SAME_FENCING_SELF_CONFLICT);
        assertFalse(result.success());
        assertTrue(result.isRetryable(), "SAME_FENCING_SELF_CONFLICT should be retryable");
    }

    @Test
    void testDBUnavailableRetryable() {
        var result = SyncManager.SaveResult.error("connection lost",
            SyncManager.SaveFailureReason.DB_UNAVAILABLE);
        assertFalse(result.success());
        assertTrue(result.isRetryable(), "DB_UNAVAILABLE should be retryable");
    }

    @Test
    void testQueueFullRetryable() {
        var result = SyncManager.SaveResult.error("queue full",
            SyncManager.SaveFailureReason.QUEUE_FULL);
        assertFalse(result.success());
        assertTrue(result.isRetryable(), "QUEUE_FULL should be retryable");
    }

    @Test
    void testSerializationErrorNotRetryable() {
        var result = SyncManager.SaveResult.error("NBT corrupt",
            SyncManager.SaveFailureReason.SERIALIZATION_ERROR);
        assertFalse(result.success());
        assertFalse(result.isRetryable(), "SERIALIZATION_ERROR should not be retryable");
    }

    @Test
    void testEntityRetiredNotRetryable() {
        var result = SyncManager.SaveResult.error("player offline",
            SyncManager.SaveFailureReason.ENTITY_RETIRED);
        assertFalse(result.success());
        assertFalse(result.isRetryable(), "ENTITY_RETIRED should not be retryable");
    }

    @Test
    void testComponentSaveRejectedNotRetryable() {
        var result = SyncManager.SaveResult.error("fencing validation failed",
            SyncManager.SaveFailureReason.COMPONENT_SAVE_REJECTED);
        assertFalse(result.success());
        assertFalse(result.isRetryable(), "COMPONENT_SAVE_REJECTED should not be retryable");
    }

    @Test
    void testDefaultErrorIsUnknown() {
        var result = SyncManager.SaveResult.error("something went wrong");
        assertEquals(SyncManager.SaveFailureReason.UNKNOWN, result.failureReason());
        assertFalse(result.isRetryable(), "UNKNOWN should not be retryable");
    }

    @Test
    void testDefaultConflictIsVersionConflict() {
        var result = SyncManager.SaveResult.conflict(10, 12, 100);
        assertEquals(SyncManager.SaveFailureReason.VERSION_CONFLICT, result.failureReason());
    }

    @Test
    void testLockNotHeldNotRetryable() {
        var result = SyncManager.SaveResult.error("not locked",
            SyncManager.SaveFailureReason.LOCK_NOT_HELD);
        assertFalse(result.isRetryable());
    }

    @Test
    void testGenerationMismatchNotRetryable() {
        var result = SyncManager.SaveResult.error("stale generation",
            SyncManager.SaveFailureReason.COMPONENT_GENERATION_MISMATCH);
        assertFalse(result.isRetryable());
    }

    @Test
    void testAllReasonsCovered() {
        // Ensure the isRetryable() logic covers all enum values without defaulting
        for (SyncManager.SaveFailureReason reason : SyncManager.SaveFailureReason.values()) {
            var result = SyncManager.SaveResult.error("test", reason);
            // Should not throw — all cases handled in switch
            boolean retryable = result.isRetryable();
            // Only SAME_FENCING_SELF_CONFLICT, DB_UNAVAILABLE, QUEUE_FULL are retryable
            boolean expected = switch (reason) {
                case SAME_FENCING_SELF_CONFLICT, DB_UNAVAILABLE, QUEUE_FULL -> true;
                default -> false;
            };
            assertEquals(expected, retryable,
                "Reason " + reason + " retryable mismatch");
        }
    }
}
