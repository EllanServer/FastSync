package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests the unified lock-release helpers in {@link SyncManager} that Round 3
 * directive #1 mandates all release paths must go through.
 *
 * <p>These tests use Mockito + reflection because {@link SyncManager}'s
 * constructor requires a {@link FastSync} plugin instance (extends
 * JavaPlugin, needs a Paper runtime), and the helpers themselves are private.
 * The public constructor only assigns fields — it does not call
 * {@code initialize()} — so {@code asyncExecutor} and {@code redissonManager}
 * remain null, which makes {@code releaseLockAsyncBestEffort} run its task
 * synchronously and {@code notifyLockReleased} a no-op. This lets us test the
 * fail-closed / best-effort logic without any Paper runtime.
 *
 * <p>Covers Round 3 tests:
 * <ul>
 *   <li>#2: apply failure with null/blank lockSession → no uncaught leak,
 *       pendingSaveCount decremented.</li>
 *   <li>#3: joined-without-pending with lock metadata → best-effort release.</li>
 * </ul>
 */
class SyncManagerLockReleaseTest {

    private FastSync plugin;
    private ConfigManager config;
    private DatabaseManager databaseManager;
    private SyncManager syncManager;
    private AtomicInteger pendingSaveCount;

    @BeforeEach
    void setup() throws Exception {
        plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("lock-release-test"));

        config = mock(ConfigManager.class);
        when(config.getServerName()).thenReturn("test-server");
        when(config.isDebug()).thenReturn(false);

        databaseManager = mock(DatabaseManager.class);

        syncManager = new SyncManager(plugin, config, databaseManager);

        // asyncExecutor and redissonManager are null after construction (no
        // initialize() call), which is exactly what we want: the async helper
        // runs its task synchronously and notifyLockReleased is a no-op.

        // Grab the pendingSaveCount field so we can assert it returns to 0.
        pendingSaveCount = (AtomicInteger) getPrivateField("pendingSaveCount");
    }

    // ==================== Test #2: null/blank lockSession — no leak ====================

    /**
     * Round 3 test #2: when fencingToken is null (e.g. apply failure before
     * the token was stored), the helper must NOT call releaseLock, must NOT
     * throw, and must decrement pendingSaveCount back to 0 in finally.
     */
    @Test
    void releaseLockAsyncBestEffort_nullFencingToken_noLockCallNoLeak() throws Exception {
        UUID uuid = UUID.randomUUID();

        invokeAsyncBestEffort(uuid, null, null, "apply failure (null token)");

        // releaseLock must NEVER be called — a null token means we never held
        // a verified lock, and calling releaseLock with null would throw
        // IllegalArgumentException (requireLockSession guard).
        verifyNoInteractions(databaseManager);

        assertEquals(0, pendingSaveCount.get(),
            "pendingSaveCount must return to 0 after the task's finally block — "
                + "a leak here would block shutdown");
    }

    /**
     * Round 3 test #2: when fencingToken is valid but lockSessionId is blank,
     * the helper must NOT call releaseLock (would clear another session's
     * lock), must NOT throw, and must decrement pendingSaveCount.
     */
    @Test
    void releaseLockAsyncBestEffort_blankLockSession_noLockCallNoLeak() throws Exception {
        UUID uuid = UUID.randomUUID();

        invokeAsyncBestEffort(uuid, 5L, "   ", "apply failure (blank session)");

        verifyNoInteractions(databaseManager);

        assertEquals(0, pendingSaveCount.get(),
            "pendingSaveCount must return to 0 — no uncaught leak");
    }

    /**
     * Round 3 test #2 (sync variant): the sync helper
     * {@code releaseOwnedLockAndNotify} must also be fail-closed on null
     * fencingToken — no releaseLock call, no exception escaped.
     */
    @Test
    void releaseOwnedLockAndNotify_nullFencingToken_noLockCallNoException() throws Exception {
        UUID uuid = UUID.randomUUID();

        invokeOwnedLockAndNotify(uuid, null, null, "checksum failure");

        verifyNoInteractions(databaseManager);
    }

    /**
     * Round 3 test #2 (sync variant): blank lockSessionId must be rejected
     * by the sync helper too.
     */
    @Test
    void releaseOwnedLockAndNotify_blankLockSession_noLockCallNoException() throws Exception {
        UUID uuid = UUID.randomUUID();

        invokeOwnedLockAndNotify(uuid, 5L, "", "load error");

        verifyNoInteractions(databaseManager);
    }

    // ==================== Test #3: valid metadata — best-effort release ====================

    /**
     * Round 3 test #3: when fencingToken + lockSessionId ARE present (the
     * joined-without-preloaded path after the Round 3 fix), the helper must
     * actually call releaseLock so the lock does not leak until timeout.
     * pendingSaveCount must still return to 0.
     */
    @Test
    void releaseLockAsyncBestEffort_validMetadata_releasesAndNotifies() throws Exception {
        UUID uuid = UUID.randomUUID();
        long fencingToken = 7L;
        String lockSession = "session-abc";

        when(databaseManager.releaseLock(uuid, "test-server", fencingToken, lockSession))
            .thenReturn(true);

        invokeAsyncBestEffort(uuid, fencingToken, lockSession, "joined without preloaded data");

        verify(databaseManager).releaseLock(uuid, "test-server", fencingToken, lockSession);

        assertEquals(0, pendingSaveCount.get(),
            "pendingSaveCount must return to 0 after successful release");
    }

    /**
     * Round 3 test #3 (sync variant): valid metadata through the sync helper
     * must call releaseLock and notify. This is the path used by checksum /
     * load-error failures during the load phase.
     */
    @Test
    void releaseOwnedLockAndNotify_validMetadata_releasesAndNotifies() throws Exception {
        UUID uuid = UUID.randomUUID();
        long fencingToken = 9L;
        String lockSession = "session-xyz";

        when(databaseManager.releaseLock(uuid, "test-server", fencingToken, lockSession))
            .thenReturn(true);

        invokeOwnedLockAndNotify(uuid, fencingToken, lockSession, "checksum failure");

        verify(databaseManager).releaseLock(uuid, "test-server", fencingToken, lockSession);
    }

    /**
     * When releaseLock throws an IllegalArgumentException (the
     * requireLockSession guard inside DatabaseManager — simulating a defensive
     * mismatch), the helper must catch it (RuntimeException) and NOT let it
     * escape, and must still decrement pendingSaveCount. This is the exact
     * scenario Round 3 directive #1 flagged: helpers must catch
     * {@code SQLException | RuntimeException}, not just SQLException.
     */
    @Test
    void releaseLockAsyncBestEffort_runtimeExceptionFromReleaseLock_noLeakNoEscape() throws Exception {
        UUID uuid = UUID.randomUUID();
        long fencingToken = 11L;
        String lockSession = "session-rt";

        when(databaseManager.releaseLock(any(), any(), anyLong(), any()))
            .thenThrow(new IllegalArgumentException("simulated requireLockSession guard"));

        // Must NOT throw — the helper catches RuntimeException.
        assertDoesNotThrow(() ->
            invokeAsyncBestEffort(uuid, fencingToken, lockSession, "rt-test"));

        verify(databaseManager).releaseLock(uuid, "test-server", fencingToken, lockSession);

        assertEquals(0, pendingSaveCount.get(),
            "pendingSaveCount must still decrement to 0 even when releaseLock throws");
    }

    // ==================== Reflection helpers ====================

    private void invokeAsyncBestEffort(UUID uuid, Long fencingToken, String lockSessionId, String reason)
            throws Exception {
        Method m = SyncManager.class.getDeclaredMethod(
            "releaseLockAsyncBestEffort", UUID.class, Long.class, String.class, String.class);
        m.setAccessible(true);
        m.invoke(syncManager, uuid, fencingToken, lockSessionId, reason);
    }

    private void invokeOwnedLockAndNotify(UUID uuid, Long fencingToken, String lockSessionId, String reason)
            throws Exception {
        Method m = SyncManager.class.getDeclaredMethod(
            "releaseOwnedLockAndNotify", UUID.class, Long.class, String.class, String.class);
        m.setAccessible(true);
        m.invoke(syncManager, uuid, fencingToken, lockSessionId, reason);
    }

    private Object getPrivateField(String name) throws Exception {
        Field f = SyncManager.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(syncManager);
    }
}
