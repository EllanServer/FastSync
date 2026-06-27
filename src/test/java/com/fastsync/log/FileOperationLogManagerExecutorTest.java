package com.fastsync.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 tests for {@link FileOperationLogManager}'s dedicated executor.
 *
 * <p>Verifies the P0 #2 fix: append() no longer uses
 * ForkJoinPool.commonPool() — it runs on a dedicated single-thread bounded
 * executor named "FastSync-OpLog-*". Also verifies close() drains the
 * executor (waits for in-flight appends) rather than letting pending writes
 * outlive the plugin.
 */
class FileOperationLogManagerExecutorTest {

    @TempDir
    Path tempDir;

    private FileOperationLogManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close();
        }
    }

    /**
     * Round 16 (P0 #2): append() must run on a thread named
     * "FastSync-OpLog-*", NOT on a ForkJoinPool.commonPool worker
     * (whose name is "ForkJoinPool.commonPool-worker-*").
     */
    @Test
    void appendRunsOnDedicatedOpLogThreadNotCommonPool() throws Exception {
        manager = new FileOperationLogManager(tempDir, 100);
        manager.initialize();

        // Capture the thread name that runs the append task. Use a latch that
        // the append task releases AFTER recording its thread name, and wait
        // on it so the assertion sees the captured value.
        AtomicReference<String> capturedThread = new AtomicReference<>("(not captured)");
        CountDownLatch captured = new CountDownLatch(1);

        OperationLog entry = new OperationLog(
            1L, java.util.UUID.randomUUID(),
            OperationType.SAVE, "test-server",
            1L, 1L, 100, "thread-name-probe", System.currentTimeMillis());

        manager.append(entry).thenRun(captured::countDown).toCompletableFuture();

        // Inject a probe task directly via the same executor path by appending
        // a second entry whose appendSync records the thread name. We can't
        // easily intercept appendSync, so instead we observe the thread name
        // from inside the CompletableFuture's thenRun (which runs on the
        // executor thread since the future was created via runAsync on it).
        AtomicReference<String> probeThread = new AtomicReference<>("(not captured)");
        CountDownLatch probeDone = new CountDownLatch(1);
        OperationLog probe = new OperationLog(
            2L, java.util.UUID.randomUUID(),
            OperationType.SAVE, "test-server",
            2L, 2L, 100, "thread-name-probe-2", System.currentTimeMillis());
        manager.append(probe)
            .thenRun(() -> {
                probeThread.set(Thread.currentThread().getName());
                probeDone.countDown();
            })
            .toCompletableFuture();

        assertTrue(probeDone.await(5, TimeUnit.SECONDS),
            "Probe append must complete within 5s");

        String threadName = probeThread.get();
        assertTrue(threadName.startsWith("FastSync-OpLog-"),
            "append() must run on a FastSync-OpLog-* thread, but ran on: " + threadName);
        assertFalse(threadName.contains("ForkJoinPool") && threadName.contains("commonPool"),
            "append() must NOT run on ForkJoinPool.commonPool, but ran on: " + threadName);
    }

    /**
     * Round 16 (P0 #2): close() must wait for in-flight appends to finish.
     * We submit an append whose appendSync sleeps briefly, then call close()
     * and verify the append completed before close returned.
     */
    @Test
    void closeWaitsForInFlightAppends() throws Exception {
        manager = new FileOperationLogManager(tempDir, 100);
        manager.initialize();

        java.util.UUID uuid = java.util.UUID.randomUUID();
        // Submit a normal append — the per-UUID file write is fast, so this
        // primarily verifies close() doesn't interrupt pending writes.
        OperationLog entry = new OperationLog(
            1L, uuid, OperationType.SAVE, "test-server",
            1L, 1L, 100, "close-drain-test", System.currentTimeMillis());

        java.util.concurrent.CompletableFuture<Void> appendFuture =
            manager.append(entry).toCompletableFuture();

        // close() should drain the executor. After close() returns, the
        // append must be complete (not still queued).
        manager.close();
        manager = null;  // prevent double-close in tearDown

        assertTrue(appendFuture.isDone(),
            "In-flight append must be done after close() returned (close must drain the executor)");
    }

    /**
     * Round 16 (P0 #2): after close(), new append() calls must NOT submit to
     * the executor (which is shutting down). They should short-circuit to a
     * completed future without throwing RejectedExecutionException.
     */
    @Test
    void appendAfterCloseDoesNotThrow() throws Exception {
        manager = new FileOperationLogManager(tempDir, 100);
        manager.initialize();
        manager.close();
        manager = null;  // prevent double-close in tearDown

        // A new append after close must not throw.
        OperationLog entry = new OperationLog(
            1L, java.util.UUID.randomUUID(),
            OperationType.SAVE, "test-server",
            1L, 1L, 100, "post-close-test", System.currentTimeMillis());

        assertDoesNotThrow(() -> {
            java.util.concurrent.CompletableFuture<Void> f = manager == null
                ? new FileOperationLogManager(tempDir, 100).append(entry)
                : null;
            // Use a fresh closed manager to test the post-close path cleanly.
            FileOperationLogManager m = new FileOperationLogManager(tempDir, 100);
            m.initialize();
            m.close();
            m.append(entry);  // must not throw
        }, "append() after close() must not throw RejectedExecutionException");
    }
}
