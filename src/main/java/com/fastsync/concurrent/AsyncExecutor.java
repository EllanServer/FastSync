package com.fastsync.concurrent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dedicated thread pool for FastSync async operations.
 *
 * <p>Replaces {@code CompletableFuture.runAsync()} (which uses
 * {@code ForkJoinPool.commonPool}) with a properly sized, named, and
 * manageable thread pool.
 *
 * <p>Thread management is critical for sync plugins — HuskSync's poor thread
 * management was specifically criticized. This executor provides:
 * <ul>
 *   <li>Named threads for easy debugging</li>
 *   <li>Bounded pool size to prevent resource exhaustion</li>
 *   <li><b>Bounded queue</b> — tasks beyond {@code queueCapacity} are rejected
 *       with {@link java.util.concurrent.RejectedExecutionException}, which
 *       callers already handle (periodic save skips, SHUTDOWN synchronous
 *       fallback, etc.)</li>
 *   <li>Graceful shutdown with timeout</li>
 *   <li>Proper exception logging via {@link Logger} (not stderr)</li>
 * </ul>
 *
 * <h2>Why not {@code Executors.newFixedThreadPool}?</h2>
 * <p>{@code Executors.newFixedThreadPool} uses an unbounded
 * {@code LinkedBlockingQueue}. That means tasks never get rejected — they
 * just queue forever. Under login storms, DB latency spikes, or Redis
 * outages, the queue grows without limit and eventually exhausts heap. The
 * {@code RejectedExecutionException} handling in SyncManager would never
 * fire because the queue never fills.
 *
 * <p>This class uses a {@link ThreadPoolExecutor} with an
 * {@link ArrayBlockingQueue} of size {@code queueCapacity} and
 * {@link ThreadPoolExecutor.AbortPolicy}. When the queue is full, the
 * caller gets a {@link RejectedExecutionException} immediately, which
 * SyncManager already handles:
 * <ul>
 *   <li>Periodic save: skip (next tick will retry)</li>
 *   <li>BULK /saveall: skip with QUEUE_FULL reason</li>
 *   <li>SHUTDOWN: synchronous fallback (must persist data)</li>
 *   <li>QUIT: synchronous fallback</li>
 * </ul>
 */
public class AsyncExecutor {

    private final ThreadPoolExecutor executor;
    private final Logger logger;
    private final String poolName;

    /**
     * Create an executor with a bounded queue.
     *
     * @param logger        logger for uncaught exceptions and lifecycle events
     * @param poolName      prefix for worker thread names
     * @param poolSize      core = max pool size (fixed pool)
     * @param queueCapacity maximum number of pending tasks before rejection.
     *                      Must be {@code >= 1}. Caller is responsible for
     *                      handling {@link java.util.concurrent.RejectedExecutionException}
     *                      when submitting tasks.
     */
    public AsyncExecutor(Logger logger, String poolName, int poolSize, int queueCapacity) {
        this.logger = logger;
        this.poolName = poolName;

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, poolName + "-worker-" + counter.getAndIncrement());
                thread.setDaemon(true);
                thread.setPriority(Thread.NORM_PRIORITY);
                return thread;
            }
        };

        int actualPoolSize = Math.max(2, poolSize);
        int actualQueueCapacity = Math.max(1, queueCapacity);

        this.executor = new ThreadPoolExecutor(
            actualPoolSize,
            actualPoolSize,
            30L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(actualQueueCapacity),
            threadFactory,
            new ThreadPoolExecutor.AbortPolicy()
        );

        logger.info("Async executor '" + poolName + "' initialized with "
            + actualPoolSize + " threads, queue capacity " + actualQueueCapacity + ".");
    }

    /**
     * Submit a task for async execution.
     * Exceptions are logged via the SLF4J/j.u.l logger (not stderr).
     *
     * <p>Catches {@link Throwable} (not just {@link Exception}) so that
     * {@link Error} subtypes ({@link OutOfMemoryError},
     * {@link StackOverflowError}, {@link NoClassDefFoundError}, etc.) are
     * logged before the worker thread dies. Otherwise the {@code Error}
     * would be silently swallowed by the worker, leaving no trace in the
     * log for operators to diagnose.
     *
     * @throws java.util.concurrent.RejectedExecutionException if the queue is full.
     *         Callers must handle this — typically by skipping (periodic save)
     *         or falling back to synchronous execution (QUIT/SHUTDOWN).
     */
    public void execute(Runnable task) {
        executor.execute(() -> {
            try {
                task.run();
            } catch (Throwable t) {
                logger.log(Level.SEVERE,
                    "[" + poolName + "] Uncaught exception in async task: " + t.getMessage(), t);
            }
        });
    }

    /**
     * Submit a task and return a CompletableFuture.
     *
     * <p>Unlike the previous bare {@code CompletableFuture.runAsync(task, executor)},
     * this wraps the task in a try/catch that records any {@link Throwable}
     * (including {@link Error} subtypes) at {@link Level#SEVERE} before
     * rethrowing. Without this wrapper, an {@code Error} thrown by the task
     * would surface only as an uncompleted future and a bare stack trace on
     * stderr — invisible to operators tailing the log file.
     *
     * @throws java.util.concurrent.RejectedExecutionException if the queue is full.
     */
    public java.util.concurrent.CompletableFuture<Void> submit(Runnable task) {
        return java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } catch (Throwable t) {
                logger.log(Level.SEVERE,
                    "[" + poolName + "] Uncaught exception in async submitted task: " + t.getMessage(), t);
                throw t;
            }
        }, executor);
    }

    /**
     * Shutdown the executor gracefully.
     *
     * @param timeoutSeconds maximum time to wait for pending tasks
     */
    public void shutdown(long timeoutSeconds) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                logger.warning("[" + poolName + "] Forcing shutdown after " + timeoutSeconds + "s timeout; "
                    + executor.shutdownNow().size() + " tasks were still pending.");
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.severe("[" + poolName + "] Executor did not terminate!");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("[" + poolName + "] Executor shut down.");
    }

    /**
     * Get the underlying ExecutorService (for callers that need to await
     * termination directly, e.g. {@link SyncManager#shutdown}).
     */
    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * Get the number of active tasks.
     */
    public int getActiveCount() {
        return executor.getActiveCount();
    }

    /**
     * Get the queue size (pending tasks).
     */
    public int getQueueSize() {
        return executor.getQueue().size();
    }

    /**
     * Get the queue capacity (max pending tasks before rejection).
     */
    public int getQueueCapacity() {
        return executor.getQueue().remainingCapacity() + executor.getQueue().size();
    }
}
