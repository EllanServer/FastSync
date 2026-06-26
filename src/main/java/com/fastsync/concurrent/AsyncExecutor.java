package com.fastsync.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dedicated thread pool for FastSync async operations.
 *
 * Replaces CompletableFuture.runAsync() (which uses ForkJoinPool.commonPool)
 * with a properly sized, named, and manageable thread pool.
 *
 * Thread management is critical for sync plugins - HuskSync's poor thread
 * management was specifically criticized. This executor provides:
 *   - Named threads for easy debugging
 *   - Bounded pool size AND bounded queue to prevent OOM under load
 *   - CallerRunsPolicy backpressure (caller thread runs the task when queue
 *     is full, naturally throttling submission rate)
 *   - Graceful shutdown with timeout
 *   - Proper exception logging
 */
public class AsyncExecutor {

    private final ThreadPoolExecutor executor;
    private final Logger logger;
    private final String poolName;

    /**
     * @param poolSize      number of threads
     * @param queueCapacity max tasks pending in queue (0 = unbounded not allowed;
     *                      use a positive value to prevent OOM under login storms)
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
        int actualQueueCapacity = Math.max(64, queueCapacity);

        this.executor = new ThreadPoolExecutor(
            actualPoolSize,
            actualPoolSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(actualQueueCapacity),
            threadFactory,
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        logger.info("Async executor '" + poolName + "' initialized: "
            + actualPoolSize + " threads, queue capacity " + actualQueueCapacity
            + ", backpressure=CallerRunsPolicy.");
    }

    /**
     * Legacy constructor — uses a default queue capacity of 256.
     */
    public AsyncExecutor(Logger logger, String poolName, int poolSize) {
        this(logger, poolName, poolSize, 256);
    }

    /**
     * Submit a task for async execution.
     * Exceptions are logged automatically.
     * If the queue is full, CallerRunsPolicy runs the task on the calling
     * thread, providing natural backpressure.
     */
    public void execute(Runnable task) {
        executor.execute(() -> {
            try {
                task.run();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "[" + poolName + "] Uncaught exception in async task", e);
            }
        });
    }

    /**
     * Returns the underlying ExecutorService for use by components that need
     * to pass an Executor to CompletableFuture.supplyAsync() etc.
     * This avoids falling back to ForkJoinPool.commonPool().
     */
    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * Submit a task and return a CompletableFuture.
     */
    public java.util.concurrent.CompletableFuture<Void> submit(Runnable task) {
        return java.util.concurrent.CompletableFuture.runAsync(task, executor);
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
                logger.warning("[" + poolName + "] Forcing shutdown after " + timeoutSeconds + "s timeout");
                executor.shutdownNow();
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
     * Get the number of active threads.
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
}
