package com.fastsync.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
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
 *   - Bounded pool size to prevent resource exhaustion
 *   - Graceful shutdown with timeout
 *   - Proper exception logging
 */
public class AsyncExecutor {

    private final ExecutorService executor;
    private final Logger logger;
    private final String poolName;

    public AsyncExecutor(Logger logger, String poolName, int poolSize) {
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

        this.executor = Executors.newFixedThreadPool(
            Math.max(2, poolSize),
            threadFactory
        );

        logger.info("Async executor '" + poolName + "' initialized with " + poolSize + " threads.");
    }

    /**
     * Submit a task for async execution.
     * Exceptions are logged automatically.
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
     * Get the number of active tasks.
     */
    public int getActiveCount() {
        if (executor instanceof java.util.concurrent.ThreadPoolExecutor tpe) {
            return tpe.getActiveCount();
        }
        return -1;
    }

    /**
     * Get the queue size (pending tasks).
     */
    public int getQueueSize() {
        if (executor instanceof java.util.concurrent.ThreadPoolExecutor tpe) {
            return tpe.getQueue().size();
        }
        return -1;
    }
}
