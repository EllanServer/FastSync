package com.fastsync.spool;

import com.fastsync.database.DatabaseManager;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitRunnable;

import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodic replay service for spooled final-saves.
 *
 * <p>On startup, scans the pending directory and replays each file through
 * the DB CAS path. Runs on a repeating timer so newly spooled saves (from
 * runtime queue-full events) are also picked up.
 *
 * <p>Replay strategy:
 * <ul>
 *   <li><b>CAS success</b>: move to done/, notify lock released.</li>
 *   <li><b>CAS fail + same-fencing self-conflict</b>: update expectedVersion
 *       in the spool file and retry (up to 3 attempts).</li>
 *   <li><b>CAS fail + fencing/session mismatch</b>: move to failed/ with
 *       reason — the lock belongs to a new session, old save must not
 *       overwrite.</li>
 *   <li><b>DB unavailable (SQLException)</b>: leave in pending/, retry
 *       on next cycle.</li>
 * </ul>
 */
public class FinalSaveReplayService {

    private final Logger logger;
    private final FinalSaveSpool spool;
    private final DatabaseManager databaseManager;
    private final Plugin plugin;
    private final int batchSize;
    private final long intervalTicks;
    private final String serverName;
    private BukkitRunnable task;
    private volatile boolean running = false;

    public FinalSaveReplayService(Logger logger, FinalSaveSpool spool,
                                   DatabaseManager databaseManager, Plugin plugin,
                                   int batchSize, long intervalTicks, String serverName) {
        this.logger = logger;
        this.spool = spool;
        this.databaseManager = databaseManager;
        this.plugin = plugin;
        this.batchSize = batchSize;
        this.intervalTicks = intervalTicks;
        this.serverName = serverName;
    }

    public void start() {
        if (running) return;
        running = true;
        // Run first replay immediately on startup (async)
        replayBatch();

        // Then schedule periodic replay
        task = new BukkitRunnable() {
            @Override
            public void run() {
                replayBatch();
            }
        };
        task.runTaskTimerAsynchronously(plugin, intervalTicks, intervalTicks);
        logger.info("[FinalSaveReplay] Started: batch=" + batchSize
            + " interval=" + intervalTicks + " ticks");
    }

    public void stop() {
        running = false;
        if (task != null) {
            task.cancel();
            task = null;
        }
    }

    private void replayBatch() {
        if (!running) return;
        List<Path> files = spool.listPending(batchSize);
        if (files.isEmpty()) return;

        logger.info("[FinalSaveReplay] Replaying " + files.size() + " spooled saves...");
        int succeeded = 0;
        int failed = 0;
        int retained = 0;

        for (Path file : files) {
            try {
                FinalSaveSpoolRecord rec = spool.read(file);
                boolean ok = false;
                try {
                    ok = databaseManager.saveDataAndReleaseLockClearComponents(
                        rec.uuid(),
                        rec.compressedBlob(),
                        rec.checksum(),
                        rec.expectedVersion(),
                        rec.fencingToken(),
                        rec.serverName(),
                        rec.lockSessionId()
                    );
                } catch (java.sql.SQLException e) {
                    // DB unavailable — leave in pending for next cycle
                    logger.log(Level.FINE, "[FinalSaveReplay] DB error replaying " + rec.uuid()
                        + " — will retry next cycle", e);
                    retained++;
                    continue;
                }

                if (ok) {
                    spool.moveToDone(file);
                    succeeded++;
                    logger.info("[FinalSaveReplay] Replayed final save for " + rec.uuid()
                        + " v" + rec.expectedVersion() + " -> v" + (rec.expectedVersion() + 1));
                } else {
                    // CAS failed — check if same-fencing self-conflict
                    long actualVersion = databaseManager.getCurrentVersion(rec.uuid());
                    long actualFencing = databaseManager.getCurrentFencingToken(rec.uuid());

                    if (actualFencing == rec.fencingToken() && actualVersion > rec.expectedVersion()) {
                        // Same-fencing self-conflict: our own earlier save advanced the version.
                        // Retry with actual version (up to 3 attempts).
                        if (rec.attempts() < 3) {
                            try {
                                spool.rewriteWithUpdatedVersion(file, actualVersion);
                                logger.info("[FinalSaveReplay] Same-fencing retry for " + rec.uuid()
                                    + ": v" + rec.expectedVersion() + " -> v" + actualVersion
                                    + " (attempt " + (rec.attempts() + 1) + "/3)");
                                retained++;
                            } catch (Exception rewriteEx) {
                                logger.log(Level.WARNING, "[FinalSaveReplay] Failed to rewrite spool for " + rec.uuid(), rewriteEx);
                                spool.moveToFailed(file, "REWRITE_FAILED: " + rewriteEx.getMessage());
                                failed++;
                            }
                        } else {
                            spool.moveToFailed(file, "MAX_RETRIES_EXCEEDED: same-fencing conflict after 3 attempts");
                            failed++;
                        }
                    } else {
                        // Fencing/session mismatch — lock belongs to a new session
                        spool.moveToFailed(file,
                            "FENCING_MISMATCH: expected ft=" + rec.fencingToken()
                            + " actual ft=" + actualFencing
                            + " actual v=" + actualVersion);
                        failed++;
                    }
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "[FinalSaveReplay] Failed to process spool file " + file, e);
                spool.moveToFailed(file, "READ_ERROR: " + e.getMessage());
                failed++;
            }
        }

        spool.recordReplay();
        if (succeeded > 0 || failed > 0) {
            logger.info("[FinalSaveReplay] Batch done: " + succeeded + " succeeded, "
                + failed + " failed, " + retained + " retained for retry.");
        }
    }
}
