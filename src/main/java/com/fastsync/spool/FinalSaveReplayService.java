package com.fastsync.spool;

import com.fastsync.database.DatabaseManager;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitRunnable;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
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
 *   <li><b>CAS success</b>: move to done/, notify lock released via Redis.</li>
 *   <li><b>CAS fail + same-fencing self-conflict</b>: update expectedVersion
 *       in the spool file and retry (up to 3 attempts).</li>
 *   <li><b>CAS fail + fencing/session mismatch</b>: move to failed/ with
 *       reason — the lock belongs to a new session, old save must not
 *       overwrite.</li>
 *   <li><b>DB unavailable (SQLException)</b>: leave in pending/, retry
 *       on next cycle.</li>
 * </ul>
 *
 * <p>The CAS-failure classification uses {@link DatabaseManager#getLockState}
 * (one round-trip reading version + fencing + locked_by + lock_session_id)
 * instead of separate getCurrentVersion + getCurrentFencingToken calls.
 * Checking locked_by and lock_session_id prevents wasting a retry attempt
 * when the lock was stolen between the CAS failure and the diagnostic read.
 */
public class FinalSaveReplayService {

    private final Logger logger;
    private final FinalSaveSpool spool;
    private final DatabaseManager databaseManager;
    private final Plugin plugin;
    private final int batchSize;
    private final long intervalTicks;
    private final String serverName;
    /** Called with the player UUID after a successful replay so other servers
     *  on Redis pub/sub are notified the lock was released. May be null. */
    private final Consumer<UUID> lockReleasedCallback;
    private BukkitRunnable task;
    private volatile boolean running = false;

    public FinalSaveReplayService(Logger logger, FinalSaveSpool spool,
                                   DatabaseManager databaseManager, Plugin plugin,
                                   int batchSize, long intervalTicks, String serverName,
                                   Consumer<UUID> lockReleasedCallback) {
        this.logger = logger;
        this.spool = spool;
        this.databaseManager = databaseManager;
        this.plugin = plugin;
        this.batchSize = batchSize;
        this.intervalTicks = intervalTicks;
        this.serverName = serverName;
        this.lockReleasedCallback = lockReleasedCallback;
    }

    public void start() {
        if (running) return;
        running = true;
        // Run first replay asynchronously so it does not block plugin enable
        // / server startup when many spool files are pending from a previous
        // crash. Using BukkitRunnable keeps this Folia-compatible.
        new BukkitRunnable() {
            @Override
            public void run() {
                replayBatch();
            }
        }.runTaskAsynchronously(plugin);

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
                    // Notify other servers via Redis that the lock was released,
                    // mirroring the normal save path. Without this, servers
                    // waiting on Redis pub/sub for a RELEASED notification would
                    // only discover the release on their next DB poll.
                    if (lockReleasedCallback != null) {
                        try {
                            lockReleasedCallback.accept(rec.uuid());
                        } catch (Exception cbEx) {
                            logger.log(Level.FINE, "[FinalSaveReplay] lock-released callback failed for "
                                + rec.uuid(), cbEx);
                        }
                    }
                    logger.info("[FinalSaveReplay] Replayed final save for " + rec.uuid()
                        + " v" + rec.expectedVersion() + " -> v" + (rec.expectedVersion() + 1));
                } else {
                    // CAS failed — read the FULL lock state (version + fencing +
                    // locked_by + lock_session_id) in ONE round-trip to classify
                    // the failure precisely. This avoids the imprecision of
                    // comparing only version + fencing token: if the lock was
                    // stolen (locked_by / lock_session_id changed) but the
                    // fencing token happened to match, the old 2-query path
                    // would waste a retry attempt on a CAS that can never
                    // succeed.
                    DatabaseManager.LockState state;
                    try {
                        state = databaseManager.getLockState(rec.uuid());
                    } catch (java.sql.SQLException dbEx) {
                        // DB unavailable during the diagnostic read — keep the
                        // file in pending and retry next cycle. Moving it to
                        // failed/ would lose data just because the DB blipped
                        // during classification.
                        logger.log(Level.FINE, "[FinalSaveReplay] DB error reading lock state for "
                            + rec.uuid() + " — will retry next cycle", dbEx);
                        retained++;
                        continue;
                    }

                    if (state.rowExists()
                        && state.isHeldBy(rec.serverName(), rec.fencingToken(), rec.lockSessionId())
                        && state.version() > rec.expectedVersion()) {
                        // Same-fencing self-conflict: our own earlier save
                        // advanced the version while this spooled save was
                        // waiting. Retry with the actual version (up to 3
                        // attempts).
                        if (rec.attempts() < 3) {
                            try {
                                spool.rewriteWithUpdatedVersion(file, state.version());
                                logger.info("[FinalSaveReplay] Same-fencing retry for " + rec.uuid()
                                    + ": v" + rec.expectedVersion() + " -> v" + state.version()
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
                        // Fencing / session / locked_by mismatch — the lock
                        // belongs to a new session. The old final save must
                        // NOT overwrite the newer data. Move to failed/ with
                        // a detailed reason for manual investigation.
                        String reason;
                        if (!state.rowExists()) {
                            reason = "ROW_NOT_FOUND: player_data row vanished";
                        } else if (!state.isHeldBy(rec.serverName(), rec.fencingToken(), rec.lockSessionId())) {
                            reason = "LOCK_NOT_OURS: expected server=" + rec.serverName()
                                + " ft=" + rec.fencingToken()
                                + " session=" + rec.lockSessionId()
                                + ", actual server=" + state.lockedBy()
                                + " ft=" + state.fencingToken()
                                + " session=" + state.lockSessionId();
                        } else {
                            // Lock is ours but version did not advance —
                            // unexpected state, do not loop.
                            reason = "VERSION_NOT_ADVANCED: expected v" + rec.expectedVersion()
                                + " actual v" + state.version();
                        }
                        spool.moveToFailed(file, reason);
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
