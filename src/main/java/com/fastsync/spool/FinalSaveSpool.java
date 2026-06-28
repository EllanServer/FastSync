package com.fastsync.spool;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Disk-based write-ahead log (WAL) for final-saves that could not be
 * submitted to the final-save executor (queue full).
 *
 * <p>Each spooled save is written as an individual file under
 * {@code pending/}. Files use atomic write (.tmp → rename) with optional
 * fsync. On replay, successful files are moved to {@code done/} and
 * permanently failed files to {@code failed/} with a .reason sidecar.
 */
public class FinalSaveSpool {

    private final Logger logger;
    private final Path pendingDir;
    private final Path failedDir;
    private final Path doneDir;
    private final boolean fsync;

    private final AtomicLong pendingCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();
    private final AtomicLong totalBytes = new AtomicLong();
    private volatile long lastReplayAt;
    private volatile String lastError;

    private static final DateTimeFormatter FILE_TS =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'").withZone(ZoneOffset.UTC);

    public FinalSaveSpool(Logger logger, Path baseDir, boolean fsync) throws IOException {
        this.logger = logger;
        this.pendingDir = baseDir.resolve("pending");
        this.failedDir = baseDir.resolve("failed");
        this.doneDir = baseDir.resolve("done");
        this.fsync = fsync;
        Files.createDirectories(pendingDir);
        Files.createDirectories(failedDir);
        Files.createDirectories(doneDir);
        // Count existing pending files on startup
        long count = 0;
        long bytes = 0;
        try (var stream = Files.list(pendingDir)) {
            var iter = stream.iterator();
            while (iter.hasNext()) {
                var f = iter.next();
                if (f.toString().endsWith(".fspool")) {
                    count++;
                    try { bytes += Files.size(f); } catch (IOException ignored) {}
                }
            }
        }
        pendingCount.set(count);
        totalBytes.set(bytes);
        logger.info("[FinalSaveSpool] Initialized: " + count + " pending files, "
            + bytes + " bytes. dir=" + baseDir + " fsync=" + fsync);
    }

    /**
     * Append a final-save to the spool. Atomic write via .tmp + rename.
     */
    public void append(EncodedFinalSave encoded) throws IOException {
        String fileName = FILE_TS.format(Instant.ofEpochMilli(encoded.createdAt()))
            + "_" + encoded.uuid()
            + "_v" + encoded.expectedVersion()
            + "_ft" + encoded.fencingToken()
            + ".fspool";
        Path tmp = pendingDir.resolve(fileName + ".tmp");
        Path dst = pendingDir.resolve(fileName);

        try (FileOutputStream fos = new FileOutputStream(tmp.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF("FASTSYNC_FINAL_SPOOL");
            out.writeInt(FinalSaveSpoolRecord.CURRENT_FORMAT);
            out.writeUTF(encoded.uuid().toString());
            out.writeUTF(encoded.clusterId());
            out.writeUTF(encoded.serverName());
            out.writeUTF(encoded.lockSessionId() != null ? encoded.lockSessionId() : "");
            out.writeLong(encoded.expectedVersion());
            out.writeLong(encoded.fencingToken());
            out.writeLong(encoded.checksum());
            out.writeUTF(encoded.saveKind());
            out.writeLong(encoded.createdAt());
            byte[] blob = encoded.compressedBlob();
            out.writeInt(blob.length);
            out.write(blob);
            out.flush();
            if (fsync) {
                fos.getFD().sync();
            }
        }
        Files.move(tmp, dst, StandardCopyOption.ATOMIC_MOVE);
        pendingCount.incrementAndGet();
        totalBytes.addAndGet(Files.size(dst));
    }

    /**
     * Move a pending file to done/ (replay succeeded).
     */
    public void moveToDone(Path file) {
        try {
            Path target = doneDir.resolve(file.getFileName());
            Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
            pendingCount.decrementAndGet();
        } catch (IOException e) {
            logger.log(Level.WARNING, "[FinalSaveSpool] Failed to move " + file + " to done/", e);
            try { Files.deleteIfExists(file); pendingCount.decrementAndGet(); } catch (IOException ignored) {}
        }
    }

    /**
     * Move a pending file to failed/ with a reason sidecar.
     */
    public void moveToFailed(Path file, String reason) {
        try {
            Path target = failedDir.resolve(file.getFileName());
            Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
            // Write reason sidecar
            Path reasonFile = failedDir.resolve(file.getFileName() + ".reason.txt");
            Files.writeString(reasonFile, "reason=" + reason + "\ntime=" + Instant.now() + "\n");
            pendingCount.decrementAndGet();
            failedCount.incrementAndGet();
            lastError = reason;
        } catch (IOException e) {
            logger.log(Level.WARNING, "[FinalSaveSpool] Failed to move " + file + " to failed/", e);
        }
    }

    /**
     * List pending .fspool files for replay.
     */
    public java.util.List<Path> listPending(int maxBatch) {
        java.util.List<Path> files = new java.util.ArrayList<>();
        try (var stream = Files.list(pendingDir)) {
            stream.filter(f -> f.toString().endsWith(".fspool"))
                  .limit(maxBatch)
                  .forEach(files::add);
        } catch (IOException e) {
            logger.log(Level.WARNING, "[FinalSaveSpool] Failed to list pending files", e);
        }
        return files;
    }

    /**
     * Read a spool file back into a record.
     */
    public FinalSaveSpoolRecord read(Path file) throws IOException {
        try (var fis = Files.newInputStream(file);
             var bis = new java.io.BufferedInputStream(fis);
             var dis = new java.io.DataInputStream(bis)) {
            String magic = dis.readUTF();
            if (!"FASTSYNC_FINAL_SPOOL".equals(magic)) {
                throw new IOException("Invalid magic: " + magic);
            }
            int formatVersion = dis.readInt();
            UUID uuid = UUID.fromString(dis.readUTF());
            String clusterId = dis.readUTF();
            String serverName = dis.readUTF();
            String lockSessionId = dis.readUTF();
            long expectedVersion = dis.readLong();
            long fencingToken = dis.readLong();
            long checksum = dis.readLong();
            String saveKind = dis.readUTF();
            long createdAt = dis.readLong();
            int blobLen = dis.readInt();
            byte[] blob = new byte[blobLen];
            dis.readFully(blob);
            return new FinalSaveSpoolRecord(
                formatVersion, uuid, clusterId, serverName,
                lockSessionId.isEmpty() ? null : lockSessionId,
                expectedVersion, fencingToken, checksum, blob,
                saveKind, createdAt, 0, null
            );
        }
    }

    /**
     * Rewrite a spool file with updated expectedVersion (for same-fencing retry).
     */
    public void rewriteWithUpdatedVersion(Path file, long newExpectedVersion) throws IOException {
        FinalSaveSpoolRecord rec = read(file);
        EncodedFinalSave updated = new EncodedFinalSave(
            rec.uuid(), rec.clusterId(), rec.serverName(), rec.lockSessionId(),
            newExpectedVersion, rec.fencingToken(), rec.checksum(),
            rec.compressedBlob(), rec.saveKind(), System.currentTimeMillis()
        );
        Files.deleteIfExists(file);
        append(updated);
    }

    // ==================== Telemetry ====================

    public long getPendingCount() { return pendingCount.get(); }
    public long getFailedCount() { return failedCount.get(); }
    public long getTotalBytes() { return totalBytes.get(); }
    public long getLastReplayAt() { return lastReplayAt; }
    public String getLastError() { return lastError; }

    public void recordReplay() {
        lastReplayAt = System.currentTimeMillis();
    }
}
