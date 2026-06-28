package com.fastsync.spool;

import com.fastsync.serialization.CompressionUtil;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Disk-backed write-ahead log for final saves rejected by the executor. */
public final class FinalSaveSpool {

    private static final String MAGIC = "FASTSYNC_FINAL_SPOOL";
    private static final String FILE_SUFFIX = ".fspool";
    private static final DateTimeFormatter FILE_TS =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'").withZone(ZoneOffset.UTC);

    private final Logger logger;
    private final Path pendingDir;
    private final Path failedDir;
    private final Path doneDir;
    private final boolean fsync;
    private final long maxFiles;
    private final long maxBytes;
    private final int retainFailedDays;
    private final AtomicLong fileSequence = new AtomicLong();

    private final AtomicLong pendingCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();
    private final AtomicLong totalBytes = new AtomicLong();
    private volatile long lastReplayAt;
    private volatile String lastError;

    public FinalSaveSpool(Logger logger, Path baseDir, boolean fsync) throws IOException {
        this(logger, baseDir, fsync, Long.MAX_VALUE, Long.MAX_VALUE, 7);
    }

    public FinalSaveSpool(Logger logger, Path baseDir, boolean fsync,
                          long maxFiles, long maxBytes, int retainFailedDays) throws IOException {
        this.logger = logger;
        this.pendingDir = baseDir.resolve("pending");
        this.failedDir = baseDir.resolve("failed");
        this.doneDir = baseDir.resolve("done");
        this.fsync = fsync;
        this.maxFiles = Math.max(1, maxFiles);
        this.maxBytes = Math.max(1, maxBytes);
        this.retainFailedDays = Math.max(0, retainFailedDays);

        Files.createDirectories(pendingDir);
        Files.createDirectories(failedDir);
        Files.createDirectories(doneDir);
        cleanupExpiredFailed();
        scanExistingFiles();

        logger.info("[FinalSaveSpool] Initialized: " + pendingCount.get() + " pending files, "
            + totalBytes.get() + " bytes, " + failedCount.get() + " failed files. dir="
            + baseDir + " fsync=" + fsync + " limits=" + this.maxFiles + " files/"
            + this.maxBytes + " bytes");
    }

    private void scanExistingFiles() throws IOException {
        long pending = 0;
        long bytes = 0;
        try (var stream = Files.list(pendingDir)) {
            for (Path file : stream.filter(this::isSpoolFile).toList()) {
                pending++;
                bytes += safeSize(file);
            }
        }
        long failed;
        try (var stream = Files.list(failedDir)) {
            failed = stream.filter(this::isSpoolFile).count();
        }
        pendingCount.set(pending);
        totalBytes.set(bytes);
        failedCount.set(failed);
    }

    /** Append one record using a unique temporary file and an atomic rename. */
    public synchronized void append(EncodedFinalSave encoded) throws IOException {
        if (pendingCount.get() >= maxFiles) {
            throw new IOException("Final-save spool file limit reached: " + pendingCount.get()
                + "/" + maxFiles);
        }

        FinalSaveSpoolRecord record = new FinalSaveSpoolRecord(
            FinalSaveSpoolRecord.CURRENT_FORMAT,
            encoded.uuid(), encoded.clusterId(), encoded.serverName(), encoded.lockSessionId(),
            encoded.expectedVersion(), encoded.fencingToken(), encoded.checksum(),
            encoded.compressedBlob(), encoded.saveKind(), encoded.createdAt(), 0, null);

        long sequence = fileSequence.incrementAndGet();
        String fileName = FILE_TS.format(Instant.ofEpochMilli(encoded.createdAt()))
            + "_" + String.format(java.util.Locale.ROOT, "%019d", sequence)
            + "_" + encoded.uuid()
            + "_v" + encoded.expectedVersion()
            + "_ft" + encoded.fencingToken()
            + FILE_SUFFIX;
        Path dst = pendingDir.resolve(fileName);
        Path tmp = pendingDir.resolve(fileName + "." + UUID.randomUUID() + ".tmp");

        try {
            writeRecord(tmp, record);
            long newSize = Files.size(tmp);
            if (newSize > maxBytes - totalBytes.get()) {
                throw new IOException("Final-save spool byte limit reached: " + totalBytes.get()
                    + " + " + newSize + " > " + maxBytes);
            }
            moveAtomically(tmp, dst, false);
            pendingCount.incrementAndGet();
            totalBytes.addAndGet(newSize);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private void writeRecord(Path path, FinalSaveSpoolRecord record) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(MAGIC);
            out.writeInt(FinalSaveSpoolRecord.CURRENT_FORMAT);
            out.writeUTF(record.uuid().toString());
            out.writeUTF(nonNull(record.clusterId()));
            out.writeUTF(nonNull(record.serverName()));
            out.writeUTF(nonNull(record.lockSessionId()));
            out.writeLong(record.expectedVersion());
            out.writeLong(record.fencingToken());
            out.writeLong(record.checksum());
            out.writeUTF(nonNull(record.saveKind()));
            out.writeLong(record.createdAt());
            out.writeInt(record.attempts());
            out.writeUTF(truncateUtf(record.lastError(), 8_192));
            byte[] blob = record.compressedBlob();
            out.writeInt(blob.length);
            out.write(blob);
            out.flush();
            if (fsync) {
                fos.getFD().sync();
            }
        }
    }

    private static String nonNull(String value) {
        return value != null ? value : "";
    }

    private static String truncateUtf(String value, int maxChars) {
        if (value == null) return "";
        return value.length() <= maxChars ? value : value.substring(0, maxChars);
    }

    private static void moveAtomically(Path source, Path target, boolean replace) throws IOException {
        try {
            if (replace) {
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            }
        } catch (AtomicMoveNotSupportedException e) {
            if (replace) {
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.move(source, target);
            }
        }
    }

    /** Move a successfully replayed record out of pending storage. */
    public synchronized void moveToDone(Path file) {
        long size = safeSize(file);
        try {
            Files.move(file, doneDir.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            decrementPending(size);
        } catch (IOException moveError) {
            logger.log(Level.WARNING, "[FinalSaveSpool] Failed to archive " + file
                + " in done/; deleting the already-applied pending record", moveError);
            try {
                if (Files.deleteIfExists(file)) {
                    decrementPending(size);
                }
            } catch (IOException deleteError) {
                logger.log(Level.WARNING, "[FinalSaveSpool] Failed to remove applied record " + file,
                    deleteError);
            }
        }
    }

    /** Move a permanently rejected record to failed/ and write its reason. */
    public synchronized void moveToFailed(Path file, String reason) {
        long size = safeSize(file);
        Path target = failedDir.resolve(file.getFileName());
        try {
            Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
            decrementPending(size);
            failedCount.incrementAndGet();
            lastError = reason;
        } catch (IOException e) {
            logger.log(Level.WARNING, "[FinalSaveSpool] Failed to move " + file + " to failed/", e);
            return;
        }

        try {
            Path reasonFile = failedDir.resolve(file.getFileName() + ".reason.txt");
            Files.writeString(reasonFile, "reason=" + nonNull(reason) + "\ntime=" + Instant.now() + "\n");
        } catch (IOException e) {
            logger.log(Level.WARNING, "[FinalSaveSpool] Failed to write reason for " + target, e);
        }
    }

    private void decrementPending(long size) {
        pendingCount.updateAndGet(value -> Math.max(0, value - 1));
        totalBytes.updateAndGet(value -> Math.max(0, value - size));
    }

    /** Return newest final states first; applying an older record first releases its lock. */
    // P1 (issue #66): synchronized to ensure consistency with concurrent
    // append/moveToDone/moveToFailed. Without sync, Files.list(pendingDir)
    // could miss files that were just atomically moved in by append — those
    // files would only be picked up on the next replay cycle, delaying lock
    // release by up to one replay interval (5s default).
    public synchronized List<Path> listPending(int maxBatch) {
        if (maxBatch <= 0) return List.of();
        List<Path> files = new ArrayList<>();
        try (var stream = Files.list(pendingDir)) {
            stream.filter(this::isSpoolFile).forEach(files::add);
        } catch (IOException e) {
            logger.log(Level.WARNING, "[FinalSaveSpool] Failed to list pending files", e);
            return List.of();
        }

        files.sort(Comparator
            .comparingLong(this::safeCreatedAt).reversed()
            .thenComparing(Comparator.comparingLong(this::safeLastModified).reversed())
            .thenComparing((Path path) -> path.getFileName().toString(), Comparator.reverseOrder()));
        return files.size() <= maxBatch
            ? files
            : new ArrayList<>(files.subList(0, maxBatch));
    }

    /** Read and validate a spool record. Version 1 remains readable. */
    public FinalSaveSpoolRecord read(Path file) throws IOException {
        try (var fis = Files.newInputStream(file);
             var bis = new java.io.BufferedInputStream(fis);
             var dis = new DataInputStream(bis)) {
            String magic = dis.readUTF();
            if (!MAGIC.equals(magic)) {
                throw new IOException("Invalid magic: " + magic);
            }
            int formatVersion = dis.readInt();
            if (formatVersion < 1 || formatVersion > FinalSaveSpoolRecord.CURRENT_FORMAT) {
                throw new IOException("Unsupported spool format: " + formatVersion);
            }

            UUID uuid;
            try {
                uuid = UUID.fromString(dis.readUTF());
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid player UUID in spool record", e);
            }
            String clusterId = dis.readUTF();
            String serverName = dis.readUTF();
            String lockSessionId = dis.readUTF();
            long expectedVersion = dis.readLong();
            long fencingToken = dis.readLong();
            long checksum = dis.readLong();
            String saveKind = dis.readUTF();
            long createdAt = dis.readLong();
            int attempts = 0;
            String lastRecordError = null;
            if (formatVersion >= 2) {
                attempts = dis.readInt();
                if (attempts < 0) {
                    throw new IOException("Invalid negative replay attempt count: " + attempts);
                }
                String encodedError = dis.readUTF();
                lastRecordError = encodedError.isEmpty() ? null : encodedError;
            }

            int blobLen = dis.readInt();
            int maxBlobBytes = CompressionUtil.getMaxWrappedBytes();
            if (blobLen <= 0 || blobLen > maxBlobBytes) {
                throw new IOException("Invalid spool blob length: " + blobLen
                    + " (limit=" + maxBlobBytes + ")");
            }
            byte[] blob = new byte[blobLen];
            dis.readFully(blob);
            if (dis.read() != -1) {
                throw new IOException("Trailing bytes after spool record payload");
            }
            return new FinalSaveSpoolRecord(
                formatVersion, uuid, clusterId, serverName,
                lockSessionId.isEmpty() ? null : lockSessionId,
                expectedVersion, fencingToken, checksum, blob,
                saveKind, createdAt, attempts, lastRecordError);
        }
    }

    /** Atomically rewrite a record for a same-session CAS retry. */
    public synchronized void rewriteWithUpdatedVersion(Path file, long newExpectedVersion,
                                                        String error) throws IOException {
        FinalSaveSpoolRecord current = read(file);
        FinalSaveSpoolRecord updated = new FinalSaveSpoolRecord(
            FinalSaveSpoolRecord.CURRENT_FORMAT,
            current.uuid(), current.clusterId(), current.serverName(), current.lockSessionId(),
            newExpectedVersion, current.fencingToken(), current.checksum(), current.compressedBlob(),
            current.saveKind(), current.createdAt(), current.attempts() + 1, error);

        Path tmp = file.resolveSibling(file.getFileName() + "." + UUID.randomUUID() + ".rewrite.tmp");
        long oldSize = safeSize(file);
        try {
            writeRecord(tmp, updated);
            long newSize = Files.size(tmp);
            long projectedBytes = totalBytes.get() - oldSize + newSize;
            if (projectedBytes > maxBytes) {
                throw new IOException("Final-save spool byte limit would be exceeded by rewrite: "
                    + projectedBytes + " > " + maxBytes);
            }
            moveAtomically(tmp, file, true);
            totalBytes.updateAndGet(value -> Math.max(0, value - oldSize + newSize));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    public void rewriteWithUpdatedVersion(Path file, long newExpectedVersion) throws IOException {
        rewriteWithUpdatedVersion(file, newExpectedVersion, "same-session version conflict");
    }

    /** Remove failed records older than the configured retention period. */
    public synchronized void cleanupExpiredFailed() {
        Instant cutoff = Instant.now().minus(retainFailedDays, ChronoUnit.DAYS);
        try (var stream = Files.list(failedDir)) {
            for (Path file : stream.filter(this::isSpoolFile).toList()) {
                try {
                    if (Files.getLastModifiedTime(file).toInstant().isAfter(cutoff)) continue;
                    if (Files.deleteIfExists(file)) {
                        failedCount.updateAndGet(value -> Math.max(0, value - 1));
                    }
                    Files.deleteIfExists(failedDir.resolve(file.getFileName() + ".reason.txt"));
                } catch (IOException e) {
                    logger.log(Level.WARNING, "[FinalSaveSpool] Failed to expire " + file, e);
                }
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "[FinalSaveSpool] Failed to scan failed records", e);
        }
    }

    private boolean isSpoolFile(Path file) {
        return Files.isRegularFile(file) && file.getFileName().toString().endsWith(FILE_SUFFIX);
    }

    private long safeSize(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            return 0;
        }
    }

    private long safeLastModified(Path file) {
        try {
            return Files.getLastModifiedTime(file).toMillis();
        } catch (IOException e) {
            return Long.MAX_VALUE;
        }
    }

    private long safeCreatedAt(Path file) {
        String name = file.getFileName().toString();
        try {
            int separator = name.indexOf('_');
            if (separator > 0) {
                return Instant.from(FILE_TS.parse(name.substring(0, separator))).toEpochMilli();
            }
        } catch (RuntimeException ignored) {
            // Fall back to filesystem metadata for legacy or manually renamed files.
        }
        return safeLastModified(file);
    }

    public long getPendingCount() { return pendingCount.get(); }
    public long getFailedCount() { return failedCount.get(); }
    public long getTotalBytes() { return totalBytes.get(); }
    public long getLastReplayAt() { return lastReplayAt; }
    public String getLastError() { return lastError; }

    public void recordReplay() {
        lastReplayAt = System.currentTimeMillis();
    }
}
