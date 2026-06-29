package com.fastsync.database;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

/**
 * Abstraction layer for database operations, inspired by CraftEngine's
 * {@code WorldDataStorage} pattern.
 *
 * <p>This interface decouples the sync logic from a specific database
 * implementation. The current default implementation is
 * {@link DatabaseManager} (MySQL via HikariCP + jOOQ), but this interface
 * allows future implementations for PostgreSQL, SQLite, or other backends
 * without modifying {@code SyncManager}.
 *
 * <p>All methods follow the OCC (Optimistic Concurrency Control) model with
 * fencing tokens:
 * <ul>
 *   <li>Locks are acquired with a monotonically increasing fencing token</li>
 *   <li>Saves use CAS (Compare-And-Swap) on version + fencing_token</li>
 *   <li>Heartbeats refresh lock timestamps in batch</li>
 * </ul>
 *
 * @see DatabaseManager for the MySQL implementation
 */
public interface DatabaseBackend {

    /**
     * Acquire a cross-server lock for a player.
     *
     * <p>Uses INSERT ... ON DUPLICATE KEY UPDATE to atomically acquire
     * or steal an expired lock. The fencing token is incremented on
     * each successful acquisition.
     *
     * @param uuid          the player UUID
     * @param serverName    the server requesting the lock
     * @param lockSessionId unique session identifier (prevents same-server races)
     * @return the lock result (success/failure with fencing token)
     * @throws SQLException on database error
     */
    LockResult acquireLock(UUID uuid, String serverName, String lockSessionId) throws SQLException;

    /**
     * Load player data (must hold the lock).
     *
     * @param uuid the player UUID
     * @return the loaded data blob, or null if no data exists
     * @throws SQLException on database error
     */
    byte[] loadData(UUID uuid) throws SQLException;

    /**
     * Save player data and release the lock atomically (CAS).
     *
     * @param uuid          the player UUID
     * @param data          the serialized+compressed data blob
     * @param checksum      CRC32 checksum of the raw data
     * @param expectedVersion the version expected by the caller (OCC)
     * @param newVersion    the new version to write
     * @param fencingToken  the fencing token (must match the lock)
     * @param serverName    the server name (must match the lock)
     * @param lockSessionId the lock session ID (must match the lock)
     * @return true if the CAS succeeded, false if version/fencing mismatch
     * @throws SQLException on database error
     */
    boolean saveDataAndReleaseLockClearComponents(UUID uuid, byte[] data, long checksum,
        long expectedVersion, long newVersion, long fencingToken,
        String serverName, String lockSessionId) throws SQLException;

    /**
     * Save player data while keeping the lock (CAS).
     *
     * @param uuid          the player UUID
     * @param data          the serialized+compressed data blob
     * @param checksum      CRC32 checksum of the raw data
     * @param expectedVersion the version expected by the caller (OCC)
     * @param newVersion    the new version to write
     * @param fencingToken  the fencing token (must match the lock)
     * @param serverName    the server name (must match the lock)
     * @param lockSessionId the lock session ID (must match the lock)
     * @return true if the CAS succeeded, false if version/fencing mismatch
     * @throws SQLException on database error
     */
    boolean saveDataKeepLockClearComponents(UUID uuid, byte[] data, long checksum,
        long expectedVersion, long newVersion, long fencingToken,
        String serverName, String lockSessionId) throws SQLException;

    /**
     * Release a lock without saving data.
     *
     * @param uuid          the player UUID
     * @param serverName    the server releasing the lock
     * @param fencingToken  the fencing token (must match)
     * @param lockSessionId the lock session ID (must match)
     * @return true if the lock was released, false if it didn't match
     * @throws SQLException on database error
     */
    boolean releaseLock(UUID uuid, String serverName, long fencingToken,
        String lockSessionId) throws SQLException;

    /**
     * Refresh (heartbeat) a lock to prevent timeout.
     *
     * @param uuid          the player UUID
     * @param serverName    the server holding the lock
     * @param fencingToken  the fencing token (must match)
     * @param lockSessionId the lock session ID (must match)
     * @return true if the lock was refreshed, false if it was lost
     * @throws SQLException on database error
     */
    boolean refreshLock(UUID uuid, String serverName, long fencingToken,
        String lockSessionId) throws SQLException;

    /**
     * Batch refresh locks for multiple players (single round-trip).
     *
     * @param playersToRefresh map of UUID → fencing token
     * @param serverName       the server holding the locks
     * @param lockSessionId    the lock session ID
     * @return map of UUID → true/false (refreshed/failed)
     * @throws SQLException on database error
     */
    Map<UUID, Boolean> refreshLockBatch(Map<UUID, Long> playersToRefresh,
        String serverName, String lockSessionId) throws SQLException;

    /**
     * Check if the database connection is healthy.
     *
     * @return true if the database is reachable and responsive
     */
    boolean isHealthy();
}
