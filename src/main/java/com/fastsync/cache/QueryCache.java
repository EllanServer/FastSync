package com.fastsync.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A short-TTL cache for database query results, using Caffeine.
 *
 * <p>Inspired by CraftEngine's {@code CachedStorage} decorator pattern,
 * this cache wraps database read operations with a short-lived in-memory
 * cache to reduce DB round-trips for frequently queried data.
 *
 * <p>Use cases in FastSync:
 * <ul>
 *   <li><strong>Lock state queries</strong> — during Spool replay, the same
 *       UUID's lock state may be queried multiple times within a short window.
 *       A 2-second TTL eliminates redundant queries.</li>
 *   <li><strong>Snapshot metadata</strong> — listing snapshots for a UUID
 *       that is being actively inspected via /fastsync status.</li>
 *   <li><strong>"No data" cache</strong> — cache negative results (UUID has
 *       no data) to skip unnecessary DB probes during login storms.</li>
 * </ul>
 *
 * <p>The cache uses {@code expireAfterWrite} (not {@code expireAfterAccess})
 * to ensure stale data is never served beyond the TTL, even under constant
 * access. This is critical for lock-state correctness: a cached lock state
 * older than the heartbeat interval could cause incorrect CAS decisions.
 *
 * <p>Thread-safe. All operations are non-blocking.
 *
 * @param <K> the cache key type (typically UUID)
 * @param <V> the cached value type
 */
public class QueryCache<K, V> {

    private final Cache<K, V> cache;
    private final String name;

    /**
     * Create a query cache with the specified TTL.
     *
     * @param name a human-readable name for logging/diagnostics
     * @param ttl  the time-to-live for cached entries
     * @param maxSize maximum number of entries in the cache
     */
    public QueryCache(String name, Duration ttl, int maxSize) {
        this.name = name;
        this.cache = Caffeine.newBuilder()
            .expireAfterWrite(ttl)
            .maximumSize(maxSize)
            .recordStats()
            .build();
    }

    /**
     * Get a cached value, or compute it via the supplied function if absent.
     *
     * @param key     the cache key
     * @param loader  the function to compute the value if not cached
     * @return the cached or computed value
     */
    public V get(K key, Function<K, V> loader) {
        return cache.get(key, loader);
    }

    /**
     * Get a cached value without computing.
     *
     * @param key the cache key
     * @return the cached value, or null if not present
     */
    public V getIfPresent(K key) {
        return cache.getIfPresent(key);
    }

    /**
     * Manually put a value into the cache.
     *
     * @param key   the cache key
     * @param value the value to cache
     */
    public void put(K key, V value) {
        cache.put(key, value);
    }

    /**
     * Invalidate a specific cache entry.
     *
     * @param key the cache key to invalidate
     */
    public void invalidate(K key) {
        cache.invalidate(key);
    }

    /**
     * Invalidate all cache entries.
     */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /**
     * Get the approximate number of entries in the cache.
     *
     * @return the estimated size
     */
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    /**
     * Get cache hit rate (0.0 to 1.0).
     *
     * @return the hit rate
     */
    public double hitRate() {
        return cache.stats().hitRate();
    }

    /**
     * Get the total number of cache hits.
     *
     * @return the hit count
     */
    public long hitCount() {
        return cache.stats().hitCount();
    }

    /**
     * Get the total number of cache misses.
     *
     * @return the miss count
     */
    public long missCount() {
        return cache.stats().missCount();
    }

    /**
     * Get the cache name for diagnostics.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Get a status line for /fastsync status display.
     *
     * @return a formatted status string
     */
    public String getStatusLine() {
        return name + ": size=" + estimatedSize()
            + ", hits=" + hitCount()
            + ", misses=" + missCount()
            + ", hitRate=" + String.format("%.1f%%", hitRate() * 100);
    }
}
