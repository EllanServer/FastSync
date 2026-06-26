package com.fastsync.redis;

import com.fastsync.redis.stream.StreamEvent;
import com.fastsync.redis.stream.StreamEventListener;
import com.fastsync.redis.stream.StreamEventType;
import org.redisson.Redisson;
import org.redisson.api.RStream;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.AutoClaimResult;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Unified Redis coordination manager built on Redisson.
 *
 * <p>This single class replaces the previous two-piece setup:
 * <ul>
 *   <li>{@code RedisManager} &mdash; Pub/Sub lock release notifications, formerly
 *       backed by sparrow-redis-message-broker + Lettuce.</li>
 *   <li>{@code StreamManager} &mdash; Redis Streams reliable event delivery,
 *       formerly backed by Lettuce directly.</li>
 * </ul>
 *
 * <p>One {@link RedissonClient} (single-server config) now backs both concerns:
 * <ul>
 *   <li><b>Pub/Sub</b> via {@link RTopic} on {@code "fastsync:lock"} &mdash; fast,
 *       fire-and-forget lock notifications ({@link LockMessage} with type
 *       {@code REQUEST} / {@code RELEASED}). A {@link CountDownLatch} per UUID is
 *       counted down when the matching {@code RELEASED} message arrives, exactly
 *       like the old {@code RedisManager#waitForLockRelease} flow.</li>
 *   <li><b>Streams</b> via {@link RStream} on {@code "fastsync:stream:events"}
 *       &mdash; recoverable, at-least-once critical sync events backed by a
 *       consumer group ({@code "fastsync-group"}). A daemon thread reads new
 *       entries with {@code readGroup}, acknowledges them after dispatch, and
 *       stale pending entries from a previous crash are reclaimed with
 *       {@code autoClaim}.</li>
 * </ul>
 *
 * <p>The public API intentionally matches the combined surface of the two old
 * managers so that {@code SyncManager} only needs its construction call updated.
 *
 * <p>Architecture layering (unchanged):
 * <ul>
 *   <li><b>Pub/Sub</b> (this class) &mdash; fast, non-critical lock notifications</li>
 *   <li><b>Streams</b> (this class) &mdash; reliable, critical handoff events</li>
 *   <li><b>DB</b> ({@code DatabaseManager}) &mdash; final source of truth</li>
 * </ul>
 */
public class RedissonManager {

    private static final Logger LOGGER = Logger.getLogger(RedissonManager.class.getName());

    // ==================== Pub/Sub (RTopic) ====================
    /** Topic used for cross-server lock coordination. Messages are {@link LockMessage} strings. */
    private final String lockTopicName;

    // ==================== Streams (RStream) ====================
    private final String streamKeyName;
    private final String consumerGroupName;
    /** readGroup block timeout (XREADGROUP BLOCK). */
    private static final long BLOCK_MS = 2000L;
    /** Reclaim pending entries idle for longer than this (XAUTOCLAIM min-idle-time). */
    private static final long AUTOCLAIM_IDLE_MS = 30000L;
    /** Maximum entries reclaimed per autoClaim cycle. */
    private static final int MAX_RECLAIM_PER_CYCLE = 10;
    /** Maximum entries fetched per readGroup call. */
    private static final int READ_BATCH_SIZE = 10;

    private final String host;
    private final int port;
    private final String password;
    private final int database;
    private final String serverName;
    private final boolean ssl;
    private final int timeout;

    private volatile boolean debug = false;

    /**
     * UUID &rarr; latch, created when a server starts waiting for a lock and
     * counted down when the matching {@code RELEASED} notification arrives.
     */
    private final ConcurrentHashMap<UUID, CountDownLatch> releaseWaiters = new ConcurrentHashMap<>();

    private final List<StreamEventListener> listeners = new ArrayList<>();

    private RedissonClient client;
    private RTopic lockTopic;
    private RStream<String, String> stream;
    private ExecutorService consumerExecutor;
    private ExecutorService messageDispatcher;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * @param host          Redis host
     * @param port          Redis port
     * @param password      Redis password (null/empty &rarr; no auth)
     * @param database      Redis logical database index
     * @param serverName    this server's name; used as the stream consumer name and
     *                      to skip self-published events
     * @param clusterId     cluster identifier for namespace isolation (empty = default)
     * @param channelPrefix Redis channel prefix (default "fastsync:lock:")
     * @param ssl           whether to use TLS (rediss:// scheme)
     * @param timeout       Redis connection + command timeout in milliseconds
     */
    public RedissonManager(String host, int port, String password, int database,
                           String serverName, String clusterId, String channelPrefix,
                           boolean ssl, int timeout) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.database = database;
        this.serverName = serverName;
        this.ssl = ssl;
        this.timeout = timeout;

        // Build namespace-aware names to prevent cross-cluster message mixing
        // when multiple FastSync deployments share the same Redis.
        // Pattern: fastsync:{clusterId}:lock, fastsync:{clusterId}:stream, fastsync:{clusterId}:group
        String ns = (clusterId == null || clusterId.isBlank()) ? "default" : clusterId;
        String prefix = (channelPrefix == null || channelPrefix.isBlank()) ? "fastsync:" : channelPrefix;
        // channelPrefix typically ends with ":" (e.g. "fastsync:lock:") — strip trailing ":"
        // to use as a base prefix, then append our own namespace segments.
        if (prefix.endsWith(":")) prefix = prefix.substring(0, prefix.length() - 1);
        // If prefix is "fastsync:lock", use "fastsync" as the base
        if (prefix.endsWith(":lock")) prefix = prefix.substring(0, prefix.indexOf(":lock"));

        this.lockTopicName = prefix + ":" + ns + ":lock";
        this.streamKeyName = prefix + ":" + ns + ":stream:events";
        this.consumerGroupName = prefix + ":" + ns + ":group";
    }

    /**
     * Legacy constructor without namespace isolation or SSL/timeout (uses "default" cluster, no TLS).
     */
    public RedissonManager(String host, int port, String password, int database, String serverName) {
        this(host, port, password, database, serverName, "", "", false, 5000);
    }

    /**
     * Enable/disable verbose debug logging (mirrors the old {@code config.isDebug()} gate).
     *
     * @param debug true to log pub/sub and stream trace messages
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * Build the single-server Redisson client, subscribe to the lock topic,
     * create the stream consumer group, recover stale pending entries and start
     * the background consumer thread.
     *
     * @throws RuntimeException if Redis cannot be reached or the consumer group
     *                          cannot be created
     */
    public void initialize() {
        Config config = new Config();
        String scheme = ssl ? "rediss://" : "redis://";
        SingleServerConfig single = config.useSingleServer()
            .setAddress(scheme + host + ":" + port)
            .setDatabase(database)
            .setConnectTimeout(timeout)
            .setTimeout(timeout);
        if (password != null && !password.isEmpty()) {
            single.setPassword(password);
        }

        try {
            client = Redisson.create(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to Redis at " + host + ":" + port, e);
        }

        // ---- Pub/Sub: plain-string topic messages ("REQUEST:<uuid>" / "RELEASED:<uuid>") ----
        lockTopic = client.getTopic(lockTopicName, StringCodec.INSTANCE);
        lockTopic.addListener(String.class, (channel, msg) -> onLockMessage(msg));

        // ---- Streams: String,String entries so StreamEvent.toMap()/fromMap() round-trip cleanly ----
        stream = client.getStream(streamKeyName, StringCodec.INSTANCE);

        createConsumerGroup();
        recoverPendingEntries();

        // IMPORTANT: create dispatcher BEFORE starting the consumer loop.
        // If messages exist in the stream, the consumer may immediately try to
        // dispatch them via messageDispatcher — if it's null, that's an NPE.
        messageDispatcher = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "FastSync-Stream-Dispatcher");
            t.setDaemon(true);
            return t;
        });

        running.set(true);
        consumerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "FastSync-Stream-Consumer");
            t.setDaemon(true);
            return t;
        });
        consumerExecutor.submit(this::consumeLoop);

        // Announce this server is up and ready to accept players.
        publish(StreamEvent.create(StreamEventType.SERVER_START, null, serverName,
            "", 0, 0, "Server started"));

        LOGGER.info("[Redisson] Redis connected: " + host + ":" + port + " (db=" + database
            + ", topic=" + lockTopicName + ", stream=" + streamKeyName + ").");
    }

    // ==================== Pub/Sub API (replaces RedisManager) ====================

    /**
     * Request a lock release from the server currently holding it. Broadcasts a
     * {@link LockMessage} of type {@code REQUEST} on the lock topic.
     *
     * @param uuid the player UUID whose lock we need
     */
    public void requestLockRelease(UUID uuid) {
        RTopic topic = lockTopic;
        if (topic == null) {
            return;
        }
        try {
            topic.publish(LockMessage.request(uuid).serialize());
            if (debug) {
                LOGGER.info("[Redisson] Published REQUEST for " + uuid);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to publish REQUEST for " + uuid, e);
        }
    }

    /**
     * Notify other servers that a lock has been released. Broadcasts a
     * {@link LockMessage} of type {@code RELEASED} on the lock topic.
     *
     * @param uuid the player UUID whose lock was released
     */
    public void notifyLockReleased(UUID uuid) {
        RTopic topic = lockTopic;
        if (topic == null) {
            return;
        }
        try {
            topic.publish(LockMessage.released(uuid).serialize());
            if (debug) {
                LOGGER.info("[Redisson] Published RELEASED for " + uuid);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to publish RELEASED for " + uuid, e);
        }
    }

    /**
     * Wait for a lock release notification for a specific player.
     *
     * <p>Registers a {@link CountDownLatch}, broadcasts a lock request and then
     * blocks until either the {@code RELEASED} message arrives or the timeout
     * elapses. On timeout the caller falls back to the database-level lock check.
     *
     * @param uuid      the player UUID to wait for
     * @param timeoutMs maximum time to wait in milliseconds
     * @return true if a RELEASED notification was received, false on timeout
     */
    public boolean waitForLockRelease(UUID uuid, long timeoutMs) {
        CountDownLatch latch = new CountDownLatch(1);
        releaseWaiters.put(uuid, latch);

        // Ask the current lock holder to notify us when it is done.
        requestLockRelease(uuid);

        try {
            boolean received = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (received && debug) {
                LOGGER.info("[Redisson] Received RELEASED notification for " + uuid);
            }
            return received;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            releaseWaiters.remove(uuid);
        }
    }

    /**
     * Handle an incoming lock topic message.
     *
     * <p>{@code REQUEST} is informational only (the holder cannot release while
     * still saving); {@code RELEASED} counts down the latch of any server waiting
     * on that UUID so it can retry acquiring the lock immediately.
     *
     * @param payload the serialized {@link LockMessage}
     */
    private void onLockMessage(String payload) {
        LockMessage msg;
        try {
            msg = LockMessage.deserialize(payload);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Malformed lock message: " + payload, e);
            return;
        }

        if (msg.type() == LockMessage.Type.RELEASED) {
            try {
                CountDownLatch latch = releaseWaiters.get(msg.uuid());
                if (latch != null) {
                    latch.countDown();
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[Redisson] Error handling lock released for " + msg.uuid(), e);
            }
        } else if (debug) {
            // REQUEST: holder will notify on save completion; nothing to do now.
            LOGGER.info("[Redisson] Lock release requested for " + msg.uuid()
                + " (will notify on save completion)");
        }
    }

    // ==================== Streams API (replaces StreamManager) ====================

    /**
     * Publish a critical event to the stream. Returns immediately (XADD is fast).
     *
     * @param event the event to append
     */
    public void publish(StreamEvent event) {
        RStream<String, String> s = stream;
        if (s == null) {
            return;
        }
        try {
            StreamMessageId id = s.add(StreamAddArgs.<String, String>entries(event.toMap()));
            if (debug) {
                LOGGER.info("[Redisson] Published " + event.type() + " (id=" + id
                    + ", uuid=" + event.uuid() + ")");
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to publish " + event.type(), e);
        }
    }

    /**
     * Register a listener for stream events received from other servers.
     *
     * @param listener callback invoked for each non-self event
     */
    public void addListener(StreamEventListener listener) {
        listeners.add(listener);
    }

    /**
     * Create the consumer group with {@code MKSTREAM}, ignoring a
     * {@code BUSYGROUP} error (group already exists).
     */
    private void createConsumerGroup() {
        try {
            stream.createGroup(StreamCreateGroupArgs.name(consumerGroupName)
                .id(StreamMessageId.ALL)
                .makeStream());
            LOGGER.info("[Redisson] Consumer group created: " + consumerGroupName);
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("BUSYGROUP")) {
                LOGGER.info("[Redisson] Consumer group already exists: " + consumerGroupName);
            } else {
                throw new RuntimeException("Failed to create consumer group " + consumerGroupName, e);
            }
        }
    }

    /**
     * Main consumer loop: {@code readGroup} ({@code >}) &rarr; dispatch &rarr; {@code ack}.
     * Runs on a daemon thread and blocks up to {@link #BLOCK_MS} per read.
     */
    private void consumeLoop() {
        while (running.get()) {
            try {
                Map<StreamMessageId, Map<String, String>> messages = stream.readGroup(
                    consumerGroupName, serverName,
                    StreamReadGroupArgs.neverDelivered()
                        .count(READ_BATCH_SIZE)
                        .timeout(Duration.ofMillis(BLOCK_MS)));

                if (messages == null || messages.isEmpty()) {
                    continue;
                }

                for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
                    StreamMessageId msgId = entry.getKey();
                    Map<String, String> body = entry.getValue();
                    messageDispatcher.submit(() -> {
                        try {
                            handleStreamMessage(msgId, body);
                            // Only ack on success — if handleStreamMessage throws,
                            // the message stays in the PEL and will be reprocessed
                            // by autoClaim on the next restart.
                            stream.ack(consumerGroupName, msgId);
                        } catch (Exception e) {
                            LOGGER.log(Level.WARNING, "[Redisson] Error dispatching stream message " + msgId
                                + " — message will remain in PEL for reprocessing", e);
                        }
                    });
                }
            } catch (Exception e) {
                if (!running.get()) {
                    break;
                }
                LOGGER.log(Level.WARNING, "[Redisson] Stream consumer loop error", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        LOGGER.info("[Redisson] Stream consumer loop stopped.");
    }

    /**
     * Dispatch a single stream entry to all registered listeners.
     *
     * <p>Events published by this server ({@code event.server().equals(serverName)})
     * are skipped to avoid re-processing our own broadcasts.
     */
    private void handleStreamMessage(StreamMessageId id, Map<String, String> body) {
        try {
            StreamEvent event = StreamEvent.fromMap(id.toString(), body);

            if (serverName.equals(event.server())) {
                if (debug) {
                    LOGGER.info("[Redisson] Skipping own event: " + event.type()
                        + " (id=" + event.id() + ")");
                }
                return;
            }

            if (debug) {
                LOGGER.info("[Redisson] Received " + event.type() + " from " + event.server()
                    + " (id=" + event.id() + ", uuid=" + event.uuid() + ")");
            }

            for (StreamEventListener listener : listeners) {
                try {
                    listener.onEvent(event);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "[Redisson] Stream listener exception for event " + event.id(), e);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to handle stream message " + id, e);
        }
    }

    /**
     * Recover pending entries left over from a previous crash.
     *
     * <p>When this server crashed and restarted, its previously delivered but
     * unacknowledged events remain in the pending entries list (PEL). They are
     * reclaimed with {@code autoClaim} (idle &gt; {@link #AUTOCLAIM_IDLE_MS}) and
     * reprocessed, then acknowledged.
     */
    private void recoverPendingEntries() {
        try {
            AutoClaimResult<String, String> claimed = stream.autoClaim(
                consumerGroupName, serverName,
                AUTOCLAIM_IDLE_MS, TimeUnit.MILLISECONDS,
                StreamMessageId.ALL, MAX_RECLAIM_PER_CYCLE);

            Map<StreamMessageId, Map<String, String>> messages = claimed.getMessages();
            if (messages == null || messages.isEmpty()) {
                return;
            }

            LOGGER.info("[Redisson] Recovered " + messages.size()
                + " pending entries from previous crash.");
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
                try {
                    handleStreamMessage(entry.getKey(), entry.getValue());
                    // Only ack on success — failed entries stay in PEL for next cycle
                    stream.ack(consumerGroupName, entry.getKey());
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "[Redisson] Failed to recover pending entry " + entry.getKey()
                        + " — will retry on next autoClaim cycle", e);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Redisson] Failed to recover pending entries", e);
        }
    }

    // ==================== Common ====================

    /**
     * Check whether the Redisson client is usable.
     *
     * @return true if the client is initialized and not shut down
     */
    public boolean isHealthy() {
        RedissonClient c = client;
        if (c == null) {
            return false;
        }
        try {
            return !c.isShutdown() && !c.isShuttingDown();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Stop the consumer thread, publish a server-stop event and shut down the
     * Redisson client (closing all pooled connections).
     */
    public void close() {
        running.set(false);
        if (consumerExecutor != null) {
            consumerExecutor.shutdownNow();
            try {
                if (!consumerExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                    LOGGER.warning("[Redisson] Stream consumer executor did not terminate in time.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (messageDispatcher != null) {
            messageDispatcher.shutdown();
            try {
                if (!messageDispatcher.awaitTermination(3, TimeUnit.SECONDS)) {
                    messageDispatcher.shutdownNow();
                }
            } catch (InterruptedException e) {
                messageDispatcher.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Announce shutdown so other servers are aware.
        publish(StreamEvent.create(StreamEventType.SERVER_STOP, null, serverName,
            "", 0, 0, "Server shutting down"));

        if (client != null) {
            try {
                client.shutdown();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[Redisson] Error shutting down Redisson client", e);
            }
            client = null;
            LOGGER.info("[Redisson] Redisson client shut down.");
        }
    }

    // ==================== Pub/Sub message type ====================

    /**
     * Lock coordination message exchanged over the {@link #lockTopicName}.
     *
     * <p>Serialized as a plain {@code "<TYPE>:<uuid>"} string and transported with
     * Redisson's {@link StringCodec} so no JSON/serialization layer is required.
     *
     * @param uuid the player UUID the message concerns
     * @param type {@link Type#REQUEST} or {@link Type#RELEASED}
     */
    public record LockMessage(UUID uuid, Type type) {

        /** Kind of lock notification. */
        public enum Type {
            /** A server is asking the current lock holder to release ASAP. */
            REQUEST,
            /** The lock holder has released the lock. */
            RELEASED
        }

        public static LockMessage request(UUID uuid) {
            return new LockMessage(uuid, Type.REQUEST);
        }

        public static LockMessage released(UUID uuid) {
            return new LockMessage(uuid, Type.RELEASED);
        }

        /** Serialize to {@code "<TYPE>:<uuid>"}. */
        public String serialize() {
            return type.name() + ":" + uuid;
        }

        /** Parse a {@code "<TYPE>:<uuid>"} payload back into a {@link LockMessage}. */
        public static LockMessage deserialize(String payload) {
            int idx = payload.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("Invalid lock message payload: " + payload);
            }
            Type t = Type.valueOf(payload.substring(0, idx));
            UUID u = UUID.fromString(payload.substring(idx + 1));
            return new LockMessage(u, t);
        }
    }
}
