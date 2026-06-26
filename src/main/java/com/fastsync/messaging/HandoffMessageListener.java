package com.fastsync.messaging;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.database.DatabaseManager;
import com.fastsync.sync.SyncManager;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.plugin.messaging.PluginMessageListener;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listens for plugin messages on the {@code fastsync:handoff} channel from
 * the Velocity proxy.
 *
 * <p>Handles:
 * <ul>
 *   <li>QUERY_LOCK: Proxy asks "do you still hold this player's lock?"</li>
 *   <li>HANDOFF_NOTIFY: Proxy notifies "player just arrived from oldServer"</li>
 *   <li>STATUS_QUERY: Proxy asks for aggregated FastSync status</li>
 * </ul>
 */
public class HandoffMessageListener implements PluginMessageListener {

    public static final String CHANNEL = "fastsync:handoff";

    // Message types (must match HandoffProtocol on the proxy side)
    private static final int QUERY_LOCK = 1;
    private static final int LOCK_STATUS = 2;
    private static final int HANDOFF_NOTIFY = 3;
    private static final int STATUS_QUERY = 4;
    private static final int STATUS_RESPONSE = 5;

    private final FastSync plugin;
    private final ConfigManager config;
    private final DatabaseManager database;
    private final SyncManager syncManager;
    private final Logger logger;

    public HandoffMessageListener(FastSync plugin, ConfigManager config,
                                  DatabaseManager database, SyncManager syncManager) {
        this.plugin = plugin;
        this.config = config;
        this.database = database;
        this.syncManager = syncManager;
        this.logger = plugin.getLogger();
    }

    @Override
    public void onPluginMessageReceived(@NotNull String channel, @NotNull Player player,
                                        byte[] message) {
        if (!CHANNEL.equals(channel)) return;
        if (message == null || message.length == 0) return;

        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(message));
            int type = in.readByte() & 0xFF;

            switch (type) {
                case QUERY_LOCK -> handleQueryLock(in, player);
                case HANDOFF_NOTIFY -> handleHandoffNotify(in, player);
                case STATUS_QUERY -> handleStatusQuery(player);
                default -> logger.warning("Unknown handoff message type: " + type);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to parse handoff message", e);
        }
    }

    /**
     * QUERY_LOCK: proxy asks if we still hold the lock for a player.
     * Respond with LOCK_STATUS.
     */
    private void handleQueryLock(DataInputStream in, Player proxyPlayer) throws IOException {
        UUID uuid = UUID.fromString(in.readUTF());
        String newServer = in.readUTF();

        // Check if we (this server) still hold the lock for this player
        boolean locked = false;
        try {
            String lockHolder = database.getLockHolder(uuid);
            String ourName = config.getServerName();
            locked = ourName.equals(lockHolder);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to check lock holder for " + uuid, e);
        }

        // Build LOCK_STATUS response
        byte[] response = encodeLockStatus(uuid, locked, config.getServerName());

        // Send response back to the proxy (via any online player connection)
        // In practice, the player who triggered this message is the one switching
        if (proxyPlayer.isOnline()) {
            proxyPlayer.sendPluginMessage(plugin, CHANNEL, response);
        }

        if (config.isDebug()) {
            logger.info("[Handoff] QUERY_LOCK for " + uuid + " → locked=" + locked +
                " (newServer=" + newServer + ")");
        }
    }

    /**
     * HANDOFF_NOTIFY: proxy tells us a player just arrived from another server.
     * This is informational — the sync system already handles data loading
     * during AsyncPlayerPreLoginEvent.
     */
    private void handleHandoffNotify(DataInputStream in, Player player) throws IOException {
        UUID uuid = UUID.fromString(in.readUTF());
        String oldServer = in.readUTF();
        String newServer = in.readUTF();

        logger.info("[Handoff] Player " + player.getName() + " (" + uuid +
            ") arrived from " + oldServer + " → " + newServer);

        // The sync system's AsyncPlayerPreLoginEvent handler already takes care
        // of loading data. This notification is informational and can be used
        // for metrics or future pre-warming logic.
    }

    /**
     * STATUS_QUERY: proxy asks for our FastSync status.
     * Respond with STATUS_RESPONSE.
     */
    private void handleStatusQuery(Player proxyPlayer) {
        boolean dbHealthy = database.isHealthy();
        boolean redisHealthy = syncManager.isRedisHealthy();
        int playerCount = Bukkit.getOnlinePlayers().size();
        int pendingSaves = syncManager.getPendingSaveCount();
        int pendingLoads = syncManager.getPendingLoadCount();

        byte[] response = encodeStatusResponse(
            config.getServerName(), dbHealthy, redisHealthy,
            playerCount, pendingSaves, pendingLoads);

        if (proxyPlayer.isOnline()) {
            proxyPlayer.sendPluginMessage(plugin, CHANNEL, response);
        }

        if (config.isDebug()) {
            logger.info("[Handoff] STATUS_QUERY → db=" + dbHealthy +
                " redis=" + redisHealthy + " players=" + playerCount);
        }
    }

    // ==================== Encoding helpers ====================

    private byte[] encodeLockStatus(UUID uuid, boolean locked, String serverName) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream out = new java.io.DataOutputStream(baos);
            out.writeByte(LOCK_STATUS);
            out.writeUTF(uuid.toString());
            out.writeBoolean(locked);
            out.writeUTF(serverName);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] encodeStatusResponse(String serverName, boolean dbHealthy,
                                        boolean redisHealthy, int playerCount,
                                        int pendingSaves, int pendingLoads) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream out = new java.io.DataOutputStream(baos);
            out.writeByte(STATUS_RESPONSE);
            out.writeUTF(serverName);
            out.writeBoolean(dbHealthy);
            out.writeBoolean(redisHealthy);
            out.writeInt(playerCount);
            out.writeInt(pendingSaves);
            out.writeInt(pendingLoads);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
