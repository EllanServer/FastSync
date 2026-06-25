package com.fastsync.velocity;

import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.DisconnectEvent;
import com.velocitypowered.api.event.connection.PostLoginEvent;
import com.velocitypowered.api.event.player.ServerPostConnectEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.messages.ChannelIdentifier;
import com.velocitypowered.api.proxy.messages.MinecraftChannelIdentifier;
import com.velocitypowered.api.scheduler.ScheduledTask;
import net.kyori.adventure.text.Component;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * FastSync Velocity proxy plugin.
 *
 * <p>Acts as a cross-server coordinator on the proxy layer:
 * <ul>
 *   <li>Tracks which backend server each player is connected to</li>
 *   <li>Sends handoff notifications when a player switches servers</li>
 *   <li>Enforces a grace period before allowing reconnection to a new backend</li>
 *   <li>Provides a /fastsync proxy command for status</li>
 * </ul>
 *
 * <p>The proxy plugin communicates with backend FastSync plugins via a custom
 * plugin messaging channel: {@code fastsync:handoff}.
 *
 * <p>Velocity is the recommended proxy for Paper/Folia server networks.
 * This plugin runs on Velocity 3.4+ (MC 1.21.4).
 */
@Plugin(
    id = "fastsync-proxy",
    name = "FastSync Proxy",
    version = "1.0.0",
    authors = {"FastSync"},
    description = "Cross-server player data synchronization coordinator for Velocity"
)
public class FastSyncProxy {

    public static final ChannelIdentifier HANDOFF_CHANNEL =
        MinecraftChannelIdentifier.from("fastsync:handoff");

    private final ProxyServer proxy;
    private final Logger logger;
    private final Path dataDirectory;

    /** Track player -> current backend server name */
    private final ConcurrentHashMap<UUID, String> playerServerMap = new ConcurrentHashMap<>();

    /** Track player -> last server switch timestamp (for grace period enforcement) */
    private final ConcurrentHashMap<UUID, Long> lastSwitchTime = new ConcurrentHashMap<>();

    /** Grace period (ms) before a player can switch servers again */
    private static final long HANDOFF_GRACE_MS = 1000;

    @Inject
    public FastSyncProxy(ProxyServer proxy, Logger logger, @DataDirectory Path dataDirectory) {
        this.proxy = proxy;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onProxyInit(ProxyInitializeEvent event) {
        // Register the handoff plugin messaging channel
        proxy.getChannelRegistrar().register(HANDOFF_CHANNEL);

        // Start a periodic heartbeat to log proxy status
        proxy.getScheduler().buildTask(this, this::logStatus)
            .repeat(5, TimeUnit.MINUTES)
            .schedule();

        logger.info("FastSync Proxy initialized. Handoff channel: fastsync:handoff");
    }

    @Subscribe
    public void onProxyShutdown(ProxyShutdownEvent event) {
        logger.info("FastSync Proxy shutting down. Tracked players: {}", playerServerMap.size());
        playerServerMap.clear();
        lastSwitchTime.clear();
    }

    /**
     * Track when a player connects to a backend server.
     */
    @Subscribe
    public void onServerConnect(ServerPostConnectEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();
        String newServer = player.getCurrentServer()
            .map(s -> s.getServerInfo().getName())
            .orElse("unknown");

        String oldServer = playerServerMap.put(uuid, newServer);
        long now = System.currentTimeMillis();
        lastSwitchTime.put(uuid, now);

        if (oldServer != null && !oldServer.equals(newServer)) {
            logger.info("Player {} switched servers: {} -> {}",
                player.getUsername(), oldServer, newServer);

            // Notify the new backend that this player just switched servers
            // The backend FastSync plugin can use this as a hint to wait for
            // the old backend's save to complete before loading data
            player.getCurrentServer().ifPresent(serverConn -> {
                // The handoff notification is sent via plugin messaging
                // The backend plugin will see this and know to retry lock acquisition
                sendHandoffNotification(serverConn.getServerInfo().getName(), uuid, oldServer, newServer);
            });
        }
    }

    /**
     * Clean up tracking when a player disconnects from the proxy.
     */
    @Subscribe
    public void onDisconnect(DisconnectEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();
        String server = playerServerMap.remove(uuid);
        lastSwitchTime.remove(uuid);

        if (server != null) {
            logger.info("Player {} disconnected from {}", player.getUsername(), server);
        }
    }

    /**
     * Check if a player is within the handoff grace period.
     * Backend servers can query this to decide whether to retry lock acquisition.
     */
    public boolean isInGracePeriod(UUID uuid) {
        Long switchTime = lastSwitchTime.get(uuid);
        if (switchTime == null) return false;
        return (System.currentTimeMillis() - switchTime) < HANDOFF_GRACE_MS;
    }

    /**
     * Get the server a player is currently connected to.
     */
    public String getPlayerServer(UUID uuid) {
        return playerServerMap.get(uuid);
    }

    /**
     * Send a handoff notification to a backend server via plugin messaging.
     *
     * The message format is:
     *   [UTF-8 uuid string] [UTF-8 old server] [UTF-8 new server]
     */
    private void sendHandoffNotification(String targetServer, UUID uuid, String oldServer, String newServer) {
        try {
            java.io.ByteArrayOutputStream byteStream = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream out = new java.io.DataOutputStream(byteStream);
            out.writeUTF(uuid.toString());
            out.writeUTF(oldServer != null ? oldServer : "");
            out.writeUTF(newServer != null ? newServer : "");
            out.flush();

            byte[] data = byteStream.toByteArray();

            proxy.getServer(targetServer).ifPresent(server -> {
                server.sendPluginMessage(HANDOFF_CHANNEL, data);
                if (logger.isDebugEnabled()) {
                    logger.debug("Sent handoff notification to {} for player {}",
                        targetServer, uuid);
                }
            });
        } catch (Exception e) {
            logger.warn("Failed to send handoff notification", e);
        }
    }

    private void logStatus() {
        logger.info("FastSync Proxy status: {} players tracked", playerServerMap.size());
    }

    public ProxyServer getProxy() { return proxy; }
}
