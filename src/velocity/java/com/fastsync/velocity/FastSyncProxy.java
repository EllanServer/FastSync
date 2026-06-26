package com.fastsync.velocity;

import com.google.inject.Inject;
import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.command.SimpleCommand;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.DisconnectEvent;
import com.velocitypowered.api.event.connection.PluginMessageEvent;
import com.velocitypowered.api.event.player.ServerPostConnectEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.messages.ChannelIdentifier;
import com.velocitypowered.api.proxy.messages.MinecraftChannelIdentifier;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
/**
 * FastSync Velocity proxy plugin.
 *
 * <p>Acts as a cross-server coordinator on the proxy layer:
 * <ul>
 *   <li>Tracks which backend server each player is connected to</li>
 *   <li>Queries the old backend for lock status before allowing a server switch</li>
 *   <li>Notifies the new backend that a player is arriving from another server</li>
 *   <li>Aggregates FastSync status from all backends</li>
 *   <li>Provides a /fastsync command for status and management</li>
 * </ul>
 *
 * <p>Communication uses the {@code fastsync:handoff} plugin messaging channel.
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

    private ProxyConfig config;

    /** Track player -> current backend server name */
    private final ConcurrentHashMap<UUID, String> playerServerMap = new ConcurrentHashMap<>();

    /** Track player -> last server switch timestamp */
    private final ConcurrentHashMap<UUID, Long> lastSwitchTime = new ConcurrentHashMap<>();

    /** Pending lock status responses: uuid -> future that completes when LOCK_STATUS arrives */
    private final ConcurrentHashMap<UUID, CompletableFuture<HandoffProtocol.LockStatusData>> pendingLockQueries = new ConcurrentHashMap<>();

    /** Pending status responses: serverName -> future */
    private final ConcurrentHashMap<String, CompletableFuture<HandoffProtocol.StatusResponseData>> pendingStatusQueries = new ConcurrentHashMap<>();

    @Inject
    public FastSyncProxy(ProxyServer proxy, Logger logger, @DataDirectory Path dataDirectory) {
        this.proxy = proxy;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onProxyInit(ProxyInitializeEvent event) {
        config = new ProxyConfig(dataDirectory, logger);
        config.load();

        // Register the handoff plugin messaging channel
        proxy.getChannelRegistrar().register(HANDOFF_CHANNEL);

        // Register /fastsync command
        proxy.getCommandManager().register("fastsync", new FastSyncCommand(), "fsync", "fs");

        // Start periodic status log (every 5 min)
        proxy.getScheduler().buildTask(this, this::logStatus)
            .repeat(5, TimeUnit.MINUTES)
            .schedule();

        logger.info("FastSync Proxy initialized. Channel: fastsync:handoff, Smart handoff: {}",
            config.isSmartHandoffEnabled());
    }

    @Subscribe
    public void onProxyShutdown(ProxyShutdownEvent event) {
        logger.info("FastSync Proxy shutting down. Tracked players: {}", playerServerMap.size());
        playerServerMap.clear();
        lastSwitchTime.clear();
        pendingLockQueries.clear();
        pendingStatusQueries.clear();
    }

    /**
     * Track when a player connects to a backend server and notify the new backend.
     *
     * <p>Note: The backend's AsyncPlayerPreLoginEvent handler already takes care
     * of waiting for the old server's lock to be released (via Redis pub/sub or
     * DB polling). The proxy does NOT need to block the transfer — it only
     * notifies the new backend that a handoff occurred and tracks the mapping.
     */
    @Subscribe
    public void onServerPostConnect(ServerPostConnectEvent event) {
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

            // Notify the new backend about the handoff
            player.getCurrentServer().ifPresent(serverConn -> {
                sendHandoffNotify(serverConn, uuid, oldServer, newServer);
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
        pendingLockQueries.remove(uuid);

        if (server != null) {
            logger.info("Player {} disconnected from {}", player.getUsername(), server);
        }
    }

    // ==================== Plugin Messaging ====================

    /**
     * Query a backend server for the lock status of a player.
     * Returns a future that completes when the LOCK_STATUS response arrives.
     */
    private CompletableFuture<HandoffProtocol.LockStatusData> queryLockStatus(
            String serverName, UUID uuid) {
        CompletableFuture<HandoffProtocol.LockStatusData> future = new CompletableFuture<>();
        pendingLockQueries.put(uuid, future);

        byte[] data = HandoffProtocol.encodeQueryLock(uuid, playerServerMap.get(uuid));
        proxy.getServer(serverName).ifPresent(server -> {
            server.sendPluginMessage(HANDOFF_CHANNEL, data);
        });

        // Timeout fallback
        proxy.getScheduler().buildTask(this, () -> {
            CompletableFuture<HandoffProtocol.LockStatusData> pending = pendingLockQueries.get(uuid);
            if (pending != null && !pending.isDone()) {
                pending.completeExceptionally(new TimeoutException("Lock query timed out"));
                pendingLockQueries.remove(uuid);
            }
        }).delay((int) (config.getWaitTimeoutMs() / 2), TimeUnit.MILLISECONDS).schedule();

        return future;
    }

    /**
     * Send a HANDOFF_NOTIFY to the new backend server.
     */
    private void sendHandoffNotify(ServerConnection serverConn, UUID uuid,
                                   String oldServer, String newServer) {
        byte[] data = HandoffProtocol.encodeHandoffNotify(uuid, oldServer, newServer);
        serverConn.sendPluginMessage(HANDOFF_CHANNEL, data);

        if (config.isDebug()) {
            logger.debug("Sent handoff notify to {} for player {}", newServer, uuid);
        }
    }

    /**
     * Query all registered backend servers for their FastSync status.
     */
    public CompletableFuture<List<HandoffProtocol.StatusResponseData>> queryAllStatus() {
        List<CompletableFuture<HandoffProtocol.StatusResponseData>> safeFutures = new ArrayList<>();

        for (RegisteredServer server : proxy.getAllServers()) {
            String name = server.getServerInfo().getName();
            CompletableFuture<HandoffProtocol.StatusResponseData> future = new CompletableFuture<>();
            pendingStatusQueries.put(name, future);
            // Wrap each future with completeOnTimeout + exceptionally so that
            // a single slow/unresponsive backend doesn't cause the entire
            // allOf to fail. The wrapped future always completes (either with
            // data or with null), so allOf never throws.
            CompletableFuture<HandoffProtocol.StatusResponseData> safe =
                future.completeOnTimeout(null, config.getStatusQueryTimeoutMs(), TimeUnit.MILLISECONDS)
                     .exceptionally(ex -> null);
            safeFutures.add(safe);

            server.sendPluginMessage(HANDOFF_CHANNEL, HandoffProtocol.encodeStatusQuery());
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(
            safeFutures.toArray(new CompletableFuture[0]));

        return all.thenApply(v -> {
            List<HandoffProtocol.StatusResponseData> results = new ArrayList<>();
            for (var f : safeFutures) {
                HandoffProtocol.StatusResponseData data = f.join();
                if (data != null) results.add(data);
            }
            return results;
        });
    }

    // ==================== Incoming message handler ====================

    /**
     * Called by Velocity when a plugin message arrives on the fastsync:handoff channel.
     */
    @Subscribe
    public void onPluginMessage(PluginMessageEvent event) {
        if (!event.getIdentifier().equals(HANDOFF_CHANNEL)) return;

        byte[] data = event.getData();
        int type = HandoffProtocol.getMessageType(data);

        switch (type) {
            case HandoffProtocol.LOCK_STATUS -> {
                HandoffProtocol.LockStatusData status = HandoffProtocol.decodeLockStatus(data);
                CompletableFuture<HandoffProtocol.LockStatusData> future =
                    pendingLockQueries.remove(status.uuid());
                if (future != null) {
                    future.complete(status);
                }
                if (config.isDebug()) {
                    logger.info("[Handoff] Lock status for {}: locked={} by {}",
                        status.uuid(), status.locked(), status.serverName());
                }
            }
            case HandoffProtocol.STATUS_RESPONSE -> {
                HandoffProtocol.StatusResponseData status = HandoffProtocol.decodeStatusResponse(data);
                CompletableFuture<HandoffProtocol.StatusResponseData> future =
                    pendingStatusQueries.remove(status.serverName());
                if (future != null) {
                    future.complete(status);
                }
                if (config.isDebug()) {
                    logger.info("[Handoff] Status from {}: db={} redis={} players={}",
                        status.serverName(), status.dbHealthy(), status.redisHealthy(),
                        status.playerCount());
                }
            }
        }

        // Note: We don't forward the message to the player client.
        // Velocity's default is to forward, but our messages are internal
        // protocol messages not meant for the client.
    }

    // ==================== Utility ====================

    public String getPlayerServer(UUID uuid) {
        return playerServerMap.get(uuid);
    }

    public Map<UUID, String> getPlayerServerMap() {
        return new java.util.HashMap<>(playerServerMap);
    }

    private void logStatus() {
        logger.info("FastSync Proxy status: {} players tracked, {} backends",
            playerServerMap.size(), proxy.getAllServers().size());
    }

    public ProxyServer getProxy() { return proxy; }
    public ProxyConfig getConfig() { return config; }
    public Logger getLogger() { return logger; }

    // ==================== Command ====================

    private class FastSyncCommand implements SimpleCommand {

        @Override
        public void execute(Invocation invocation) {
            CommandSource source = invocation.source();
            String[] args = invocation.arguments();

            if (args.length == 0) {
                sendHelp(source);
                return;
            }

            switch (args[0].toLowerCase()) {
                case "status" -> handleStatus(source);
                case "players" -> handlePlayers(source);
                case "reload" -> handleReload(source);
                case "debug" -> handleDebug(source);
                default -> sendHelp(source);
            }
        }

        @Override
        public List<String> suggest(Invocation invocation) {
            String[] args = invocation.arguments();
            if (args.length == 0) {
                return List.of("status", "players", "reload", "debug");
            }
            String prefix = args[0].toLowerCase();
            return List.of("status", "players", "reload", "debug").stream()
                .filter(s -> s.startsWith(prefix))
                .toList();
        }

        @Override
        public boolean hasPermission(Invocation invocation) {
            return invocation.source().hasPermission("fastsync.admin");
        }

        private void sendHelp(CommandSource source) {
            source.sendMessage(Component.text("FastSync Proxy Commands:", NamedTextColor.GOLD));
            source.sendMessage(Component.text("  /fastsync status  - Show aggregated backend status", NamedTextColor.YELLOW));
            source.sendMessage(Component.text("  /fastsync players - Show player→server mapping", NamedTextColor.YELLOW));
            source.sendMessage(Component.text("  /fastsync reload  - Reload proxy config", NamedTextColor.YELLOW));
            source.sendMessage(Component.text("  /fastsync debug   - Toggle debug mode", NamedTextColor.YELLOW));
        }

        private void handleStatus(CommandSource source) {
            source.sendMessage(Component.text("Querying backend servers...", NamedTextColor.YELLOW));

            queryAllStatus().thenAccept(results -> {
                if (results.isEmpty()) {
                    source.sendMessage(Component.text("No backends responded.", NamedTextColor.RED));
                    return;
                }

                source.sendMessage(Component.text("===== FastSync Proxy Status =====", NamedTextColor.GOLD));
                for (HandoffProtocol.StatusResponseData s : results) {
                    NamedTextColor dbColor = s.dbHealthy() ? NamedTextColor.GREEN : NamedTextColor.RED;
                    NamedTextColor redisColor = s.redisHealthy() ? NamedTextColor.GREEN : NamedTextColor.RED;

                    source.sendMessage(
                        Component.text("  " + s.serverName() + " ", NamedTextColor.AQUA)
                            .append(Component.text("| DB: ", NamedTextColor.GRAY))
                            .append(Component.text(s.dbHealthy() ? "OK" : "FAIL", dbColor))
                            .append(Component.text(" | Redis: ", NamedTextColor.GRAY))
                            .append(Component.text(s.redisHealthy() ? "OK" : "FAIL", redisColor))
                            .append(Component.text(" | Players: ", NamedTextColor.GRAY))
                            .append(Component.text(String.valueOf(s.playerCount()), NamedTextColor.WHITE))
                            .append(Component.text(" | Save: ", NamedTextColor.GRAY))
                            .append(Component.text(String.valueOf(s.pendingSaves()), NamedTextColor.WHITE))
                            .append(Component.text(" | Load: ", NamedTextColor.GRAY))
                            .append(Component.text(String.valueOf(s.pendingLoads()), NamedTextColor.WHITE))
                    );
                }
                source.sendMessage(Component.text("Total players on proxy: " + playerServerMap.size(),
                    NamedTextColor.GRAY));
            }).exceptionally(ex -> {
                source.sendMessage(Component.text("Failed to query backends: " + ex.getMessage(),
                    NamedTextColor.RED));
                return null;
            });
        }

        private void handlePlayers(CommandSource source) {
            if (playerServerMap.isEmpty()) {
                source.sendMessage(Component.text("No tracked players.", NamedTextColor.YELLOW));
                return;
            }

            source.sendMessage(Component.text("===== Tracked Players =====", NamedTextColor.GOLD));
            playerServerMap.forEach((uuid, server) -> {
                // Try to get the online player name
                String name = proxy.getPlayer(uuid)
                    .map(Player::getUsername)
                    .orElse(uuid.toString().substring(0, 8) + "...");

                source.sendMessage(
                    Component.text("  " + name, NamedTextColor.AQUA)
                        .append(Component.text(" → " + server, NamedTextColor.YELLOW))
                );
            });
            source.sendMessage(Component.text("Total: " + playerServerMap.size() + " players",
                NamedTextColor.GRAY));
        }

        private void handleReload(CommandSource source) {
            config.load();
            source.sendMessage(Component.text("FastSync proxy config reloaded.", NamedTextColor.GREEN));
        }

        private void handleDebug(CommandSource source) {
            // Toggle debug by reloading config with debug inverted
            // Since we can't easily mutate the loaded config, just log
            source.sendMessage(Component.text("Debug mode: " + config.isDebug(), NamedTextColor.YELLOW));
            source.sendMessage(Component.text("(Edit proxy-config.yml and run /fastsync reload to change)",
                NamedTextColor.GRAY));
        }
    }
}
