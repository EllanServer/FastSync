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
import net.kyori.adventure.text.minimessage.MiniMessage;
import org.slf4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
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

    /** i18n messages loaded from proxy-messages.yml (dot-notation keys) */
    private final Map<String, String> messages = new HashMap<>();

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

        // Load i18n messages from the bundled proxy-messages.yml resource
        loadMessages();

        // Register the handoff plugin messaging channel
        proxy.getChannelRegistrar().register(HANDOFF_CHANNEL);

        // Register /fastsync command
        var commandManager = proxy.getCommandManager();
        var commandMeta = commandManager.metaBuilder("fastsync")
            .aliases("fsync", "fs")
            .plugin(this)
            .build();
        commandManager.register(commandMeta, new FastSyncCommand());

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
        List<CompletableFuture<HandoffProtocol.StatusResponseData>> futures = new ArrayList<>();

        for (RegisteredServer server : proxy.getAllServers()) {
            String name = server.getServerInfo().getName();
            CompletableFuture<HandoffProtocol.StatusResponseData> raw = new CompletableFuture<>();
            pendingStatusQueries.put(name, raw);

            // Wrap with timeout + exceptionally so one slow backend
            // doesn't cause allOf to throw.
            CompletableFuture<HandoffProtocol.StatusResponseData> safe =
                raw.orTimeout(config.getStatusQueryTimeoutMs(), TimeUnit.MILLISECONDS)
                   .exceptionally(ex -> {
                       if (config.isDebug()) {
                           logger.warn("[Handoff] Status query timed out or failed for backend {}", name, ex);
                       }
                       return null;
                   })
                   .whenComplete((result, ex) -> pendingStatusQueries.remove(name, raw));
            futures.add(safe);

            boolean sent = server.sendPluginMessage(HANDOFF_CHANNEL, HandoffProtocol.encodeStatusQuery());
            if (!sent) {
                raw.complete(null);
            }
        }

        return CompletableFuture
            .allOf(futures.toArray(new CompletableFuture<?>[0]))
            .thenApply(v -> {
                List<HandoffProtocol.StatusResponseData> results = new ArrayList<>();
                for (CompletableFuture<HandoffProtocol.StatusResponseData> f : futures) {
                    HandoffProtocol.StatusResponseData data = f.getNow(null);
                    if (data != null) {
                        results.add(data);
                    }
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

        // Mark as handled so Velocity does not forward internal protocol
        // messages to the player client or other backends.
        event.setResult(PluginMessageEvent.ForwardResult.handled());

        // Only accept messages from backend servers, not from player clients.
        if (!(event.getSource() instanceof com.velocitypowered.api.proxy.ServerConnection)) {
            if (config != null && config.isDebug()) {
                logger.warn("[Handoff] Rejected plugin message from non-server source: {}",
                    event.getSource().getClass().getName());
            }
            return;
        }

        byte[] data = event.getData();
        try {
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
                default -> {
                    if (config.isDebug()) {
                        logger.warn("[Handoff] Unknown message type: {}", type);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("[Handoff] Failed to decode plugin message", e);
        }
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

    // ==================== i18n ====================

    /**
     * Resolve a message key to an Adventure Component.
     *
     * <p>Templates are loaded from {@code proxy-messages.yml} (bundled in the
     * jar). Placeholders use {@code {0}}, {@code {1}}, ... notation and are
     * replaced before MiniMessage parsing, so trusted formatting arguments may
     * themselves contain MiniMessage tags.
     *
     * @param key  the dot-notation message key (e.g. "proxy.status.header")
     * @param args placeholder values for {0}, {1}, ...
     * @return the deserialized Component, or the key itself if not found
     */
    private Component msg(String key, Object... args) {
        String template = messages.getOrDefault(key, key);
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                template = template.replace("{" + i + "}", String.valueOf(args[i]));
            }
        }
        return MiniMessage.miniMessage().deserialize(template);
    }

    /**
     * Load proxy messages from the bundled {@code /proxy-messages.yml} resource
     * into the flat {@link #messages} map using dot-notation keys.
     */
    private void loadMessages() {
        try (InputStream in = getClass().getResourceAsStream("/proxy-messages.yml")) {
            if (in == null) {
                logger.warn("proxy-messages.yml resource not found; message keys will be used as-is");
                return;
            }
            Yaml yaml = new Yaml();
            Map<String, Object> root = yaml.load(new InputStreamReader(in, StandardCharsets.UTF_8));
            if (root != null) {
                flattenMessages("", root);
            }
            logger.info("Loaded {} proxy messages", messages.size());
        } catch (Exception e) {
            logger.warn("Failed to load proxy-messages.yml", e);
        }
    }

    /**
     * Recursively flatten a nested YAML map into dot-notation keys.
     */
    @SuppressWarnings("unchecked")
    private void flattenMessages(String prefix, Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                flattenMessages(key, (Map<String, Object>) value);
            } else {
                messages.put(key, String.valueOf(value));
            }
        }
    }

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
            source.sendMessage(msg("proxy.command.header"));
            source.sendMessage(msg("proxy.command.status"));
            source.sendMessage(msg("proxy.command.players"));
            source.sendMessage(msg("proxy.command.reload"));
            source.sendMessage(msg("proxy.command.debug"));
        }

        private void handleStatus(CommandSource source) {
            source.sendMessage(msg("proxy.status.querying"));

            queryAllStatus().thenAccept(results -> {
                if (results.isEmpty()) {
                    source.sendMessage(msg("proxy.status.no-backends"));
                    return;
                }

                source.sendMessage(msg("proxy.status.header"));
                for (HandoffProtocol.StatusResponseData s : results) {
                    source.sendMessage(msg("proxy.status.backend",
                        s.serverName(),
                        s.dbHealthy() ? "<green>OK" : "<red>FAIL",
                        s.redisHealthy() ? "<green>OK" : "<red>FAIL",
                        s.playerCount(),
                        s.pendingSaves(),
                        s.pendingLoads()));
                }
                source.sendMessage(msg("proxy.status.total-players", playerServerMap.size()));
            }).exceptionally(ex -> {
                source.sendMessage(msg("proxy.status.query-failed", ex.getMessage()));
                return null;
            });
        }

        private void handlePlayers(CommandSource source) {
            if (playerServerMap.isEmpty()) {
                source.sendMessage(msg("proxy.players.none"));
                return;
            }

            source.sendMessage(msg("proxy.players.header"));
            playerServerMap.forEach((uuid, server) -> {
                // Try to get the online player name
                String name = proxy.getPlayer(uuid)
                    .map(Player::getUsername)
                    .orElse(uuid.toString().substring(0, 8) + "...");

                source.sendMessage(msg("proxy.players.entry", name, server));
            });
            source.sendMessage(msg("proxy.players.total", playerServerMap.size()));
        }

        private void handleReload(CommandSource source) {
            config.load();
            source.sendMessage(msg("proxy.reload.success"));
        }

        private void handleDebug(CommandSource source) {
            // Toggle debug by reloading config with debug inverted
            // Since we can't easily mutate the loaded config, just log
            source.sendMessage(msg("proxy.debug.state", config.isDebug()));
            source.sendMessage(msg("proxy.debug.hint"));
        }
    }
}
