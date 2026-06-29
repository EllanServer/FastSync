package com.fastsync.sync.strategy;

import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Player;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Location synchronization with strict world validation.
 * 
 * <p>Location sync is disabled by default because it's contextual:
 * teleporting a player to coordinates from a different world can trap
 * them inside walls or void. When enabled, this strategy validates
 * world name and UUID before applying.
 */
public class LocationSyncStrategy {
    
    private final ConfigManager config;
    private final Logger logger;
    
    public LocationSyncStrategy(ConfigManager config, Logger logger) {
        this.config = config;
        this.logger = logger;
    }
    
    /**
     * Apply saved location data to the player, with validation.
     * 
     * @return true if location was applied, false if skipped
     */
    public boolean apply(Player player, PlayerData data) {
        if (!config.isSyncLocation()) return false;
        
        String worldName = data.getWorldName();
        if (worldName == null || worldName.isEmpty()) return false;
        
        World world = Bukkit.getWorld(worldName);
        if (world == null) {
            fallbackToSpawn(player, "world '" + worldName + "' was not found");
            return false;
        }
        
        // Validate world name matches
        if (config.isLocationRequireSameWorldName() 
                && !world.getName().equals(worldName)) {
            fallbackToSpawn(player, "world name did not match");
            return false;
        }
        
        if (config.isLocationRequireSameWorldUuid()) {
            String savedUuid = data.getWorldUuid();
            if (savedUuid == null || !world.getUID().toString().equals(savedUuid)) {
                fallbackToSpawn(player, "world UUID did not match");
                return false;
            }
        }

        Location loc = new Location(world, data.getX(), data.getY(), data.getZ(),
                                     data.getYaw(), data.getPitch());
        // Paper loads/generates the destination chunks asynchronously before
        // teleporting. This avoids a synchronous chunk-load stall on Paper and
        // is the supported cross-region/cross-world path on Folia.
        teleportAsync(player, loc, "saved location");
        return true;
    }

    private void fallbackToSpawn(Player player, String reason) {
        if (config.isLocationFallbackToSpawn()) {
            teleportAsync(player, player.getWorld().getSpawnLocation(), "fallback spawn");
        }
        if (config.isDebug()) {
            logger.fine("[Location] Skipped saved location because " + reason);
        }
    }

    private void teleportAsync(Player player, Location destination, String description) {
        java.util.UUID playerId = player.getUniqueId();
        player.teleportAsync(destination).whenComplete((success, error) -> {
            if (error != null) {
                logger.log(Level.WARNING, "[Location] Failed to teleport "
                    + playerId + " to " + description, error);
            } else if (!Boolean.TRUE.equals(success)) {
                logger.warning("[Location] Teleport rejected for " + playerId
                    + " to " + description);
            }
        });
    }
}
