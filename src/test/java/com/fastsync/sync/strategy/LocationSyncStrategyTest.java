package com.fastsync.sync.strategy;

import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Player;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.concurrent.CompletableFuture;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class LocationSyncStrategyTest {

    @Test
    void appliesLocationThroughPaperAsyncTeleport() {
        ConfigManager config = mock(ConfigManager.class);
        when(config.isSyncLocation()).thenReturn(true);
        World world = mock(World.class);
        when(world.getName()).thenReturn("world");
        Player player = mock(Player.class);
        when(player.teleportAsync(any(Location.class)))
            .thenReturn(CompletableFuture.completedFuture(true));

        PlayerData data = new PlayerData();
        data.setWorldName("world");
        data.setX(10.5);
        data.setY(70);
        data.setZ(-4.25);
        data.setYaw(90);
        data.setPitch(15);

        LocationSyncStrategy strategy =
            new LocationSyncStrategy(config, Logger.getLogger("location-test"));
        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(() -> Bukkit.getWorld("world")).thenReturn(world);
            assertTrue(strategy.apply(player, data));
        }

        ArgumentCaptor<Location> target = ArgumentCaptor.forClass(Location.class);
        verify(player).teleportAsync(target.capture());
        verify(player, never()).teleport(any(Location.class));
        assertEquals(10.5, target.getValue().getX());
        assertEquals(70, target.getValue().getY());
        assertEquals(-4.25, target.getValue().getZ());
    }

    @Test
    void rejectsDifferentWorldIdentityAndFallsBackAsynchronously() {
        ConfigManager config = mock(ConfigManager.class);
        when(config.isSyncLocation()).thenReturn(true);
        when(config.isLocationRequireSameWorldUuid()).thenReturn(true);
        when(config.isLocationFallbackToSpawn()).thenReturn(true);
        World savedNameWorld = mock(World.class);
        when(savedNameWorld.getUID()).thenReturn(UUID.randomUUID());
        World currentWorld = mock(World.class);
        Location spawn = new Location(currentWorld, 0, 64, 0);
        when(currentWorld.getSpawnLocation()).thenReturn(spawn);
        Player player = mock(Player.class);
        when(player.getWorld()).thenReturn(currentWorld);
        when(player.teleportAsync(any(Location.class)))
            .thenReturn(CompletableFuture.completedFuture(true));

        PlayerData data = new PlayerData();
        data.setWorldName("world");
        data.setWorldUuid(UUID.randomUUID().toString());

        LocationSyncStrategy strategy =
            new LocationSyncStrategy(config, Logger.getLogger("location-uuid-test"));
        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(() -> Bukkit.getWorld("world")).thenReturn(savedNameWorld);
            org.junit.jupiter.api.Assertions.assertFalse(strategy.apply(player, data));
        }

        verify(player).teleportAsync(spawn);
        verify(player, never()).teleport(any(Location.class));
    }
}
