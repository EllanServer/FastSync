package com.fastsync.listeners.dirty;

import com.fastsync.sync.dirty.ComponentDirtyMask;
import org.bukkit.entity.Player;
import org.bukkit.event.entity.EntityRegainHealthEvent;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class DirtyTrackingListenerTest {

    @Test
    void deathMarksEveryRespawnResetComponentDirty() {
        ComponentDirtyMask mask = new ComponentDirtyMask(5);
        DirtyTrackingListener listener = new DirtyTrackingListener(mask);
        Player player = mock(Player.class);
        UUID uuid = UUID.randomUUID();
        when(player.getUniqueId()).thenReturn(uuid);
        PlayerDeathEvent event = mock(PlayerDeathEvent.class);
        when(event.getEntity()).thenReturn(player);

        listener.onDeath(event);

        var dirty = mask.getDirty(uuid);
        assertTrue(dirty.contains(ComponentDirtyMask.Component.INVENTORY));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.VITALS));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.FOOD));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.EXPERIENCE));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.POTION_EFFECTS));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.FIRE_TICKS));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.AIR));
    }

    @Test
    void healingMarksVitalsDirty() {
        ComponentDirtyMask mask = new ComponentDirtyMask(5);
        DirtyTrackingListener listener = new DirtyTrackingListener(mask);
        Player player = mock(Player.class);
        UUID uuid = UUID.randomUUID();
        when(player.getUniqueId()).thenReturn(uuid);
        EntityRegainHealthEvent event = mock(EntityRegainHealthEvent.class);
        when(event.getEntity()).thenReturn(player);

        listener.onEntityRegainHealth(event);

        assertTrue(mask.getDirty(uuid).contains(ComponentDirtyMask.Component.VITALS));
    }
}
