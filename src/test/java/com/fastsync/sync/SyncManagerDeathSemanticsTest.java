package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import org.bukkit.entity.Player;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SyncManagerDeathSemanticsTest {

    @Test
    void convertsPaperRespawnExperiencePoints() {
        assertEquals(new SyncManager.DeathState(1, 0.0F, 7),
            SyncManager.experienceAfterDeath(0, 0, 7));

        SyncManager.DeathState partial = SyncManager.experienceAfterDeath(0, 0, 8);
        assertEquals(1, partial.level());
        assertEquals(1.0F / 9.0F, partial.progress(), 0.000_001F);
        assertEquals(8, partial.totalExperience());

        assertEquals(new SyncManager.DeathState(16, 0.0F, 137),
            SyncManager.experienceAfterDeath(15, 100, 37));
        assertEquals(Integer.MAX_VALUE,
            SyncManager.experienceAfterDeath(0, Integer.MAX_VALUE, 1).totalExperience());
    }

    @Test
    void collectsTheStatePaperAppliesOnRespawn() throws Exception {
        FastSync plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("death-semantics-test"));
        ConfigManager config = mock(ConfigManager.class);
        when(config.isSyncFood()).thenReturn(true);
        when(config.isSyncExperience()).thenReturn(true);
        when(config.isSyncPotionEffects()).thenReturn(true);
        when(config.isSyncFireTicks()).thenReturn(true);
        when(config.isSyncAir()).thenReturn(true);

        SyncManager manager = new SyncManager(plugin, config, mock(DatabaseManager.class));
        Player player = mock(Player.class);
        UUID uuid = UUID.randomUUID();
        when(player.getUniqueId()).thenReturn(uuid);
        when(player.getMaximumAir()).thenReturn(420);

        PlayerDeathEvent event = mock(PlayerDeathEvent.class);
        when(event.getEntity()).thenReturn(player);
        when(event.getNewLevel()).thenReturn(3);
        when(event.getNewTotalExp()).thenReturn(25);
        when(event.getNewExp()).thenReturn(4);
        manager.recordDeathState(event);

        Method collect = SyncManager.class.getDeclaredMethod("collectPlayerData", Player.class);
        collect.setAccessible(true);
        PlayerData data = (PlayerData) collect.invoke(manager, player);

        assertEquals(20, data.getFoodLevel());
        assertEquals(5.0F, data.getSaturation());
        assertEquals(0.0F, data.getExhaustion());
        assertEquals(3, data.getExpLevel());
        assertEquals(4.0F / 13.0F, data.getExpProgress(), 0.000_001F);
        assertEquals(29, data.getTotalExperience());
        assertTrue(data.getPotionEffects().isEmpty());
        assertEquals(0, data.getFireTicks());
        assertEquals(420, data.getMaximumAir());
        assertEquals(420, data.getRemainingAir());
        verify(player, never()).getActivePotionEffects();
    }
}
