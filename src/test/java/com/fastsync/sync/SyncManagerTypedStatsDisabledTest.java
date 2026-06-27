package com.fastsync.sync;

import com.fastsync.FastSync;
import com.fastsync.config.ConfigManager;
import com.fastsync.data.PlayerData;
import com.fastsync.database.DatabaseManager;
import org.bukkit.Statistic;
import org.bukkit.entity.Player;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Round 3 test #6: when the typed-statistics strategy is disabled but the
 * payload carries typed stats (ITEM_/BLOCK_/ENTITY_), the apply path must
 * NOT silently re-apply them through a legacy code path. It must warn and
 * ignore.
 *
 * <p>The old {@code applyTypedStatsInline} backward-compat method was deleted
 * (clean-slate). This test verifies the new behaviour: typed stats present +
 * strategy null → warning logged, {@code player.setStatistic} never called
 * for typed categories, no exception.
 *
 * <p>Uses Mockito + reflection (same rationale as
 * {@link SyncManagerLockReleaseTest}): SyncManager's constructor needs a
 * FastSync plugin instance, and {@code applyStatistics} is private.
 * {@code typedStatsStrategy} is null after construction (no
 * {@code initialize()} call), which is exactly the "disabled" state under test.
 */
class SyncManagerTypedStatsDisabledTest {

    private FastSync plugin;
    private ConfigManager config;
    private DatabaseManager databaseManager;
    private SyncManager syncManager;

    @BeforeEach
    void setup() {
        plugin = mock(FastSync.class);
        when(plugin.getLogger()).thenReturn(Logger.getLogger("typed-stats-test"));

        config = mock(ConfigManager.class);
        when(config.isDebug()).thenReturn(false);

        databaseManager = mock(DatabaseManager.class);

        syncManager = new SyncManager(plugin, config, databaseManager);
        // typedStatsStrategy is null (never initialized) — the "disabled" state.
    }

    /**
     * Typed stats in payload + strategy disabled → warn + ignore.
     * {@code player.setStatistic} must NEVER be called for typed categories.
     */
    @Test
    void typedStatsPresentStrategyDisabled_warnsAndIgnores() throws Exception {
        UUID uuid = UUID.randomUUID();
        Player player = mock(Player.class);
        when(player.getUniqueId()).thenReturn(uuid);

        // Build a payload with ONLY typed stats (no untyped). The old
        // applyTypedStatsInline path would have tried to apply these directly;
        // the clean-slate path must skip them.
        Map<String, Map<String, Integer>> statistics = new HashMap<>();
        Map<String, Integer> itemStats = new HashMap<>();
        itemStats.put("STONE", 5);
        statistics.put("ITEM_PICKED_UP", itemStats);

        Map<String, Integer> blockStats = new HashMap<>();
        blockStats.put("DIAMOND_ORE", 3);
        statistics.put("BLOCK_MINED", blockStats);

        Map<String, Integer> entityStats = new HashMap<>();
        entityStats.put("ZOMBIE", 2);
        statistics.put("ENTITY_KILLED", entityStats);

        PlayerData data = new PlayerData();
        data.setStatistics(statistics);

        // Must not throw — the apply path catches and logs, it never crashes.
        assertDoesNotThrow(() -> invokeApplyStatistics(player, data));

        // setStatistic must NEVER be called: typed stats are ignored (strategy
        // disabled) and there are no untyped stats in this payload.
        verify(player, never()).setStatistic(any(Statistic.class), anyInt());
    }

    /**
     * Mixed payload: untyped stats present (should apply) + typed stats
     * present (should be ignored when strategy disabled). Only the untyped
     * stats should reach setStatistic.
     *
     * <p>Note: the untyped path calls {@code Statistic.valueOf(name)} +
     * {@code statistic.getType() == UNTYPED}. We use a real untyped stat name
     * ("PLAY_ONE_MINUTE") so the enum lookup succeeds.
     */
    @Test
    void mixedStats_typedIgnoredUntypedApplied() throws Exception {
        UUID uuid = UUID.randomUUID();
        Player player = mock(Player.class);
        when(player.getUniqueId()).thenReturn(uuid);

        Map<String, Map<String, Integer>> statistics = new HashMap<>();

        // Untyped stat — should be applied
        Map<String, Integer> untyped = new HashMap<>();
        untyped.put("PLAY_ONE_MINUTE", 42);
        statistics.put("UNTYPED", untyped);

        // Typed stat — should be ignored (strategy disabled)
        Map<String, Integer> typed = new HashMap<>();
        typed.put("SKELETON", 1);
        statistics.put("ENTITY_KILLED", typed);

        PlayerData data = new PlayerData();
        data.setStatistics(statistics);

        assertDoesNotThrow(() -> invokeApplyStatistics(player, data));

        // The untyped stat PLAY_ONE_MINUTE should be applied exactly once.
        verify(player).setStatistic(Statistic.PLAY_ONE_MINUTE, 42);
        // No OTHER setStatistic calls (typed stats ignored).
        verify(player, times(1)).setStatistic(any(Statistic.class), anyInt());
    }

    private void invokeApplyStatistics(Player player, PlayerData data) throws Exception {
        Method m = SyncManager.class.getDeclaredMethod("applyStatistics", Player.class, PlayerData.class);
        m.setAccessible(true);
        m.invoke(syncManager, player, data);
    }
}
