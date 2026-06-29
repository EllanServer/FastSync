package com.fastsync.util;

import org.bukkit.Bukkit;
import org.bukkit.entity.Entity;
import org.bukkit.plugin.Plugin;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class SchedulerUtilPaperTest {

    @Test
    void entityTaskRunsInlineOnPaperPrimaryThread() {
        assertFalse(SchedulerUtil.isFolia(), "unit-test classpath should exercise Paper path");
        AtomicBoolean ran = new AtomicBoolean();

        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(Bukkit::isPrimaryThread).thenReturn(true);
            SchedulerUtil.runAtEntity(mock(Plugin.class), mock(Entity.class),
                () -> ran.set(true), null, true);
        }

        assertTrue(ran.get(), "shutdown collection must not wait for a future tick");
    }

    @Test
    void zeroDelayGlobalTaskUsesPaperScheduler() {
        AtomicBoolean ran = new AtomicBoolean();
        org.bukkit.scheduler.BukkitScheduler scheduler =
            mock(org.bukkit.scheduler.BukkitScheduler.class);

        try (MockedStatic<Bukkit> bukkit = mockStatic(Bukkit.class)) {
            bukkit.when(Bukkit::getScheduler).thenReturn(scheduler);
            SchedulerUtil.runGlobalDelayed(mock(Plugin.class), () -> ran.set(true), 0);
        }

        assertFalse(ran.get());
        org.mockito.Mockito.verify(scheduler)
            .runTask(org.mockito.ArgumentMatchers.any(Plugin.class),
                org.mockito.ArgumentMatchers.any(Runnable.class));
    }
}
