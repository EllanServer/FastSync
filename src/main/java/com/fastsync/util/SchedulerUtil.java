package com.fastsync.util;

import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Player;
import org.bukkit.plugin.Plugin;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Unified scheduler abstraction for Paper and Folia compatibility.
 *
 * <p>Per the official PaperMC documentation: the Paper API (1.21.4+) includes
 * Folia's scheduler interfaces. On standard Paper, these are internally adapted
 * to provide equivalent single-threaded behavior. On Folia, they dispatch to
 * the correct region thread.
 *
 * <p>Key principle: <strong>never assume a single main thread</strong>. All
 * entity/world operations are dispatched to the correct region or entity scheduler.
 *
 * <p>API signature notes:
 * <ul>
 *   <li>AsyncScheduler: uses TimeUnit (milliseconds)</li>
 *   <li>GlobalRegionScheduler: uses ticks (no TimeUnit)</li>
 *   <li>RegionScheduler: uses ticks (no TimeUnit)</li>
 *   <li>EntityScheduler: uses ticks, requires a retired Runnable</li>
 * </ul>
 */
public final class SchedulerUtil {

    private static final boolean FOLIA;

    static {
        boolean folia;
        try {
            Class.forName("io.papermc.paper.threadedregions.RegionizedServer");
            folia = true;
        } catch (ClassNotFoundException e) {
            folia = false;
        }
        FOLIA = folia;
    }

    private SchedulerUtil() {}

    public static boolean isFolia() {
        return FOLIA;
    }

    // ==================== Async Tasks (TimeUnit = milliseconds) ====================

    public static void runAsync(Plugin plugin, Runnable task) {
        if (FOLIA) {
            Bukkit.getAsyncScheduler().runNow(plugin, t -> task.run());
        } else {
            Bukkit.getScheduler().runTaskAsynchronously(plugin, task);
        }
    }

    public static void runAsyncDelayed(Plugin plugin, Runnable task, long delayTicks) {
        if (FOLIA) {
            long delayMs = ticksToMillis(delayTicks);
            Bukkit.getAsyncScheduler().runDelayed(plugin, t -> task.run(), delayMs, TimeUnit.MILLISECONDS);
        } else {
            Bukkit.getScheduler().runTaskLaterAsynchronously(plugin, task, delayTicks);
        }
    }

    public static Object runAsyncTimer(Plugin plugin, Runnable task, long delayTicks, long periodTicks) {
        if (FOLIA) {
            long delayMs = ticksToMillis(delayTicks);
            long periodMs = ticksToMillis(periodTicks);
            return Bukkit.getAsyncScheduler().runAtFixedRate(plugin, t -> task.run(),
                delayMs, periodMs, TimeUnit.MILLISECONDS);
        } else {
            return Bukkit.getScheduler().runTaskTimerAsynchronously(plugin, task, delayTicks, periodTicks);
        }
    }

    // ==================== Global Region Tasks (TimeUnit = ticks) ====================

    public static void runGlobal(Plugin plugin, Runnable task) {
        if (FOLIA) {
            Bukkit.getGlobalRegionScheduler().run(plugin, t -> task.run());
        } else {
            // Inline execution when already on the main thread — avoids
            // scheduling overhead and prevents self-deadlock during shutdown
            // (onDisable calls saveAllOnlinePlayers which waits on futures
            // that would otherwise be queued to the next tick).
            if (Bukkit.isPrimaryThread()) {
                task.run();
            } else {
                Bukkit.getScheduler().runTask(plugin, task);
            }
        }
    }

    /**
     * Run a repeating task on the global region.
     * Delay and period are in TICKS on both Paper and Folia.
     */
    public static Object runGlobalTimer(Plugin plugin, Runnable task, long delayTicks, long periodTicks) {
        if (FOLIA) {
            return Bukkit.getGlobalRegionScheduler().runAtFixedRate(plugin, t -> task.run(),
                delayTicks, periodTicks);
        } else {
            return Bukkit.getScheduler().runTaskTimer(plugin, task, delayTicks, periodTicks);
        }
    }

    // ==================== Entity Tasks (TimeUnit = ticks) ====================

    /**
     * Run a task on the region that owns the given entity.
     * The task follows the entity as it moves between regions (Folia).
     *
     * @param plugin  the plugin
     * @param entity  the entity whose region to run on
     * @param task    the task to run
     * @param retired fallback if the entity is no longer valid (can be null)
     */
    public static void runAtEntity(Plugin plugin, Entity entity, Runnable task, Runnable retired) {
        if (FOLIA) {
            entity.getScheduler().run(plugin, t -> task.run(),
                retired != null ? retired : () -> {});
        } else {
            // CRITICAL: When already on the main thread (e.g., during onDisable),
            // execute inline. Otherwise, the task is scheduled for the next tick,
            // but the calling thread may be blocking on a future that this task
            // must complete — a classic self-deadlock that causes shutdown saves
            // to time out after 30s with zero players saved.
            if (Bukkit.isPrimaryThread()) {
                task.run();
            } else {
                Bukkit.getScheduler().runTask(plugin, task);
            }
        }
    }

    /**
     * Execute a task on the entity's region after a delay (ticks).
     */
    public static void runAtEntityDelayed(Plugin plugin, Entity entity, Runnable task, long delayTicks) {
        if (FOLIA) {
            entity.getScheduler().execute(plugin, task, null, delayTicks);
        } else {
            Bukkit.getScheduler().runTaskLater(plugin, task, delayTicks);
        }
    }

    /**
     * Run a task on the region that owns the player with the given UUID.
     * If the player is offline, the retired callback is invoked.
     *
     * @param plugin  the plugin
     * @param uuid    the player UUID
     * @param task    receives the Player if online
     * @param retired fallback if the player is not online
     */
    public static void runForPlayer(Plugin plugin, UUID uuid, Consumer<Player> task, Runnable retired) {
        Player player = Bukkit.getPlayer(uuid);
        if (player == null) {
            if (retired != null) retired.run();
            return;
        }
        runAtEntity(plugin, player, () -> {
            // Re-check online status inside the task — player may have logged out
            // between the Bukkit.getPlayer() call and the task execution.
            Player p = Bukkit.getPlayer(uuid);
            if (p != null && p.isOnline()) {
                task.accept(p);
            } else if (retired != null) {
                retired.run();
            }
        }, retired);
    }

    // ==================== Region Tasks (TimeUnit = ticks) ====================

    /**
     * Run a task on the region that owns the given location.
     */
    public static void runAtLocation(Plugin plugin, Location location, Runnable task) {
        if (FOLIA) {
            Bukkit.getRegionScheduler().run(plugin, location, t -> task.run());
        } else {
            Bukkit.getScheduler().runTask(plugin, task);
        }
    }

    /**
     * Run a repeating task on the region that owns the given location.
     * Delay and period are in TICKS.
     */
    public static Object runRegionTimer(Plugin plugin, Location location, Runnable task,
                                         long delayTicks, long periodTicks) {
        if (FOLIA) {
            return Bukkit.getRegionScheduler().runAtFixedRate(plugin, location, t -> task.run(),
                delayTicks, periodTicks);
        } else {
            return Bukkit.getScheduler().runTaskTimer(plugin, task, delayTicks, periodTicks);
        }
    }

    // ==================== Cancellation ====================

    public static void cancel(Object task) {
        if (task == null) return;
        if (task instanceof org.bukkit.scheduler.BukkitTask bt) {
            bt.cancel();
        } else if (task instanceof ScheduledTask st) {
            st.cancel();
        }
    }

    // ==================== Helpers ====================

    private static long ticksToMillis(long ticks) {
        return ticks * 50L;
    }
}
