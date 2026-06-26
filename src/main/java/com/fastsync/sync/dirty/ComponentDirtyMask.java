package com.fastsync.sync.dirty;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks which player data components have been modified since the last
 * successful save, so that periodic saves can skip serialization + DB writes
 * for unchanged components.
 *
 * <h2>Design</h2>
 * <p>Each player has a per-UUID {@link DirtyMask} holding:
 * <ul>
 *   <li>An {@link EnumSet} of dirty {@link Component}s</li>
 *   <li>A counter of saves since the last full-collect validation</li>
 * </ul>
 *
 * <p>Dirty state is set by event listeners (e.g. {@code InventoryClickEvent}
 * marks {@link Component#INVENTORY} dirty) and cleared after a successful
 * save of that component.
 *
 * <h2>Fallback validation</h2>
 * <p>Event-driven dirty tracking can miss changes (e.g. a plugin modifies
 * inventory via API without firing a Bukkit event). To prevent silent data
 * loss, every {@code validationInterval} saves we force a full collect +
 * checksum comparison. If the checksum differs from the last saved value,
 * we mark all components dirty and re-save. This is the Spanner "lazy
 * validation" pattern: trust the fast path, verify periodically.
 *
 * <h2>Thread safety</h2>
 * <p>All operations on {@link DirtyMask} are thread-safe via
 * {@link ConcurrentHashMap} + synchronized fallback. The mask is keyed by
 * player UUID so different players never contend.
 */
public class ComponentDirtyMask {

    /** Components that can be independently marked dirty. */
    public enum Component {
        INVENTORY,       // inventory + armor + offhand
        ENDER_CHEST,
        VITALS,          // health + maxHealth
        FOOD,            // foodLevel + saturation + exhaustion
        EXPERIENCE,      // level + exp progress + total exp
        POTION_EFFECTS,
        GAME_MODE,
        FIRE_TICKS,
        AIR,
        FLIGHT,
        ADVANCEMENTS,
        STATISTICS,
        ATTRIBUTES,
        PDC,
        LOCATION
    }

    /** All components — used for full-collect fallback. */
    public static final Set<Component> ALL = EnumSet.allOf(Component.class);

    /** Basic components that are cheap to collect and always worth syncing. */
    public static final Set<Component> BASIC = EnumSet.of(
        Component.INVENTORY, Component.ENDER_CHEST, Component.VITALS,
        Component.FOOD, Component.EXPERIENCE, Component.POTION_EFFECTS,
        Component.GAME_MODE, Component.FIRE_TICKS, Component.AIR, Component.FLIGHT
    );

    private final ConcurrentHashMap<UUID, DirtyMask> masks = new ConcurrentHashMap<>();
    private final int validationInterval;

    public ComponentDirtyMask(int validationInterval) {
        // validationInterval = how many saves before we force a full checksum
        // comparison. 0 = never validate (event-driven only, risky).
        // Default: 5 — every 5th save does a full collect + checksum.
        this.validationInterval = Math.max(1, validationInterval);
    }

    /**
     * Mark a component dirty for a player.
     * Called from event listeners on the main thread (Bukkit events are sync).
     */
    public void markDirty(UUID uuid, Component component) {
        masks.computeIfAbsent(uuid, k -> new DirtyMask()).mark(component);
    }

    /**
     * Mark multiple components dirty at once.
     */
    public void markDirty(UUID uuid, Set<Component> components) {
        DirtyMask mask = masks.computeIfAbsent(uuid, k -> new DirtyMask());
        for (Component c : components) mask.mark(c);
    }

    /**
     * Mark all components dirty — forces a full save next cycle.
     * Used on join, quit, death, and validation mismatches.
     */
    public void markAllDirty(UUID uuid) {
        masks.computeIfAbsent(uuid, k -> new DirtyMask()).markAll();
    }

    /**
     * Get the current dirty set for a player (snapshot).
     * Returns an empty set if the player has no dirty mask.
     */
    public Set<Component> getDirty(UUID uuid) {
        DirtyMask mask = masks.get(uuid);
        return mask != null ? mask.snapshot() : EnumSet.noneOf(Component.class);
    }

    /**
     * Check if any component is dirty for a player.
     */
    public boolean isAnyDirty(UUID uuid) {
        DirtyMask mask = masks.get(uuid);
        return mask != null && !mask.isEmpty();
    }

    /**
     * Clear dirty flags for the given components after a successful save.
     */
    public void clearDirty(UUID uuid, Set<Component> components) {
        DirtyMask mask = masks.get(uuid);
        if (mask != null) mask.clear(components);
    }

    /**
     * Clear all dirty flags for a player (used after a full save).
     */
    public void clearAll(UUID uuid) {
        DirtyMask mask = masks.get(uuid);
        if (mask != null) mask.clearAll();
    }

    /**
     * Record that a save completed for a player, and decide whether the next
     * save should do a full-collect validation.
     *
     * @return true if the next save should force a full collect + checksum
     *         comparison (validation interval reached).
     */
    public boolean recordSaveAndCheckValidation(UUID uuid) {
        DirtyMask mask = masks.computeIfAbsent(uuid, k -> new DirtyMask());
        return mask.recordSaveAndCheckValidation(validationInterval);
    }

    /**
     * Remove the dirty mask for a player (used on quit).
     */
    public void remove(UUID uuid) {
        masks.remove(uuid);
    }

    /**
     * Get the number of players currently being tracked.
     */
    public int getTrackedPlayerCount() {
        return masks.size();
    }

    // ==================== Per-player mask ====================

    private static final class DirtyMask {
        private final EnumSet<Component> dirty = EnumSet.noneOf(Component.class);
        private final AtomicLong saveCount = new AtomicLong(0);

        synchronized void mark(Component c) {
            dirty.add(c);
        }

        synchronized void markAll() {
            dirty.addAll(ALL);
        }

        synchronized void clear(Set<Component> components) {
            dirty.removeAll(components);
        }

        synchronized void clearAll() {
            dirty.clear();
        }

        synchronized Set<Component> snapshot() {
            return dirty.isEmpty()
                ? EnumSet.noneOf(Component.class)
                : EnumSet.copyOf(dirty);
        }

        synchronized boolean isEmpty() {
            return dirty.isEmpty();
        }

        boolean recordSaveAndCheckValidation(int interval) {
            long count = saveCount.incrementAndGet();
            return (count % interval) == 0;
        }
    }
}
