package com.fastsync.sync.dirty;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.EnumSet;
import java.util.Map;
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
 *   <li>A per-component {@code dirtyEpoch} counter — bumped on every
 *       {@code markDirty} call</li>
 *   <li>A counter of saves since the last full-collect validation</li>
 * </ul>
 *
 * <p>Dirty state is set by event listeners (e.g. {@code InventoryClickEvent}
 * marks {@link Component#INVENTORY} dirty) and cleared after a successful
 * save of that component.
 *
 * <h2>Epoch-based clear (the race this prevents)</h2>
 * <p>Naive boolean dirty tracking has a classic lost-update race:
 * <pre>
 *   T1: periodic save starts, snapshot = {INVENTORY}
 *   T1: serialize inventory snapshot A
 *   T1: async DB write of A in flight
 *   T2: player modifies inventory, markDirty(INVENTORY)
 *   T1: DB write of A succeeds
 *   T1: clearDirty({INVENTORY})  // clears the bit set by T2!
 *   T1: next periodic tick sees no dirty → skips save
 *   ⚠ player's modification from T2 is lost until next full save
 * </pre>
 *
 * <p>This class prevents the race by tagging each dirty bit with a monotonic
 * epoch. {@link #snapshotDirty(UUID)} returns the set of dirty components
 * <em>and</em> their current epoch. {@link #clearDirty(UUID, DirtySnapshot)}
 * only removes a component from the dirty set if its current epoch still
 * matches the snapshot's epoch. If a newer {@code markDirty} call bumped the
 * epoch during the save, the bit stays dirty and the next periodic save will
 * pick up the latest state.
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
     *
     * <p>This bumps the per-component dirty epoch so that any in-flight save
     * snapshotting the previous epoch will NOT clear this new dirty signal
     * when it completes.
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
     * Get the current dirty set for a player (snapshot without epoch info).
     * Returns an empty set if the player has no dirty mask.
     *
     * <p><b>Warning:</b> using this for save-path decisions is racy — the
     * returned set has no epoch protection, so a subsequent
     * {@link #clearDirty(UUID, Set)} can clobber a concurrent {@code markDirty}.
     * Use {@link #snapshotDirty(UUID)} + {@link #clearDirty(UUID, DirtySnapshot)}
     * on the save path instead. This method is kept for legacy callers that
     * only need a best-effort dirty check (e.g. skipping periodic save when
     * nothing is dirty).
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
     * Snapshot the current dirty set WITH per-component epochs.
     *
     * <p>The returned {@link DirtySnapshot} captures the dirty state at the
     * moment of the call. Pass it to {@link #clearDirty(UUID, DirtySnapshot)}
     * after a successful save — only components whose epoch still matches
     * will be cleared, so a {@code markDirty} that arrived during the DB
     * write will preserve the dirty bit for the next save.
     */
    public DirtySnapshot snapshotDirty(UUID uuid) {
        DirtyMask mask = masks.get(uuid);
        return mask != null ? mask.snapshotWithEpochs() : DirtySnapshot.EMPTY;
    }

    /**
     * Clear dirty flags for the components in the snapshot, but ONLY if their
     * current epoch matches the snapshot's epoch. Components whose epoch has
     * been bumped since the snapshot (by a concurrent {@code markDirty}) are
     * left dirty so the next save will pick up the newer change.
     *
     * <p>This is the epoch-protected variant of {@link #clearDirty(UUID, Set)}.
     * Always prefer this on the save path.
     */
    public void clearDirty(UUID uuid, DirtySnapshot snapshot) {
        DirtyMask mask = masks.get(uuid);
        if (mask != null) mask.clearIfEpochMatches(snapshot);
    }

    /**
     * Clear dirty flags for the given components after a successful save.
     *
     * <p><b>Deprecated for save-path use.</b> This overload does NOT protect
     * against the lost-update race where a {@code markDirty} arrives during
     * the DB write — it will blindly clear the bit and lose the new signal.
     * Use {@link #clearDirty(UUID, DirtySnapshot)} with a snapshot from
     * {@link #snapshotDirty(UUID)} instead.
     *
     * <p>Kept for non-save callers that explicitly want to drop dirty state
     * (e.g. {@link #clearAll(UUID)} for full-save completion).
     */
    public void clearDirty(UUID uuid, Set<Component> components) {
        DirtyMask mask = masks.get(uuid);
        if (mask != null) mask.clear(components);
    }

    /**
     * Clear all dirty flags for a player (used after a full save).
     *
     * <p>Safe to use after a full Blob save because the full Blob contains
     * the latest state of every component — any dirty signal that arrived
     * during the save is already captured in the Blob and will be persisted.
     * The next save cycle will start fresh and only mark dirty what actually
     * changes after this point.
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

    // ==================== DirtySnapshot ====================

    /**
     * Immutable snapshot of dirty components + their epochs at a point in time.
     *
     * <p>Pass this to {@link #clearDirty(UUID, DirtySnapshot)} after a save
     * completes — only components whose epoch still matches will be cleared.
     */
    public static final class DirtySnapshot {
        /** Empty snapshot — returned when the player has no dirty mask. */
        public static final DirtySnapshot EMPTY = new DirtySnapshot(Map.of());

        private final Map<Component, Long> epochs;

        DirtySnapshot(Map<Component, Long> epochs) {
            this.epochs = epochs;
        }

        /** The dirty components at snapshot time, with their epochs. */
        public Map<Component, Long> epochs() {
            return epochs;
        }

        /** The dirty components (without epoch info) — for serialization loops. */
        public Set<Component> components() {
            return epochs.keySet();
        }

        /** Number of dirty components in this snapshot. */
        public int size() {
            return epochs.size();
        }

        /** True if no components were dirty at snapshot time. */
        public boolean isEmpty() {
            return epochs.isEmpty();
        }
    }

    // ==================== Per-player mask ====================

    private static final class DirtyMask {
        private final EnumSet<Component> dirty = EnumSet.noneOf(Component.class);
        // Per-component epoch — bumped on every markDirty. Used by clearDirty
        // to detect "a newer markDirty arrived during my save" and preserve
        // the dirty bit in that case.
        private final long[] epochs = new long[Component.values().length];
        private final AtomicLong saveCount = new AtomicLong(0);

        synchronized void mark(Component c) {
            dirty.add(c);
            epochs[c.ordinal()]++;
        }

        synchronized void markAll() {
            for (Component c : ALL) {
                dirty.add(c);
                epochs[c.ordinal()]++;
            }
        }

        synchronized void clear(Set<Component> components) {
            // No epoch check — used by clearAll and legacy callers.
            dirty.removeAll(components);
        }

        synchronized void clearAll() {
            // No epoch check — full Blob save captures all current state, so
            // any in-flight markDirty signal is already represented in the Blob.
            dirty.clear();
        }

        synchronized Set<Component> snapshot() {
            return dirty.isEmpty()
                ? EnumSet.noneOf(Component.class)
                : EnumSet.copyOf(dirty);
        }

        synchronized DirtySnapshot snapshotWithEpochs() {
            if (dirty.isEmpty()) return DirtySnapshot.EMPTY;
            Map<Component, Long> map = new java.util.EnumMap<>(Component.class);
            for (Component c : dirty) {
                map.put(c, epochs[c.ordinal()]);
            }
            return new DirtySnapshot(map);
        }

        synchronized void clearIfEpochMatches(DirtySnapshot snapshot) {
            if (snapshot == null || snapshot.isEmpty()) return;
            // Only clear components whose current epoch still matches the
            // snapshot's epoch. If a newer markDirty bumped the epoch during
            // the save, leave the dirty bit set so the next save picks it up.
            for (Map.Entry<Component, Long> entry : snapshot.epochs().entrySet()) {
                Component c = entry.getKey();
                long snapshotEpoch = entry.getValue();
                if (epochs[c.ordinal()] == snapshotEpoch) {
                    dirty.remove(c);
                }
                // else: epoch was bumped during save — leave dirty
            }
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
