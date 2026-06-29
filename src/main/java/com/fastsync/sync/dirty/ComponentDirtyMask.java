package com.fastsync.sync.dirty;

import java.util.UUID;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Tracks which player data components have been modified since the last
 * successful save, so that periodic saves can skip serialization + DB writes
 * for unchanged components.
 *
 * <h2>Design: atomic component state</h2>
 * <p>Each component is stored in one atomic state word. Bit 0 is the dirty
 * flag and the remaining bits are a monotonically increasing epoch. Keeping
 * the flag and epoch in the same CAS word is important: a separate dirty
 * bitset can lose a concurrent mark between an epoch check and bit clearing.
 * This also reduces the event-thread hot path from two atomic writes to one.
 *
 * <ul>
 *   <li>{@code states[i] & 1}: component i is dirty</li>
 *   <li>{@code states[i] >>> 1}: mutation epoch for component i</li>
 * </ul>
 *
 * <p>{@code markDirty} is a single CAS-style {@code getAndUpdate} — no
 * monitor and no allocation. A snapshot is one primitive {@code long[]} with
 * no {@code EnumMap} or boxed epoch values.
 *
 * <h2>Epoch-based clear (the race this prevents)</h2>
 * <p>Naive boolean dirty tracking has a classic lost-update race. This class
 * prevents it by tagging each dirty bit with a monotonic epoch.
 * {@link #snapshotDirty(UUID)} returns the set of dirty components <em>and</em>
 * their current epoch. {@link #clearDirty(UUID, DirtySnapshot)} only removes
 * a component from the dirty set if its current epoch still matches the
 * snapshot's epoch.
 *
 * <h2>Thread safety</h2>
 * <p>All operations are lock-free. Every component state uses an
 * {@link AtomicLongArray}, so {@code markDirty} (event thread) never blocks
 * {@code snapshotDirty} / {@code clearDirty} (async save thread).
 */
public class ComponentDirtyMask {

    /** Components that can be independently marked dirty. */
    public enum Component {
        INVENTORY(0),       // inventory + armor + offhand
        ENDER_CHEST(1),
        VITALS(2),          // health + maxHealth
        FOOD(3),            // foodLevel + saturation + exhaustion
        EXPERIENCE(4),      // level + exp progress + total exp
        POTION_EFFECTS(5),
        GAME_MODE(6),
        FIRE_TICKS(7),
        AIR(8),
        FLIGHT(9),
        ADVANCEMENTS(10),
        STATISTICS(11),
        ATTRIBUTES(12),
        PDC(13),
        LOCATION(14);

        private final int storageBit;

        Component(int storageBit) {
            if (storageBit < 0 || storageBit >= Long.SIZE) {
                throw new IllegalArgumentException("storageBit must be in [0, 63]");
            }
            this.storageBit = storageBit;
        }

        /** Stable on-disk bit; unlike ordinal, this may never be renumbered. */
        public int storageBit() { return storageBit; }
        public long storageMask() { return 1L << storageBit; }
    }

    /** All components — used for full-collect fallback. */
    public static final Set<Component> ALL = EnumSet.allOf(Component.class);

    /** Basic components that are cheap to collect and always worth syncing. */
    public static final Set<Component> BASIC = EnumSet.of(
        Component.INVENTORY, Component.ENDER_CHEST, Component.VITALS,
        Component.FOOD, Component.EXPERIENCE, Component.POTION_EFFECTS,
        Component.GAME_MODE, Component.FIRE_TICKS, Component.AIR, Component.FLIGHT
    );

    private static final int NUM_COMPONENTS = Component.values().length;
    private static final int SAVE_COUNT_INDEX = NUM_COMPONENTS;
    private static final int API_SCAN_COUNT_INDEX = NUM_COMPONENTS + 1;
    private static final int STATE_WORDS = NUM_COMPONENTS + 2;
    private static final long DIRTY_FLAG = 1L;
    private static final long EPOCH_INCREMENT = 2L;

    private final ConcurrentHashMap<UUID, PlayerDirtyState> masks = new ConcurrentHashMap<>();
    private final Set<UUID> suppressedPlayers = ConcurrentHashMap.newKeySet();
    private final int validationInterval;

    public ComponentDirtyMask(int validationInterval) {
        this.validationInterval = Math.max(1, validationInterval);
    }

    private static long bit(Component c) {
        return c.storageMask();
    }

    private static long markState(long state) {
        return (state + EPOCH_INCREMENT) | DIRTY_FLAG;
    }

    /**
     * Mark a component dirty for a player with one atomic transition that both
     * bumps the epoch and publishes the dirty flag.
     */
    public void markDirty(UUID uuid, Component component) {
        if (suppressedPlayers.contains(uuid)) return;
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        state.states.getAndUpdate(component.ordinal(), ComponentDirtyMask::markState);
    }

    public void markDirty(UUID uuid, Set<Component> components) {
        if (suppressedPlayers.contains(uuid)) return;
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        for (Component c : components) {
            state.states.getAndUpdate(c.ordinal(), ComponentDirtyMask::markState);
        }
    }

    public void markAllDirty(UUID uuid) {
        if (suppressedPlayers.contains(uuid)) return;
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        for (int i = 0; i < NUM_COMPONENTS; i++) {
            state.states.getAndUpdate(i, ComponentDirtyMask::markState);
        }
    }

    /**
     * Get the current dirty set for a player (without epoch info).
     * Allocates an EnumSet — use {@link #isAnyDirty} for cheap checks.
     */
    public Set<Component> getDirty(UUID uuid) {
        PlayerDirtyState state = masks.get(uuid);
        if (state == null) return EnumSet.noneOf(Component.class);
        return state.getDirtySet();
    }

    /**
     * Check if any component is dirty. Lock-free, zero allocation.
     */
    public boolean isAnyDirty(UUID uuid) {
        PlayerDirtyState state = masks.get(uuid);
        return state != null && state.isAnyDirty();
    }

    /**
     * Snapshot the current dirty set WITH per-component epochs.
     * Allocates one primitive state array — called once per save cycle.
     */
    public DirtySnapshot snapshotDirty(UUID uuid) {
        PlayerDirtyState state = masks.get(uuid);
        if (state == null) return DirtySnapshot.EMPTY;
        return state.snapshotWithEpochs();
    }

    /**
     * Clear dirty flags using epoch-protected clear. Lock-free.
     */
    public void clearDirty(UUID uuid, DirtySnapshot snapshot) {
        PlayerDirtyState state = masks.get(uuid);
        if (state != null) state.clearIfEpochMatches(snapshot);
    }

    /**
     * Clear dirty flags for given components (no epoch check). Lock-free.
     */
    public void clearDirty(UUID uuid, Set<Component> components) {
        PlayerDirtyState state = masks.get(uuid);
        if (state != null) state.clearBits(components);
    }

    /**
     * Clear all dirty flags. Lock-free.
     */
    public void clearAll(UUID uuid) {
        PlayerDirtyState state = masks.get(uuid);
        if (state != null) state.clearAll();
    }

    public boolean recordSaveAndCheckValidation(UUID uuid) {
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        long count = state.states.incrementAndGet(SAVE_COUNT_INDEX);
        return (count % validationInterval) == 0;
    }

    public boolean recordApiMutationScanAndCheck(UUID uuid, int interval) {
        if (interval <= 1) return true;
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        while (true) {
            long current = state.states.get(API_SCAN_COUNT_INDEX);
            long next = current + 1 >= interval ? 0 : current + 1;
            if (state.states.compareAndSet(API_SCAN_COUNT_INDEX, current, next)) {
                return next == 0;
            }
        }
    }

    public void remove(UUID uuid) {
        masks.remove(uuid);
        suppressedPlayers.remove(uuid);
    }

    /** Ignore Bukkit events caused by FastSync's own join-time apply phase. */
    public void beginApply(UUID uuid) {
        suppressedPlayers.add(uuid);
    }

    public void endApply(UUID uuid) {
        suppressedPlayers.remove(uuid);
    }

    public int getTrackedPlayerCount() {
        return masks.size();
    }

    // ==================== DirtySnapshot ====================

    public static final class DirtySnapshot {
        public static final DirtySnapshot EMPTY = new DirtySnapshot(0L, new long[0]);
        private final long bits;
        private final long[] states;

        DirtySnapshot(long bits, long[] states) {
            this.bits = bits;
            this.states = states;
        }

        public Set<Component> components() {
            if (bits == 0) return EnumSet.noneOf(Component.class);
            EnumSet<Component> result = EnumSet.noneOf(Component.class);
            for (Component component : Component.values()) {
                if ((bits & bit(component)) != 0) result.add(component);
            }
            return result;
        }

        public boolean contains(Component component) {
            return (bits & bit(component)) != 0;
        }

        public int size() { return Long.bitCount(bits); }
        public boolean isEmpty() { return bits == 0; }
    }

    // ==================== Per-player state (lock-free) ====================

    private static final class PlayerDirtyState {
        // One primitive backing array replaces 15 AtomicLong objects plus two
        // separate counter objects/maps per player. Component words occupy
        // [0, NUM_COMPONENTS); the final two words are save/scan counters.
        final AtomicLongArray states = new AtomicLongArray(STATE_WORDS);

        boolean isAnyDirty() {
            for (int i = 0; i < NUM_COMPONENTS; i++) {
                if ((states.get(i) & DIRTY_FLAG) != 0) return true;
            }
            return false;
        }

        Set<Component> getDirtySet() {
            EnumSet<Component> set = EnumSet.noneOf(Component.class);
            for (Component c : Component.values()) {
                if ((states.get(c.ordinal()) & DIRTY_FLAG) != 0) set.add(c);
            }
            return set;
        }

        DirtySnapshot snapshotWithEpochs() {
            long bits = 0L;
            long[] snapshotStates = new long[NUM_COMPONENTS];
            for (Component c : Component.values()) {
                long value = states.get(c.ordinal());
                if ((value & DIRTY_FLAG) != 0) {
                    bits |= bit(c);
                    snapshotStates[c.ordinal()] = value;
                }
            }
            return bits == 0 ? DirtySnapshot.EMPTY : new DirtySnapshot(bits, snapshotStates);
        }

        void clearIfEpochMatches(DirtySnapshot snapshot) {
            if (snapshot == null || snapshot.isEmpty()) return;
            for (Component component : Component.values()) {
                if (!snapshot.contains(component)) continue;
                int index = component.ordinal();
                long expected = snapshot.states[index];
                states.compareAndSet(index, expected, expected & ~DIRTY_FLAG);
            }
        }

        void clearBits(Set<Component> components) {
            for (Component component : components) {
                states.getAndUpdate(component.ordinal(), value -> value & ~DIRTY_FLAG);
            }
        }

        void clearAll() {
            for (int i = 0; i < NUM_COMPONENTS; i++) {
                states.getAndUpdate(i, value -> value & ~DIRTY_FLAG);
            }
        }
    }
}
