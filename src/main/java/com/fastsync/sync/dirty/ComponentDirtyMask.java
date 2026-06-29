package com.fastsync.sync.dirty;

import java.util.UUID;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks which player data components have been modified since the last
 * successful save, so that periodic saves can skip serialization + DB writes
 * for unchanged components.
 *
 * <h2>Design (round 20: AtomicLong bitset)</h2>
 * <p>Each player's dirty state is stored as a single {@link AtomicLong} bitset
 * plus a per-component epoch array (also lock-free via {@link AtomicLong}).
 * This eliminates the {@code synchronized} blocks that were on the Bukkit
 * event thread hot path (InventoryClickEvent, etc.).
 *
 * <ul>
 *   <li>{@code dirtyBits}: AtomicLong — bit i set means Component.ordinal()==i is dirty</li>
 *   <li>{@code epochs[i]}: AtomicLong — bumped on every markDirty for component i</li>
 * </ul>
 *
 * <p>{@code markDirty} is now a single CAS-style {@code getAndUpdate} — no
 * monitor, no EnumSet allocation, no EnumSet.copyOf on snapshot.
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
 * <p>All operations are lock-free. {@code dirtyBits} and {@code epochs[]}
 * use {@link AtomicLong}, so {@code markDirty} (event thread) never blocks
 * {@code snapshotDirty} / {@code clearDirty} (async save thread).
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

    private static final int NUM_COMPONENTS = Component.values().length;
    private static final long ALL_DIRTY_BITS;
    static {
        long bits = 0;
        for (int i = 0; i < NUM_COMPONENTS; i++) {
            bits |= (1L << i);
        }
        ALL_DIRTY_BITS = bits;
    }

    private final ConcurrentHashMap<UUID, PlayerDirtyState> masks = new ConcurrentHashMap<>();
    private final Set<UUID> suppressedPlayers = ConcurrentHashMap.newKeySet();
    private final int validationInterval;

    public ComponentDirtyMask(int validationInterval) {
        this.validationInterval = Math.max(1, validationInterval);
    }

    private static long bit(Component c) {
        return 1L << c.ordinal();
    }

    /**
     * Mark a component dirty for a player.
     * Lock-free: epoch bump followed by AtomicLong.getAndUpdate on dirty bits.
     * The order is intentional: publishing the bit first would let a concurrent
     * saver snapshot+clear the old epoch before the writer bumps it, losing the
     * dirty signal.
     */
    public void markDirty(UUID uuid, Component component) {
        if (suppressedPlayers.contains(uuid)) return;
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        state.epochs[component.ordinal()].incrementAndGet();
        state.dirtyBits.getAndUpdate(v -> v | bit(component));
    }

    public void markDirty(UUID uuid, Set<Component> components) {
        if (suppressedPlayers.contains(uuid)) return;
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        long mask = 0;
        for (Component c : components) {
            mask |= bit(c);
            state.epochs[c.ordinal()].incrementAndGet();
        }
        final long maskFinal = mask;
        state.dirtyBits.getAndUpdate(v -> v | maskFinal);
    }

    public void markAllDirty(UUID uuid) {
        if (suppressedPlayers.contains(uuid)) return;
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        for (int i = 0; i < NUM_COMPONENTS; i++) {
            state.epochs[i].incrementAndGet();
        }
        state.dirtyBits.getAndUpdate(v -> v | ALL_DIRTY_BITS);
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
        return state != null && state.dirtyBits.get() != 0;
    }

    /**
     * Snapshot the current dirty set WITH per-component epochs.
     * Allocates a DirtySnapshot (Map + Long values) — called once per save cycle.
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
        if (state != null) state.dirtyBits.set(0);
    }

    public boolean recordSaveAndCheckValidation(UUID uuid) {
        PlayerDirtyState state = masks.computeIfAbsent(uuid, k -> new PlayerDirtyState());
        long count = state.saveCount.incrementAndGet();
        return (count % validationInterval) == 0;
    }

    private final ConcurrentHashMap<UUID, java.util.concurrent.atomic.AtomicInteger> apiMutationScanCounters = new ConcurrentHashMap<>();

    public boolean recordApiMutationScanAndCheck(UUID uuid, int interval) {
        if (interval <= 1) return true;
        java.util.concurrent.atomic.AtomicInteger counter =
            apiMutationScanCounters.computeIfAbsent(uuid, k -> new java.util.concurrent.atomic.AtomicInteger());
        int value = counter.incrementAndGet();
        if (value >= interval) {
            counter.set(0);
            return true;
        }
        return false;
    }

    public void remove(UUID uuid) {
        masks.remove(uuid);
        apiMutationScanCounters.remove(uuid);
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
        public static final DirtySnapshot EMPTY = new DirtySnapshot(Map.of());
        private final Map<Component, Long> epochs;
        DirtySnapshot(Map<Component, Long> epochs) { this.epochs = epochs; }
        public Map<Component, Long> epochs() { return epochs; }
        public Set<Component> components() { return epochs.keySet(); }
        public int size() { return epochs.size(); }
        public boolean isEmpty() { return epochs.isEmpty(); }
    }

    // ==================== Per-player state (lock-free) ====================

    private static final class PlayerDirtyState {
        final AtomicLong dirtyBits = new AtomicLong(0);
        final AtomicLong[] epochs = new AtomicLong[NUM_COMPONENTS];
        final AtomicLong saveCount = new AtomicLong(0);

        PlayerDirtyState() {
            for (int i = 0; i < NUM_COMPONENTS; i++) {
                epochs[i] = new AtomicLong(0);
            }
        }

        Set<Component> getDirtySet() {
            long bits = dirtyBits.get();
            if (bits == 0) return EnumSet.noneOf(Component.class);
            EnumSet<Component> set = EnumSet.noneOf(Component.class);
            for (Component c : Component.values()) {
                if ((bits & bit(c)) != 0) set.add(c);
            }
            return set;
        }

        DirtySnapshot snapshotWithEpochs() {
            long bits = dirtyBits.get();
            if (bits == 0) return DirtySnapshot.EMPTY;
            Map<Component, Long> map = new java.util.EnumMap<>(Component.class);
            for (Component c : Component.values()) {
                if ((bits & bit(c)) != 0) {
                    map.put(c, epochs[c.ordinal()].get());
                }
            }
            return new DirtySnapshot(map);
        }

        void clearIfEpochMatches(DirtySnapshot snapshot) {
            if (snapshot == null || snapshot.isEmpty()) return;
            long clearMask = 0;
            for (Map.Entry<Component, Long> entry : snapshot.epochs().entrySet()) {
                Component c = entry.getKey();
                long snapshotEpoch = entry.getValue();
                if (epochs[c.ordinal()].get() == snapshotEpoch) {
                    clearMask |= bit(c);
                }
            }
            if (clearMask != 0) {
                final long clearMaskFinal = clearMask;
                dirtyBits.getAndUpdate(v -> v & ~clearMaskFinal);
            }
        }

        void clearBits(Set<Component> components) {
            long clearMask = 0;
            for (Component c : components) clearMask |= bit(c);
            final long clearMaskFinal = clearMask;
            dirtyBits.getAndUpdate(v -> v & ~clearMaskFinal);
        }
    }
}
