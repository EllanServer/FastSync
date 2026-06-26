package com.fastsync.sync.dirty;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.UUID;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ComponentDirtyMask}.
 *
 * <p>Verifies the core invariants of the dirty-tracking system:
 * <ol>
 *   <li>markDirty → getDirty returns the component</li>
 *   <li>clearDirty → getDirty no longer returns it</li>
 *   <li>Per-UUID isolation — marking one player doesn't affect another</li>
 *   <li>Validation interval — recordSaveAndCheckValidation returns true
 *       every Nth call</li>
 *   <li>markAllDirty forces all components dirty</li>
 *   <li>remove cleans up the per-UUID mask</li>
 * </ol>
 */
class ComponentDirtyMaskTest {

    private ComponentDirtyMask mask;
    private UUID player1;
    private UUID player2;

    @BeforeEach
    void setUp() {
        mask = new ComponentDirtyMask(5);  // validation every 5 saves
        player1 = UUID.randomUUID();
        player2 = UUID.randomUUID();
    }

    @Test
    void testMarkAndQueryDirty() {
        assertFalse(mask.isAnyDirty(player1));

        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        assertTrue(mask.isAnyDirty(player1));
        assertTrue(mask.getDirty(player1).contains(ComponentDirtyMask.Component.INVENTORY));
    }

    @Test
    void testClearDirtyAfterSave() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.markDirty(player1, ComponentDirtyMask.Component.VITALS);

        Set<ComponentDirtyMask.Component> dirty = mask.getDirty(player1);
        mask.clearDirty(player1, dirty);

        assertFalse(mask.isAnyDirty(player1));
        assertTrue(mask.getDirty(player1).isEmpty());
    }

    @Test
    void testPerUuidIsolation() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);

        assertTrue(mask.isAnyDirty(player1));
        assertFalse(mask.isAnyDirty(player2));
        assertTrue(mask.getDirty(player2).isEmpty());

        mask.markDirty(player2, ComponentDirtyMask.Component.EXPERIENCE);
        assertTrue(mask.getDirty(player1).contains(ComponentDirtyMask.Component.INVENTORY));
        assertTrue(mask.getDirty(player2).contains(ComponentDirtyMask.Component.EXPERIENCE));
        assertFalse(mask.getDirty(player2).contains(ComponentDirtyMask.Component.INVENTORY));
    }

    @Test
    void testMarkAllDirty() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.markAllDirty(player1);

        Set<ComponentDirtyMask.Component> dirty = mask.getDirty(player1);
        assertEquals(ComponentDirtyMask.ALL.size(), dirty.size(),
            "markAllDirty should set every component dirty");
        assertTrue(dirty.containsAll(ComponentDirtyMask.ALL));
    }

    @Test
    void testValidationInterval() {
        // With interval=5, validation should fire on 5th, 10th, 15th...
        for (int i = 1; i <= 4; i++) {
            assertFalse(mask.recordSaveAndCheckValidation(player1),
                "Save " + i + " should not trigger validation");
        }
        assertTrue(mask.recordSaveAndCheckValidation(player1),
            "Save 5 should trigger validation");
        for (int i = 6; i <= 9; i++) {
            assertFalse(mask.recordSaveAndCheckValidation(player1),
                "Save " + i + " should not trigger validation");
        }
        assertTrue(mask.recordSaveAndCheckValidation(player1),
            "Save 10 should trigger validation");
    }

    @Test
    void testValidationIntervalPerUuid() {
        // Validation counters are per-UUID, not global
        for (int i = 1; i <= 4; i++) {
            mask.recordSaveAndCheckValidation(player1);
        }
        // player1 is one save away from validation
        assertFalse(mask.recordSaveAndCheckValidation(player2),
            "player2's first save should not trigger validation");
        assertTrue(mask.recordSaveAndCheckValidation(player1),
            "player1's 5th save should trigger validation");
    }

    @Test
    void testRemoveClearsMask() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.markAllDirty(player1);
        assertTrue(mask.isAnyDirty(player1));

        mask.remove(player1);
        assertFalse(mask.isAnyDirty(player1));
        assertTrue(mask.getDirty(player1).isEmpty());
    }

    @Test
    void testClearAll() {
        mask.markAllDirty(player1);
        mask.clearAll(player1);
        assertFalse(mask.isAnyDirty(player1));
    }

    @Test
    void testMultipleComponentsDirty() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.markDirty(player1, ComponentDirtyMask.Component.VITALS);
        mask.markDirty(player1, ComponentDirtyMask.Component.EXPERIENCE);

        Set<ComponentDirtyMask.Component> dirty = mask.getDirty(player1);
        assertEquals(3, dirty.size());
        assertTrue(dirty.contains(ComponentDirtyMask.Component.INVENTORY));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.VITALS));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.EXPERIENCE));
    }

    @Test
    void testTrackedPlayerCount() {
        assertEquals(0, mask.getTrackedPlayerCount());

        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        assertEquals(1, mask.getTrackedPlayerCount());

        mask.markDirty(player2, ComponentDirtyMask.Component.VITALS);
        assertEquals(2, mask.getTrackedPlayerCount());

        mask.remove(player1);
        assertEquals(1, mask.getTrackedPlayerCount());
    }

    @Test
    void testClearDirtySubset() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.markDirty(player1, ComponentDirtyMask.Component.VITALS);
        mask.markDirty(player1, ComponentDirtyMask.Component.EXPERIENCE);

        // Clear only INVENTORY + VITALS
        mask.clearDirty(player1, Set.of(
            ComponentDirtyMask.Component.INVENTORY,
            ComponentDirtyMask.Component.VITALS
        ));

        Set<ComponentDirtyMask.Component> dirty = mask.getDirty(player1);
        assertEquals(1, dirty.size());
        assertTrue(dirty.contains(ComponentDirtyMask.Component.EXPERIENCE));
    }

    @Test
    void testGetDirtyReturnsDefensiveCopy() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        Set<ComponentDirtyMask.Component> snapshot = mask.getDirty(player1);

        // Mutating the snapshot should not affect the internal state
        snapshot.add(ComponentDirtyMask.Component.VITALS);

        Set<ComponentDirtyMask.Component> fresh = mask.getDirty(player1);
        assertFalse(fresh.contains(ComponentDirtyMask.Component.VITALS),
            "Mutating the returned set must not affect the mask's internal state");
    }

    @Test
    void testValidationIntervalMinimum() {
        ComponentDirtyMask m = new ComponentDirtyMask(1);
        // Every save triggers validation
        for (int i = 0; i < 10; i++) {
            assertTrue(m.recordSaveAndCheckValidation(player1),
                "With interval=1, every save should trigger validation");
        }
    }

    /**
     * Reproduces the lost-update race that the epoch-based clearDirty prevents:
     *
     *   T1: snapshot dirty = {INVENTORY}
     *   T2: markDirty(INVENTORY)  // new change arrives during DB write
     *   T1: clearDirty(snapshot)  // must NOT clear the new signal
     *   ⚠ old behavior (boolean set): INVENTORY gets cleared, T2's change is lost
     *   ✅ new behavior (epoch): INVENTORY stays dirty because epoch differs
     */
    @Test
    void testEpochProtectsAgainstLostUpdateDuringSave() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);

        // Step 1: snapshot dirty state at the start of a save
        ComponentDirtyMask.DirtySnapshot snapshot = mask.snapshotDirty(player1);
        assertEquals(1, snapshot.size(), "snapshot should have 1 dirty component");
        assertTrue(snapshot.components().contains(ComponentDirtyMask.Component.INVENTORY));

        // Step 2: simulate a concurrent markDirty arriving during the DB write
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);

        // Step 3: clearDirty with the old snapshot — must NOT clear INVENTORY
        // because its epoch was bumped by the concurrent markDirty.
        mask.clearDirty(player1, snapshot);

        assertTrue(mask.isAnyDirty(player1),
            "INVENTORY must remain dirty — concurrent markDirty bumped the epoch");
        assertTrue(mask.getDirty(player1).contains(ComponentDirtyMask.Component.INVENTORY),
            "INVENTORY must still be in the dirty set");
    }

    /**
     * Verifies that clearDirty(snapshot) DOES clear components whose epoch
     * matches the snapshot — i.e. the normal happy path still works.
     */
    @Test
    void testClearDirtyWithMatchingEpoch() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.markDirty(player1, ComponentDirtyMask.Component.VITALS);

        ComponentDirtyMask.DirtySnapshot snapshot = mask.snapshotDirty(player1);
        // No concurrent markDirty — epochs still match
        mask.clearDirty(player1, snapshot);

        assertFalse(mask.isAnyDirty(player1),
            "All dirty components should be cleared when epochs match");
        assertTrue(mask.getDirty(player1).isEmpty());
    }

    /**
     * Verifies that clearDirty(snapshot) only clears components whose epoch
     * matches — a concurrent markDirty on ONE component must NOT cause other
     * components (whose epochs still match) to be skipped.
     */
    @Test
    void testClearDirtyClearsMatchingComponentsOnly() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.markDirty(player1, ComponentDirtyMask.Component.VITALS);
        mask.markDirty(player1, ComponentDirtyMask.Component.EXPERIENCE);

        ComponentDirtyMask.DirtySnapshot snapshot = mask.snapshotDirty(player1);

        // Concurrent markDirty on INVENTORY only — bumps its epoch
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);

        mask.clearDirty(player1, snapshot);

        // INVENTORY stays dirty (epoch mismatch), VITALS + EXPERIENCE cleared
        Set<ComponentDirtyMask.Component> dirty = mask.getDirty(player1);
        assertEquals(1, dirty.size(),
            "Only INVENTORY should remain dirty (epoch mismatch)");
        assertTrue(dirty.contains(ComponentDirtyMask.Component.INVENTORY));
    }

    /**
     * Verifies that snapshotDirty on a player with no mask returns EMPTY
     * (not null) and clearDirty(EMPTY) is a no-op.
     */
    @Test
    void testSnapshotDirtyForUnknownPlayerIsEmpty() {
        ComponentDirtyMask.DirtySnapshot snapshot = mask.snapshotDirty(player2);
        assertSame(ComponentDirtyMask.DirtySnapshot.EMPTY, snapshot);
        assertTrue(snapshot.isEmpty());
        // clearDirty on EMPTY is safe — no-op
        mask.clearDirty(player2, snapshot);
        assertFalse(mask.isAnyDirty(player2));
    }
}
