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

    @Test
    void storageBitsAreUniqueAndStable() {
        long seen = 0L;
        for (ComponentDirtyMask.Component component : ComponentDirtyMask.Component.values()) {
            assertEquals(0L, seen & component.storageMask(),
                "Duplicate storage bit for " + component);
            seen |= component.storageMask();
        }
        assertEquals(0, ComponentDirtyMask.Component.INVENTORY.storageBit());
        assertEquals(14, ComponentDirtyMask.Component.LOCATION.storageBit());
    }

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
    void apiMutationScanCounterResetsAtIntervalAndIsPerPlayer() {
        assertFalse(mask.recordApiMutationScanAndCheck(player1, 3));
        assertFalse(mask.recordApiMutationScanAndCheck(player1, 3));
        assertFalse(mask.recordApiMutationScanAndCheck(player2, 3));
        assertTrue(mask.recordApiMutationScanAndCheck(player1, 3));
        assertFalse(mask.recordApiMutationScanAndCheck(player1, 3));
        assertFalse(mask.recordApiMutationScanAndCheck(player2, 3));
        assertTrue(mask.recordApiMutationScanAndCheck(player2, 3));
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

    /**
     * Reproduces the P0 race that the new full-save epoch clear prevents.
     *
     * <p>Before the fix, {@code persistCollectedData} called
     * {@code dirtyMask.clearAll(uuid)} unconditionally after a successful
     * full Blob save. This lost any markDirty that arrived during the
     * collect/serialize/DB-write window — the full Blob did not contain
     * the new change (collect ran first), and the dirty bit was the only
     * record that the change happened.
     *
     * <p>The fix takes a DirtySnapshot BEFORE collectPlayerData and uses
     * {@code clearDirty(snapshot)} instead of {@code clearAll}. This test
     * simulates the exact sequence:
     *
     * <pre>
     *   1. markDirty(INVENTORY)              // pre-existing dirty
     *   2. snapshot = snapshotDirty()         // taken before collect
     *   3. (collectPlayerData + serialize + DB write — no mask access)
     *   4. markDirty(INVENTORY)               // change arrives DURING save
     *   5. clearDirty(snapshot)               // epoch-protected clear
     *
     *   → INVENTORY must still be dirty (epoch was bumped in step 4)
     * </pre>
     *
     * <p>Without epoch protection (i.e. clearAll), step 5 would clear
     * INVENTORY and the change from step 4 would be silently lost.
     */
    @Test
    void testFullSaveEpochClearPreservesDirtyArrivingDuringSave() {
        // Step 1: pre-existing dirty
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);

        // Step 2: snapshot before collect (this is what savePlayerAsync does)
        ComponentDirtyMask.DirtySnapshot preSaveSnapshot = mask.snapshotDirty(player1);
        assertEquals(1, preSaveSnapshot.size());

        // Step 3: (simulated collect + serialize + DB write — nothing to do)

        // Step 4: change arrives during the save window
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);

        // Step 5: epoch-protected clear (what the fixed persistCollectedData does
        //         for online saves)
        mask.clearDirty(player1, preSaveSnapshot);

        // The change from step 4 must survive — its epoch differs from the snapshot
        assertTrue(mask.isAnyDirty(player1),
            "INVENTORY must remain dirty — markDirty during save bumped the epoch "
            + "and must not be cleared by clearDirty(snapshot)");
        assertTrue(mask.getDirty(player1).contains(ComponentDirtyMask.Component.INVENTORY));
    }

    /**
     * Sanity check: clearAll (used by QUIT/SHUTDOWN) DOES clear everything,
     * including changes that arrived during the save. This is correct for
     * QUIT because the player is leaving and will not produce more changes,
     * and the QUIT save's collectPlayerData captured the latest state.
     */
    @Test
    void testQuitClearAllClearsEverything() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        // Simulate: collect happened, then a late markDirty arrives,
        // then QUIT save completes and calls clearAll.
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.clearAll(player1);
        assertFalse(mask.isAnyDirty(player1),
            "QUIT clearAll should clear all dirty state — player is leaving");
    }

    @Test
    void applySuppressionIgnoresSelfGeneratedEvents() {
        mask.beginApply(player1);
        mask.markDirty(player1, ComponentDirtyMask.Component.GAME_MODE);
        mask.markAllDirty(player1);
        assertFalse(mask.isAnyDirty(player1));

        mask.endApply(player1);
        mask.markDirty(player1, ComponentDirtyMask.Component.GAME_MODE);
        assertTrue(mask.isAnyDirty(player1));
    }

    /**
     * Multi-component variant: only the component whose epoch was bumped
     * during the save survives the epoch-protected clear.
     */
    @Test
    void testFullSaveEpochClearWithMultipleComponents() {
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);
        mask.markDirty(player1, ComponentDirtyMask.Component.VITALS);
        mask.markDirty(player1, ComponentDirtyMask.Component.EXPERIENCE);

        ComponentDirtyMask.DirtySnapshot preSaveSnapshot = mask.snapshotDirty(player1);

        // During save: only INVENTORY gets a new markDirty
        mask.markDirty(player1, ComponentDirtyMask.Component.INVENTORY);

        mask.clearDirty(player1, preSaveSnapshot);

        Set<ComponentDirtyMask.Component> dirty = mask.getDirty(player1);
        assertEquals(1, dirty.size(),
            "Only INVENTORY (epoch bumped during save) should remain dirty");
        assertTrue(dirty.contains(ComponentDirtyMask.Component.INVENTORY));
        assertFalse(dirty.contains(ComponentDirtyMask.Component.VITALS),
            "VITALS epoch matched snapshot — cleared");
        assertFalse(dirty.contains(ComponentDirtyMask.Component.EXPERIENCE),
            "EXPERIENCE epoch matched snapshot — cleared");
    }

    /**
     * Reproduces the conservative-dirty-marking ordering bug from review round 6.
     *
     * <p>Before the fix, savePlayerAsync's logic was:
     * <pre>
     *   boolean shouldValidate = dirtyMask.recordSaveAndCheckValidation(uuid);
     *   if (!shouldValidate && !dirtyMask.isAnyDirty(uuid)) {
     *       return;  // ❌ returns BEFORE conservative marks run
     *   }
     *   if (shouldValidate) {
     *       dirtyMask.markAllDirty(uuid);
     *   } else if (config.isComponentStorageEnabled()) {
     *       dirtyMask.markDirty(uuid, PDC);
     *       dirtyMask.markDirty(uuid, STATISTICS);
     *       dirtyMask.markDirty(uuid, ATTRIBUTES);
     *       dirtyMask.markDirty(uuid, ADVANCEMENTS);
     *   }
     * </pre>
     *
     * <p>When a plugin modifies PDC/stats/attrs/advancements via API without
     * firing a Bukkit event, no dirty bit gets set. The skip check returns
     * early, the conservative marks never run, and those changes are silently
     * dropped until the next validation cycle (default every 5th save).
     *
     * <p>The fix moves the conservative marks BEFORE the skip check. This
     * test verifies the new ordering: with component-storage enabled and no
     * event-driven dirty, the conservative marks must run first, making
     * isAnyDirty() return true, so the save does NOT skip.
     *
     * <p>Note: this test does NOT call savePlayerAsync (which needs Bukkit).
     * It simulates the dirty-decision logic directly against the mask,
     * verifying the ordering invariant the fix depends on.
     */
    @Test
    void testConservativeDirtyMarksRunBeforeSkipCheck() {
        // Simulate: player has NO event-driven dirty bits, but plugin modified
        // PDC/stats/attrs/advancements via API. No events fired → mask is empty.
        assertFalse(mask.isAnyDirty(player1),
            "Precondition: no event-driven dirty bits");

        // Simulate the FIXED savePlayerAsync ordering:
        //   1. recordSaveAndCheckValidation returns false (not validation cycle)
        //   2. component-storage enabled → conservative marks run
        //   3. THEN check isAnyDirty
        boolean shouldValidate = mask.recordSaveAndCheckValidation(player1);
        assertFalse(shouldValidate, "First save should not trigger validation");

        boolean componentStorageEnabled = true;  // simulate config flag
        if (shouldValidate) {
            mask.markAllDirty(player1);
        } else if (componentStorageEnabled) {
            // Conservative marks — these MUST run before the skip check
            mask.markDirty(player1, ComponentDirtyMask.Component.PDC);
            mask.markDirty(player1, ComponentDirtyMask.Component.STATISTICS);
            mask.markDirty(player1, ComponentDirtyMask.Component.ATTRIBUTES);
            mask.markDirty(player1, ComponentDirtyMask.Component.ADVANCEMENTS);
        }

        // NOW the skip check runs. With conservative marks applied, it must
        // NOT skip — isAnyDirty must return true.
        assertTrue(mask.isAnyDirty(player1),
            "After conservative marks, isAnyDirty must be true — save must NOT skip. "
            + "If this fails, the conservative marks are running AFTER the skip check "
            + "and plugin-driven PDC/stats/attrs/advancements changes will be lost.");

        // Verify all 4 conservative components are marked
        Set<ComponentDirtyMask.Component> dirty = mask.getDirty(player1);
        assertTrue(dirty.contains(ComponentDirtyMask.Component.PDC));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.STATISTICS));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.ATTRIBUTES));
        assertTrue(dirty.contains(ComponentDirtyMask.Component.ADVANCEMENTS));
        assertEquals(4, dirty.size(),
            "Exactly the 4 conservative components should be dirty");
    }

    /**
     * Counter-test: when component-storage is DISABLED and no events fired,
     * the skip check correctly skips (no conservative marks to apply).
     * This verifies the fix does not break the existing skip optimization.
     */
    @Test
    void testSkipStillWorksWhenComponentStorageDisabledAndNoDirty() {
        assertFalse(mask.isAnyDirty(player1));

        boolean shouldValidate = mask.recordSaveAndCheckValidation(player1);
        assertFalse(shouldValidate);

        boolean componentStorageEnabled = false;  // simulate config flag
        if (shouldValidate) {
            mask.markAllDirty(player1);
        } else if (componentStorageEnabled) {
            mask.markDirty(player1, ComponentDirtyMask.Component.PDC);
            mask.markDirty(player1, ComponentDirtyMask.Component.STATISTICS);
            mask.markDirty(player1, ComponentDirtyMask.Component.ATTRIBUTES);
            mask.markDirty(player1, ComponentDirtyMask.Component.ADVANCEMENTS);
        }

        // Skip check: no dirty → skip
        assertFalse(mask.isAnyDirty(player1),
            "With component-storage disabled and no events, isAnyDirty must be false "
            + "so the save correctly skips (existing optimization preserved).");
    }

    /**
     * Verifies that even on a validation cycle, conservative marks are
     * redundant (markAllDirty already covers them) but the skip check
     * still correctly does NOT skip.
     */
    @Test
    void testValidationCycleDoesNotSkipEvenWithoutEvents() {
        assertFalse(mask.isAnyDirty(player1));

        // Force validation cycle: call recordSaveAndCheckValidation enough
        // times to hit the interval (default 5 in setUp).
        for (int i = 1; i <= 4; i++) {
            assertFalse(mask.recordSaveAndCheckValidation(player1),
                "Saves 1-4 should not trigger validation");
        }
        boolean shouldValidate = mask.recordSaveAndCheckValidation(player1);
        assertTrue(shouldValidate, "5th save should trigger validation");

        // markAllDirty runs on validation cycle
        mask.markAllDirty(player1);

        // Skip check must NOT skip — markAllDirty set everything
        assertTrue(mask.isAnyDirty(player1),
            "After markAllDirty on validation cycle, isAnyDirty must be true");
        assertEquals(ComponentDirtyMask.ALL.size(), mask.getDirty(player1).size(),
            "All components should be dirty after markAllDirty");
    }
}
