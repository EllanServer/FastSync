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
}
