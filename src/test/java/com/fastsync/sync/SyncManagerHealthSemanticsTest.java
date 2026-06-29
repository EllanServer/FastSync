package com.fastsync.sync;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SyncManagerHealthSemanticsTest {

    @Test
    void deadPlayerPersistsEffectiveRespawnHealth() {
        // ServerPlayer.reset() uses LivingEntity#getMaxHealth(), i.e. the
        // effective attribute value after modifiers, not the base value.
        assertEquals(30.0, SyncManager.healthForSave(true, 0.0, 30.0));
    }

    @Test
    void livingPlayerKeepsCurrentHealth() {
        assertEquals(17.5, SyncManager.healthForSave(false, 17.5, 30.0));
    }
}
