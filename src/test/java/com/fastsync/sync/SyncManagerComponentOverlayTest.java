package com.fastsync.sync;

import com.fastsync.database.DatabaseManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SyncManagerComponentOverlayTest {

    @Test
    void persistedBitmapUsesStableIdsAndRejectsUnknownBits() throws IOException {
        long bitmap = com.fastsync.sync.dirty.ComponentDirtyMask.Component.INVENTORY.storageMask()
            | com.fastsync.sync.dirty.ComponentDirtyMask.Component.PDC.storageMask();
        assertEquals(Set.of("INVENTORY", "PDC"), SyncManager.componentNamesForBitmap(bitmap));

        IOException error = assertThrows(IOException.class,
            () -> SyncManager.componentNamesForBitmap(1L << 40));
        assertTrue(error.getMessage().contains("unknown storage bits"));
    }

    @Test
    void verifyComponentOverlayCompletenessAcceptsFullyLoadedBitmap() {
        UUID uuid = UUID.randomUUID();
        Set<String> expected = Set.of("INVENTORY", "ENDER_CHEST");
        Map<String, DatabaseManager.ComponentData> loaded = Map.of(
            "INVENTORY", new DatabaseManager.ComponentData(new byte[]{1}, 1L, 11L),
            "ENDER_CHEST", new DatabaseManager.ComponentData(new byte[]{2}, 1L, 22L)
        );

        assertDoesNotThrow(() ->
                SyncManager.verifyComponentOverlayCompleteness(uuid, expected, loaded, 7L),
            "Bitmap and loaded component rows match; load should continue");
    }

    @Test
    void verifyComponentOverlayCompletenessRejectsMissingRows() {
        UUID uuid = UUID.randomUUID();
        Set<String> expected = Set.of("INVENTORY", "ENDER_CHEST");
        Map<String, DatabaseManager.ComponentData> loaded = Map.of(
            "INVENTORY", new DatabaseManager.ComponentData(new byte[]{1}, 1L, 11L)
        );

        IOException ex = assertThrows(IOException.class, () ->
                SyncManager.verifyComponentOverlayCompleteness(uuid, expected, loaded, 9L),
            "Missing component rows referenced by component_bitmap must fail closed");

        assertTrue(ex.getMessage().contains("ENDER_CHEST"),
            "Error must identify which component row is missing");
        assertTrue(ex.getMessage().contains("gen=9"),
            "Error must include the component generation for diagnosis");
    }
}
