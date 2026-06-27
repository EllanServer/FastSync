package com.fastsync.sync.strategy;

import org.bukkit.NamespacedKey;
import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataContainer;
import org.bukkit.persistence.PersistentDataType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the entry-count guard added to
 * {@link RegisteredKeysPdcStrategy#restore(Player, byte[])}.
 *
 * <p>A corrupted payload could declare a huge count and make the read loop
 * spin until EOF. The strategy now rejects counts outside
 * [0, MAX_PDC_ENTRIES] BEFORE clearing the target container, so a bad sync
 * cannot wipe good local PDC state.
 */
class RegisteredKeysPdcStrategyCountTest {

    @Test
    void restoreRejectsOversizedEntryCountWithoutTouchingPdc() throws Exception {
        RegisteredKeysPdcStrategy strategy = new RegisteredKeysPdcStrategy(
            List.of(new RegisteredKeysPdcStrategy.KeyBinding(
                NamespacedKey.fromString("test:foo"), PersistentDataType.STRING)),
            Logger.getLogger("test"), true);

        // Build a payload whose count field is 1_000_000 (far above the 256 cap).
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(1_000_000);
        }
        byte[] payload = baos.toByteArray();

        Player player = Mockito.mock(Player.class);
        // The count is validated BEFORE any clear, so getPersistentDataContainer()
        // must never be called — i.e. local PDC state is left untouched.
        strategy.restore(player, payload);

        Mockito.verifyNoInteractions(player);
    }

    @Test
    void restoreRejectsNegativeEntryCountWithoutTouchingPdc() throws Exception {
        RegisteredKeysPdcStrategy strategy = new RegisteredKeysPdcStrategy(
            List.of(new RegisteredKeysPdcStrategy.KeyBinding(
                NamespacedKey.fromString("test:foo"), PersistentDataType.STRING)),
            Logger.getLogger("test"), true);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(-1);
        }
        byte[] payload = baos.toByteArray();

        Player player = Mockito.mock(Player.class);
        strategy.restore(player, payload);

        Mockito.verifyNoInteractions(player);
    }

    @Test
    void restoreAllowsZeroEntryCountAndClearsRegisteredKeys() throws Exception {
        // count == 0 is the legitimate "no entries" payload — it must still
        // clear all registered keys on the target (ghost-key prevention).
        RegisteredKeysPdcStrategy strategy = new RegisteredKeysPdcStrategy(
            List.of(new RegisteredKeysPdcStrategy.KeyBinding(
                NamespacedKey.fromString("test:foo"), PersistentDataType.STRING)),
            Logger.getLogger("test"), true);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(0);
        }
        byte[] payload = baos.toByteArray();

        Player player = Mockito.mock(Player.class);
        PersistentDataContainer pdc = Mockito.mock(PersistentDataContainer.class);
        Mockito.when(player.getPersistentDataContainer()).thenReturn(pdc);

        strategy.restore(player, payload);

        // The registered key should have been removed (clear-before-restore).
        Mockito.verify(pdc).remove(NamespacedKey.fromString("test:foo"));
    }
}
