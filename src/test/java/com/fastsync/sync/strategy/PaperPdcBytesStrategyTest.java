package com.fastsync.sync.strategy;

import org.bukkit.NamespacedKey;
import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataAdapterContext;
import org.bukkit.persistence.PersistentDataContainer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class PaperPdcBytesStrategyTest {

    @Test
    void malformedBytesDoNotClearLiveContainer() throws Exception {
        Player player = mock(Player.class);
        PersistentDataContainer live = mock(PersistentDataContainer.class);
        PersistentDataContainer decoded = mock(PersistentDataContainer.class);
        PersistentDataAdapterContext context = mock(PersistentDataAdapterContext.class);
        when(player.getPersistentDataContainer()).thenReturn(live);
        when(live.getAdapterContext()).thenReturn(context);
        when(context.newPersistentDataContainer()).thenReturn(decoded);
        doThrow(new IOException("bad nbt")).when(decoded).readFromBytes(any(byte[].class), eq(true));

        PaperPdcBytesStrategy strategy = new PaperPdcBytesStrategy(
            Logger.getLogger("pdc-test"), true, true);
        assertThrows(IllegalStateException.class,
            () -> strategy.restore(player, new byte[]{1, 2, 3}));

        verify(live, never()).remove(any(NamespacedKey.class));
        verify(decoded, never()).copyTo(eq(live), anyBoolean());
    }

    @Test
    void validBytesDecodeBeforeReplacingLiveContainer() throws Exception {
        Player player = mock(Player.class);
        PersistentDataContainer live = mock(PersistentDataContainer.class);
        PersistentDataContainer decoded = mock(PersistentDataContainer.class);
        PersistentDataAdapterContext context = mock(PersistentDataAdapterContext.class);
        NamespacedKey oldKey = NamespacedKey.fromString("test:old");
        when(player.getPersistentDataContainer()).thenReturn(live);
        when(live.getAdapterContext()).thenReturn(context);
        when(context.newPersistentDataContainer()).thenReturn(decoded);
        when(live.getKeys()).thenReturn(Set.of(oldKey));

        PaperPdcBytesStrategy strategy = new PaperPdcBytesStrategy(
            Logger.getLogger("pdc-test"), true, true);
        strategy.restore(player, new byte[]{10});

        var order = inOrder(decoded, live);
        order.verify(decoded).readFromBytes(any(byte[].class), eq(true));
        order.verify(live).remove(oldKey);
        order.verify(decoded).copyTo(live, true);
    }
}
