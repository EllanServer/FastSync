package com.fastsync.i18n;

import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MessageManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void recursivelyFlattensNestedMessages() {
        JavaPlugin plugin = pluginWithResources(
            "command:\n  status:\n    header: '<gold>FastSync Status</gold>'\n",
            "player:\n  kick:\n    busy: '<red>繁忙</red>'\n");

        MessageManager manager = new MessageManager(plugin, Logger.getAnonymousLogger(), "en");

        assertEquals("<gold>FastSync Status</gold>", manager.raw("command.status.header"));
    }

    @Test
    void reloadInvalidatesAlreadyLoadedPlayerLocale() {
        AtomicReference<String> chinese = new AtomicReference<>(
            "player:\n  kick:\n    busy: '<red>旧消息</red>'\n");
        JavaPlugin plugin = mock(JavaPlugin.class);
        when(plugin.getDataFolder()).thenReturn(tempDir.toFile());
        when(plugin.getResource("messages_en.yml")).thenAnswer(ignored -> stream(
            "player:\n  kick:\n    busy: '<red>busy</red>'\n"));
        when(plugin.getResource("messages_zh_CN.yml"))
            .thenAnswer(ignored -> stream(chinese.get()));

        MessageManager manager = new MessageManager(plugin, Logger.getAnonymousLogger(), "en");
        Player player = mock(Player.class);
        when(player.locale()).thenReturn(Locale.SIMPLIFIED_CHINESE);
        assertEquals("旧消息", PlainTextComponentSerializer.plainText()
            .serialize(manager.component(player, "player.kick.busy")));

        chinese.set("player:\n  kick:\n    busy: '<red>新消息</red>'\n");
        manager.reload(plugin);

        assertEquals("新消息", PlainTextComponentSerializer.plainText()
            .serialize(manager.component(player, "player.kick.busy")));
    }

    private JavaPlugin pluginWithResources(String english, String chinese) {
        JavaPlugin plugin = mock(JavaPlugin.class);
        when(plugin.getDataFolder()).thenReturn(tempDir.toFile());
        when(plugin.getResource("messages_en.yml")).thenAnswer(ignored -> stream(english));
        when(plugin.getResource("messages_zh_CN.yml")).thenAnswer(ignored -> stream(chinese));
        return plugin;
    }

    private static ByteArrayInputStream stream(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }
}
