package com.fastsync.i18n;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Internationalization manager for FastSync.
 *
 * <p>Loads messages from YAML resource files ({@code messages_en.yml},
 * {@code messages_zh_CN.yml}, etc.) bundled in the jar. Server operators
 * can select a language via {@code settings.language} in {@code config.yml}
 * and can override individual messages by placing a custom
 * {@code messages_<lang>.yml} in the plugin's {@code lang/} data folder.
 *
 * <h2>Message key format</h2>
 * Keys use dot notation, e.g. {@code command.reload.success}.
 * Placeholders use {@code {0}}, {@code {1}} style (0-indexed).
 *
 * <h2>Formatting</h2>
 * Messages use <a href="https://docs.advntr.dev/minimessage/format.html">
 * MiniMessage</a> tags for formatting, e.g. {@code <red>},
 * {@code <green>}, {@code <bold>}, {@code <click:run_command:/fsync status>}.
 * This is the modern Adventure API standard, replacing legacy {@code &}
 * color codes.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Player-facing (returns Component)
 * sender.sendMessage(msg.component("command.reload.success"));
 *
 * // With placeholders
 * sender.sendMessage(msg.component("command.saveall.result",
 *     result.success(), result.total(), result.failed()));
 *
 * // Console log (returns String, no formatting)
 * logger.info(msg.console("console.startup.enabled"));
 * }</pre>
 */
public class MessageManager {

    private static final MiniMessage MINI_MESSAGE = MiniMessage.miniMessage();
    private static final PlainTextComponentSerializer PLAIN_SERIALIZER =
        PlainTextComponentSerializer.plainText();

    private final Map<String, String> messages = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> localeOverrides = new ConcurrentHashMap<>();
    private final Logger logger;
    private final String language;
    private final JavaPlugin plugin;

    /**
     * Create and initialize the message manager.
     *
     * @param plugin   the plugin instance (for resource loading)
     * @param logger   the plugin logger
     * @param language the language code (e.g. "en", "zh_CN")
     */
    public MessageManager(JavaPlugin plugin, Logger logger, String language) {
        this.plugin = plugin;
        this.logger = logger;
        this.language = (language == null || language.isBlank()) ? "en" : language;

        // 1. Load default English messages (always loaded as fallback)
        loadFromResource(plugin, "messages_en.yml");

        // 2. Load selected language if not English (overrides English keys)
        if (!this.language.equals("en")) {
            String resourceFile = "messages_" + this.language + ".yml";
            if (plugin.getResource(resourceFile) != null) {
                loadFromResource(plugin, resourceFile);
            } else {
                logger.warning("[i18n] Language file not found: " + resourceFile
                    + ". Falling back to English (en).");
            }
        }

        // 3. Load user overrides from plugin data folder (highest priority)
        loadUserOverrides(plugin);

        logger.info("[i18n] Loaded " + messages.size() + " messages for language: " + this.language);
    }

    /**
     * Get a raw message string with placeholders filled.
     *
     * @param key  the message key (dot notation, e.g. "command.reload.success")
     * @param args placeholder values for {0}, {1}, ...
     * @return the formatted string, or the key itself if not found
     */
    public String raw(String key, Object... args) {
        String template = messages.get(key);
        if (template == null) {
            return args.length == 0 ? key : format(key, args);
        }
        return args.length == 0 ? template : format(template, args);
    }

    /**
     * Get a message as an Adventure Component (for player-facing messages).
     * Parses MiniMessage tags (e.g. {@code <red>}, {@code <bold>}).
     *
     * @param key  the message key
     * @param args placeholder values
     * @return the deserialized Component
     */
    public Component component(String key, Object... args) {
        return MINI_MESSAGE.deserialize(raw(key, args));
    }

    /**
     * Get a message as an Adventure Component, using the recipient's locale
     * if available. Falls back to the configured global language.
     *
     * <p>For {@link Player} recipients, this method attempts to use the player's
     * client locale (e.g. "zh_CN", "en_US") by loading the corresponding
     * {@code messages_<locale>.yml} resource. If no matching resource exists,
     * it falls back to the global language.
     *
     * @param recipient the message recipient (Player or CommandSender)
     * @param key       the message key
     * @param args      placeholder values
     * @return the deserialized Component in the recipient's locale
     */
    public Component component(CommandSender recipient, String key, Object... args) {
        if (recipient instanceof Player player) {
            Map<String, String> localeMessages = getLocaleMessages(player);
            String template = localeMessages.get(key);
            if (template == null) {
                template = messages.get(key);
            }
            if (template == null) {
                template = key;
            }
            String text = args.length == 0 ? template : format(template, args);
            return MINI_MESSAGE.deserialize(text);
        }
        // Non-player senders (console, command block) use the global language
        return component(key, args);
    }

    /**
     * Get a console-formatted message string (plain text, no formatting).
     * Strips all MiniMessage tags for clean console output.
     *
     * @param key  the message key
     * @param args placeholder values
     * @return the formatted plain text string
     */
    public String console(String key, Object... args) {
        String text = raw(key, args);
        // Parse and serialize to plain text to strip all MiniMessage tags
        Component component = MINI_MESSAGE.deserialize(text);
        return PLAIN_SERIALIZER.serialize(component);
    }

    /**
     * Get the current language code.
     *
     * @return the language code (e.g. "en", "zh_CN")
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Reload messages from resources (for {@code /fastsync reload}).
     *
     * @param plugin the plugin instance
     */
    public void reload(JavaPlugin plugin) {
        messages.clear();
        loadFromResource(plugin, "messages_en.yml");
        if (!language.equals("en")) {
            String resourceFile = "messages_" + language + ".yml";
            if (plugin.getResource(resourceFile) != null) {
                loadFromResource(plugin, resourceFile);
            }
        }
        loadUserOverrides(plugin);
        logger.info("[i18n] Reloaded " + messages.size() + " messages for language: " + language);
    }

    /**
     * Check if a message key exists.
     *
     * @param key the message key
     * @return true if the key is defined in the current language
     */
    public boolean has(String key) {
        return messages.containsKey(key);
    }

    // ==================== Internal ====================

    /**
     * Get the message map for a player's locale, loading it lazily.
     * Falls back to the global messages map if no locale-specific resource exists.
     */
    private Map<String, String> getLocaleMessages(Player player) {
        String locale;
        try {
            // Paper API: player.locale() returns a Locale object
            locale = player.locale().toString();
        } catch (Throwable t) {
            // Pre-1.12 or non-Paper: fall back to global language
            return messages;
        }
        if (locale == null || locale.isBlank() || locale.equals(language)) {
            return messages;
        }

        // Check if we already loaded this locale
        return localeOverrides.computeIfAbsent(locale, loc -> {
            String resourceFile = "messages_" + loc + ".yml";
            if (plugin.getResource(resourceFile) == null) {
                // Try short form (e.g. "zh" instead of "zh_CN")
                String shortLocale = loc.contains("_") ? loc.substring(0, loc.indexOf('_')) : loc;
                resourceFile = "messages_" + shortLocale + ".yml";
                if (plugin.getResource(resourceFile) == null) {
                    // No locale-specific resource — use global messages
                    return messages;
                }
            }

            // Load locale-specific messages on top of English fallback
            Map<String, String> localeMessages = new ConcurrentHashMap<>();
            // Start with English as fallback
            flattenYamlTo("", loadYamlResource("messages_en.yml"), localeMessages);
            // Override with locale-specific
            flattenYamlTo("", loadYamlResource(resourceFile), localeMessages);
            return localeMessages;
        });
    }

    private YamlConfiguration loadYamlResource(String resourceFile) {
        try (InputStream in = plugin.getResource(resourceFile);
             InputStreamReader reader = in != null
             ? new InputStreamReader(in, StandardCharsets.UTF_8) : null) {
            if (reader == null) {
                return new YamlConfiguration();
            }
            return YamlConfiguration.loadConfiguration(reader);
        } catch (Exception e) {
            logger.log(Level.WARNING, "[i18n] Failed to load resource: " + resourceFile, e);
            return new YamlConfiguration();
        }
    }

    @SuppressWarnings("unchecked")
    private void flattenYamlTo(String prefix, YamlConfiguration yaml, Map<String, String> target) {
        for (String key : yaml.getKeys(false)) {
            String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
            Object value = yaml.get(key);
            if (value instanceof org.bukkit.configuration.ConfigurationSection section) {
                for (String subKey : section.getKeys(false)) {
                    String subFullKey = fullKey + "." + subKey;
                    Object subValue = section.get(subKey);
                    if (subValue instanceof org.bukkit.configuration.ConfigurationSection) {
                        YamlConfiguration subYaml = new YamlConfiguration();
                        subYaml.set("", subValue);
                        flattenYamlTo(fullKey, subYaml, target);
                    } else {
                        target.put(subFullKey, String.valueOf(subValue));
                    }
                }
            } else {
                target.put(fullKey, String.valueOf(value));
            }
        }
    }

    private void loadFromResource(JavaPlugin plugin, String resourceFile) {
        try (InputStream in = plugin.getResource(resourceFile);
             InputStreamReader reader = in != null
             ? new InputStreamReader(in, StandardCharsets.UTF_8) : null) {
            if (reader == null) {
                return;
            }
            YamlConfiguration yaml = YamlConfiguration.loadConfiguration(reader);
            flattenYaml("", yaml, messages);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[i18n] Failed to load resource: " + resourceFile, e);
        }
    }

    private void loadUserOverrides(JavaPlugin plugin) {
        File langDir = new File(plugin.getDataFolder(), "lang");
        if (!langDir.isDirectory()) {
            return;
        }
        File overrideFile = new File(langDir, "messages_" + language + ".yml");
        if (!overrideFile.isFile()) {
            return;
        }
        try {
            YamlConfiguration yaml = YamlConfiguration.loadConfiguration(overrideFile);
            int before = messages.size();
            flattenYaml("", yaml, messages);
            int added = messages.size() - before;
            if (added > 0) {
                logger.info("[i18n] Applied " + added + " user overrides from "
                    + overrideFile.getName());
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "[i18n] Failed to load user override file: "
                + overrideFile, e);
        }
    }

    /**
     * Recursively flatten a YAML config into dot-notation keys.
     */
    @SuppressWarnings("unchecked")
    private void flattenYaml(String prefix, YamlConfiguration yaml, Map<String, String> target) {
        for (String key : yaml.getKeys(false)) {
            String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
            Object value = yaml.get(key);
            if (value instanceof org.bukkit.configuration.ConfigurationSection section) {
                // Recurse into nested section
                for (String subKey : section.getKeys(false)) {
                    String subFullKey = fullKey + "." + subKey;
                    Object subValue = section.get(subKey);
                    if (subValue instanceof org.bukkit.configuration.ConfigurationSection) {
                        // Deeper nesting — use a sub-config
                        YamlConfiguration subYaml = new YamlConfiguration();
                        subYaml.set("", subValue);
                        flattenYaml(fullKey, subYaml, target);
                    } else {
                        target.put(subFullKey, String.valueOf(subValue));
                    }
                }
            } else {
                target.put(fullKey, String.valueOf(value));
            }
        }
    }

    /**
     * Replace {0}, {1}, ... placeholders with argument values.
     */
    private String format(String template, Object... args) {
        if (args == null || args.length == 0) {
            return template;
        }
        String result = template;
        for (int i = 0; i < args.length; i++) {
            String placeholder = "{" + i + "}";
            String replacement = args[i] == null ? "null" : String.valueOf(args[i]);
            result = result.replace("{" + i + "}", replacement);
        }
        return result;
    }
}
