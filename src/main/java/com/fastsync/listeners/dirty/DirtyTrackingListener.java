package com.fastsync.listeners.dirty;

import com.fastsync.sync.dirty.ComponentDirtyMask;
import com.fastsync.sync.dirty.ComponentDirtyMask.Component;
import org.bukkit.entity.HumanEntity;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.EntityDamageEvent;
import org.bukkit.event.entity.EntityPickupItemEvent;
import org.bukkit.event.entity.EntityPotionEffectEvent;
import org.bukkit.event.entity.EntityRegainHealthEvent;
import org.bukkit.event.entity.FoodLevelChangeEvent;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryCloseEvent;
import org.bukkit.event.inventory.InventoryDragEvent;
import org.bukkit.event.player.PlayerDropItemEvent;
import org.bukkit.event.player.PlayerExpChangeEvent;
import org.bukkit.event.player.PlayerGameModeChangeEvent;
import org.bukkit.event.player.PlayerItemBreakEvent;
import org.bukkit.event.player.PlayerItemConsumeEvent;
import org.bukkit.event.player.PlayerLevelChangeEvent;
import org.bukkit.event.player.PlayerRespawnEvent;
import org.bukkit.event.player.PlayerSwapHandItemsEvent;
import org.bukkit.event.player.PlayerTeleportEvent;
import org.bukkit.event.player.PlayerToggleFlightEvent;
import org.bukkit.event.inventory.InventoryType;
import org.bukkit.inventory.InventoryHolder;

import java.util.UUID;

/**
 * Event listener that marks player data components dirty based on Bukkit
 * events. This is the "fast path" of dirty tracking — when an event fires,
 * we know exactly which component changed, and the next periodic save only
 * needs to serialize + write that component.
 *
 * <h2>Coverage</h2>
 * <p>This listener covers the most common Bukkit events. It does NOT cover:
 * <ul>
 *   <li>Direct inventory manipulation via the Bukkit API (no event fires)</li>
 *   <li>PDC changes (PDC has no events — always assumed dirty)</li>
 *   <li>Advancement/Statistics changes (handled separately or always-dirty)</li>
 *   <li>Attribute changes (rare, always-dirty on save)</li>
 * </ul>
 *
 * <p>The {@link ComponentDirtyMask#recordSaveAndCheckValidation(UUID)} method
 * periodically forces a full-collect + checksum comparison to catch any
 * changes that slipped through events. This is the safety net.
 *
 * <h2>Priority</h2>
 * <p>All handlers use {@link EventPriority#MONITOR} — we only observe, never
 * interfere with other plugins' event handling. We also do not cancel
 * anything; we just record that a component changed.
 */
public class DirtyTrackingListener implements Listener {

    private final ComponentDirtyMask dirtyMask;

    public DirtyTrackingListener(ComponentDirtyMask dirtyMask) {
        this.dirtyMask = dirtyMask;
    }

    // ==================== Inventory ====================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onInventoryClick(InventoryClickEvent event) {
        if (event.isCancelled()) return;
        if (event.getWhoClicked() instanceof Player p) {
            markDirty(p, Component.INVENTORY);
            // Ender chest detection: if the clicked inventory is an ender chest,
            // mark ENDER_CHEST dirty separately. The inventory holder check
            // distinguishes ender chest from regular inventory/crafting.
            if (event.getView().getTopInventory().getType() == InventoryType.ENDER_CHEST) {
                markDirty(p, Component.ENDER_CHEST);
            }
        }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onInventoryDrag(InventoryDragEvent event) {
        if (event.isCancelled()) return;
        if (event.getWhoClicked() instanceof Player p) {
            markDirty(p, Component.INVENTORY);
            // Check if any of the dragged slots are in an ender chest inventory
            if (event.getView().getTopInventory().getType() == InventoryType.ENDER_CHEST) {
                markDirty(p, Component.ENDER_CHEST);
            }
        }
    }

    /**
     * When a player closes an inventory, check if it was an ender chest.
     * This catches ender chest modifications that don't fire click/drag
     * events (e.g. shift-click moves that complete on close).
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onInventoryClose(InventoryCloseEvent event) {
        if (event.getPlayer() instanceof Player p) {
            if (event.getInventory().getType() == InventoryType.ENDER_CHEST) {
                markDirty(p, Component.ENDER_CHEST);
            }
        }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onItemDrop(PlayerDropItemEvent event) {
        if (event.isCancelled()) return;
        markDirty(event.getPlayer(), Component.INVENTORY);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onItemPickup(EntityPickupItemEvent event) {
        if (event.isCancelled()) return;
        if (event.getEntity() instanceof Player player) {
            markDirty(player, Component.INVENTORY);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onItemBreak(PlayerItemBreakEvent event) {
        markDirty(event.getPlayer(), Component.INVENTORY);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onItemConsume(PlayerItemConsumeEvent event) {
        if (event.isCancelled()) return;
        markDirty(event.getPlayer(), Component.INVENTORY);
        markDirty(event.getPlayer(), Component.FOOD);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onSwapHandItems(PlayerSwapHandItemsEvent event) {
        if (event.isCancelled()) return;
        markDirty(event.getPlayer(), Component.INVENTORY);
    }

    // ==================== Vitals (health) ====================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onEntityDamage(EntityDamageEvent event) {
        if (event.isCancelled()) return;
        if (event.getEntity() instanceof Player p) {
            markDirty(p, Component.VITALS);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onEntityRegainHealth(EntityRegainHealthEvent event) {
        if (event.isCancelled()) return;
        if (event.getEntity() instanceof Player player) {
            markDirty(player, Component.VITALS);
        }
    }

    // ==================== Food ====================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onFoodLevelChange(FoodLevelChangeEvent event) {
        if (event.isCancelled()) return;
        if (event.getEntity() instanceof Player p) {
            markDirty(p, Component.FOOD);
        }
    }

    // ==================== Experience ====================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onExpChange(PlayerExpChangeEvent event) {
        markDirty(event.getPlayer(), Component.EXPERIENCE);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onLevelChange(PlayerLevelChangeEvent event) {
        markDirty(event.getPlayer(), Component.EXPERIENCE);
    }

    // ==================== Potion effects ====================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPotionEffectChange(EntityPotionEffectEvent event) {
        if (event.isCancelled()) return;
        if (event.getEntity() instanceof Player p) {
            markDirty(p, Component.POTION_EFFECTS);
        }
    }

    // ==================== Game mode ====================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onGameModeChange(PlayerGameModeChangeEvent event) {
        if (event.isCancelled()) return;
        markDirty(event.getPlayer(), Component.GAME_MODE);
    }

    // ==================== Location ====================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onTeleport(PlayerTeleportEvent event) {
        if (event.isCancelled()) return;
        markDirty(event.getPlayer(), Component.LOCATION);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onToggleFlight(PlayerToggleFlightEvent event) {
        if (event.isCancelled()) return;
        markDirty(event.getPlayer(), Component.FLIGHT);
    }

    // ==================== Death / Respawn ====================

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onDeath(PlayerDeathEvent event) {
        // ServerPlayer.reset changes all of these on respawn.
        markDirty(event.getEntity(), Component.INVENTORY);
        markDirty(event.getEntity(), Component.VITALS);
        markDirty(event.getEntity(), Component.FOOD);
        markDirty(event.getEntity(), Component.EXPERIENCE);
        markDirty(event.getEntity(), Component.POTION_EFFECTS);
        markDirty(event.getEntity(), Component.FIRE_TICKS);
        markDirty(event.getEntity(), Component.AIR);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onRespawn(PlayerRespawnEvent event) {
        // Respawn resets vitals, food, fire, potion effects
        markDirty(event.getPlayer(), Component.VITALS);
        markDirty(event.getPlayer(), Component.FOOD);
        markDirty(event.getPlayer(), Component.FIRE_TICKS);
        markDirty(event.getPlayer(), Component.AIR);
        markDirty(event.getPlayer(), Component.POTION_EFFECTS);
        markDirty(event.getPlayer(), Component.LOCATION);
    }

    // ==================== Helper ====================

    private void markDirty(Player player, Component component) {
        dirtyMask.markDirty(player.getUniqueId(), component);
    }
}
