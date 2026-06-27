package com.fastsync.serialization;

/**
 * Thrown when an {@link org.bukkit.inventory.ItemStack} cannot be serialized
 * to (or deserialized from) bytes via Paper's native
 * {@code ItemStack.serializeAsBytes()} / {@code ItemStack.deserializeBytes()}.
 *
 * <p>This used to be swallowed and turned into an empty array / {@code null}
 * item, which silently corrupted player inventories (a slot that failed to
 * serialize was saved as an air slot). Throwing instead lets the caller fail
 * the whole save or record a conflict snapshot, so no data is silently lost.
 */
public class ItemSerializationException extends RuntimeException {
    public ItemSerializationException(String message) {
        super(message);
    }

    public ItemSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
