package com.fastsync.velocity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * Defines the plugin messaging protocol between the Velocity proxy
 * and backend Paper/Folia FastSync plugins.
 *
 * <p>All messages use the {@code fastsync:handoff} channel.
 *
 * <p>Message types:
 * <ul>
 *   <li>1 = QUERY_LOCK: proxy → old server, "do you still hold this player's lock?"</li>
 *   <li>2 = LOCK_STATUS: old server → proxy, "yes/no, here's my status"</li>
 *   <li>3 = HANDOFF_NOTIFY: proxy → new server, "player just arrived from oldServer"</li>
 *   <li>4 = STATUS_QUERY: proxy → any server, "send me your FastSync status"</li>
 *   <li>5 = STATUS_RESPONSE: server → proxy, aggregated status data</li>
 * </ul>
 */
public final class HandoffProtocol {

    public static final int QUERY_LOCK = 1;
    public static final int LOCK_STATUS = 2;
    public static final int HANDOFF_NOTIFY = 3;
    public static final int STATUS_QUERY = 4;
    public static final int STATUS_RESPONSE = 5;

    private HandoffProtocol() {}

    // ==================== QUERY_LOCK ====================
    // [type=1][uuid][newServer]

    public static byte[] encodeQueryLock(UUID uuid, String newServer) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeByte(QUERY_LOCK);
            out.writeUTF(uuid.toString());
            out.writeUTF(newServer != null ? newServer : "");
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static QueryLockData decodeQueryLock(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            in.readByte(); // type
            UUID uuid = UUID.fromString(in.readUTF());
            String newServer = in.readUTF();
            return new QueryLockData(uuid, newServer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public record QueryLockData(UUID uuid, String newServer) {}

    // ==================== LOCK_STATUS ====================
    // [type=2][uuid][locked:bool][serverName]

    public static byte[] encodeLockStatus(UUID uuid, boolean locked, String serverName) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeByte(LOCK_STATUS);
            out.writeUTF(uuid.toString());
            out.writeBoolean(locked);
            out.writeUTF(serverName != null ? serverName : "");
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static LockStatusData decodeLockStatus(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            in.readByte(); // type
            UUID uuid = UUID.fromString(in.readUTF());
            boolean locked = in.readBoolean();
            String serverName = in.readUTF();
            return new LockStatusData(uuid, locked, serverName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public record LockStatusData(UUID uuid, boolean locked, String serverName) {}

    // ==================== HANDOFF_NOTIFY ====================
    // [type=3][uuid][oldServer][newServer]

    public static byte[] encodeHandoffNotify(UUID uuid, String oldServer, String newServer) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeByte(HANDOFF_NOTIFY);
            out.writeUTF(uuid.toString());
            out.writeUTF(oldServer != null ? oldServer : "");
            out.writeUTF(newServer != null ? newServer : "");
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static HandoffNotifyData decodeHandoffNotify(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            in.readByte(); // type
            UUID uuid = UUID.fromString(in.readUTF());
            String oldServer = in.readUTF();
            String newServer = in.readUTF();
            return new HandoffNotifyData(uuid, oldServer, newServer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public record HandoffNotifyData(UUID uuid, String oldServer, String newServer) {}

    // ==================== STATUS_QUERY ====================
    // [type=4]

    public static byte[] encodeStatusQuery() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeByte(STATUS_QUERY);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== STATUS_RESPONSE ====================
    // [type=5][serverName][dbHealth][redisHealth][playerCount][pendingSaves][pendingLoads]

    public static byte[] encodeStatusResponse(
            String serverName, boolean dbHealthy, boolean redisHealthy,
            int playerCount, int pendingSaves, int pendingLoads) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeByte(STATUS_RESPONSE);
            out.writeUTF(serverName);
            out.writeBoolean(dbHealthy);
            out.writeBoolean(redisHealthy);
            out.writeInt(playerCount);
            out.writeInt(pendingSaves);
            out.writeInt(pendingLoads);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static StatusResponseData decodeStatusResponse(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            in.readByte(); // type
            String serverName = in.readUTF();
            boolean dbHealthy = in.readBoolean();
            boolean redisHealthy = in.readBoolean();
            int playerCount = in.readInt();
            int pendingSaves = in.readInt();
            int pendingLoads = in.readInt();
            return new StatusResponseData(serverName, dbHealthy, redisHealthy,
                playerCount, pendingSaves, pendingLoads);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public record StatusResponseData(
        String serverName, boolean dbHealthy, boolean redisHealthy,
        int playerCount, int pendingSaves, int pendingLoads) {}

    // ==================== Utility ====================

    public static int getMessageType(byte[] data) {
        if (data == null || data.length == 0) return -1;
        return data[0] & 0xFF;
    }
}
