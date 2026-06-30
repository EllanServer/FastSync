package com.fastsync.velocity;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HandoffProtocolTest {

    @Test
    void roundTripsEveryMessageWithPayload() {
        UUID uuid = UUID.randomUUID();

        assertEquals(new HandoffProtocol.QueryLockData(uuid, "target"),
            HandoffProtocol.decodeQueryLock(HandoffProtocol.encodeQueryLock(uuid, "target")));
        assertEquals(new HandoffProtocol.LockStatusData(uuid, true, "source"),
            HandoffProtocol.decodeLockStatus(HandoffProtocol.encodeLockStatus(uuid, true, "source")));
        assertEquals(new HandoffProtocol.HandoffNotifyData(uuid, "source", "target"),
            HandoffProtocol.decodeHandoffNotify(
                HandoffProtocol.encodeHandoffNotify(uuid, "source", "target")));
        assertEquals(new HandoffProtocol.StatusResponseData("source", true, false, 5, 2, 1),
            HandoffProtocol.decodeStatusResponse(
                HandoffProtocol.encodeStatusResponse("source", true, false, 5, 2, 1)));
    }

    @Test
    void rejectsWrongTypeAndTrailingBytes() {
        UUID uuid = UUID.randomUUID();
        byte[] lockStatus = HandoffProtocol.encodeLockStatus(uuid, false, "source");
        assertThrows(IllegalArgumentException.class,
            () -> HandoffProtocol.decodeQueryLock(lockStatus));

        byte[] valid = HandoffProtocol.encodeQueryLock(uuid, "target");
        byte[] withTrailingByte = Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IllegalArgumentException.class,
            () -> HandoffProtocol.decodeQueryLock(withTrailingByte));
    }

    @Test
    void rejectsNegativeStatusCounters() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(HandoffProtocol.STATUS_RESPONSE);
            out.writeUTF("source");
            out.writeBoolean(true);
            out.writeBoolean(true);
            out.writeInt(-1);
            out.writeInt(0);
            out.writeInt(0);
        }

        assertThrows(IllegalArgumentException.class,
            () -> HandoffProtocol.decodeStatusResponse(bytes.toByteArray()));
    }
}
