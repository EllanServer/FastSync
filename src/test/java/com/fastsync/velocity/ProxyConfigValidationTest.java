package com.fastsync.velocity;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class ProxyConfigValidationTest {

    @Test
    void clampsInvalidStatusQueryTimeout() throws Exception {
        ProxyConfig config = new ProxyConfig(Path.of("."), mock(Logger.class));
        set(config, "statusQueryTimeoutMs", Long.MAX_VALUE);

        config.validateRanges();

        assertEquals(60_000L, config.getStatusQueryTimeoutMs());
    }

    private static void set(ProxyConfig config, String fieldName, Object value) throws Exception {
        Field field = ProxyConfig.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(config, value);
    }
}
