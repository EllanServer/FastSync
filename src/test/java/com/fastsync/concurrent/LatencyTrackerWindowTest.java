package com.fastsync.concurrent;

import org.junit.jupiter.api.Test;

import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LatencyTrackerWindowTest {

    @Test
    void exactWindowKeepsNewestSamples() {
        LatencyTracker tracker = new LatencyTracker(
            "test", Logger.getLogger("latency-window-test"), 16);
        for (int i = 1; i <= 20; i++) {
            tracker.record(i);
        }

        assertEquals(16, tracker.getSampleCount());
        assertEquals(5, tracker.getPercentile(0));
        assertEquals(20, tracker.getPercentile(100));
    }
}
