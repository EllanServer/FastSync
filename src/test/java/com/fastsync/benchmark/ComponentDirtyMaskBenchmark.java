package com.fastsync.benchmark;

import com.fastsync.sync.dirty.ComponentDirtyMask;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Measures the allocation-sensitive component dirty-tracking hot path. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class ComponentDirtyMaskBenchmark {

    private ComponentDirtyMask mask;
    private UUID playerId;

    @Setup(Level.Iteration)
    public void setup() {
        mask = new ComponentDirtyMask(5);
        playerId = UUID.randomUUID();
        mask.markDirty(playerId, ComponentDirtyMask.Component.INVENTORY);
    }

    @Benchmark
    public void markDirty() {
        mask.markDirty(playerId, ComponentDirtyMask.Component.INVENTORY);
    }

    @Benchmark
    public boolean isAnyDirty() {
        return mask.isAnyDirty(playerId);
    }

    @Benchmark
    public ComponentDirtyMask.DirtySnapshot snapshotAndClear() {
        ComponentDirtyMask.DirtySnapshot snapshot = mask.snapshotDirty(playerId);
        mask.clearDirty(playerId, snapshot);
        mask.markDirty(playerId, ComponentDirtyMask.Component.INVENTORY);
        return snapshot;
    }
}
