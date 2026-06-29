# FastSync CHANGELOG

## [Unreleased] — Upstream regression and Paper 26.2 audit

- Completed the component-storage hot path and enabled it by default for fresh
  or key-less configurations. Disabling new writes remains a safe rollback:
  persisted bitmap overlays are still read until a full save folds them in.
- Packed each player's 15 dirty epochs plus save/API-scan counters into one
  `AtomicLongArray`, eliminating the per-player forest of atomic objects and
  the second counter map. Added a focused JMH benchmark (mark ~10.5 ns,
  dirty check ~4.9 ns on the audit host).
- Replaced the normal component batch's two hash maps and boxed checksums with
  one ordered `ComponentWrite` list; the MySQL cursor path remains a metadata
  CAS plus one rewritten JDBC batch in a single transaction.
- Component overlays now fail closed when their NBT envelope, component root,
  or mandatory `_present` marker is absent, preventing a malformed row from
  silently leaving stale baseline fields in place.
- Added one-JAR compatibility gates for Paper/Folia 1.21.11 through 26.2:
  baseline builds emit Java 21 bytecode; the 26.2 API check uses JDK 25 to
  consume `26.2.build.40-alpha` without raising the artifact bytecode level.
- Added real Paper E2E matrix entries for 1.21.11 and 26.2 (Java 25 image),
  with strict server-version, plugin-list, linkage-error, command and schema checks.
- Fixed ZSTD in the final shadow JAR. `zstd-jni` must not be relocated because
  native symbols bind to `com.github.luben.zstd`; the previous artifact failed
  on first ZSTD use with `UnsatisfiedLinkError`.
- Replaced per-call ZSTD native contexts with one-shot compression into a
  reusable thread-local scratch buffer. Added a final-artifact JNI probe.
- `compression.enabled=false` now really disables compression, including after
  reload. Codec names/levels are validated and status reports LZ4 versus ZSTD.
- Compression v2 now rejects unknown algorithm encodings and algorithm bits on
  uncompressed frames; ZSTD native error codes fail closed. Legacy v1 LZ4 remains readable.
- Removed the spool lock-state TTL cache. Ownership/fencing state is a safety
  decision and must be read fresh; the implementation invalidated after every
  successful lookup anyway, so it provided no hits while adding stale-state risk.
- Fixed config migration ordering (migrate before parsing) and bumped config
  schema to v3 to advertise wrapper format v2 correctly.
- Fixed recursive i18n flattening, locale-cache invalidation on reload, and
  leftover legacy Velocity color arguments after the MiniMessage migration.
- Fixed `plugin.yml` version expansion and added a final-JAR metadata gate so
  unresolved Gradle placeholders cannot reach a Paper server again.
- Hardened the real-server harness for cold boots: 512 MiB per no-player JVM,
  exact startup-version checks, failure cleanup, and serialized server startup.

## [Unreleased] — PR: Component Storage (Phase 2)

### Phase 2: Per-Component Storage

Splits the single `player_data` Blob into per-component rows in a new
`player_component` table. A dirty save that touches 1 of 15 components
now writes 1 small row (~100-500 bytes) instead of rewriting the full
Blob (~5-20KB).

**Schema changes** (zero-downtime, backward compatible):
- New table: `fastsync_player_component (uuid, component, data, version, checksum, updated_at)`
- New column on `player_data`: `component_bitmap BIGINT DEFAULT 0`
  (tracks which components are migrated to the per-component table)
- Migration uses `ADD COLUMN IF NOT EXISTS` + `CREATE TABLE IF NOT EXISTS`

**Save path**:
- New `persistComponentsOnly` fast path in `SyncManager.persistCollectedData`
- Triggers when `component-storage.enabled=true` AND dirty mask has entries
  AND save kind is online (PERIODIC/DEATH/WORLD_SAVE, not QUIT)
- Collects only dirty components on the Paper/Folia entity thread; unrelated
  inventory, advancement, statistics, attribute and PDC APIs are not touched
- Serializes only dirty components via `PlayerDataSerializer.serializeComponent`
- Successful DB hot path is two statements: fenced metadata CAS UPDATE +
  batched component upsert (no DB-time SELECT or SELECT FOR UPDATE)
- Updates `component_bitmap` to mark newly-migrated components
- Clears dirty mask, returns `SaveResult`
- Full-validation/all-dirty cycles deliberately refresh one full Blob instead
  of writing all 15 component rows
- Partial collections are fail-closed and can never fall back into a full-Blob write

**Load path**:
- After deserializing the full Blob, checks `component_bitmap`
- Always honors a persisted non-zero bitmap even when new component writes are
  disabled after restart/config rollback
- If non-zero, batch-loads migrated components from `player_component`
- Calls `PlayerDataSerializer.deserializeComponent` to overwrite the
  corresponding fields — result is freshest state = Blob base + overrides

**QUIT saves** always use the full Blob path (atomic lock release + version CAS).

### Files Added
- `src/main/java/com/fastsync/sync/dirty/ComponentDirtyMask.java` (phase 1)
- `src/main/java/com/fastsync/listeners/dirty/DirtyTrackingListener.java` (phase 1)
- `src/test/java/com/fastsync/sync/dirty/ComponentDirtyMaskTest.java` (phase 1, 13 tests)
- `src/test/java/com/fastsync/serialization/PlayerDataSerializerComponentTest.java` (phase 2, 13 tests)

### Files Modified
- `src/main/java/com/fastsync/database/DatabaseManager.java` — new table, CRUD methods
- `src/main/java/com/fastsync/serialization/PlayerDataSerializer.java` — per-component ser/deser
- `src/main/java/com/fastsync/sync/SyncManager.java` — save/load integration
- `src/main/java/com/fastsync/config/ConfigManager.java` — new config keys
- `src/main/java/com/fastsync/redis/RedissonManager.java` — Redisson 4.6.1 API fix
- `src/main/resources/config.yml` — new config sections
- `src/main/java/com/fastsync/FastSync.java` — dirty listener registration

### Config
```yaml
sync:
  dirty-tracking:
    enabled: true
    validation-interval: 5
  component-storage:
    enabled: false        # opt-in until battle-tested
    batch-size: 15        # max components per transaction
```

### Performance Model

200-player server, 5-min periodic save, ~20% of players with changes per cycle:

| Scenario | Saves/cycle | DB writes/cycle | Bytes written/cycle |
|---|---|---|---|
| Phase 0 (original) | 200 | 200 full Blobs | 200 × ~10KB = 2.0 MB |
| Phase 1 (dirty skip) | 80 (60 skip + 20 actual) | 60 full + 20 full = 80 | 80 × ~10KB = 800 KB |
| **Phase 2 (component storage)** | 20 actual + 60 validation | **20 component + 60 full** | **20 × ~500B + 60 × ~10KB = 610 KB** |

For idle server (everyone AFK, validation every 5th cycle):
- Phase 0: 200 full Blobs per cycle = 2 MB
- Phase 1: 40 validation saves = 400 KB
- **Phase 2: 0 component saves + 40 validation full saves = 400 KB** (validation still writes full Blob)

The big win is on **active servers with mixed workloads**:
- 50 players actively building (inventory changes only)
- Phase 1: 50 full Blob saves × 10KB = 500 KB per cycle
- **Phase 2: 50 component saves × 500B = 25 KB per cycle** ← 95% reduction

### Safety Properties
- Lock heartbeat runs independently (component save doesn't touch locked_at)
- Component save doesn't increment player_data.version (only full saves do)
- Component save doesn't release the lock (online save)
- All failures fall back to full Blob save transparently
- QUIT saves always use full Blob (atomic lock release required)
- `validation-interval` still forces full saves every Nth cycle (phase 1 safety net)

### Final hot-path shape

- Dirty state uses one atomic CAS word per component (dirty flag + epoch),
  eliminating both boxed snapshot epochs and the mark-vs-clear lost-update race
- Normal one-component saves perform one component-specific entity-thread read,
  one encode/compress/checksum, and one two-statement MySQL transaction
- Failure classification performs an extra metadata SELECT only on the cold path

---

## [Previous] — Phase 1: Component Dirty Tracking

See commit history for phase 1 details.

### PDC Strategy Interface
- `PdcSyncStrategy` with 4 implementations (off / safe-all-paper / registered-only / unsafe-reflection)
- Default: `registered-only`

### Login Backpressure
- `loginLoadSemaphore` caps concurrent pre-login loads
- New `LoadResult.Status.BUSY` + configurable `busy-kick-message`

### Online Save Version Refresh
- `savePlayerAsync` calls `refreshVersionAndFencingToken()` after acquiring save lock

### Redis Stream MAXLEN Trim
- `XADD MAXLEN ~` prevents unbounded stream growth

### PDC Ghost Key Fix
- Empty PDC now properly clears target container

### Config-Respecting collectPlayerData
- All basic fields gated by `config.isSyncXxx()` checks

### Table Prefix Validation
- `[A-Za-z0-9_]*` regex validation
