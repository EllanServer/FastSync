# FastSync 维护告示 (Maintenance Notes)

> **本文件面向未来的 FastSync 维护者**。记录了在对 `component-storage` 进行严格审查（对照 Minecraft 1.21.11 / Paper 源码）过程中发现的所有问题、修复状态、以及需要长期关注的事项。
>
> **任何修改 `component-storage` / 序列化路径 / 跨服同步逻辑的 PR 都应该先读完本文件相关章节。**

---

## TL;DR

- `component-storage` 默认 `enabled: false`，生产模式下 `validateProductionSafety()` 会告警，**不要在没有完整测试覆盖前打开**。
- 跨服同步是 **MC 原版 `PlayerList` / `ServerPlayer` 语义的镜像**，不是发明。任何"自创"语义都会和 MC 内部状态机冲突。
- 序列化格式变更 = **数据迁移**。任何对 `PlayerData` 字段或 `serializeComponentFields` / `deserializeComponentFields` 的修改都需要考虑老数据兼容性。
- `ComponentDirtyMask.Component` 枚举顺序 = 数据库 `component_bitmap` 的位定义，**不能随意重排**（详见 §5）。
- 任何 P0 修复必须配套单元测试 + chaos test，否则不要合入。

---

## 1. 已修复问题（已合入 main）

### Round 1 — PR #61（commit `f74386f`）

| Issue | 严重度 | 修复要点 |
|---|---|---|
| #53 | P0 | `applyAdvancements` 加 revoke 分支（对称 award + revoke，匹配 MC `PlayerAdvancements.load`） |
| #54 | P0 | `FullTypedStatsStrategy.dump` 不再跳过 0 值；`restore` 全量替换语义 |
| #55 | P0 | `parseAttributeOperation()` 适配 1.21+ 新旧枚举名（`ADD_NUMBER` ↔ `ADD_VALUE` 等） |
| #56 | P0 | `collectAttributes` 捕获 `slotGroup`；`applyAttributes` 用 5-arg 构造器，保留头盔专属等槽位限制 |
| #58 | P1 | `ENDER_CHEST` 加 `enderChestPresent` flag，与 `INVENTORY.offhandPresent` 对称 |

### Round 2 — 本 PR

| Issue | 严重度 | 修复要点 |
|---|---|---|
| #57 | P0 | `setInventoryContents` / `setEnderChestContents` 全量替换语义，超长 slot 显式置 null |
| #59 | P1 | `Attribute.values()` → `Registry.ATTRIBUTE`；`refreshConfigCache` 失效缓存以支持后注册的自定义属性 |
| #60 | P1 | `collectPlayerData` 在玩家 dead 时保存 `health = maxHealth`（post-respawn 状态），堵死 death-quit exploit |
| (audit) | P1 | FLIGHT 收集时强制 `allowFlight=true` 如果 `flying=true`，避免保存自相矛盾的状态 |
| (audit) | P2 | `Material.values()` / `EntityType.values()` → `Registry.MATERIAL` / `Registry.ENTITY_TYPE`，带 legacy fallback |
| (audit) | P2 | `sparseContents` 加注释说明 Bukkit `getContents()` 返回 copy 的语义前提 |

---

## 2. 已知未修复问题（设计性 / 需要 NMS）

这些问题不修不会立即出事，但启用 `component-storage` 前必须评估：

### 2.1 PotionEffect duration 跨服漂移（P0-2）

`PotionEffect.getDuration()` 返回**剩余 tick**，不是总 duration。MC 自身 `MobEffects.save` 也是写剩余 tick，所以单服内 round-trip 没问题。但**跨服场景下**：玩家在 A 服饮下 8 分钟抗火，3 分钟后 quit → B 服 load，剩余 5 分钟 — 这是正确的。但如果 A 服 save 后 30 分钟才 load（队列卡、玩家延时上服），玩家拿到一个"原本应该已过期"的 5 分钟抗火。

**修复方向**：在 `PotionEffectData` 加 `startedAtEpoch` 字段，apply 时如果 `now - startedAt > duration` 则不应用。需要 NMS 反射拿 `MobEffectInstance.startTime`，或者记录 effect 创建时刻的近似值（`System.currentTimeMillis() - (totalDuration - remainingDuration) * 50L`）。

**临时缓解**：在文档中告知运维"长 save→load 间隔会让 Buff 时长不准"。

### 2.2 Advancement criterion 时间戳未还原（P1-2）

`collectAdvancements` 用 `System.currentTimeMillis()` 作为 criterion 时间戳（因为 Bukkit 不暴露 `CriterionProgress.obtained`）。`applyAdvancements` 完全忽略时间戳，只 award/revoke criterion。

**修复方向**：NMS 反射读写 `CriterionProgress.obtained` 字段。

**临时缓解**：从 `PlayerData` 移除 timestamps 字段省空间，或保留作为审计元数据。当前是"半同步"状态，最容易踩坑——审计日志里以为有时间戳，实际没还原。

### 2.3 FoodData 内部 tick timer 未同步（P1-4）

MC 1.21+ `FoodData` 新增字段：`foodTickTimer`、`starveTickTimer`、`lastFoodLevel`。Bukkit API 不暴露。跨服同步后这些 timer 重置为 0 — 玩家在 A 服饿了 30 秒即将扣血，跳到 B 服后 B 服 timer 从 0 重新计数 30 秒才扣血。轻微不公平但不会丢数据。

**修复方向**：NMS 反射读写 `FoodData.foodTickTimer` 等字段。

**临时缓解**：文档说明 "food 同步不保留内部 tick timer"。这是 MC sync 插件通病，HuskSync / PlayerSync 等都有此问题。

### 2.4 `ComponentDirtyMask.Component` ordinal 用作 bitmap 位（P2-2）

`Component` 枚举顺序就是 `component_bitmap` 的位定义。如果将来在中间插入 `RECIPE_BOOK`，所有 ordinal 偏移，**DB 里旧的 component_bitmap 全错**。

**当前约束**：**只能在枚举末尾追加新 Component**，不能在中间插入。已有位定义：

| Bit | Component | 加入版本 |
|---|---|---|
| 0 | INVENTORY | Round 2 |
| 1 | ENDER_CHEST | Round 2 |
| 2 | VITALS | Round 2 |
| 3 | FOOD | Round 2 |
| 4 | EXPERIENCE | Round 2 |
| 5 | POTION_EFFECTS | Round 2 |
| 6 | GAME_MODE | Round 2 |
| 7 | FIRE_TICKS | Round 2 |
| 8 | AIR | Round 2 |
| 9 | FLIGHT | Round 2 |
| 10 | ADVANCEMENTS | Round 2 |
| 11 | STATISTICS | Round 2 |
| 12 | ATTRIBUTES | Round 2 |
| 13 | PDC | Round 2 |
| 14 | LOCATION | Round 2 |
| 15+ | (reserved) | — |

**长期修复方向**：用 enum 显式编号（`INVENTORY(1), ENDER_CHEST(2), ...`），或用 String key 替代 bitmap。

### 2.5 `verifyComponentOverlayCompleteness` 严格 fail-closed 的 GC race（P2-4）

如果某个 component row 在 GC 时被意外清掉（`gcOldComponentRows` 的 boundary 条件），整个 load 失败，玩家被 kick。当前 GC 实现 `WHERE generation < ?`（参数 `currentGeneration - 1`），load 读的是 `generation = current`，理论上不会冲突。但生产环境如果有并发 GC + load，建议加测试覆盖。

---

## 3. 修改 `component-storage` 必读

### 3.1 设计原则

1. **MC 原版语义优先**。任何"我以为更好的"做法大概率跟 MC 内部状态机冲突。具体冲突点：
   - `PlayerList.placeNewPlayer` 的状态加载顺序：先 `load(ServerPlayer)` → `setLevel` → `setPos` → `readAdditionalSaveData`（advancements/stats/inventory）→ `setPrimaryHand` → `gameType` → `containerMenu` 重建。
   - `PlayerList.respawn` 的状态重置：health=maxHealth，food=20，saturation=5.0，fireTicks=0，potionEffects 清空。
   - `ServerPlayer.doTick` 每 tick 写入 `StatsCounter`、`RecipeBook`、`Advancements` 的内存状态。

2. **CAS + fencing + lock_session_id 是不可妥协的**。任何 save 路径（full-Blob 或 component-only）都必须经过 `version + fencing_token + locked_by + lock_session_id` 四元校验。**绝不允许裸 `UPDATE player_data SET data = ? WHERE uuid = ?`** — 这会覆盖新会话。

3. **Fail-closed 优于 fail-silent**。component 反序列化失败时，应该让整个 load 失败 + kick 玩家，而不是 silently 应用 baseline。`verifyComponentOverlayCompleteness` 就是这个原则的体现。

4. **Clean-slate 优于 legacy compat**。FastSync 没有生产数据要兼容，所以对老格式的 payload 直接 throw 而不是静默 fallback。例如 `enderChestPresent` flag 缺失时 throw，而不是"假设 baseline 是对的"。

### 3.2 修改 `serializeComponentFields` / `deserializeComponentFields`

任何字段加/减/重命名都需要：

1. **同步更新 serialize 和 deserialize 两边**。常见 bug：serialize 写了新字段，deserialize 没读 → 字段静默丢失。
2. **present flag 模式**：如果字段值可能是 null 但 null 有语义（"明确清空"），必须加 `_present` boolean。参见 `offhandPresent`、`enderChestPresent`。
3. **throw on missing flag**：deserialize 时如果遇到老格式 payload（flag 缺失），throw 而不是静默保留 baseline。
4. **不要在 deserialize 里做 NMS 调用** — deserialize 可能发生在离线场景（重放）。

### 3.3 修改 `applyPlayerData` / `collectPlayerData`

1. **顺序敏感**。apply 必须按 MC `PlayerList.placeNewPlayer` 的顺序：clear → inventory → armor → offhand → ender chest → vitals → food → experience → potion → game mode → fire → air → flight → advancements → statistics → attributes → pdc → location。
2. **clear-before-apply 的语义边界**。当前实现 `player.getInventory().clear()` + `setArmorContents(null)` + `setItemInOffHand(null)` + 移除所有 potion effect。但**不** clear advancements/statistics/attributes — 这些走"覆盖"语义（apply 时显式 revoke/reset/remove）。
3. **null 与 empty 的区分**：
   - `data.getInventory() == null`：collect 时跳过了 → apply 时**不动**当前状态
   - `data.getInventory() == ItemStack[36]` 全 null：明确"空背包" → apply 时 clear
   - 同理 `getPotionEffects()`：null=不动，empty list=clear all
4. **不要在 apply 路径里触发事件**。`setHealth`、`setGameMode` 等会触发 Bukkit 事件，可能被其他插件取消。当前实现 catch 所有 Exception，但最好避免在 apply 中调用会取消的 API。

### 3.4 修改 `DatabaseManager.upsertComponentsIfLockHeld`

1. **必须用事务**。SELECT FOR UPDATE → 校验 → upsert component rows → UPDATE player_data metadata → commit。任何中途失败必须 rollback。
2. **校验四元组**：`locked_by + fencing_token + lock_session_id + expected_version`。少一个都会让旧会话写入。
3. **`expectedVersion` 检查是 STALE_VERSION rejection 的关键**。如果不检查，一个 stale component snapshot 会 bump version 并 mask 一个 still in flight 的 newer component save。

### 3.5 添加新 Component

1. **追加到 `ComponentDirtyMask.Component` 枚举末尾**（不要中间插入，参见 §2.4）。
2. **同步更新 `BASIC` 集合**（如果适用）。
3. **同步更新 `serializeComponentFields` / `deserializeComponentFields`**。
4. **同步更新 `isComponentSyncEnabled`**（在 SyncManager 里映射到 config 项）。
5. **同步更新 `DirtyTrackingListener`**（监听对应的 Bukkit 事件）。
6. **加 chaos test**：A 服修改 → save → load B 服 → 验证 round-trip。
7. **加 `verifyComponentOverlayCompleteness` 覆盖**：bitmap 设了但 row 缺失时必须 fail-closed。

---

## 4. CI / 测试

### 4.1 必须跑的 task

```bash
./gradlew clean ci              # 单元测试 + 集成测试（不需要 Docker，26 个跳过）
./gradlew stressTest            # 压测（需要 Docker，约 5-10 分钟）
```

CI workflow 会自动跑：
- `Build & Test`：编译 + 单元测试
- `Full CI`：含 MySQL 集成测试（testcontainers）
- `Real Paper Server E2E`：真实 Paper 1.21.11 服务器 + Redis + MySQL，跨服同步验证
- `DB Stress (Testcontainers)`：高并发压测

### 4.2 测试覆盖要求

任何 PR 必须包含：
- **单元测试**：新方法的 happy path + edge case
- **chaos test**：如果修改了 component-storage 任何路径，必须有"A 服改 → save → load B 服 → 验证"的端到端测试
- **fail-closed 测试**：如果加了 present flag 之类的"老格式拒绝"逻辑，必须有对应 throw 测试

### 4.3 已知的 flaky 测试历史

- `FileOperationLogManagerExecutorTest.appendRunsOnDedicatedOpLogThreadNotCommonPool` — 曾因 `thenRun`（非 async）竞态失败。修复：用 `thenRunAsync(action, executor)` 强制回调在专用 executor 上执行（PR #52 round 1 修复）。**如果再 flaky，检查是否引入了新的同步回调路径。**

---

## 5. 数据库 Schema

### 5.1 `fastsync_player_data` 表

```
cluster_id          VARCHAR — 主键之一（v2 schema）
uuid                VARCHAR — 主键之一
data                LONGBLOB — full Blob 序列化数据
version             BIGINT — OCC 版本号
checksum            BIGINT — data 的 CRC32
fencing_token       BIGINT — Kleppmann fencing token
locked_by           VARCHAR — 当前持锁 server name
locked_at           BIGINT — 锁获取时间
lock_session_id     VARCHAR — 锁会话 ID（防止 stale session 写入）
last_server         VARCHAR — 最后保存的 server
last_updated        BIGINT — 最后保存时间
component_bitmap    BIGINT — 哪些 component 已迁移到 player_component 表
component_generation BIGINT — 当前 generation（full save 时 +1 使旧 component rows 失效）
```

主键：`(cluster_id, uuid)`

### 5.2 `fastsync_player_component` 表

```
cluster_id    VARCHAR — 主键之一
uuid          VARCHAR — 主键之一
component     VARCHAR — 主键之一（Component.name()）
generation    BIGINT — 该 row 属于哪个 generation
data          LONGBLOB — 单 component 序列化数据
version       BIGINT — 该 row 被更新了多少次
checksum      BIGINT — data 的 CRC32
updated_at    BIGINT — 最后更新时间
```

主键：`(cluster_id, uuid, component)`

### 5.3 Schema 变更

任何 schema 变更必须：
1. 在 `DatabaseManager.initialize()` 的 CREATE TABLE 里同步更新
2. 加 ALTER TABLE 迁移代码（如果生产已有数据）
3. 更新 `schema_version` 表的版本号

**当前 schema 版本**：v2（`cluster_id` 加入主键）

---

## 6. 依赖与 Paper 版本

### 6.1 当前目标

- **Paper 1.21.11**（`paper.version=1.21.11` in `gradle.properties`）
- **Java 21**（toolchain）
- **Folia 兼容**（`folia-supported: true` in plugin.yml）

### 6.2 1.21+ 已废弃但仍在用的 API（需要未来迁移）

| API | 替代 | 状态 |
|---|---|---|
| `Attribute.values()` | `Registry.ATTRIBUTE.iterator()` | ✅ 已迁移（#59） |
| `Material.values()` | `Registry.MATERIAL.iterator()` | ✅ 已迁移（P2 audit） |
| `EntityType.values()` | `Registry.ENTITY_TYPE.iterator()` | ✅ 已迁移（P2 audit） |
| `AttributeModifier.Operation.ADD_NUMBER` 等 | `ADD_VALUE` 等 | ⚠️ 仍在用旧名（#55 适配器兜底） |
| `AttributeModifier` 4-arg 构造器 | 5-arg 带 `EquipmentSlotGroup` | ⚠️ fallback 用于 legacy payload（#56） |
| `Player.getHealth()` / `setMaxHealth()` | `Attribute.GENERIC_MAX_HEALTH` | ⚠️ 仍在用，1.22+ 可能废弃 |

### 6.3 1.22+ 升级路径

当 Paper 1.22 发布时，预期需要：
1. 移除 `Attribute.values()` 的 legacy fallback（如果 Paper 完全删除）
2. 切换 `AttributeModifier.Operation` 默认写新名（`ADD_VALUE` 等）
3. 验证 `EquipmentSlotGroup.getByName` 仍然存在
4. 检查 `FoodData` 是否暴露新 API（如果暴露，可以做 P1-4 修复）
5. 跑完整 CI 套件

---

## 7. 安全注意

### 7.1 Lock 释放

- **Fail-closed**：`releaseLock` 必须传 `fencingToken + lockSessionId`，缺任何一个都拒绝释放。**绝不允许 tokenless release**（旧的 2-arg `releaseLock` 已删除）。
- **不释放不属于自己的锁**：`releaseOwnedLockAndNotify` 会校验 `locked_by + fencing + session` 三元组。

### 7.2 Final-Save Spool

- `final-save.spool.enabled=true` 是生产必须（`validateProductionSafety` 强制）
- `allow-sync-fallback=false` 是推荐配置（同步 fallback 会卡游戏 tick）
- spool 文件以 `pending/` → `done/` / `failed/` 状态机管理
- replay 必须用 CAS 路径，不允许裸 UPDATE

### 7.3 Token 安全

**任何包含 GitHub token / DB 密码 / Redis 密码的提交都会被自动告警**。提交前检查 `git diff` 是否包含敏感字段。

---

## 8. 联系与历史

- 仓库：https://github.com/Arbousier1/FastSync
- 主分支：`main`（保护分支，PR 必须 CI 全绿才能合）
- 重大设计变更参考 `CHANGELOG.md`
- 本文件由 component-storage 严格审查（2026-06-28）产出，后续修改请同步更新

---

**最后更新**：2026-06-28（PR #61 + 本 PR）
