# FastSync

高性能跨服务器 Minecraft 玩家数据同步插件，支持 Paper / Folia 后端和 Velocity 代理端。采用 NBT 字节流序列化 + LZ4 压缩 + MySQL 持久化 + Redis 分布式锁协调，通过 fencing token + 版本号 CAS 保证数据一致性。

## 核心特性

### 数据安全

- **Dynamo 风格乐观并发控制**：版本号 CAS + fencing token 双重校验，防止脑裂写覆盖
- **DB 时间锁过期**：锁过期判断使用 MySQL `UNIX_TIMESTAMP()`，不依赖各节点 JVM 本地时钟，消除时钟漂移问题
- **Session ID 防重入**：同一服务器的快速重连不会被旧 session 干扰
- **冲突快照**：写冲突时自动保存旧版本快照，支持人工恢复
- **登录背压**：`loginLoadSemaphore` 限制并发加载数，登录风暴时快速拒绝而非排队拖死 DB
- **生产安全校验**：启动时拒绝 root/password 默认密码、拒绝远程明文连接、拒绝 cluster-id 与默认 table-prefix 的危险组合

### 性能优化

- **专用线程池**：主 async executor（bounded queue + AbortPolicy）+ finalSaveExecutor（QUIT/SHUTDOWN 专用通道）+ opLog executor（单线程审计日志）
- **Dirty Tracking**：事件驱动标记变更组件，周期保存跳过未变更部分，减少 80-95% 序列化成本
- **组件存储（Phase 2）**：按组件写入独立行（inventory/vitals/exp/pdc...），而非每次全量 Blob
- **配置感知采集**：关闭某同步项真正跳过采集和序列化，而非仅跳过写入
- **LZ4 压缩**：带 2 字节格式头 + 可选 4 字节原始长度，256KB 吞吐 3,600+ MB/s
- **心跳批量刷新**：单 JDBC batch 刷新所有在线玩家锁，500 玩家 p99 < 100ms

### Folia 兼容

- **SchedulerUtil 抽象层**：自动检测 Folia 调度器，`runGlobal`/`runEntity`/`runRegion` 统一调度
- **WorldSaveEvent 线程安全**：用 `runGlobal` 包裹 `Bukkit.getOnlinePlayers()`
- **entity/region 线程零 DB I/O**：所有 JDBC 操作都在 async executor 上执行

### Redis 协调

- **RTopic Pub/Sub**：锁释放通知（fire-and-forget，低延迟）
- **RStream 可靠事件**：关键事件至少一次交付 + XAUTOCLAIM 恢复（消费者崩溃后自动重新投递）
- **MAXLEN 裁剪**：XADD 时自动裁剪旧条目，防止 Redis 内存无限增长（默认 100,000 条上限）
- **健康检查缓存**：`isHealthy()` 缓存 2 秒，避免登录风暴时频繁 ping Redis

## 架构

### 五层分离

| 层 | 技术 | 职责 |
|---|---|---|
| **序列化** | Sparrow-NBT + Paper `ItemStack.serializeAsBytes()` | 玩家数据 → NBT byte[]，无 base64/Kryo/Gson |
| **压缩** | `at.yawk.lz4:lz4-java` | NBT byte[] → LZ4 压缩 Blob，带格式头 |
| **存储** | MySQL 8.0+ + jOOQ DSL + HikariCP | 乐观并发写入，`WHERE version=? AND fencing_token<=?` CAS |
| **协调** | Redis 6.2+ + Redisson | RTopic 锁通知 + RStream 可靠事件 |
| **日志** | 纯 Java NIO `FileChannel` | 每玩家独立 append-only 顺序文件日志 |

### 安全模型

核心安全边界在**数据库层**，不在 Redis：

```
玩家登录 (AsyncPlayerPreLoginEvent)
  ↓
loginLoadSemaphore.tryAcquire()  ← 登录背压
  ↓
acquireLock: INSERT ... ON DUPLICATE KEY UPDATE
  fencing_token = IF(locked_at IS NULL OR locked_at < expiredTime,  ← DB 时间
                     LAST_INSERT_ID(fencing_token + 1), fencing_token)
  ↓
loadPlayerData: SELECT data, version, checksum, ... FROM player_data
  ↓ decompress + deserialize
applyPlayerData: 清空 → 写入背包/血量/经验/...
  ↓
玩家退出 (PlayerQuitEvent)
  ↓
finalSaveExecutor: CAS 写入 + 释放锁
  WHERE uuid=? AND version=? AND fencing_token=? AND locked_by=? AND lock_session_id=?
  ↓
Redis RStream: 发布 PLAYER_CHECKOUT 事件 → 其他服务器收到通知
```

Redis 只负责协调（通知谁该重试），数据库的 fencing token + version CAS 才是防脑裂写覆盖的真正保障。

### 线程模型

| 线程 | 用途 | DB I/O |
|------|------|--------|
| Paper 主线程 / Folia region 线程 | 事件采集、数据应用 | 禁止 |
| Folia global 线程 | `Bukkit.getOnlinePlayers()` 快照 | 禁止 |
| FastSync-Async-N | 主保存线程池（periodic/death/world_save） | 允许 |
| FastSync-FinalSave-N | QUIT/SHUTDOWN 最终保存专用 | 允许 |
| FastSync-OpLog-1 | 操作日志写入 | 无（纯文件 I/O） |
| AsyncPlayerPreLoginEvent 线程 | 登录加载（受 semaphore 限制） | 允许 |

## 支持版本

| 运行环境 | 版本 |
|---|---|
| Paper / Folia | 1.21+（api-version: '1.21'） |
| Velocity | 3.5.0-SNAPSHOT（官方只发布 snapshot） |
| JDK | 21+ |
| MySQL | 8.0+（或 MariaDB 兼容） |
| Redis | 6.2+（可选但强烈建议） |

## 安装

### 1. 准备依赖

- MySQL 8.0+
- Redis 6.2+（可选但强烈建议）

### 2. 安装后端插件

将 `build/libs/FastSync-1.0.0.jar` 放到每个 Paper/Folia 服务端的 `plugins/` 目录。Sparrow-NBT 已 shade 进 JAR，其余 Maven Central 依赖由 Paper 在首次启动时自动下载。无需 `--add-opens` 或任何 JVM 参数。

### 3. 安装 Velocity 代理插件（可选）

代理插件不是必须的。后端插件可独立运行。安装后获得：
- `/fastsync status`：代理端聚合查看所有后端健康状态
- `/fastsync players`：查看所有玩家当前所在子服
- 玩家切服通知

### 4. 基本配置

首次启动后编辑 `plugins/FastSync/config.yml`：

```yaml
server-name: "survival-1"    # 每个子服唯一标识

database:
  host: "mysql.example.com"
  port: 3306
  database: "minecraft"
  username: "fastsync"
  password: "YOUR_PASSWORD"

redis:
  enabled: true
  host: "redis.example.com"
  port: 6379
```

所有子服的 `database` 和 `redis` 必须指向同一套服务。`server-name` 必须各不相同。

## 配置详解

### 数据库 (`database:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `type` | `mysql` | 数据库类型（mariaDB 兼容） |
| `host` | `localhost` | MySQL 主机地址 |
| `port` | `3306` | MySQL 端口 |
| `database` | `minecraft` | 数据库名 |
| `username` | `root` | 数据库用户名 |
| `password` | `password` | 数据库密码 |
| `table-prefix` | `fastsync_` | 表名前缀，仅允许字母数字下划线。多集群共用同一 MySQL 时必须各不相同 |
| `pool-size` | `10` | HikariCP 连接池大小 |
| `queue-capacity` | `256` | async executor 队列容量，满时 AbortPolicy |
| `connection-timeout` | `10000` | 连接超时（ms） |
| `idle-timeout` | `300000` | 空闲超时（ms） |
| `max-lifetime` | `1800000` | 连接最大生命周期（ms） |
| `leak-detection-threshold` | `60000` | 泄漏检测阈值（ms） |
| `parameters` | `sslMode=DISABLED&...` | JDBC 参数。不要用 `autoReconnect`。TLS 用 `sslMode=REQUIRED` |
| `allow-insecure-remote` | `false` | 远程明文连接开关。默认拒绝非回环 + DISABLED 的组合 |

### Redis (`redis:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `false` | 关闭后退化为 DB 轮询 |
| `host` | `localhost` | Redis 主机 |
| `port` | `6379` | Redis 端口 |
| `password` | `""` | Redis 密码 |
| `database` | `0` | Redis 数据库编号 |
| `ssl` | `false` | SSL 加密 |
| `timeout` | `5000` | 连接超时（ms） |
| `channel-prefix` | `fastsync:lock:` | Pub/Sub 频道前缀 |
| `cache-enabled` | `false` | Redis 数据缓存 |
| `streams-enabled` | `true` | Redis Streams 可靠事件 |
| `stream-maxlen` | `100000` | Stream 最大条数，0 = 不裁剪 |
| `stream-trim-approx` | `true` | 近似裁剪（~MAXLEN），性能更好 |

### 同步设置 (`sync:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `sync-inventory` | `true` | 同步背包 |
| `sync-ender-chest` | `true` | 同步末影箱 |
| `sync-health` | `true` | 同步血量 |
| `sync-food` | `true` | 同步饥饿值 |
| `sync-experience` | `true` | 同步经验 |
| `sync-potion-effects` | `true` | 同步药水效果 |
| `sync-game-mode` | `true` | 同步游戏模式 |
| `sync-fire-ticks` | `true` | 同步火焰 tick |
| `sync-air` | `true` | 同步氧气值 |
| `sync-advancements` | `true` | 同步成就 |
| `sync-statistics` | `true` | 同步统计 |
| `sync-attributes` | `true` | 同步属性基础值（Paper API 无法区分永久/临时 modifier，故不复制 modifier） |
| `sync-flight` | `true` | 同步飞行状态 |
| `sync-pdc` | `true` | 同步 PersistentDataContainer |
| `sync-location` | `false` | 同步位置（需同世界名+UUID 匹配） |
| `lock-timeout` | `60` | 锁超时（秒），使用 MySQL DB 时间 |
| `heartbeat-interval-seconds` | `10` | 心跳间隔（自动校正 ≤ lock-timeout/3） |
| `lock-retry-interval-ms` | `300` | 锁重试间隔 |
| `lock-max-retries` | `15` | 锁最大重试次数 |
| `save-delay` | `0` | 保存延迟（tick） |
| `clear-before-apply` | `true` | 应用前清空（防复制） |
| `periodic-save` | `false` | 周期保存开关 |
| `periodic-save-interval-seconds` | `300` | 周期保存间隔 |
| `periodic-save-batch-size` | `10` | 每批保存玩家数 |
| `max-concurrent-loads` | `min(pool-size-3, 6)` | 最大并发登录加载 |
| `save-on-death` | `false` | 死亡时保存 |
| `save-on-world-save` | `false` | 世界保存时触发 |
| `cancel-commands-while-locked` | `false` | 加载中禁止命令 |

### Dirty Tracking (`sync.dirty-tracking:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `true` | 事件驱动脏标记 |
| `validation-interval` | `5` | 每 N 次保存做一次完整校验 |

### 组件存储 (`sync.component-storage:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `false` | 组件级写入（Phase 2，灰度前不要开） |
| `batch-size` | `15` | 每事务最大组件数 |

### 关闭超时 (`shutdown:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `pending-save-timeout-ms` | `30000` | 等待 pending 保存完成（ms），最小 5000 |
| `final-save-executor-timeout-seconds` | `30` | final-save executor 关闭超时（秒），最小 5 |

### PDC (`pdc:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `mode` | `registered-only` | `off` / `safe-all-paper`（Paper 1.21+ 公开 API）/ `registered-only`（推荐） |
| `clear-before-restore` | `true` | safe-all-paper: true=全量同步, false=合并 |
| `registered-keys` | `[]` | registered-only 模式下的 key 列表，格式 `namespace:key=TYPE` |

### 快照 (`snapshot:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `true` | 冲突快照开关 |
| `max-snapshots` | `16` | 每玩家最大快照数 |
| `backup-frequency-ms` | `14400000` | 定期备份间隔（4 小时） |
| `save-trigger` | `never` | `never` / `always` / 逗号分隔原因列表 |

### 集群 (`cluster-id`)

```yaml
cluster-id: ""
```

cluster-id 只隔离 Redis 消息（topic/stream/consumer group），**不隔离数据库行**。非空 cluster-id + 默认 `fastsync_` table-prefix = **拒绝启动**。多集群必须使用不同 table-prefix 或不同 database。

### 操作日志 (`operation-log:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `true` | 每玩家 append-only 操作日志 |
| `retention` | `100` | 每玩家最大条目数 |

### 延迟监控 (`latency:`)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `false` | p50/p99/p99.9 百分位统计 |
| `window-size` | `1000` | 滑动窗口样本数 |

## 管理命令

| 命令 | 说明 |
|------|------|
| `/fastsync reload` | 重载配置（重置心跳任务、保护模式） |
| `/fastsync status` | 查看 DB/Redis 状态、在线玩家、pending 数、final-save 队列/fallback 计数、OpLog 状态、HikariCP 池、延迟百分位 |
| `/fastsync debug` | 开关调试模式 |
| `/fastsync saveall` | 强制保存所有在线玩家（Folia 安全两阶段） |
| `/fastsync log <player> [n]` | 查看玩家操作日志（默认 20 条，最多 50） |

权限：`fastsync.admin`（默认 op）。命令别名：`/fsync`、`/fs`。

## 构建

```bash
git clone --recursive https://github.com/Arbousier1/FastSync.git
cd FastSync

# 如果子模块为空
git submodule update --init --recursive

# 构建并测试
./gradlew build

# 完整 CI 流水线
./gradlew ci

# 仅运行故障注入压测（需要 Docker）
./gradlew stressTest
```

产物：
- `build/libs/FastSync-1.0.0.jar`：Paper/Folia 后端插件
- `build/libs/FastSync-Proxy-1.0.0.jar`：Velocity 代理插件

## 数据库表结构

### `player_data` 表

```sql
CREATE TABLE fastsync_player_data (
    uuid                VARCHAR(36) NOT NULL,
    data                LONGBLOB NOT NULL,
    version             BIGINT NOT NULL DEFAULT 0,
    checksum            BIGINT NOT NULL DEFAULT 0,
    fencing_token       BIGINT NOT NULL DEFAULT 0,
    locked_by           VARCHAR(64) DEFAULT NULL,
    locked_at           BIGINT DEFAULT NULL,
    lock_session_id     VARCHAR(64) DEFAULT NULL,
    last_server         VARCHAR(64) DEFAULT NULL,
    last_updated        BIGINT NOT NULL DEFAULT 0,
    component_bitmap    BIGINT NOT NULL DEFAULT 0,
    component_generation BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (uuid),
    INDEX idx_locked (locked_by, locked_at)
);
```

### `player_component` 表（Phase 2）

```sql
CREATE TABLE fastsync_player_component (
    uuid        VARCHAR(36) NOT NULL,
    component   VARCHAR(32) NOT NULL,
    generation  BIGINT NOT NULL DEFAULT 0,
    data        LONGBLOB NOT NULL,
    version     BIGINT NOT NULL DEFAULT 0,
    checksum    BIGINT NOT NULL DEFAULT 0,
    updated_at  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (uuid, component),
    INDEX idx_uuid_generation (uuid, generation)
);
```

## 灰度上线建议

### 推荐第一波配置

```yaml
redis:
  enabled: true
  streams-enabled: true

sync:
  periodic-save: true
  periodic-save-interval-seconds: 300
  dirty-tracking:
    enabled: true
  component-storage:
    enabled: false    # 灰度阶段不要开
  sync-location: false
```

### 上线前必须压测的 5 个场景

1. 100 人同时登录
2. 100 人同时退出
3. 玩家 A 服退出后 1 秒内进 B 服
4. MySQL 暂停 10-30 秒后恢复
5. Redis 暂停 10-30 秒后恢复

### 重点观察指标

- `/fastsync status` 中的 `finalSaveSyncFallbackTotal`：> 0 说明 DB 或队列不健康
- `pending saves` / `pending loads`：不应持续增长
- `DB pool waiting`：不应 > 0
- `protection mode`：不应频繁激活
- `conflict snapshot` 数量：不应大量出现

## 故障注入测试

项目包含 FoundationDB 风格的确定性仿真测试（`src/test/java/com/fastsync/stress/FaultInjectionStressTest.java`），覆盖：

- 同 UUID 双服登录拒绝
- 旧 fencing token 写入被拒
- QUIT 成功后锁释放
- QUIT 失败后锁不释放
- 周期保存与 QUIT 保存交错
- DB 延迟 + 随机失败下的 50 玩家高并发
- 服务器崩溃后锁恢复

```bash
./gradlew stressTest
```

## 性能基准

以下数据在 GitHub Actions CI 共享 runner 上测得：

| 组件 | 操作 | 吞吐 |
|------|------|------|
| LZ4 压缩 (256KB) | 压缩 / 解压 | 3,614 MB/s / 1,231 MB/s |
| CRC32 校验 (256KB) | 计算校验和 | 12,140 MB/s |
| StreamEvent 序列化 | toMap + fromMap 往返 | 661,098 ops/sec |

## 常见问题

**Q: 不使用 Redis 可以吗？**
A: 可以。关闭 `redis.enabled` 后退化为数据库轮询，延迟和数据库负载都会增加。

**Q: 玩家跨服时数据被覆盖怎么办？**
A: 版本号 CAS + fencing token 双重校验防止覆盖。冲突时自动保存快照，可用 `/fastsync log <player>` 查看。

**Q: 多集群共用一个 MySQL 怎么办？**
A: 每个集群必须使用不同的 `table-prefix`（如 `survival_`、`creative_`）或不同的 `database`。cluster-id 只隔离 Redis 消息。非空 cluster-id + 默认 `fastsync_` 前缀会被拒绝启动。

**Q: 需要给 Paper/Folia 节点配 NTP 吗？**
A: 不再是硬性要求。锁过期使用 MySQL 服务器时间（`UNIX_TIMESTAMP`），不依赖各节点 JVM 时钟。但建议 MySQL 和 Redis 配置 NTP。

**Q: Folia 上有注意事项吗？**
A: 插件已自动适配 Folia 区域调度器，无需额外配置。

**Q: 操作日志文件在哪？**
A: `plugins/FastSync/data/player-log/{uuid}.log`，每玩家独立 append-only 文件。可安全删除整个目录清理历史。

**Q: component-storage 什么时候可以开？**
A: 等 full Blob 路径在生产稳定后，再单独灰度组件存储。第一波生产建议关闭。

## 致谢

### 理论参考

- **Dynamo**（Amazon SOSP 2007）— 乐观并发控制，版本号 CAS，primary-key-only access
- **Kleppmann "Designing Data-Intensive Applications"** — Fencing token 防脑裂写覆盖
- **FoundationDB**（SIGMOD 2021）— 确定性仿真测试，读写路径分离
- **DynamoDB**（USENIX ATC 2022）— 公平性限流，尾延迟 SLA
- **Google "The Tail at Scale"** — p99/p99.9 监控
- **Spanner**（Google）— 自增 ID 排序而非墙钟时间戳
- **Nakama Storage Engine** — collection/key/version 组件存储模型
- **Unity Cloud Save** — write lock 语义
- **PlayFab Player Data** — additive update / DataVersion

### 依赖项目

| 项目 | 用途 |
|------|------|
| [Redisson](https://github.com/redisson/redisson) | Redis 分布式协调 |
| [jOOQ](https://github.com/jOOQ/jOOQ) | 类型安全 SQL DSL |
| [LZ4 Java](https://github.com/yawkat/lz4-java) | LZ4 压缩 |
| [HikariCP](https://github.com/brettwooldridge/HikariCP) | JDBC 连接池 |
| [Sparrow-NBT](https://github.com/MrCraftCod/Sparrow) | NBT 序列化 |
| [Caffeine](https://github.com/ben-manes/caffeine) | 高性能缓存 |
| [PaperMC](https://papermc.io/) | Paper / Folia API |
| [Velocity](https://papermc.io/software/velocity) | Velocity 代理 API |

## 许可

MIT License
