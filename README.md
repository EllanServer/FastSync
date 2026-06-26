# FastSync

FastSync 是一个跨服务器的 Minecraft 玩家数据同步插件，支持 Paper / Folia 后端和 Velocity 代理端。它把玩家数据序列化为 NBT 字节流，再用 LZ4 压缩写入 MySQL，通过 Redis 协调跨服锁，保证同一时刻只有一个服务端持有某玩家的写入权。

## 功能

- **跨服务器同步背包、末影箱、血量、饥饿值、经验、药水效果、游戏模式、火焰 tick、氧气值等**
- **LZ4 压缩**：相比 base64 字符串方案体积更小、速度更快
- **Dynamo 风格乐观并发控制**：版本号 + fencing token，防止脑裂写覆盖
- **Redis 锁协调**：基于 Redisson，RTopic 做 Pub/Sub 锁通知，RStream 做关键事件恢复
- **冲突快照**：写冲突时保存旧版本快照，方便人工恢复
- **本地操作日志**：基于纯 Java NIO 的 append-only 顺序文件日志，用于审计和问题排查，无需 `--add-opens`
- **类型安全 SQL**：基于 jOOQ DSL 构建所有数据库查询
- **Paper / Folia 双兼容**：自动检测 Folia 调度器并切换
- **Velocity 代理插件**：记录玩家所在子服和切换时间

## 架构

FastSync 采用五层分离架构，每层职责明确：

| 层 | 技术栈 | 职责 |
|---|---|---|
| **LZ4 压缩** | `at.yawk.lz4:lz4-java` | 序列化后的 NBT byte[] 压缩，带 2 字节格式头 + 可选 4 字节原始长度 |
| **Redis 协调** | `org.redisson:redisson` | RTopic 做 Pub/Sub 锁释放通知（fire-and-forget），RStream 做关键事件可靠交付（至少一次 + autoClaim 恢复） |
| **OCC 写入** | `org.jooq:jooq` + MySQL | jOOQ DSL 构建类型安全的 `WHERE version=? AND fencing_token=?` 双重 CAS 查询 |
| **快照** | MySQL `fastsync_snapshots` 表 | 独立表存储点时备份，按自增 ID 排序（Spanner 教训：不用墙钟） |
| **顺序日志** | 纯 Java NIO（`FileChannel` + `DataOutputStream`） | 每玩家独立 append-only 文件日志（`data/player-log/{uuid}.log`），无需 `--add-opens` |

### 安全模型

核心安全边界在**数据库层**，不在 Redis：

```
玩家写入请求
  ↓
Redisson RTopic 广播锁释放通知 → 对端 CountDownLatch 唤醒重试
  ↓
读取 PlayerState: version, last_fence_token, compressed_blob
  ↓
CAS 写入主库:
    WHERE uuid = ?
      AND version = expectedVersion        ← Dynamo 风格乐观并发
      AND fencing_token = currentToken    ← Kleppmann 风格过期写防御
  ↓
成功: version+1, 释放锁, RStream 发布 PLAYER_CHECKOUT
失败: 保存冲突快照, 记录 CONFLICT 日志
```

Redis 只负责协调（通知谁该重试），数据库的 fencing token + version CAS 才是防脑裂写覆盖的真正保障。

## 支持的版本

| 运行环境 | 版本 |
|---|---|
| Paper / Folia | 1.21.4 及兼容 1.21.x 的版本 |
| Velocity | 3.5.0-SNAPSHOT+ |
| JDK | 21+ |

## 安装

### 1. 准备依赖环境

FastSync 需要以下外部服务：

- **MySQL 8.0+**（数据持久化）
- **Redis 6.2+**（可选但强烈建议，用于跨服锁协调）

### 2. 安装后端插件（Paper / Folia）

把 `build/libs/FastSync-1.0.0.jar` 放到 **每个 Paper/Folia 服务端的 `plugins/` 目录** 即可。

Sparrow 系列库（不在 Maven Central）已经 shade 进了 jar 里；Redisson、Chronicle Queue、jOOQ、HikariCP、Caffeine、MySQL 驱动等 Maven Central 依赖会在首次启动时由 Paper 自动下载。无需手动复制任何 `lib/` 目录。

### 3. 安装 Velocity 代理插件（可选）

**Velocity 代理插件不是必须的。** 后端 Paper/Folia 插件可以独立运行，不安装代理插件也不影响数据同步功能。

如果使用了 Velocity 代理，可以把 `build/libs/FastSync-Proxy-1.0.0.jar` 放到 Velocity 的 `plugins/` 目录，获得以下额外功能：

- **`/fastsync status`**：在代理端聚合查看所有后端的 DB/Redis 健康状态、在线玩家数、待保存/待加载数
- **`/fastsync players`**：查看所有玩家当前所在子服
- **玩家切换通知**：玩家切服时通知新后端"该玩家从 X 服过来"（后端可据此做日志记录）
- **`proxy-config.yml`**：可配置切换等待超时、状态查询超时等参数

## 配置

首次启动后，插件会在 `plugins/FastSync/config.yml` 生成默认配置。至少修改以下字段：

```yaml
# 每个子服的唯一标识，必须全网唯一
server-name: "survival-1"

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
  password: ""
```

所有子服的 `database` 和 `redis` 必须指向同一套服务。`server-name` 必须各不相同。

**即插即用，无需修改 JVM 启动参数。** FastSync 所有依赖都使用纯 Java 公开 API，不需要 `--add-opens` 或任何 JVM 参数。直接把 JAR 放入 `plugins/` 即可。

## 管理命令

```
/fastsync reload    # 重载配置
/fastsync status    # 查看当前锁、缓存、连接状态
/fastsync debug     # 开关调试模式
/fastsync saveall   # 强制保存所有在线玩家数据
/fastsync log <player> [n]  # 查看玩家操作日志（最近 n 条，默认 10）
```

权限：`fastsync.admin`（默认 op）

## 构建

仓库包含 Sparrow 库的 Git 子模块，用于 CI 中直接从源码构建（因为 Sparrow 只发布到私有 Maven 仓库）。

```bash
# 克隆包含子模块
git clone --recursive https://github.com/Arbousier1/FastSync.git

# 如果已克隆但子模块为空
git submodule update --init --recursive

# 构建并测试
./gradlew build

# 完整 CI 流水线（clean + build + check）
./gradlew ci
```

产物位于 `build/libs/`：

- `FastSync-1.0.0.jar`：Paper/Folia 后端插件（Sparrow 已 shade，Maven Central 依赖自动下载）
- `FastSync-Proxy-1.0.0.jar`：Velocity 代理插件

## 性能基准

以下数据在 GitHub Actions CI 共享 runner 上测得（实际生产环境性能会更高）：

| 组件 | 操作 | 吞吐 |
|---|---|---|
| LZ4 压缩 (256KB) | 压缩 / 解压 | 3,614 MB/s / 1,231 MB/s |
| CRC32 校验 (256KB) | 计算校验和 | 12,140 MB/s |
| StreamEvent 序列化 | toMap + fromMap 往返 | 661,098 ops/sec |

## 常见问题

**Q: 不使用 Redis 可以吗？**
A: 可以。关闭 `redis.enabled` 后，插件会退化为数据库轮询方式获取锁，延迟和数据库负载都会增加。

**Q: 玩家跨服时数据被覆盖怎么办？**
A: 插件使用版本号 CAS + fencing token 双重校验；如果检测到冲突，默认会把旧数据保存为快照（`conflict.recovery-strategy: snapshot`），不会直接覆盖。可用 `/fastsync log <player>` 查看操作日志。

**Q: Folia 上有没有注意事项？**
A: 插件已自动适配 Folia 的区域调度器。无需额外配置。

**Q: Chronicle Queue 的日志文件在哪？**
A: 位于 `plugins/FastSync/data/player-log/{playerUuid}/` 下。每个玩家一个独立目录，append-only 不可变。可安全删除整个 `data/player-log/` 目录来清理历史日志。

## 致谢

FastSync 的设计借鉴了以下分布式系统理念，并使用了以下开源项目：

### 理论参考

- **Dynamo**（Amazon）— 乐观并发控制，版本号 CAS
- **Kleppmann "Designing Data-Intensive Applications"** — Fencing token 防脑裂写覆盖
- **Raft**（Ongaro & Ousterhout）— Per-UUID 有序操作日志
- **Spanner**（Google）— 用自增 ID 而非墙钟时间戳排序（避免跨服务器时钟偏移）

### 依赖项目

| 项目 | 用途 |
|---|---|
| [Redisson](https://github.com/redisson/redisson) | Redis 分布式协调（RTopic Pub/Sub、RStream 可靠事件） |
| [jOOQ](https://github.com/jOOQ/jOOQ) | 类型安全 SQL DSL（OCC + fencing token CAS） |
| [LZ4 Java](https://github.com/yawkat/lz4-java)（at.yawk.lz4 fork） | LZ4 压缩 |
| [HikariCP](https://github.com/brettwooldridge/HikariCP) | JDBC 连接池 |
| [Sparrow-NBT](https://github.com/MrCraftCod/Sparrow) | NBT 序列化 |
| [Caffeine](https://github.com/ben-manes/caffeine) | 高性能缓存 |
| [PaperMC](https://papermc.io/) | Paper / Folia 服务端 API |
| [Velocity](https://papermc.io/software/velocity) | Velocity 代理 API |

## 许可

MIT License
