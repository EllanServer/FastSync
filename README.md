# FastSync

FastSync 是一个跨服务器的 Minecraft 玩家数据同步插件，支持 Paper / Folia 后端和 Velocity 代理端。它把玩家数据序列化为 NBT 字节流，再用 LZ4 压缩写入 MySQL，通过 Redis 协调跨服锁，保证同一时刻只有一个服务端持有某玩家的写入权。

## 功能

- **跨服务器同步背包、末影箱、血量、饥饿值、经验、药水效果、游戏模式、火焰 tick、氧气值等**
- **LZ4 压缩**：相比 base64 字符串方案体积更小、速度更快
- **Dynamo 风格乐观并发控制**：版本号 + fencing token，防止脑裂写覆盖
- **Redis 锁协调**：支持 Pub/Sub 通知、Redis Streams 关键事件恢复
- **冲突快照**：写冲突时保存旧版本快照，方便人工恢复
- **操作日志**：每个玩家独立的顺序日志，用于审计和问题排查
- **Paper / Folia 双兼容**：自动检测 Folia 调度器并切换
- **Velocity 代理插件**：记录玩家所在子服和切换时间

## 支持的版本

| 运行环境 | 版本 |
|---|---|
| Paper / Folia | 1.21.4 及兼容 1.21.x 的版本 |
| Velocity | 3.4.0-SNAPSHOT+ |
| JDK | 21+ |

## 安装

### 1. 准备依赖环境

FastSync 需要以下外部服务：

- **MySQL 8.0+**（数据持久化）
- **Redis 6.2+**（可选但强烈建议，用于跨服锁协调）

### 2. 安装后端插件（Paper / Folia）

从 `build/libs/` 复制以下文件到 **每个 Paper/Folia 服务端的 `plugins/` 目录**：

```
FastSync-1.0.0.jar
lib/
├── HikariCP-5.1.0.jar
├── lz4-java-1.8.0.jar
├── mysql-connector-j-9.0.0.jar
├── lettuce-core-6.4.0.RELEASE.jar
├── caffeine-3.2.3.jar
├── reactive-streams-1.0.4.jar
└── ...（其他运行时依赖）
```

运行时依赖放在 `lib/` 目录下，`FastSync-1.0.0.jar` 的 MANIFEST 已配置 `Class-Path`，无需 shade 或重定位。如果目录结构不一致，Paper 可能找不到依赖。

### 3. 安装 Velocity 代理插件（可选）

把 `build/libs/FastSync-Proxy-1.0.0.jar` 放到 Velocity 的 `plugins/` 目录。它目前用于追踪玩家当前所在子服和切换时间，未来会扩展为强制路由等待/重定向功能。

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

## 管理命令

```
/fastsync reload    # 重载配置
/fastsync status    # 查看当前锁、缓存、连接状态
/fastsync debug     # 开关调试模式
/fastsync saveall   # 强制保存所有在线玩家数据
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

- `FastSync-1.0.0.jar`：Paper/Folia 后端插件
- `FastSync-Proxy-1.0.0.jar`：Velocity 代理插件

## 常见问题

**Q: 不使用 Redis 可以吗？**
A: 可以。关闭 `redis.enabled` 后，插件会退化为数据库轮询方式获取锁，延迟和数据库负载都会增加。

**Q: 玩家跨服时数据被覆盖怎么办？**
A: 插件使用版本号 CAS；如果检测到冲突，默认会把旧数据保存为快照（`conflict.recovery-strategy: snapshot`），不会直接覆盖。可关闭 `operation-log.enabled` 减少日志写入。

**Q: Folia 上有没有注意事项？**
A: 插件已自动适配 Folia 的区域调度器。无需额外配置。

## 许可

MIT License
