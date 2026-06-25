#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# FastSync Paper 端到端测试脚本
# =============================================================================
# 启动两套真实的 Paper 1.21.4 服务器，并通过 RCON 验证 FastSync 加载、
# 数据库连接和基础命令响应。需要本地安装 Docker + docker compose。
#
# 用法：
#   ./scripts/run-paper-e2e.sh
#
# 退出状态：
#   0 - 所有检查通过
#   非 0 - 构建失败、服务启动超时或验证失败
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.paper-test.yml"
PLUGINS_DIR="${PROJECT_ROOT}/e2e/plugins"

cd "${PROJECT_ROOT}"

echo "[1/5] Building FastSync plugin..."
./gradlew build --no-daemon -x test

MAIN_JAR="FastSync-1.0.0.jar"

if [[ ! -f "build/libs/${MAIN_JAR}" ]]; then
    echo "ERROR: build/libs/${MAIN_JAR} not found"
    exit 1
fi

echo "[2/5] Preparing e2e/plugins directory..."
rm -rf "${PLUGINS_DIR}"
mkdir -p "${PLUGINS_DIR}"

# Single JAR: Sparrow libs are shaded in, Maven Central deps are
# auto-downloaded by Paper via plugin.yml libraries.
cp "build/libs/${MAIN_JAR}" "${PLUGINS_DIR}/"

echo "Plugin: ${PLUGINS_DIR}/${MAIN_JAR}"

# The itzg/minecraft-server container runs as uid 1000 and needs write access
# to /data/plugins so Paper can create its .paper-remapped cache.
chmod -R 777 "${PLUGINS_DIR}"

echo "[3/5] Starting MySQL, Redis and two Paper servers..."
docker compose -f "${COMPOSE_FILE}" down -v || true
docker compose -f "${COMPOSE_FILE}" up -d

wait_for_log() {
    local container="$1"
    local pattern="$2"
    local timeout="${3:-180}"
    echo "Waiting up to ${timeout}s for ${container} to log: ${pattern}"
    for ((i = 0; i < timeout; i++)); do
        if docker logs "${container}" 2>&1 | grep -qE "${pattern}"; then
            echo "${container} is ready (${i}s)"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: ${container} did not become ready within ${timeout}s"
    docker logs "${container}" --tail 50 || true
    return 1
}

echo "[4/5] Waiting for Paper servers to finish startup..."
wait_for_log "fastsync-paper-a" "Done \([0-9]+\.[0-9]+s\)! For help, type \"help\""
wait_for_log "fastsync-paper-b" "Done \([0-9]+\.[0-9]+s\)! For help, type \"help\""

echo "[5/5] Running E2E verification..."
"${SCRIPT_DIR}/e2e-verify.sh"

echo ""
echo "============================================================================="
echo "E2E test passed. Servers are still running:"
echo "  paper-a: localhost:25565 (RCON 25575)"
echo "  paper-b: localhost:25566 (RCON 25576)"
echo ""
echo "To stop: docker compose -f ${COMPOSE_FILE} down -v"
echo "============================================================================="
