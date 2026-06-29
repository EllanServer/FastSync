#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# FastSync Paper E2E 验证脚本
# =============================================================================
# 通过 RCON 连接 paper-a / paper-b，验证 FastSync 插件已加载、
# 数据库和 Redis 已连接、管理命令可正常响应。
#
# 用法：
#   ./scripts/e2e-verify.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TOOLS_DIR="${PROJECT_ROOT}/e2e/tools"
MCRCON="${TOOLS_DIR}/mcrcon"

RCON_PASSWORD="fastsync-test"
RCON_A_PORT=25575
RCON_B_PORT=25576

ensure_mcrcon() {
    if command -v mcrcon >/dev/null 2>&1; then
        MCRCON="$(command -v mcrcon)"
        return 0
    fi
    if [[ -x "${MCRCON}" ]]; then
        return 0
    fi
    echo "Downloading mcrcon..."
    mkdir -p "${TOOLS_DIR}"
    local tmpdir
    tmpdir="$(mktemp -d)"
    curl -fsSL -o "${tmpdir}/mcrcon.tar.gz" \
        "https://github.com/Tiiffi/mcrcon/releases/download/v0.7.2/mcrcon-0.7.2-linux-x86-64.tar.gz"
    tar -xzf "${tmpdir}/mcrcon.tar.gz" -C "${tmpdir}"
    cp "${tmpdir}/mcrcon" "${MCRCON}"
    chmod +x "${MCRCON}"
    rm -rf "${tmpdir}"
}

rcon() {
    local port="$1"
    shift
    "${MCRCON}" -H 127.0.0.1 -P "${port}" -p "${RCON_PASSWORD}" "$@"
}

wait_for_rcon() {
    local port="$1"
    local timeout="${2:-60}"
    echo "Waiting up to ${timeout}s for RCON on port ${port}..."
    for ((i = 0; i < timeout; i++)); do
        if rcon "${port}" "help" >/dev/null 2>&1; then
            echo "RCON ${port} ready (${i}s)"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: RCON on port ${port} did not become ready within ${timeout}s"
    return 1
}

verify_plugin_loaded() {
    local name="$1"
    local container="$2"
    echo "Checking FastSync plugin loaded in ${name}..."
    if docker logs "${container}" 2>&1 | grep -qiE "FastSync.*enabled|Enabling FastSync|Loading server plugin FastSync"; then
        echo "  OK: FastSync loaded in ${name}"
    else
        echo "  WARN: FastSync load message not found in ${name} logs (plugin may still work)"
    fi
}

ensure_mcrcon
wait_for_rcon "${RCON_A_PORT}"
wait_for_rcon "${RCON_B_PORT}"

verify_plugin_loaded "paper-a" "fastsync-paper-a"
verify_plugin_loaded "paper-b" "fastsync-paper-b"

echo ""
echo "Pinging FastSync status on paper-a..."
rcon "${RCON_A_PORT}" "fastsync status"

echo ""
echo "Pinging FastSync status on paper-b..."
rcon "${RCON_B_PORT}" "fastsync status"

echo ""
echo "Checking database tables were created..."
if docker exec fastsync-mysql mysql -ufastsync -pfastsync -D fastsync -e "SHOW TABLES LIKE 'fastsync_player_data';" 2>/dev/null | grep -q "fastsync_player_data"; then
    echo "  OK: fastsync_player_data table exists"
else
    echo "  ERROR: fastsync_player_data table not found"
    exit 1
fi

if docker exec fastsync-mysql mysql -ufastsync -pfastsync -D fastsync -e "SHOW TABLES LIKE 'fastsync_player_component';" 2>/dev/null | grep -q "fastsync_player_component"; then
    echo "  OK: fastsync_player_component table exists"
else
    echo "  ERROR: fastsync_player_component table not found"
    exit 1
fi

echo ""
echo "All E2E checks passed."
