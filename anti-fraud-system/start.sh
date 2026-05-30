#!/bin/bash
# ============================================
# 反欺诈系统 - 项目启动/停止脚本
# 用法:
#   ./start.sh start     启动所有服务
#   ./start.sh stop      停止所有服务
#   ./start.sh restart   重启所有服务
#   ./start.sh status    查看服务状态
#   ./start.sh kb        仅启动知识库服务
# ============================================

set -e

DEPLOY_DIR="$(cd "$(dirname "$0")/anti-fraud-deployment" && pwd)"
COMPOSE_FILE="$DEPLOY_DIR/docker-compose.yml"

# 自动识别 docker compose 命令
if docker compose version &>/dev/null; then
    DOCKER_COMPOSE="docker compose"
elif docker-compose version &>/dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "错误: 未找到 docker compose 或 docker-compose"
    exit 1
fi

start_all() {
    echo "======================================"
    echo "  启动反欺诈系统所有服务"
    echo "======================================"
    cd "$DEPLOY_DIR"

    # 先停掉旧容器（如果有）
    $DOCKER_COMPOSE down 2>/dev/null || true

    # 启动所有服务
    $DOCKER_COMPOSE up -d

    echo ""
    echo "等待核心服务就绪..."
    sleep 10

    echo ""
    echo "======================================"
    echo "  服务状态"
    echo "======================================"
    show_status

    echo ""
    echo "======================================"
    echo "  访问地址"
    echo "======================================"
    echo "  API网关:       http://localhost:8888"
    echo "  Nacos控制台:   http://localhost:8848/nacos"
    echo "  Prometheus:    http://localhost:9090"
    echo "  Grafana:       http://localhost:3000"
    echo "  知识库服务:     http://localhost:8088"
    echo "  智能体服务:     http://localhost:8080"
    echo "  Flink仪表盘:   http://localhost:8081"
    echo "======================================"
}

stop_all() {
    echo "停止所有服务..."
    cd "$DEPLOY_DIR"
    $DOCKER_COMPOSE down
    echo "所有服务已停止"
}

restart_all() {
    stop_all
    start_all
}

start_kb() {
    echo "仅启动知识库服务..."
    cd "$DEPLOY_DIR"

    # 启动依赖服务
    $DOCKER_COMPOSE up -d mysql milvus etcd minio
    sleep 5

    # 启动知识库
    $DOCKER_COMPOSE up -d knowledge-base

    echo "知识库服务已启动: http://localhost:8088"
}

show_status() {
    cd "$DEPLOY_DIR"
    $DOCKER_COMPOSE ps
}

# 主逻辑
case "${1:-}" in
    start)
        start_all
        ;;
    stop)
        stop_all
        ;;
    restart)
        restart_all
        ;;
    status)
        show_status
        ;;
    kb)
        start_kb
        ;;
    *)
        echo "用法: $0 {start|stop|restart|status|kb}"
        echo ""
        echo "  start    - 启动所有服务"
        echo "  stop     - 停止所有服务"
        echo "  restart  - 重启所有服务"
        echo "  status   - 查看服务状态"
        echo "  kb       - 仅启动知识库服务"
        exit 1
        ;;
esac
