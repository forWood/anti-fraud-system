#!/bin/bash
# ============================================================================
# 反欺诈与反恐融资风险识别智能体系统 - Linux 一键部署脚本
# 用法: chmod +x deploy.sh && ./deploy.sh
# ============================================================================
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $1"; }
warn() { echo -e "${YELLOW}[警告]${NC} $1"; }
err()  { echo -e "${RED}[错误]${NC} $1"; }

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$SCRIPT_DIR/anti-fraud-deployment"

log "========== 反欺诈系统一键部署 =========="

# -------------------------------------------------------------------
# 1. 检查 Docker
# -------------------------------------------------------------------
log "1/6 检查 Docker 环境..."
if ! command -v docker &>/dev/null; then
    err "Docker 未安装，请先安装 Docker"
    exit 1
fi

DOCKER_VERSION=$(docker compose version 2>/dev/null || docker-compose --version 2>/dev/null)
log "   Docker: $(docker --version | awk '{print $3}' | tr -d ',')"
log "   Compose: $DOCKER_VERSION"

# 判断 compose 命令
if docker compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

# -------------------------------------------------------------------
# 2. 配置 Docker 镜像加速
# -------------------------------------------------------------------
log "2/6 配置镜像加速..."
if [ -f /etc/docker/daemon.json ]; then
    warn "daemon.json 已存在，跳过"
else
    sudo mkdir -p /etc/docker
    sudo tee /etc/docker/daemon.json > /dev/null <<-'DAEMON'
{
  "registry-mirrors": [
    "https://docker.m.daocloud.io",
    "https://docker.1panel.live",
    "https://mirror.ccs.tencentyun.com"
  ],
  "max-concurrent-downloads": 10
}
DAEMON
    sudo systemctl daemon-reload
    sudo systemctl restart docker
    log "   镜像加速已配置"
fi

# -------------------------------------------------------------------
# 3. 拉取所有镜像
# -------------------------------------------------------------------
log "3/6 拉取 Docker 镜像（耗时较长，请耐心等待）..."

IMAGES=(
    "redis:7.0-alpine"
    "mysql:8.0.33"
    "nacos/nacos-server:v2.2.3"
    "clickhouse/clickhouse-server:23.8-alpine"
    "prom/prometheus:v2.47.0"
    "grafana/grafana:10.1.0"
    "confluentinc/cp-zookeeper:7.5.0"
    "confluentinc/cp-kafka:7.5.0"
    "quay.io/coreos/etcd:v3.5.5"
    "minio/minio:RELEASE.2023-03-20T20-16-18Z"
    "milvusdb/milvus:v2.3.3"
    "flink:1.16.3-scala_2.12"
)

for img in "${IMAGES[@]}"; do
    if docker image inspect "$img" &>/dev/null 2>&1; then
        log "   [跳过] $img 已存在"
    else
        log "   [拉取] $img ..."
        docker pull "$img" || warn "拉取失败: $img，将跳过"
    fi
done

# -------------------------------------------------------------------
# 4. 构建 Java 微服务（仅当有 Maven 时）
# -------------------------------------------------------------------
log "4/6 构建微服务..."
if command -v mvn &>/dev/null; then
    log "   检测到 Maven，开始编译..."
    cd "$SCRIPT_DIR"
    if mvn clean package -DskipTests -T 4 2>&1 | grep -E '(BUILD|ERROR|WARNING|Compiling)'; then
        log "   ✅ Maven 编译完成"
    else
        warn "编译可能有问题，将使用已有 JAR"
    fi
else
    warn "未安装 Maven，跳过编译（使用已有 JAR 包）"
fi

# -------------------------------------------------------------------
# 5. 启动所有容器
# -------------------------------------------------------------------
log "5/6 启动容器..."
cd "$DEPLOY_DIR"

# 先停掉旧容器
$COMPOSE_CMD down 2>/dev/null || true

# 启动
$COMPOSE_CMD up -d

# 等基础服务就绪
log "   等待 MySQL 就绪..."
for i in $(seq 1 30); do
    if docker exec risk-mysql mysqladmin ping -h localhost --silent 2>/dev/null; then
        log "   MySQL 已就绪"
        break
    fi
    sleep 2
done

log "   等待 Nacos 就绪..."
for i in $(seq 1 30); do
    if curl -s http://localhost:8848/nacos/ &>/dev/null; then
        log "   Nacos 已就绪"
        break
    fi
    sleep 2
done

# -------------------------------------------------------------------
# 6. 验证结果
# -------------------------------------------------------------------
log "6/6 验证部署结果..."
echo ""
echo "========================================"
echo "  容器状态"
echo "========================================"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | head -20

echo ""
echo "========================================"
echo "  访问地址"
echo "========================================"
IP=$(hostname -I | awk '{print $1}')
echo "  Nacos 注册中心:  http://${IP}:8848/nacos     (nacos/nacos)"
echo "  Grafana 监控:    http://${IP}:3000           (admin/admin123)"
echo "  Prometheus:      http://${IP}:9090"
echo "  API 网关:        http://${IP}:8888"
echo "  MySQL:           ${IP}:3306     (risk_user/risk_pass_2026)"
echo "  Redis:           ${IP}:6379     (无密码)"
echo "  ClickHouse:      http://${IP}:8123"
echo ""

# Nacos 服务注册检查
REG=$(curl -s "http://localhost:8848/nacos/v1/ns/service/list?pageNo=1&pageSize=20" 2>/dev/null)
if echo "$REG" | grep -q "api-gateway"; then
    log "✅ Nacos 服务注册正常，微服务已上线"
else
    warn "Nacos 中暂未发现微服务，可能仍在启动中，等几分钟再检查"
fi

log "========== 部署完成 =========="
