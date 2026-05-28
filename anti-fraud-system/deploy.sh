#!/bin/bash
# ============================================================================
# 反欺诈系统 - Linux 一键部署脚本
# 用途: 打包项目 + 构建Docker镜像 + 启动所有服务
# 前提: Java 8 + Maven + Docker 已安装
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$SCRIPT_DIR/anti-fraud-deployment"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
step()  { echo -e "\n${CYAN}============================================================${NC}"; echo -e "${CYAN}[==>] $*${NC}"; echo -e "${CYAN}============================================================${NC}"; }

# ============================================================================
# 1. 环境检查
# ============================================================================
step "Step 1/5: 检查环境..."

if ! command -v java &>/dev/null; then
    error "Java 未安装！请执行: apt install openjdk-8-jdk 或 yum install java-1.8.0-openjdk"
    exit 1
fi
info "Java: $(java -version 2>&1 | head -1)"

if ! command -v mvn &>/dev/null; then
    error "Maven 未安装！请执行: apt install maven 或 yum install maven"
    exit 1
fi
info "Maven: $(mvn --version 2>&1 | head -1)"

if ! docker info &>/dev/null; then
    error "Docker 未运行！请先启动 Docker: systemctl start docker"
    exit 1
fi
info "Docker: $(docker --version)"
info "Docker Compose: $(docker compose version 2>/dev/null || docker-compose --version 2>/dev/null)"

# ============================================================================
# 2. Maven 打包
# ============================================================================
step "Step 2/5: Maven 编译打包 (跳过测试)..."

cd "$SCRIPT_DIR"
mvn clean package -DskipTests

if [ $? -ne 0 ]; then
    error "Maven 打包失败！请检查 build_log.txt"
    exit 1
fi
info "打包完成！"

# 检查关键 JAR 是否生成
JARS=(
    "anti-fraud-agent/target/anti-fraud-agent-2.0.0.jar"
    "anti-fraud-microservices/transaction-monitor/target/transaction-monitor-2.0.0.jar"
    "anti-fraud-microservices/alert-management/target/alert-management-2.0.0.jar"
    "anti-fraud-microservices/case-management/target/case-management-2.0.0.jar"
    "anti-fraud-microservices/report-generation/target/report-generation-2.0.0.jar"
    "anti-fraud-microservices/api-gateway/target/api-gateway-2.0.0.jar"
)
for jar in "${JARS[@]}"; do
    if [ -f "$jar" ]; then
        info "  ✓ $jar"
    else
        warn "  ✗ $jar 未找到（将跳过对应服务）"
    fi
done

# ============================================================================
# 3. 停止旧服务
# ============================================================================
step "Step 3/5: 停止旧容器..."

cd "$DEPLOY_DIR"
docker compose down 2>/dev/null || true
info "旧容器已清理"

# ============================================================================
# 4. 构建并启动
# ============================================================================
step "Step 4/5: 构建镜像并启动服务..."

# 拉取基础镜像 (可选，加速后续构建)
info "预拉取基础镜像..."
docker pull openjdk:8-jre-slim &
docker pull mysql:8.0.33 &
docker pull redis:7.0-alpine &
docker pull confluentinc/cp-kafka:7.5.0 &
docker pull confluentinc/cp-zookeeper:7.5.0 &
wait
info "基础镜像拉取完成"

# 构建并启动
info "启动所有服务 (docker compose up --build)..."
docker compose up -d --build

if [ $? -ne 0 ]; then
    error "启动失败！请查看日志: docker compose logs"
    exit 1
fi

# ============================================================================
# 5. 等待服务就绪
# ============================================================================
step "Step 5/5: 等待服务就绪..."

info "等待 MySQL 健康检查..."
for i in $(seq 1 60); do
    STATUS=$(docker inspect risk-mysql --format='{{.State.Health.Status}}' 2>/dev/null || echo "")
    if [ "$STATUS" = "healthy" ]; then
        info "MySQL 已就绪 (${i}s)"
        break
    fi
    sleep 5
done

# 验证数据库初始化
info "验证数据库初始化..."
docker exec risk-mysql mysql -u risk_user -prisk_pass_2026 -e "SHOW TABLES;" risk_db 2>/dev/null | head -5
info "数据库验证完成"

# 等待微服务启动
info "等待微服务启动 (30s)..."
sleep 30

# 健康检查
info "服务健康检查..."
echo ""

check_health() {
    local name=$1
    local url=$2
    local code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 "$url" 2>/dev/null || echo "000")
    if [ "$code" = "200" ]; then
        echo -e "  ${GREEN}✓${NC} $name UP ($url)"
    else
        echo -e "  ${YELLOW}○${NC} $name 启动中... ($url, HTTP $code)"
    fi
}

check_health "AI Agent"          "http://localhost:8080/actuator/health"
check_health "Transaction Monitor" "http://localhost:8081/actuator/health"
check_health "Alert Management"  "http://localhost:8082/actuator/health"
check_health "Case Management"   "http://localhost:8083/actuator/health"
check_health "Report Generation" "http://localhost:8084/actuator/health"
check_health "API Gateway"       "http://localhost:8888/actuator/health"
check_health "Prometheus"        "http://localhost:9090"
check_health "Grafana"           "http://localhost:3000"

# ============================================================================
# 完成
# ============================================================================
echo ""
echo "========================================"
echo -e "  ${GREEN}反欺诈系统部署完成！${NC}"
echo "========================================"
echo ""
echo -e "  ${CYAN}核心服务:${NC}"
echo "    AI Agent (决策引擎): http://localhost:8080"
echo "    交易监控:            http://localhost:8081"
echo "    预警管理:            http://localhost:8082"
echo "    案件管理:            http://localhost:8083"
echo "    报告生成:            http://localhost:8084"
echo "    API 网关:            http://localhost:8888"
echo ""
echo -e "  ${CYAN}监控面板:${NC}"
echo "    Grafana:    http://localhost:3000 (admin/admin123)"
echo "    Prometheus: http://localhost:9090"
echo ""
echo -e "  ${CYAN}基础设施:${NC}"
echo "    MySQL:   localhost:3306 (risk_user / risk_pass_2026)"
echo "    Redis:   localhost:6379"
echo "    Kafka:   localhost:9092"
echo ""
echo -e "  ${CYAN}常用命令:${NC}"
echo "    docker compose ps                 查看服务状态"
echo "    docker compose logs -f [服务名]    查看实时日志"
echo "    docker compose logs -f anti-fraud-agent  只看Agent日志"
echo "    docker compose restart [服务名]    重启指定服务"
echo "    docker compose down               停止所有服务"
echo ""
echo -e "  ${CYAN}测试 API:${NC}"
echo "    curl -X POST http://localhost:8080/api/v1/agent/assess \\"
echo "      -H 'Content-Type: application/json' \\"
echo "      -d @test-transaction.json"
echo ""
