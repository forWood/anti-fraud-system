# ============================================================================
# 反欺诈系统 - 快速启动脚本
# 前提: 已执行 deploy.ps1，Docker/Java/Maven 已安装
# ============================================================================

param(
    [switch]$Build,      # 是否重新构建
    [switch]$Stop,       # 停止所有服务
    [switch]$Status,     # 查看状态
    [switch]$Demo        # 发送测试交易
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$deployDir = "$scriptDir\anti-fraud-deployment"

function Write-Step { Write-Host "`n[==>] $args" -ForegroundColor Cyan }
function Write-OK   { Write-Host "  [OK] $args" -ForegroundColor Green }
function Write-Err  { Write-Host "  [ERR] $args" -ForegroundColor Red }

# ============================================================================
# 检查环境
# ============================================================================
Write-Step "检查环境..."
$dockerOk = Get-Command docker -ErrorAction SilentlyContinue
if (-not $dockerOk) {
    Write-Err "Docker 未安装或未运行！请先启动 Docker Desktop"
    exit 1
}
docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Err "Docker 未运行！请启动 Docker Desktop"
    exit 1
}
Write-OK "Docker 运行中"

# ============================================================================
# 处理参数
# ============================================================================
if ($Stop) {
    Write-Step "停止所有服务..."
    Set-Location $deployDir
    docker compose down
    Write-OK "所有服务已停止"
    exit 0
}

if ($Status) {
    Set-Location $deployDir
    docker compose ps
    exit 0
}

if ($Demo) {
    Write-Step "发送测试交易到智能体..."
    $body = @{
        transactionId = "TX-DEMO-" + (Get-Date -Format "HHmmss")
        accountIdHash = "abc123"
        amount = "600000.00"
        transactionType = "TRANSFER"
        channelCode = "EBANK"
        ipAddress = "185.220.101.50"
        ipRiskLevel = "TOR"
        isEmulator = $false
        isRooted = $false
        isFirstDevice = $true
        deviceId = "NEW-DEVICE-01"
        isCrossBorder = $true
        counterpartyCountry = "IR"
        isNightTime = $true
        enableKnowledgeSearch = $true
        knowledgeTopK = 5
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8080/api/v1/monitor/assess" -Method Post -Body $body -ContentType "application/json"
        Write-Host ""
        Write-Host "风险评估结果:" -ForegroundColor Yellow
        $response | ConvertTo-Json -Depth 5 | Write-Host -ForegroundColor White
        Write-Host ""
        Write-Host "说明:" -ForegroundColor Cyan
        Write-Host "  - riskScore: 综合风险评分 (0-100)" -ForegroundColor Green
        Write-Host "  - riskLevel: 风险等级 (HIGH/MEDIUM/LOW)" -ForegroundColor Green
        Write-Host "  - decision: 决策建议 (BLOCK/REVIEW/PASS)" -ForegroundColor Green
        Write-Host "  - matchedRules: 命中的风控规则列表" -ForegroundColor Green
    } catch {
        Write-Err "请求失败: $_"
        Write-Warn "请确保 anti-fraud-agent 服务已启动 (docker compose ps)"
    }
    exit 0
}

# ============================================================================
# 启动服务
# ============================================================================
Write-Step "启动基础设施 + 微服务..."
Set-Location $deployDir

# 启动 Docker Compose
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Err "启动失败！"
    exit 1
}

# 等待服务就绪
Write-Host "正在等待服务就绪..." -ForegroundColor Yellow
$maxWait = 120
$waited = 0
while ($waited -lt $maxWait) {
    $mysqlHealthy = docker inspect risk-mysql --format='{{.State.Health.Status}}' 2>$null
    if ($mysqlHealthy -eq "healthy") {
        Write-OK "MySQL 就绪"
        break
    }
    Start-Sleep -Seconds 5
    $waited += 5
    Write-Host "  等待中... ($waited s)"
}

# 验证数据库
Write-Step "验证数据库..."
$ruleCount = docker exec risk-mysql mysql -u risk_user -prisk_pass_2026 -e "SELECT COUNT(*) as cnt FROM risk_db.rule_definition" 2>$null | Select-String -Pattern '\d+'
Write-OK "规则数量: $ruleCount"
$caseCount = docker exec risk-mysql mysql -u risk_user -prisk_pass_2026 -e "SELECT COUNT(*) as cnt FROM risk_db.fraud_case" 2>$null | Select-String -Pattern '\d+'
Write-OK "案例数量: $caseCount"

# 等待微服务启动
Write-Host "等待微服务启动..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# 健康检查
Write-Step "服务健康检查..."
$services = @(
    @{Name="Agent"; Url="http://localhost:8080/actuator/health"},
    @{Name="Gateway"; Url="http://localhost:8888/actuator/health"},
    @{Name="Monitor"; Url="http://localhost:8081/api/v1/monitor/health"}
)

foreach ($svc in $services) {
    try {
        $result = Invoke-RestMethod -Uri $svc.Url -TimeoutSec 5 -ErrorAction Stop
        Write-OK "$($svc.Name): UP"
    } catch {
        Write-Err "$($svc.Name): DOWN ($_)"
    }
}

# ============================================================================
# 构建项目 (如需)
# ============================================================================
if ($Build) {
    Write-Step "构建项目..."
    Set-Location $scriptDir
    
    $mvnOk = Get-Command mvn -ErrorAction SilentlyContinue
    if (-not $mvnOk) {
        Write-Err "Maven 未找到！请先执行 deploy.ps1"
        exit 1
    }
    
    mvn clean package -DskipTests
    if ($LASTEXITCODE -ne 0) {
        Write-Err "构建失败！"
        exit 1
    }
    Write-OK "构建完成"
}

# ============================================================================
# 完成提示
# ============================================================================
Write-Host "`n========================================" -ForegroundColor Green
Write-Host " 反欺诈系统已启动！" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "  入口地址:" -ForegroundColor Cyan
Write-Host "    API Gateway:  http://localhost:8888"
Write-Host "    AI Agent:     http://localhost:8080"
Write-Host "    交易监控:     http://localhost:8081"
Write-Host "    预警管理:     http://localhost:8082"
Write-Host "    案例管理:     http://localhost:8083"
Write-Host "    报告生成:     http://localhost:8084"
Write-Host ""
Write-Host "  监控地址:" -ForegroundColor Cyan
Write-Host "    Grafana:      http://localhost:3000 (admin/admin123)"
Write-Host "    Prometheus:   http://localhost:9090"
Write-Host ""
Write-Host "  常用命令:" -ForegroundColor Cyan
Write-Host "    .\start.ps1 -Status     查看服务状态"
Write-Host "    .\start.ps1 -Demo       发送测试交易"
Write-Host "    .\start.ps1 -Stop       停止所有服务"
Write-Host "    .\start.ps1 -Build      重新构建并启动"
Write-Host "    docker compose logs -f  查看实时日志"
