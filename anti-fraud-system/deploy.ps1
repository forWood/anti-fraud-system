# ============================================================================
# 反欺诈与反恐融资风险识别智能体系统 - 一键部署脚本
# 所有软件安装到 D 盘
# 用法: 以管理员身份运行 PowerShell，执行 .\deploy.ps1
# ============================================================================

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# ============================================================================
# 配置
# ============================================================================
$JDK_VERSION = "8u432b06"
$JDK_DOWNLOAD_URL = "https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u432-b06/OpenJDK8U-jdk_x64_windows_hotspot_8u432b06.zip"
$JDK_INSTALL_DIR = "D:\tools\java\jdk8"
$JDK_ZIP = "D:\tools\java\jdk8.zip"

$MAVEN_VERSION = "3.9.9"
$MAVEN_DOWNLOAD_URL = "https://dlcdn.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.zip"
$MAVEN_INSTALL_DIR = "D:\tools\maven\apache-maven-$MAVEN_VERSION"
$MAVEN_ZIP = "D:\tools\maven\maven.zip"
$MAVEN_REPO = "D:\tools\maven\repository"

$DOCKER_DATA_DIR = "D:\docker-data"

# ============================================================================
# 颜色输出函数
# ============================================================================
function Write-Step { Write-Host "`n[==>] $args" -ForegroundColor Cyan }
function Write-OK   { Write-Host "  [OK] $args" -ForegroundColor Green }
function Write-Warn { Write-Host "  [WARN] $args" -ForegroundColor Yellow }
function Write-Err  { Write-Host "  [ERR] $args" -ForegroundColor Red }

# ============================================================================
# 1. 检查管理员权限
# ============================================================================
Write-Step "Step 1/7: 检查管理员权限..."
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Err "请以管理员身份运行此脚本！"
    Write-Err "右键 PowerShell -> 以管理员身份运行 -> cd 到项目目录 -> .\deploy.ps1"
    exit 1
}
Write-OK "管理员权限已确认"

# ============================================================================
# 2. 创建 D 盘目录
# ============================================================================
Write-Step "Step 2/7: 创建 D 盘安装目录..."
$dirs = @("D:\tools", "D:\tools\java", "D:\tools\maven", $DOCKER_DATA_DIR, "$MAVEN_REPO")
foreach ($d in $dirs) {
    if (-not (Test-Path $d)) {
        New-Item -ItemType Directory -Path $d -Force | Out-Null
        Write-OK "创建目录: $d"
    } else {
        Write-OK "目录已存在: $d"
    }
}

# ============================================================================
# 3. 下载并安装 JDK 8
# ============================================================================
Write-Step "Step 3/7: 安装 JDK 8 (Temurin)..."
$javaCmd = "$JDK_INSTALL_DIR\bin\java.exe"
if (Test-Path $javaCmd) {
    Write-OK "JDK 8 已安装: $(& $javaCmd -version 2>&1 | Select-Object -First 1)"
} else {
    # 清理旧的解压目录
    if (Test-Path $JDK_INSTALL_DIR) { Remove-Item -Recurse -Force $JDK_INSTALL_DIR }
    
    # 下载
    if (-not (Test-Path $JDK_ZIP) -or (Get-Item $JDK_ZIP).Length -lt 1000000) {
        Write-Host "  正在下载 JDK 8 (~100MB)，请稍候..."
        Remove-Item $JDK_ZIP -ErrorAction SilentlyContinue
        try {
            curl.exe --noproxy "*" -L --connect-timeout 30 --max-time 600 -o $JDK_ZIP $JDK_DOWNLOAD_URL
        } catch {
            # 备用: 使用华为云镜像
            Write-Warn "主下载失败，尝试备用镜像..."
            curl.exe --noproxy "*" -L --connect-timeout 30 --max-time 600 -o $JDK_ZIP "https://mirrors.huaweicloud.com/openjdk/8u432/jdk-8u432-windows-x64.zip"
        }
    }
    
    if (-not (Test-Path $JDK_ZIP) -or (Get-Item $JDK_ZIP).Length -lt 50000000) {
        Write-Err "JDK 下载失败！请手动下载 OpenJDK 8 并解压到: $JDK_INSTALL_DIR"
        Write-Err "下载地址: $JDK_DOWNLOAD_URL"
        Write-Err "或访问: https://adoptium.net/download/ 选择 Temurin 8"
        throw "JDK download failed"
    }
    
    # 解压
    Write-Host "  正在解压 JDK..."
    Expand-Archive -Path $JDK_ZIP -DestinationPath "D:\tools\java" -Force
    
    # Temurin zip 解压后目录名可能是 jdk8u432-b06，需要重命名
    $extractedDir = Get-ChildItem "D:\tools\java" -Directory | Where-Object { $_.Name -like "*jdk*" -or $_.Name -like "*temurin*" } | Select-Object -First 1
    if ($extractedDir -and $extractedDir.FullName -ne $JDK_INSTALL_DIR) {
        if (Test-Path $JDK_INSTALL_DIR) { Remove-Item -Recurse -Force $JDK_INSTALL_DIR }
        Rename-Item $extractedDir.FullName "jdk8"
    }
    
    Write-OK "JDK 8 安装完成: $(& $javaCmd -version 2>&1 | Select-Object -First 1)"
}

# ============================================================================
# 4. 下载并安装 Maven
# ============================================================================
Write-Step "Step 4/7: 安装 Maven $MAVEN_VERSION..."
$mvnCmd = "$MAVEN_INSTALL_DIR\bin\mvn.cmd"
if (Test-Path $mvnCmd) {
    Write-OK "Maven 已安装"
} else {
    # 下载
    if (-not (Test-Path $MAVEN_ZIP) -or (Get-Item $MAVEN_ZIP).Length -lt 1000000) {
        Write-Host "  正在下载 Maven (~9MB)，请稍候..."
        Remove-Item $MAVEN_ZIP -ErrorAction SilentlyContinue
        try {
            curl.exe --noproxy "*" -L --connect-timeout 30 --max-time 300 -o $MAVEN_ZIP $MAVEN_DOWNLOAD_URL
        } catch {
            # 备用: 阿里云镜像
            Write-Warn "主下载失败，尝试备用镜像..."
            curl.exe --noproxy "*" -L --connect-timeout 30 --max-time 300 -o $MAVEN_ZIP "https://mirrors.aliyun.com/apache/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.zip"
        }
    }
    
    if (-not (Test-Path $MAVEN_ZIP) -or (Get-Item $MAVEN_ZIP).Length -lt 1000000) {
        Write-Err "Maven 下载失败！请手动下载并解压到: $MAVEN_INSTALL_DIR"
        Write-Err "下载地址: $MAVEN_DOWNLOAD_URL"
        throw "Maven download failed"
    }
    
    # 解压
    Write-Host "  正在解压 Maven..."
    Expand-Archive -Path $MAVEN_ZIP -DestinationPath "D:\tools\maven" -Force
    
    Write-OK "Maven 安装完成"
}

# ============================================================================
# 5. 配置 Maven settings.xml (阿里云镜像 + D盘仓库)
# ============================================================================
Write-Step "Step 5/7: 配置 Maven (阿里云镜像 + D盘仓库)..."
$settingsFile = "$MAVEN_INSTALL_DIR\conf\settings.xml"
if (Test-Path $settingsFile) {
    $settings = [xml](Get-Content $settingsFile -Encoding UTF8)
    
    # 配置本地仓库路径
    $localRepo = $settings.settings.localRepository
    if (-not $localRepo) {
        $localRepoNode = $settings.CreateElement("localRepository")
        $localRepoNode.InnerText = $MAVEN_REPO.Replace("\", "/")
        $settings.settings.PrependChild($localRepoNode) | Out-Null
    }
    
    # 配置阿里云镜像
    $mirrors = $settings.settings.mirrors
    if (-not $mirrors) {
        $mirrors = $settings.CreateElement("mirrors")
        $settings.settings.AppendChild($mirrors) | Out-Null
    }
    
    $hasAliyun = $mirrors.mirror | Where-Object { $_.id -eq "aliyun" }
    if (-not $hasAliyun) {
        $mirror = $settings.CreateElement("mirror")
        $id = $settings.CreateElement("id"); $id.InnerText = "aliyun"
        $name = $settings.CreateElement("name"); $name.InnerText = "Aliyun Maven Mirror"
        $url = $settings.CreateElement("url"); $url.InnerText = "https://maven.aliyun.com/repository/public"
        $mirrorOf = $settings.CreateElement("mirrorOf"); $mirrorOf.InnerText = "central"
        $mirror.AppendChild($id) | Out-Null
        $mirror.AppendChild($name) | Out-Null
        $mirror.AppendChild($url) | Out-Null
        $mirror.AppendChild($mirrorOf) | Out-Null
        $mirrors.AppendChild($mirror) | Out-Null
    }
    
    $settings.Save($settingsFile)
    Write-OK "Maven 配置完成 (仓库: $MAVEN_REPO)"
}

# ============================================================================
# 6. 配置环境变量
# ============================================================================
Write-Step "Step 6/7: 配置系统环境变量..."
$envVars = @(
    @{Name="JAVA_HOME"; Value=$JDK_INSTALL_DIR},
    @{Name="MAVEN_HOME"; Value=$MAVEN_INSTALL_DIR}
)

foreach ($var in $envVars) {
    [Environment]::SetEnvironmentVariable($var.Name, $var.Value, "Machine")
    Write-OK "设置 $($var.Name)=$($var.Value)"
}

# 更新 PATH
$currentPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
$newPaths = @(
    "$JDK_INSTALL_DIR\bin",
    "$MAVEN_INSTALL_DIR\bin"
)
$pathChanged = $false
foreach ($p in $newPaths) {
    if ($currentPath -notlike "*$p*") {
        $currentPath = "$p;$currentPath"
        $pathChanged = $true
    }
}
if ($pathChanged) {
    [Environment]::SetEnvironmentVariable("Path", $currentPath, "Machine")
    Write-OK "PATH 已更新"
}

# 刷新当前会话
$env:JAVA_HOME = $JDK_INSTALL_DIR
$env:MAVEN_HOME = $MAVEN_INSTALL_DIR
$env:Path = "$JDK_INSTALL_DIR\bin;$MAVEN_INSTALL_DIR\bin;$env:Path"

# 验证
Write-OK "Java 版本: $(& java -version 2>&1 | Select-Object -First 1)"
Write-OK "Maven 版本: $(& mvn --version 2>&1 | Select-Object -First 1)"

# ============================================================================
# 7. 检查/安装 Docker Desktop
# ============================================================================
Write-Step "Step 7/7: 检查 Docker..."
$dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerCmd) {
    Write-OK "Docker 已安装: $(docker --version)"
    
    # 配置 Docker 数据目录到 D 盘
    Write-Host "  提示: 请手动将 Docker 数据目录改为 D:\docker-data"
    Write-Host "  Docker Desktop -> Settings -> Resources -> Advanced -> Disk image location -> D:\docker-data"
    Write-Host "  然后点击 Apply & Restart"
} else {
    Write-Warn "Docker Desktop 未安装！"
    Write-Host ""
    Write-Host "  ============================================"
    Write-Host "  请手动安装 Docker Desktop:"
    Write-Host "  1. 访问 https://www.docker.com/products/docker-desktop/"
    Write-Host "  2. 下载 Windows 版并安装"
    Write-Host "  3. 安装后，在 Settings 中将数据目录改为 D:\docker-data"
    Write-Host "  4. 确保 Docker Desktop 正在运行"
    Write-Host "  ============================================"
}

# ============================================================================
# 8. 后续步骤提示
# ============================================================================
Write-Host "`n" -NoNewline
Write-Host "========================================" -ForegroundColor Green
Write-Host " 环境安装完成！后续步骤：" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "  1. 关闭此窗口，重新打开 PowerShell (刷新环境变量)"
Write-Host ""
Write-Host "  2. 启动基础设施 (需要 Docker 运行):" -ForegroundColor Yellow
Write-Host "     cd $scriptDir\anti-fraud-deployment"
Write-Host "     docker compose up -d"
Write-Host ""
Write-Host "  3. 等待所有容器就绪 (~2分钟):" -ForegroundColor Yellow
Write-Host "     docker compose ps"
Write-Host ""
Write-Host "  4. 构建项目:" -ForegroundColor Yellow
Write-Host "     cd $scriptDir"
Write-Host "     mvn clean package -DskipTests"
Write-Host ""
Write-Host "  5. 验证服务:" -ForegroundColor Yellow
Write-Host "     curl http://localhost:8888/actuator/health"
Write-Host ""
Write-Host "  6. 发送测试交易:" -ForegroundColor Yellow
Write-Host "     curl -X POST http://localhost:8888/agent/api/v1/monitor/assess"
Write-Host "       -H 'Content-Type: application/json'"
Write-Host "       -d '{...交易特征JSON...}'"
Write-Host ""
Write-Host "  7. 查看 Grafana 监控:" -ForegroundColor Yellow
Write-Host "     浏览器打开 http://localhost:3000 (admin/admin123)"
Write-Host ""
