# ============================================================================
# AI智能体服务 Dockerfile
# ============================================================================
FROM eclipse-temurin:8-jre

LABEL maintainer="银行科技部"
LABEL description="反欺诈AI智能体服务"

# 创建应用目录
RUN mkdir -p /app/logs /app/rules

WORKDIR /app

# 添加应用JAR
COPY target/anti-fraud-agent-*.jar app.jar

# 添加规则文件
COPY src/main/resources/rules/ /app/rules/

# JVM配置
ENV JAVA_OPTS="-Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/logs/"
ENV SPRING_PROFILES_ACTIVE=prod
ENV DROOLS_RULES_PATH=/app/rules

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8080/actuator/health || exit 1

EXPOSE 8080 9090

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
