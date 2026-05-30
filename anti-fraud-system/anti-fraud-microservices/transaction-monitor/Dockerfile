# ============================================================================
# 交易监控微服务 Dockerfile
# ============================================================================
FROM eclipse-temurin:8-jre

LABEL maintainer="银行科技部"
LABEL description="交易监控微服务"

RUN mkdir -p /app/logs

WORKDIR /app

COPY target/transaction-monitor-*.jar app.jar

ENV JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
ENV SPRING_PROFILES_ACTIVE=prod

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8081/actuator/health || exit 1

EXPOSE 8081

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
