# ============================================================================
# 预警管理微服务 Dockerfile
# ============================================================================
FROM eclipse-temurin:8-jre

LABEL maintainer="银行科技部"
LABEL description="预警管理微服务"

RUN mkdir -p /app/logs

WORKDIR /app

COPY target/alert-management-*.jar app.jar

ENV JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
ENV SPRING_PROFILES_ACTIVE=prod

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8082/actuator/health || exit 1

EXPOSE 8082

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
