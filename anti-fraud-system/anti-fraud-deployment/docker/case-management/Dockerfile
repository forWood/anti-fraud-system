# ============================================================================
# 案件管理微服务 Dockerfile
# ============================================================================
FROM openjdk:8-jre-slim

LABEL maintainer="银行科技部"
LABEL description="案件管理微服务"

RUN mkdir -p /app/logs

WORKDIR /app

COPY target/case-management-*.jar app.jar

ENV JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
ENV SPRING_PROFILES_ACTIVE=prod

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8083/actuator/health || exit 1

EXPOSE 8083

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
