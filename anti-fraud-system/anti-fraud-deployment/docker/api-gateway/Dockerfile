# ============================================================================
# API网关 Dockerfile
# ============================================================================
FROM eclipse-temurin:8-jre

LABEL maintainer="银行科技部"
LABEL description="API网关"

RUN mkdir -p /app/logs

WORKDIR /app

COPY target/api-gateway-*.jar app.jar

ENV JAVA_OPTS="-Xms128m -Xmx256m -XX:+UseSerialGC -XX:MaxRAMPercentage=75.0"
ENV SPRING_PROFILES_ACTIVE=prod

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8888/actuator/health || exit 1

EXPOSE 8888

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
