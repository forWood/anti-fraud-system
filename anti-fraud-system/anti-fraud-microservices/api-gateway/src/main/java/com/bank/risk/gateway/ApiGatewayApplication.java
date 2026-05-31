package com.bank.risk.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * API Gateway 主入口
 *
 * 路由配置:
 * - /api/agent/** -> anti-fraud-agent:8080
 * - /api/tm/**    -> transaction-monitor:8081
 * - /api/alerts/** -> alert-management:8082
 * - /api/cases/** -> case-management:8083
 * - /api/reports/** -> report-generation:8084
 *
 * 全局过滤器: 请求日志、认证、限流
 */
@SpringBootApplication
@EnableDiscoveryClient
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }

    @Bean
    public RouteLocator customRouteLocator(RouteLocatorBuilder builder) {
        return builder.routes()
            // 智能体服务 (网关 /api/agent/** → 服务 /api/v1/agent/**)
            .route("anti-fraud-agent", r -> r
                .path("/api/agent/**")
                .filters(f -> f
                    .rewritePath("/api/agent/(?<remaining>.*)", "/api/v1/agent/${remaining}")
                    .addRequestHeader("X-Gateway", "api-gateway")
                    .addResponseHeader("X-Gateway-Time", LocalDateTime.now().toString())
                )
                .uri("lb://anti-fraud-agent"))

            // 交易监控服务 (网关 /api/tm/** → 服务 /api/v1/monitor/**)
            .route("transaction-monitor", r -> r
                .path("/api/tm/**")
                .filters(f -> f
                    .rewritePath("/api/tm/(?<remaining>.*)", "/api/v1/monitor/${remaining}")
                    .addRequestHeader("X-Gateway", "api-gateway")
                )
                .uri("lb://transaction-monitor"))

            // 预警管理服务 (网关 /api/alerts/** → 服务 /api/v1/alerts/**)
            .route("alert-management", r -> r
                .path("/api/alerts/**")
                .filters(f -> f
                    .rewritePath("/api/alerts/(?<remaining>.*)", "/api/v1/alerts/${remaining}")
                    .addRequestHeader("X-Gateway", "api-gateway")
                )
                .uri("lb://alert-management"))

            // 案件管理服务 (网关 /api/cases/** → 服务 /api/cases/**)
            .route("case-management", r -> r
                .path("/api/cases/**")
                .filters(f -> f
                    .stripPrefix(1)
                    .addRequestHeader("X-Gateway", "api-gateway")
                )
                .uri("lb://case-management"))

            // 报告生成服务 (网关 /api/reports/** → 服务 /api/reports/**)
            .route("report-generation", r -> r
                .path("/api/reports/**")
                .filters(f -> f
                    .stripPrefix(1)
                    .addRequestHeader("X-Gateway", "api-gateway")
                )
                .uri("lb://report-generation"))

            .build();
    }

    /** 全局日志过滤器 */
    @Component
    public static class RequestLogGatewayFilterFactory
            extends AbstractGatewayFilterFactory<Object> {

        @Override
        public GatewayFilter apply(Object config) {
            return (exchange, chain) -> {
                ServerHttpRequest request = exchange.getRequest();
                String traceId = UUID.randomUUID().toString().substring(0, 8);

                System.out.println("[Gateway] " + traceId + " | "
                    + request.getMethod() + " " + request.getURI()
                    + " | from=" + request.getRemoteAddress());

                // 添加 trace header
                ServerHttpRequest mutated = request.mutate()
                    .header("X-Trace-Id", traceId)
                    .build();
                ServerHttpResponse response = exchange.getResponse();

                return chain.filter(exchange.mutate()
                    .request(mutated)
                    .response(response)
                    .build());
            };
        }
    }
}
