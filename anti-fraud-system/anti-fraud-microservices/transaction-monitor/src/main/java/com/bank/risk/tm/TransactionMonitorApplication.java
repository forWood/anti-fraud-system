package com.bank.risk.tm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * 交易监控微服务
 *
 * 功能:
 * 1. 接收交易请求(来自网关或上游系统)
 * 2. 调用AI智能体服务进行风险评估
 * 3. 返回风险等级和决策建议
 *
 * @author 银行科技部
 */
@SpringBootApplication
@EnableDiscoveryClient
public class TransactionMonitorApplication {
    public static void main(String[] args) {
        SpringApplication.run(TransactionMonitorApplication.class, args);
    }
}
