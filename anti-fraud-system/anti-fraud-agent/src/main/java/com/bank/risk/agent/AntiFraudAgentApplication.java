package com.bank.risk.agent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 反欺诈AI智能体服务 - 启动类
 *
 * 核心功能:
 * 1. gRPC接口: assess (交易风险评估)
 * 2. Drools规则引擎: 加载fraud-rules.drl，动态刷新
 * 3. TensorFlow模型调用: 欺诈分类模型推理
 * 4. 决策融合: riskScore = 0.4*ruleScore + 0.6*mlScore
 *
 * @author 银行科技部
 */
@SpringBootApplication
@EnableScheduling
public class AntiFraudAgentApplication {

    public static void main(String[] args) {
        SpringApplication.run(AntiFraudAgentApplication.class, args);
    }
}
