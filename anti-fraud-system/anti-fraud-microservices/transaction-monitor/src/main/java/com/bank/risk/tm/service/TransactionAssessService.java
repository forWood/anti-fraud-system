package com.bank.risk.tm.service;

import com.bank.risk.agent.grpc.*;
import com.bank.risk.common.model.RiskAssessment;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 交易风险评估服务
 *
 * 通过gRPC调用AI智能体服务进行风险评估，
 * 异步发送预警到Kafka。
 *
 * @author 银行科技部
 */
@Service
public class TransactionAssessService {

    private static final Logger log = LoggerFactory.getLogger(TransactionAssessService.class);

    @GrpcClient("anti-fraud-agent")
    private AntiFraudAgentServiceGrpc.AntiFraudAgentServiceBlockingStub agentStub;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.alert:alert-created}")
    private String alertTopic;

    /**
     * 单笔交易评估
     */
    public RiskAssessment assess(Map<String, Object> request) {
        long startTime = System.currentTimeMillis();

        // 构建gRPC请求
        AssessRequest assessRequest = buildAssessRequest(request);

        // 调用智能体服务
        AssessResponse grpcResponse = agentStub.assessTransaction(assessRequest);

        // 转换响应
        RiskAssessment assessment = convertResponse(grpcResponse);

        // 如果是高风险，异步创建预警
        if ("HIGH".equals(assessment.getRiskLevel())) {
            createAlertAsync(assessment);
        }

        log.info("Transaction assessed: txId={}, score={}, level={}, decision={}, time={}ms",
            assessment.getTransactionId(), assessment.getRiskScore(),
            assessment.getRiskLevel(), assessment.getDecision(),
            System.currentTimeMillis() - startTime);

        return assessment;
    }

    /**
     * 批量评估
     */
    public List<RiskAssessment> batchAssess(List<Map<String, Object>> requests) {
        List<CompletableFuture<RiskAssessment>> futures = requests.stream()
            .map(req -> CompletableFuture.supplyAsync(() -> assess(req)))
            .collect(Collectors.toList());

        return futures.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList());
    }

    /**
     * 异步创建预警
     */
    private void createAlertAsync(RiskAssessment assessment) {
        CompletableFuture.runAsync(() -> {
            try {
                Map<String, Object> alert = new LinkedHashMap<>();
                alert.put("alertId", "ALT" + UUID.randomUUID().toString().substring(0, 12));
                alert.put("transactionId", assessment.getTransactionId());
                alert.put("riskScore", assessment.getRiskScore());
                alert.put("riskLevel", assessment.getRiskLevel());
                alert.put("decision", assessment.getDecision());
                alert.put("matchedRules", assessment.getMatchedRules());
                alert.put("similarCases", assessment.getSimilarCases());
                alert.put("createdAt", System.currentTimeMillis());

                String alertJson = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(alert);
                kafkaTemplate.send(alertTopic, assessment.getTransactionId(), alertJson);

                log.info("Alert created: alertId={}", alert.get("alertId"));
            } catch (Exception e) {
                log.error("Failed to create alert: {}", e.getMessage());
            }
        });
    }

    private AssessRequest buildAssessRequest(Map<String, Object> request) {
        AssessRequest.Builder builder = AssessRequest.newBuilder();

        // 基本信息
        builder.setTransactionId(getString(request, "transaction_id", UUID.randomUUID().toString()));
        builder.setAccountId(getString(request, "account_id", ""));
        builder.setAccountIdHash(getString(request, "account_id_hash", ""));
        builder.setAmount(getDouble(request, "amount"));
        builder.setCurrency(getString(request, "currency", "CNY"));
        builder.setTransactionType(getString(request, "transaction_type", "TRANSFER"));
        builder.setChannelCode(getString(request, "channel_code", "EBANK"));
        builder.setTransactionTime(getLong(request, "transaction_time", System.currentTimeMillis()));

        // 对手方
        builder.setCounterpartyAccount(getString(request, "counterparty_account", ""));
        builder.setCounterpartyHash(getString(request, "counterparty_hash", ""));
        builder.setCounterpartyCountry(getString(request, "counterparty_country", "CN"));

        // 设备信息
        builder.setDeviceId(getString(request, "device_id", ""));
        builder.setIsEmulator(getBoolean(request, "is_emulator"));
        builder.setIsRooted(getBoolean(request, "is_rooted"));
        builder.setIsFirstDevice(getBoolean(request, "is_first_device"));

        // 网络信息
        builder.setIpAddress(getString(request, "ip_address", ""));
        builder.setIpRiskLevel(getString(request, "ip_risk_level", "NORMAL"));

        // 实时特征
        builder.setAmount1H(getDouble(request, "amount_1h"));
        builder.setCount1H(getInt(request, "count_1h"));
        builder.setAmount24H(getDouble(request, "amount_24h"));
        builder.setCount24H(getInt(request, "count_24h"));
        builder.setDeviceCount30D(getInt(request, "device_count_30d"));
        builder.setCityCount30D(getInt(request, "city_count_30d"));
        builder.setIsNightTime(getBoolean(request, "is_night_time"));
        builder.setIsCrossBorder(getBoolean(request, "is_cross_border"));
        builder.setAmountDeviation(getDouble(request, "amount_deviation"));

        // 检索配置
        builder.setEnableKnowledgeSearch(true);
        builder.setKnowledgeTopK(5);
        builder.setEnableDetailExplain(true);

        return builder.build();
    }

    private RiskAssessment convertResponse(AssessResponse response) {
        return RiskAssessment.builder()
            .transactionId(response.getTransactionId())
            .riskScore(BigDecimal.valueOf(response.getRiskScore()))
            .riskLevel(response.getRiskLevel())
            .decision(response.getDecision())
            .confidence(response.getConfidence())
            .ruleScore(response.getRuleScore())
            .mlScore(response.getMlScore())
            .kbScore(response.getKbScore())
            .riskFactors(response.getRiskFactorsList())
            .processingTimeMs(response.getProcessingTimeMs())
            .build();
    }

    // 辅助方法
    private String getString(Map<String, Object> map, String key, String defaultValue) {
        Object val = map.get(key);
        return val != null ? val.toString() : defaultValue;
    }

    private double getDouble(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val instanceof Number) return ((Number) val).doubleValue();
        if (val instanceof String) return Double.parseDouble((String) val);
        return 0.0;
    }

    private int getInt(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val instanceof Number) return ((Number) val).intValue();
        if (val instanceof String) return Integer.parseInt((String) val);
        return 0;
    }

    private long getLong(Map<String, Object> map, String key, long defaultValue) {
        Object val = map.get(key);
        if (val instanceof Number) return ((Number) val).longValue();
        return defaultValue;
    }

    private boolean getBoolean(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val instanceof Boolean) return (Boolean) val;
        if (val instanceof String) return "true".equalsIgnoreCase((String) val);
        return false;
    }
}
