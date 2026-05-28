package com.bank.risk.tm.service;

import com.bank.risk.common.model.AssessRequest;
import com.bank.risk.common.model.AssessResponse;
import com.bank.risk.common.model.RiskAssessment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 交易风险评估服务
 *
 * 通过HTTP REST调用AI智能体服务进行风险评估，
 * 异步发送预警到Kafka。
 *
 * @author 银行科技部
 */
@Service
public class TransactionAssessService {

    private static final Logger log = LoggerFactory.getLogger(TransactionAssessService.class);

    private final WebClient webClient;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${agent.service.url:http://localhost:8080}")
    private String agentServiceUrl;

    @Value("${kafka.topic.alert:alert-created}")
    private String alertTopic;

    public TransactionAssessService() {
        this.webClient = WebClient.builder()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
            .build();
    }

    /**
     * 单笔交易评估
     */
    public RiskAssessment assess(Map<String, Object> request) {
        long startTime = System.currentTimeMillis();

        // 构建请求
        AssessRequest assessRequest = buildAssessRequest(request);

        // 调用智能体服务 (HTTP REST)
        AssessResponse response = callAgentService(assessRequest);

        // 转换响应
        RiskAssessment assessment = convertResponse(response);

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
     * 调用智能体HTTP REST接口
     */
    private AssessResponse callAgentService(AssessRequest request) {
        try {
            AssessResponse response = webClient.post()
                .uri(agentServiceUrl + "/api/v1/agent/assess")
                .bodyValue(request.toMap())
                .retrieve()
                .bodyToMono(AssessResponse.class)
                .block();

            if (response == null) {
                return fallbackResponse(request.getTransactionId());
            }
            return response;

        } catch (Exception e) {
            log.warn("Agent service call failed, using fallback: {}", e.getMessage());
            return fallbackResponse(request.getTransactionId());
        }
    }

    /**
     * 降级响应（Agent不可用时）
     */
    private AssessResponse fallbackResponse(String transactionId) {
        AssessResponse response = new AssessResponse();
        response.setTransactionId(transactionId);
        response.setRiskScore(30.0);
        response.setRiskLevel("LOW");
        response.setDecision("PASS");
        response.setConfidence(0.5);
        response.setRuleScore(0.0);
        response.setMlScore(0.0);
        response.setKbScore(0.0);
        response.setProcessingTimeMs(0);
        return response;
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
        AssessRequest req = new AssessRequest();

        // 基本信息
        req.setTransactionId(getString(request, "transaction_id", UUID.randomUUID().toString()));
        req.setAccountId(getString(request, "account_id", ""));
        req.setAccountIdHash(getString(request, "account_id_hash", ""));
        req.setAmount(getDouble(request, "amount"));
        req.setCurrency(getString(request, "currency", "CNY"));
        req.setTransactionType(getString(request, "transaction_type", "TRANSFER"));
        req.setChannelCode(getString(request, "channel_code", "EBANK"));
        req.setTransactionTime(getLong(request, "transaction_time", System.currentTimeMillis()));

        // 对手方
        req.setCounterpartyAccount(getString(request, "counterparty_account", ""));
        req.setCounterpartyHash(getString(request, "counterparty_hash", ""));
        req.setCounterpartyCountry(getString(request, "counterparty_country", "CN"));

        // 设备信息
        req.setDeviceId(getString(request, "device_id", ""));
        req.setIsEmulator(getBoolean(request, "is_emulator"));
        req.setIsRooted(getBoolean(request, "is_rooted"));
        req.setIsFirstDevice(getBoolean(request, "is_first_device"));

        // 网络信息
        req.setIpAddress(getString(request, "ip_address", ""));
        req.setIpRiskLevel(getString(request, "ip_risk_level", "NORMAL"));

        // 实时特征
        req.setAmount1h(getDouble(request, "amount_1h"));
        req.setCount1h(getInt(request, "count_1h"));
        req.setAmount24h(getDouble(request, "amount_24h"));
        req.setCount24h(getInt(request, "count_24h"));
        req.setDeviceCount30d(getInt(request, "device_count_30d"));
        req.setCityCount30d(getInt(request, "city_count_30d"));
        req.setIsNightTime(getBoolean(request, "is_night_time"));
        req.setIsCrossBorder(getBoolean(request, "is_cross_border"));
        req.setAmountDeviation(getDouble(request, "amount_deviation"));

        // 检索配置
        req.setEnableKnowledgeSearch(true);
        req.setKnowledgeTopK(5);
        req.setEnableDetailExplain(true);

        return req;
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
            .riskFactors(response.getRiskFactors())
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
