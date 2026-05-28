package com.bank.risk.agent.service;

import com.bank.risk.agent.decision.DecisionFusionEngine;
import com.bank.risk.agent.grpc.SimilarCase;
import com.bank.risk.agent.ml.MLModelClient;
import com.bank.risk.agent.ml.MLPrediction;
import com.bank.risk.agent.rule.DroolsRuleEngine;
import com.bank.risk.agent.rule.RuleEngineResult;
import com.bank.risk.agent.rule.RuleMetricsCollector;
import com.bank.risk.common.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * AI智能体REST API控制器
 *
 * 核心流程:
 * 1. 接收交易请求 → 提取特征
 * 2. 规则引擎评估 (Drools)
 * 3. ML模型推理 (TensorFlow Serving)
 * 4. 知识库RAG检索 (HTTP调用Python服务)
 * 5. 决策融合 (40%规则 + 60%模型)
 * 6. 返回评分、等级、决策、解释
 *
 * @author 银行科技部
 */
@RestController
@RequestMapping("/api/v1/agent")
public class AntiFraudAgentService {

    private static final Logger log = LoggerFactory.getLogger(AntiFraudAgentService.class);

    /** 规则权重 */
    private static final double RULE_WEIGHT = 0.4;
    /** 模型权重 */
    private static final double ML_WEIGHT = 0.6;

    @Autowired
    private DroolsRuleEngine ruleEngine;

    @Autowired
    private MLModelClient mlModelClient;

    @Autowired
    private DecisionFusionEngine fusionEngine;

    @Autowired
    private KnowledgeBaseClient knowledgeBaseClient;

    @Autowired
    private RuleMetricsCollector metricsCollector;

    // ========================================================================
    // 交易风险评估 (核心接口)
    // ========================================================================

    @PostMapping("/assess")
    public AssessResponse assessTransaction(@RequestBody AssessRequest request) {

        long startTime = System.currentTimeMillis();
        String requestId = UUID.randomUUID().toString().substring(0, 12);

        try {
            log.info("[{}] Assessing transaction: txId={}, amount={}, type={}, channel={}",
                requestId,
                request.getTransactionId(),
                request.getAmount(),
                request.getTransactionType(),
                request.getChannelCode());

            // ===== Step 1: 提取特征 =====
            Map<String, Object> features = extractFeatures(request);

            // ===== Step 2: 名单检查 (黑名单) =====
            boolean sanctionMatched = checkSanctionList(request, features);
            String sanctionDetail = sanctionMatched ? "交易对手命中制裁名单" : "";

            // ===== Step 3: 规则引擎评估 =====
            RuleEngineResult ruleResult = ruleEngine.evaluate(features);
            double ruleScore = ruleResult.getRuleScore();
            metricsCollector.recordRuleExecution(System.currentTimeMillis() - startTime,
                ruleResult.getRuleCount());

            // ===== Step 4: ML模型推理 =====
            double mlScore = 0.0;
            List<RiskAssessment.FeatureContribution> topFeatures = Collections.emptyList();
            try {
                MLPrediction mlPrediction = mlModelClient.predict(features);
                mlScore = mlPrediction.getFraudProbability() * 100;
                topFeatures = mlPrediction.getTopFeatures();
            } catch (Exception e) {
                log.error("[{}] ML model prediction failed: {}", requestId, e.getMessage());
                // ML服务不可用时，使用规则评分作为fallback
                mlScore = ruleScore;
            }

            // ===== Step 5: 知识库RAG检索 (可选) =====
            List<SimilarCase> similarCases = Collections.emptyList();
            double kbScore = 0.0;
            if (request.isEnableKnowledgeSearch()) {
                try {
                    KnowledgeBaseClient.KBSearchResult kbResult =
                        knowledgeBaseClient.searchSimilar(features, request.getKnowledgeTopK());
                    similarCases = kbResult.getCases();
                    kbScore = kbResult.getRelevanceScore();
                } catch (Exception e) {
                    log.warn("[{}] Knowledge search failed: {}", requestId, e.getMessage());
                }
            }

            // ===== Step 6: 决策融合 =====
            // finalScore = 0.4 * ruleScore + 0.6 * mlScore
            double finalScore = RULE_WEIGHT * ruleScore + ML_WEIGHT * mlScore;
            finalScore = Math.min(100, Math.max(0, finalScore));

            // 风险等级判定
            String riskLevel;
            if (finalScore >= 80) riskLevel = "HIGH";
            else if (finalScore >= 50) riskLevel = "MEDIUM";
            else riskLevel = "LOW";

            // 决策建议
            String decision;
            if (sanctionMatched || (finalScore >= 80 && mlScore >= 85)) {
                decision = "BLOCK";
            } else if (finalScore >= 50) {
                decision = "REVIEW";
            } else {
                decision = "PASS";
            }

            // 置信度
            double confidence = calculateConfidence(ruleScore, mlScore, ruleResult.getRuleCount());

            // 风险因子
            List<String> riskFactors = buildRiskFactors(ruleResult, mlScore, features);

            // ===== Step 7: 构建响应 =====
            long processingTimeMs = System.currentTimeMillis() - startTime;

            AssessResponse response = new AssessResponse();
            response.setTransactionId(request.getTransactionId());
            response.setRiskScore(round(finalScore, 2));
            response.setRiskLevel(riskLevel);
            response.setDecision(decision);
            response.setConfidence(round(confidence, 2));
            response.setRuleScore(round(ruleScore, 2));
            response.setMlScore(round(mlScore, 2));
            response.setKbScore(round(kbScore, 2));
            response.setRiskFactors(riskFactors);
            response.setSanctionMatched(sanctionMatched);
            response.setSanctionDetail(sanctionDetail);
            response.setProcessingTimeMs(processingTimeMs);
            response.setRequestId(requestId);

            // 记录指标
            metricsCollector.recordDecision(finalScore, riskLevel, decision, processingTimeMs);

            log.info("[{}] Assessment completed: score={}, level={}, decision={}, time={}ms, rules={}, mlScore={}",
                requestId, round(finalScore, 2), riskLevel, decision, processingTimeMs,
                ruleResult.getRuleCount(), round(mlScore, 2));

            return response;

        } catch (Exception e) {
            log.error("[{}] Assessment failed: {}", requestId, e.getMessage(), e);
            AssessResponse errorResponse = new AssessResponse();
            errorResponse.setTransactionId(request.getTransactionId());
            errorResponse.setRiskScore(0);
            errorResponse.setRiskLevel("ERROR");
            errorResponse.setDecision("ERROR");
            errorResponse.setRequestId(requestId);
            return errorResponse;
        }
    }

    // ========================================================================
    // 批量评估
    // ========================================================================

    @PostMapping("/batch-assess")
    public Map<String, Object> batchAssess(@RequestBody List<AssessRequest> requests) {

        int total = requests.size();
        int success = 0;
        int failed = 0;
        List<AssessResponse> results = new ArrayList<>();

        for (AssessRequest tx : requests) {
            try {
                results.add(assessSingleInBatch(tx));
                success++;
            } catch (Exception e) {
                failed++;
                log.error("Batch assessment failed for txId={}: {}", tx.getTransactionId(), e.getMessage());
            }
        }

        Map<String, Object> batchResult = new LinkedHashMap<>();
        batchResult.put("results", results);
        batchResult.put("total", total);
        batchResult.put("success", success);
        batchResult.put("failed", failed);
        return batchResult;
    }

    private AssessResponse assessSingleInBatch(AssessRequest request) {
        Map<String, Object> features = extractFeatures(request);
        RuleEngineResult ruleResult = ruleEngine.evaluate(features);
        double mlScore = 0.0;
        try {
            mlScore = mlModelClient.predict(features).getFraudProbability() * 100;
        } catch (Exception e) {
            mlScore = ruleResult.getRuleScore();
        }
        double finalScore = RULE_WEIGHT * ruleResult.getRuleScore() + ML_WEIGHT * mlScore;
        finalScore = Math.min(100, Math.max(0, finalScore));

        AssessResponse resp = new AssessResponse();
        resp.setTransactionId(request.getTransactionId());
        resp.setRiskScore(round(finalScore, 2));
        resp.setRiskLevel(finalScore >= 80 ? "HIGH" : (finalScore >= 50 ? "MEDIUM" : "LOW"));
        resp.setDecision(finalScore >= 80 ? "BLOCK" : (finalScore >= 50 ? "REVIEW" : "PASS"));
        return resp;
    }

    // ========================================================================
    // 健康检查
    // ========================================================================

    @GetMapping("/health")
    public Map<String, Object> healthCheck() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "SERVING");
        result.put("message", "AI Agent is healthy");
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }

    // ========================================================================
    // 辅助方法
    // ========================================================================

    private Map<String, Object> extractFeatures(AssessRequest request) {
        Map<String, Object> features = new HashMap<>();
        features.put("transaction_id", request.getTransactionId());
        features.put("account_id_hash", request.getAccountIdHash());
        features.put("amount", request.getAmount());
        features.put("currency", request.getCurrency());
        features.put("transaction_type", request.getTransactionType());
        features.put("channel_code", request.getChannelCode());
        features.put("transaction_time", request.getTransactionTime());

        features.put("device_id", request.getDeviceId());
        features.put("device_type", request.getDeviceType());
        features.put("is_emulator", request.isEmulator());
        features.put("is_rooted", request.isRooted());
        features.put("is_first_device", request.isFirstDevice());

        features.put("ip_address", request.getIpAddress());
        features.put("ip_risk_level", request.getIpRiskLevel());

        features.put("amount_1h", request.getAmount1h());
        features.put("count_1h", request.getCount1h());
        features.put("amount_24h", request.getAmount24h());
        features.put("count_24h", request.getCount24h());
        features.put("device_count_30d", request.getDeviceCount30d());
        features.put("city_count_30d", request.getCityCount30d());
        features.put("is_night_time", request.isNightTime());
        features.put("is_cross_border", request.isCrossBorder());
        features.put("amount_deviation", request.getAmountDeviation());
        features.put("counterparty_country", request.getCounterpartyCountry());
        features.put("counterparty_hash", request.getCounterpartyHash());

        return features;
    }

    private boolean checkSanctionList(AssessRequest request, Map<String, Object> features) {
        // 简化实现：在实际生产环境中，这里会查询黑名单Redis/MySQL
        // 检查交易对手是否在制裁名单中
        return false;
    }

    private double calculateConfidence(double ruleScore, double mlScore, int ruleCount) {
        double ruleConfidence = Math.min(1.0, ruleCount / 10.0);
        double mlConfidence = mlScore / 100.0;
        return Math.min(1.0, (ruleConfidence * 0.3 + mlConfidence * 0.7));
    }

    private List<String> buildRiskFactors(RuleEngineResult ruleResult, double mlScore,
                                           Map<String, Object> features) {
        List<String> factors = new ArrayList<>();
        
        for (RuleResult rule : ruleResult.getMatchedRules()) {
            factors.add(String.format("[%s] %s", rule.getRuleId(), rule.getRuleName()));
        }
        
        if (features.containsKey("is_first_device") &&
            Boolean.TRUE.equals(features.get("is_first_device"))) {
            factors.add("新设备首次交易");
        }
        if (features.containsKey("is_night_time") &&
            Boolean.TRUE.equals(features.get("is_night_time"))) {
            factors.add("夜间交易");
        }
        if (features.containsKey("is_cross_border") &&
            Boolean.TRUE.equals(features.get("is_cross_border"))) {
            factors.add("跨境交易");
        }
        
        if (mlScore >= 80) {
            factors.add("ML模型判定高风险 (得分≥80)");
        }
        
        return factors;
    }

    private double round(double value, int places) {
        return BigDecimal.valueOf(value)
            .setScale(places, RoundingMode.HALF_UP)
            .doubleValue();
    }
}
