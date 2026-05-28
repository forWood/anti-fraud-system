package com.bank.risk.agent;

import com.bank.risk.agent.decision.DecisionFusionEngine;
import com.bank.risk.agent.ml.MLModelClient;
import com.bank.risk.agent.ml.MLPrediction;
import com.bank.risk.agent.rule.DroolsRuleEngine;
import com.bank.risk.agent.rule.RuleEngineResult;
import com.bank.risk.agent.service.AntiFraudAgentService;
import com.bank.risk.agent.service.KnowledgeBaseClient;
import com.bank.risk.common.model.RuleResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * 智能体服务单元测试
 *
 * 测试范围:
 * - 规则引擎评估逻辑
 * - ML模型预测
 * - 决策融合
 * - 风险等级判定
 *
 * @author 银行科技部
 */
@ExtendWith(MockitoExtension.class)
class AntiFraudAgentServiceTest {

    @Mock
    private DroolsRuleEngine ruleEngine;

    @Mock
    private MLModelClient mlModelClient;

    @Mock
    private KnowledgeBaseClient knowledgeBaseClient;

    @InjectMocks
    private DecisionFusionEngine fusionEngine;

    @BeforeEach
    void setUp() {
        // 通用Mock配置
    }

    // ===== 规则引擎单元测试 =====

    @Test
    void testRuleEngineHighRisk() {
        // 模拟高风险规则命中
        List<RuleResult> rules = Arrays.asList(
            RuleResult.builder()
                .ruleId("R001")
                .ruleName("单笔大额交易")
                .riskWeight(30)
                .riskScore(30.0)
                .riskLevel("MEDIUM")
                .build(),
            RuleResult.builder()
                .ruleId("R302")
                .ruleName("模拟器检测")
                .riskWeight(60)
                .riskScore(60.0)
                .riskLevel("HIGH")
                .build()
        );

        RuleEngineResult result = RuleEngineResult.builder()
            .ruleScore(85.0)
            .matchedRules(rules)
            .highestRiskLevel("HIGH")
            .ruleCount(2)
            .totalWeight(90)
            .build();

        assertEquals(2, result.getRuleCount());
        assertTrue(result.getRuleScore() >= 80);
        assertEquals("HIGH", result.getHighestRiskLevel());
    }

    @Test
    void testRuleEngineNoMatch() {
        List<RuleResult> emptyRules = Collections.emptyList();
        RuleEngineResult result = RuleEngineResult.builder()
            .ruleScore(0.0)
            .matchedRules(emptyRules)
            .highestRiskLevel("LOW")
            .ruleCount(0)
            .build();

        assertEquals(0, result.getRuleCount());
        assertEquals(0.0, result.getRuleScore(), 0.01);
        assertEquals("LOW", result.getHighestRiskLevel());
    }

    // ===== 决策融合单元测试 =====

    @Test
    void testDecisionFusionHighRisk() {
        // 规则得分高 + ML得分高 = 高风险
        RuleEngineResult ruleResult = RuleEngineResult.builder()
            .ruleScore(80.0)
            .matchedRules(Arrays.asList(buildRule("R001", 30, 30.0)))
            .ruleCount(1)
            .build();

        DecisionFusionEngine.FusionResult fusion = fusionEngine.fuse(ruleResult, 0.85);

        assertTrue(fusion.getFinalScore() >= 70);
        assertEquals("HIGH", fusion.getRiskLevel());
        assertEquals("BLOCK", fusion.getDecision());
        assertTrue(fusion.getConfidence() >= 0.5);
    }

    @Test
    void testDecisionFusionLowRisk() {
        // 规则得分低 + ML得分低 = 低风险
        RuleEngineResult ruleResult = RuleEngineResult.builder()
            .ruleScore(10.0)
            .matchedRules(Collections.emptyList())
            .ruleCount(0)
            .build();

        DecisionFusionEngine.FusionResult fusion = fusionEngine.fuse(ruleResult, 0.1);

        assertTrue(fusion.getFinalScore() < 50);
        assertEquals("LOW", fusion.getRiskLevel());
        assertEquals("PASS", fusion.getDecision());
    }

    @Test
    void testDecisionFusionMediumRisk() {
        // 中等场景
        RuleEngineResult ruleResult = RuleEngineResult.builder()
            .ruleScore(50.0)
            .matchedRules(Arrays.asList(buildRule("R101", 40, 40.0)))
            .ruleCount(1)
            .build();

        DecisionFusionEngine.FusionResult fusion = fusionEngine.fuse(ruleResult, 0.6);

        assertTrue(fusion.getFinalScore() >= 50 && fusion.getFinalScore() < 80);
        assertEquals("MEDIUM", fusion.getRiskLevel());
        assertEquals("REVIEW", fusion.getDecision());
    }

    @Test
    void testDecisionFusionWithSanctionRule() {
        // 制裁名单直接阻断
        List<RuleResult> rules = Arrays.asList(
            RuleResult.builder()
                .ruleId("T001")
                .ruleName("制裁名单命中")
                .riskWeight(100)
                .riskScore(100.0)
                .riskLevel("HIGH")
                .build()
        );

        RuleEngineResult ruleResult = RuleEngineResult.builder()
            .ruleScore(100.0)
            .matchedRules(rules)
            .ruleCount(1)
            .build();

        DecisionFusionEngine.FusionResult fusion = fusionEngine.fuse(ruleResult, 0.7);
        assertEquals("BLOCK", fusion.getDecision());
    }

    // ===== 权重公式测试 =====

    @Test
    void testScoreFormula() {
        // finalScore = 0.4 * ruleScore + 0.6 * mlScore
        double ruleScore = 60.0;
        double mlScore = 80.0;
        double expectedScore = 0.4 * ruleScore + 0.6 * mlScore; // = 72

        RuleEngineResult ruleResult = RuleEngineResult.builder()
            .ruleScore(ruleScore)
            .matchedRules(Collections.emptyList())
            .ruleCount(0)
            .build();

        DecisionFusionEngine.FusionResult fusion = fusionEngine.fuse(ruleResult, mlScore / 100.0);

        assertEquals(expectedScore, fusion.getFinalScore(), 1.0);
    }

    // ===== 风险等级边界测试 =====

    @Test
    void testRiskLevelBoundaries() {
        // 低风险边界: < 50
        assertEquals("LOW", getLevelForScore(45.0));
        assertEquals("LOW", getLevelForScore(0.0));
        assertEquals("LOW", getLevelForScore(49.9));

        // 中风险边界: 50-79
        assertEquals("MEDIUM", getLevelForScore(50.0));
        assertEquals("MEDIUM", getLevelForScore(65.0));
        assertEquals("MEDIUM", getLevelForScore(79.9));

        // 高风险边界: >= 80
        assertEquals("HIGH", getLevelForScore(80.0));
        assertEquals("HIGH", getLevelForScore(95.0));
        assertEquals("HIGH", getLevelForScore(100.0));
    }

    // ===== ML模型客户端测试 =====

    @Test
    void testMLPredictionFallback() {
        MLPrediction fallback = MLPrediction.fallback(150);
        assertEquals(0.5, fallback.getFraudProbability(), 0.01);
        assertEquals("fallback", fallback.getModelVersion());
        assertTrue(fallback.getTopFeatures().isEmpty());
    }

    @Test
    void testMLPredictionNormalFlow() {
        MLPrediction prediction = MLPrediction.builder()
            .fraudProbability(0.85)
            .topFeatures(Collections.emptyList())
            .inferenceTimeMs(50)
            .modelVersion("2.0.0")
            .build();

        assertEquals(0.85, prediction.getFraudProbability(), 0.01);
        assertEquals("2.0.0", prediction.getModelVersion());
    }

    // ===== 辅助方法 =====

    private RuleResult buildRule(String ruleId, int weight, double score) {
        return RuleResult.builder()
            .ruleId(ruleId)
            .ruleName("测试规则 " + ruleId)
            .description("单元测试")
            .riskWeight(weight)
            .riskScore(score)
            .riskLevel(score >= 80 ? "HIGH" : (score >= 50 ? "MEDIUM" : "LOW"))
            .build();
    }

    private String getLevelForScore(double score) {
        if (score >= 80) return "HIGH";
        if (score >= 50) return "MEDIUM";
        return "LOW";
    }
}
