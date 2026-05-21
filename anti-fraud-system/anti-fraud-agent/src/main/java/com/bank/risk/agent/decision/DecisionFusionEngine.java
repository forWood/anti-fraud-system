package com.bank.risk.agent.decision;

import com.bank.risk.agent.rule.RuleEngineResult;
import com.bank.risk.common.model.RuleResult;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * 决策融合引擎
 *
 * 将规则引擎评分和ML模型评分进行加权融合，
 * 生成最终风险评分、风险等级和处置决策。
 *
 * 融合公式:
 *   finalScore = 0.4 * ruleScore + 0.6 * mlScore
 *
 * @author 银行科技部
 */
@Component
public class DecisionFusionEngine {

    /** 规则权重 (可配置) */
    private static final double RULE_WEIGHT = 0.4;

    /** 模型权重 (可配置) */
    private static final double ML_WEIGHT = 0.6;

    /**
     * 融合评分
     *
     * @param ruleResult 规则引擎结果
     * @param mlScore ML模型评分的欺诈概率(0-1)
     * @return 融合后的最终评分(0-100)
     */
    public FusionResult fuse(RuleEngineResult ruleResult, double mlScore) {
        // 标准化ML评分到0-100
        double normalizedMlScore = Math.min(100, mlScore * 100);

        // 加权融合
        double finalScore = RULE_WEIGHT * ruleResult.getRuleScore() +
                           ML_WEIGHT * normalizedMlScore;
        finalScore = Math.min(100, Math.max(0, finalScore));

        // 风险等级判定
        String riskLevel;
        if (finalScore >= 80) {
            riskLevel = "HIGH";
        } else if (finalScore >= 50) {
            riskLevel = "MEDIUM";
        } else {
            riskLevel = "LOW";
        }

        // 决策建议
        String decision;
        List<RuleResult> matchedRules = ruleResult.getMatchedRules();

        // 检查是否有需要立即阻断的规则 (制裁名单等)
        boolean requiresBlock = matchedRules.stream()
            .anyMatch(r -> "T001".equals(r.getRuleId()) || r.getRiskWeight() >= 90);

        if (requiresBlock) {
            decision = "BLOCK";
        } else if (finalScore >= 80 && normalizedMlScore >= 85) {
            // 高分+高模型置信度 = 阻断
            decision = "BLOCK";
        } else if (finalScore >= 50) {
            // 中风险 = 人工审核
            decision = "REVIEW";
        } else {
            // 低风险 = 放行
            decision = "PASS";
        }

        // 置信度计算
        double confidence = calculateConfidence(ruleResult, mlScore);

        return new FusionResult(
            round(finalScore, 2),
            riskLevel,
            decision,
            round(confidence, 2)
        );
    }

    /**
     * 计算置信度
     *
     * 规则命中数越多 + ML得分越高 = 置信度越高
     */
    private double calculateConfidence(RuleEngineResult ruleResult, double mlScore) {
        double ruleConfidence = Math.min(1.0, ruleResult.getRuleCount() / 10.0);
        double mlConfidence = mlScore; // mlScore本身就是0-1的概率置信度
        return Math.min(1.0, ruleConfidence * 0.3 + mlConfidence * 0.7);
    }

    private double round(double value, int places) {
        return BigDecimal.valueOf(value)
            .setScale(places, RoundingMode.HALF_UP)
            .doubleValue();
    }

    /**
     * 融合结果
     */
    public static class FusionResult {
        private final double finalScore;
        private final String riskLevel;
        private final String decision;
        private final double confidence;

        public FusionResult(double finalScore, String riskLevel, String decision, double confidence) {
            this.finalScore = finalScore;
            this.riskLevel = riskLevel;
            this.decision = decision;
            this.confidence = confidence;
        }

        public double getFinalScore() { return finalScore; }
        public String getRiskLevel() { return riskLevel; }
        public String getDecision() { return decision; }
        public double getConfidence() { return confidence; }
    }
}
