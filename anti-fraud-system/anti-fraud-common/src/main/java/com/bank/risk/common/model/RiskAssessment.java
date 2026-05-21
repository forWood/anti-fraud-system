package com.bank.risk.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

/**
 * 风险评估结果模型
 *
 * @author 银行科技部
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RiskAssessment {

    /** 交易流水号 */
    private String transactionId;

    /** 综合风险评分(0-100) */
    private BigDecimal riskScore;

    /** 风险等级: LOW(0-49) / MEDIUM(50-79) / HIGH(80-100) */
    private String riskLevel;

    /** 决策建议: PASS(放行) / REVIEW(人工审核) / BLOCK(阻断) */
    private String decision;

    /** 置信度(0-1) */
    @Builder.Default
    private Double confidence = 0.0;

    // ===== 子评分 =====

    /** 规则引擎评分 */
    private Double ruleScore;

    /** 机器学习模型评分 */
    private Double mlScore;

    /** 知识库关联评分 */
    private Double kbScore;

    // ===== 命中信息 =====

    /** 命中规则列表 */
    private List<RuleResult> matchedRules;

    /** 命中名单条目 */
    private List<String> matchedListEntries;

    /** 相似案例列表 */
    private List<CaseSummary> similarCases;

    /** 主要风险因子 */
    private List<String> riskFactors;

    /** 模型主要贡献特征(SHAP Top-5) */
    private List<FeatureContribution> topFeatureContributions;

    // ===== 处理信息 =====

    /** 处理耗时(毫秒) */
    private Long processingTimeMs;

    /** 是否需要立即阻断 */
    @Builder.Default
    private Boolean requiresImmediateBlock = false;

    /**
     * 风险等级枚举
     */
    public enum RiskLevel {
        LOW(0, 49, "低风险"),
        MEDIUM(50, 79, "中风险"),
        HIGH(80, 100, "高风险");

        private final int minScore;
        private final int maxScore;
        private final String label;

        RiskLevel(int minScore, int maxScore, String label) {
            this.minScore = minScore;
            this.maxScore = maxScore;
            this.label = label;
        }

        public static RiskLevel fromScore(double score) {
            if (score >= HIGH.minScore) return HIGH;
            if (score >= MEDIUM.minScore) return MEDIUM;
            return LOW;
        }

        public int getMinScore() { return minScore; }
        public int getMaxScore() { return maxScore; }
        public String getLabel() { return label; }
    }

    /**
     * 决策枚举
     */
    public enum Decision {
        PASS("放行", "交易风险可控，正常放行"),
        REVIEW("人工审核", "交易存在可疑特征，需人工复核"),
        BLOCK("阻断", "交易风险较高，立即阻断");

        private final String name;
        private final String description;

        Decision(String name, String description) {
            this.name = name;
            this.description = description;
        }

        public String getName() { return name; }
        public String getDescription() { return description; }
    }

    /**
     * 特征贡献模型
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FeatureContribution {
        private String featureName;
        private Double shapValue;
        private String featureDescription;
    }

    /**
     * 案例摘要模型
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CaseSummary {
        private String caseId;
        private String caseTitle;
        private String caseType;
        private String modusOperandi;
        private String resolution;
        private Double similarity;
    }
}
