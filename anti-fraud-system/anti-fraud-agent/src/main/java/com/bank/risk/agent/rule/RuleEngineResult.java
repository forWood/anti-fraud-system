package com.bank.risk.agent.rule;

import com.bank.risk.common.model.RuleResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * 规则引擎评估结果
 *
 * @author 银行科技部
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RuleEngineResult {

    /** 规则评估得分 (0-100) */
    @Builder.Default
    private double ruleScore = 0.0;

    /** 命中的规则列表 */
    @Builder.Default
    private List<RuleResult> matchedRules = Collections.emptyList();

    /** 最高风险等级 */
    @Builder.Default
    private String highestRiskLevel = "LOW";

    /** 命中规则数量 */
    @Builder.Default
    private int ruleCount = 0;

    /** 所有权重之和 */
    @Builder.Default
    private double totalWeight = 0.0;

    /**
     * 创建空结果
     */
    public static RuleEngineResult empty() {
        return RuleEngineResult.builder()
            .ruleScore(0.0)
            .highestRiskLevel("LOW")
            .ruleCount(0)
            .build();
    }

    /**
     * 创建空结果 (带交易ID)
     */
    public static RuleEngineResult empty(String transactionId) {
        return empty();
    }
}
