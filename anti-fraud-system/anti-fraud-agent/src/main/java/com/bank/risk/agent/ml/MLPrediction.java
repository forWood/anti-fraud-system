package com.bank.risk.agent.ml;

import com.bank.risk.common.model.RiskAssessment;
import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.List;

/**
 * ML模型预测结果
 */
@Data
@Builder
public class MLPrediction {
    /** 欺诈概率 (0-1) */
    private double fraudProbability;

    /** 主要特征贡献 (SHAP Top-5) */
    private List<RiskAssessment.FeatureContribution> topFeatures;

    /** 推理耗时(ms) */
    private long inferenceTimeMs;

    /** 模型版本 */
    private String modelVersion;

    /**
     * ML服务不可用时的降级预测
     */
    public static MLPrediction fallback(long elapsedMs) {
        return MLPrediction.builder()
            .fraudProbability(0.5)  // 中性降级
            .topFeatures(Collections.emptyList())
            .inferenceTimeMs(elapsedMs)
            .modelVersion("fallback")
            .build();
    }
}
