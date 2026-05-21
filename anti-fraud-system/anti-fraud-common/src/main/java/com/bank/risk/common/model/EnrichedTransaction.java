package com.bank.risk.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

/**
 * 增强交易模型 - 包含实时特征计算结果
 * 用于Flink流处理后输出到下游
 *
 * @author 银行科技部
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedTransaction {

    /** 原始交易数据 */
    private Transaction transaction;

    // ===== 实时特征 (Flink计算) =====

    /** 1小时交易总额 */
    @Builder.Default
    private BigDecimal amount1h = BigDecimal.ZERO;

    /** 1小时交易次数 */
    @Builder.Default
    private Integer count1h = 0;

    /** 24小时交易总额 */
    @Builder.Default
    private BigDecimal amount24h = BigDecimal.ZERO;

    /** 24小时交易次数 */
    @Builder.Default
    private Integer count24h = 0;

    /** 30天使用设备数量 */
    @Builder.Default
    private Integer deviceCount30d = 0;

    /** 30天涉及城市数量 */
    @Builder.Default
    private Integer cityCount30d = 0;

    /** 30天交易对手数量 */
    @Builder.Default
    private Integer counterpartyCount30d = 0;

    /** 是否为夜间交易 */
    @Builder.Default
    private Boolean isNightTime = false;

    /** 是否为跨境交易 */
    @Builder.Default
    private Boolean isCrossBorder = false;

    /** 是否为首次使用设备 */
    @Builder.Default
    private Boolean isFirstDevice = false;

    /** IP风险等级: NORMAL/LOW_RISK/MEDIUM_RISK/HIGH_RISK */
    private String ipRiskLevel;

    /** 交易金额偏离度(与历史均值比较) */
    private Double amountDeviation;

    // ===== 决策结果 =====

    /** 风险评分(0-100) */
    private BigDecimal riskScore;

    /** 风险等级 */
    private String riskLevel;

    /** 决策建议: PASS/REVIEW/BLOCK */
    private String decision;

    /** 命中规则ID列表 */
    private List<String> matchedRuleIds;

    /** 相似案例ID列表 */
    private List<String> similarCaseIds;

    /** 处理时间戳 */
    @Builder.Default
    private Long processingTimestamp = System.currentTimeMillis();
}
