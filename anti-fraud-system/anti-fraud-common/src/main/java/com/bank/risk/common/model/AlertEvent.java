package com.bank.risk.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 预警事件模型
 *
 * @author 银行科技部
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertEvent {

    /** 预警编号(唯一) */
    private String alertId;

    /** 关联交易编号 */
    private String transactionId;

    /** 关联客户编号 */
    private String customerId;

    /** 预警类型: FRAUD/AML/CTF/SANCTION */
    private String alertType;

    /** 风险评分(0-100) */
    private BigDecimal riskScore;

    /** 风险等级: LOW/MEDIUM/HIGH */
    private String riskLevel;

    /** 命中规则明细(JSON) */
    private String matchedRulesJson;

    /** 模型评分 */
    private BigDecimal mlScore;

    /** 智能体建议: PASS/REVIEW/BLOCK */
    private String agentDecision;

    /** 预警状态: NEW/PENDING/PROCESSING/RESOLVED/CLOSED */
    @Builder.Default
    private String alertStatus = "NEW";

    /** 处理人ID */
    private String assignee;

    /** 升级等级: 0/1/2/3 */
    @Builder.Default
    private Integer escalationLevel = 0;

    /** 处理截止时间 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime processingDeadline;

    /** 创建时间 */
    @Builder.Default
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createdAt = LocalDateTime.now();

    /** 更新时间 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updatedAt;

    /** 相似案例列表 */
    private List<RiskAssessment.CaseSummary> similarCases;

    /** 风险因子 */
    private List<String> riskFactors;

    /** 交易金额 */
    private BigDecimal transactionAmount;

    /** 交易类型 */
    private String transactionType;

    /** 渠道编码 */
    private String channelCode;
}
