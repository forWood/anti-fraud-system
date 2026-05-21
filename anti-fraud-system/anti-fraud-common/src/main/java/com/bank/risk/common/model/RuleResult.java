package com.bank.risk.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 规则执行结果模型
 *
 * @author 银行科技部
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RuleResult {

    /** 规则编号，如 R001, T001 */
    private String ruleId;

    /** 规则名称 */
    private String ruleName;

    /** 规则描述 */
    private String description;

    /** 规则触发时的详细说明 */
    private String triggerDetail;

    /** 风险权重(0-100) */
    private Integer riskWeight;

    /** 风险等级: LOW/MEDIUM/HIGH */
    private String riskLevel;

    /** 该规则对风险得分的贡献 */
    @Builder.Default
    private Double riskScore = 0.0;

    /** 规则触发时间 */
    private Long triggeredAt;
}
