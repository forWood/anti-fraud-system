package com.bank.risk.agent.rule;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 规则执行指标收集器
 *
 * 暴露Prometheus指标:
 * - risk_rule_execution_total: 规则执行次数 (counter)
 * - risk_rule_execution_duration: 规则执行耗时 (histogram)
 * - risk_rule_fired_total: 规则触发次数 (counter)
 * - risk_decision_total: 决策结果分布 (counter)
 *
 * @author 银行科技部
 */
@Component
public class RuleMetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(RuleMetricsCollector.class);

    private final MeterRegistry meterRegistry;

    // 计数器
    private final AtomicLong totalAssessments = new AtomicLong(0);
    private final AtomicLong highRiskCount = new AtomicLong(0);
    private final AtomicLong mediumRiskCount = new AtomicLong(0);
    private final AtomicLong lowRiskCount = new AtomicLong(0);
    private final AtomicLong blockDecisionCount = new AtomicLong(0);
    private final AtomicLong reviewDecisionCount = new AtomicLong(0);
    private final AtomicLong passDecisionCount = new AtomicLong(0);

    public RuleMetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * 记录规则执行
     */
    public void recordRuleExecution(long elapsedMs, int ruleFiredCount) {
        meterRegistry.counter("risk_rule_execution_total").increment();
        meterRegistry.counter("risk_rule_fired_total").increment(ruleFiredCount);

        meterRegistry.timer("risk_rule_execution_duration")
            .record(elapsedMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * 记录决策结果
     */
    public void recordDecision(double riskScore, String riskLevel, String decision, long processingTimeMs) {
        totalAssessments.incrementAndGet();

        // 风险等级分布
        switch (riskLevel) {
            case "HIGH":
                highRiskCount.incrementAndGet();
                break;
            case "MEDIUM":
                mediumRiskCount.incrementAndGet();
                break;
            case "LOW":
                lowRiskCount.incrementAndGet();
                break;
        }

        // 决策分布
        meterRegistry.counter("risk_decision_total",
            "decision", decision).increment();

        // 风险得分分布
        meterRegistry.summary("risk_score_distribution")
            .record(riskScore);

        // 处理延迟
        meterRegistry.timer("risk_assessment_duration")
            .record(processingTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);

        // 模型得分分布
        Timer.Sample sample = Timer.start(meterRegistry);
        sample.stop(meterRegistry.timer("risk_assessment_timer"));
    }

    /**
     * 记录知识库检索
     */
    public void recordKnowledgeSearch(long elapsedMs, int resultCount, boolean emptyResult) {
        meterRegistry.timer("knowledge_search_duration")
            .record(elapsedMs, java.util.concurrent.TimeUnit.MILLISECONDS);

        if (emptyResult) {
            meterRegistry.counter("knowledge_search_empty_total").increment();
        }
    }

    // ===== 指标查询 (用于日志/控制台) =====

    public long getTotalAssessments() { return totalAssessments.get(); }
    public long getHighRiskCount() { return highRiskCount.get(); }
    public long getMediumRiskCount() { return mediumRiskCount.get(); }
    public long getLowRiskCount() { return lowRiskCount.get(); }
}
