package com.bank.risk.common.model;

import java.util.Collections;
import java.util.List;

/**
 * 风险评估响应 - POJO替代gRPC stub
 *
 * @author 银行科技部
 */
public class AssessResponse {
    private String transactionId;
    private double riskScore;
    private String riskLevel;
    private String decision;
    private double confidence;
    private double ruleScore;
    private double mlScore;
    private double kbScore;
    private List<String> riskFactors = Collections.emptyList();
    private long processingTimeMs;
    private String requestId;
    private boolean sanctionMatched;
    private String sanctionDetail;

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String v) { this.transactionId = v; }
    public double getRiskScore() { return riskScore; }
    public void setRiskScore(double v) { this.riskScore = v; }
    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String v) { this.riskLevel = v; }
    public String getDecision() { return decision; }
    public void setDecision(String v) { this.decision = v; }
    public double getConfidence() { return confidence; }
    public void setConfidence(double v) { this.confidence = v; }
    public double getRuleScore() { return ruleScore; }
    public void setRuleScore(double v) { this.ruleScore = v; }
    public double getMlScore() { return mlScore; }
    public void setMlScore(double v) { this.mlScore = v; }
    public double getKbScore() { return kbScore; }
    public void setKbScore(double v) { this.kbScore = v; }
    public List<String> getRiskFactors() { return riskFactors; }
    public void setRiskFactors(List<String> v) { this.riskFactors = v; }
    public long getProcessingTimeMs() { return processingTimeMs; }
    public void setProcessingTimeMs(long v) { this.processingTimeMs = v; }
    public String getRequestId() { return requestId; }
    public void setRequestId(String v) { this.requestId = v; }
    public boolean isSanctionMatched() { return sanctionMatched; }
    public void setSanctionMatched(boolean v) { this.sanctionMatched = v; }
    public String getSanctionDetail() { return sanctionDetail; }
    public void setSanctionDetail(String v) { this.sanctionDetail = v; }
}
