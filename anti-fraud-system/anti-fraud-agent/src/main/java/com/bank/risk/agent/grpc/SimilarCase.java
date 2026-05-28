package com.bank.risk.agent.grpc;

/**
 * 相似案例 - 手写POJO替代Proto生成代码
 *
 * @author 银行科技部
 */
public class SimilarCase {
    private String caseId;
    private String caseTitle;
    private String caseType;
    private String fraudPattern;
    private String modusOperandi;
    private String resolution;
    private double similarity;

    public String getCaseId() { return caseId; }
    public void setCaseId(String v) { this.caseId = v; }
    public String getCaseTitle() { return caseTitle; }
    public void setCaseTitle(String v) { this.caseTitle = v; }
    public String getCaseType() { return caseType; }
    public void setCaseType(String v) { this.caseType = v; }
    public String getFraudPattern() { return fraudPattern; }
    public void setFraudPattern(String v) { this.fraudPattern = v; }
    public String getModusOperandi() { return modusOperandi; }
    public void setModusOperandi(String v) { this.modusOperandi = v; }
    public String getResolution() { return resolution; }
    public void setResolution(String v) { this.resolution = v; }
    public double getSimilarity() { return similarity; }
    public void setSimilarity(double v) { this.similarity = v; }

    public static SimilarCaseBuilder newBuilder() {
        return new SimilarCaseBuilder();
    }

    public static class SimilarCaseBuilder {
        private final SimilarCase instance = new SimilarCase();

        public SimilarCaseBuilder setCaseId(String v) { instance.caseId = v; return this; }
        public SimilarCaseBuilder setCaseTitle(String v) { instance.caseTitle = v; return this; }
        public SimilarCaseBuilder setCaseType(String v) { instance.caseType = v; return this; }
        public SimilarCaseBuilder setFraudPattern(String v) { instance.fraudPattern = v; return this; }
        public SimilarCaseBuilder setModusOperandi(String v) { instance.modusOperandi = v; return this; }
        public SimilarCaseBuilder setResolution(String v) { instance.resolution = v; return this; }
        public SimilarCaseBuilder setSimilarity(double v) { instance.similarity = v; return this; }
        public SimilarCase build() { return instance; }
    }
}
