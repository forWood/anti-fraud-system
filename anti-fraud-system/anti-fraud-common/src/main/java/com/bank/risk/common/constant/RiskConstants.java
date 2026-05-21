package com.bank.risk.common.constant;

/**
 * 风险系统常量定义
 *
 * @author 银行科技部
 */
public final class RiskConstants {

    private RiskConstants() {}

    // ===== Kafka Topics =====
    public static final String TOPIC_TRANSACTION_RAW = "transaction-raw";
    public static final String TOPIC_TRANSACTION_CLEAN = "transaction-clean";
    public static final String TOPIC_TRANSACTION_ASSESSED = "transaction-assessed";
    public static final String TOPIC_ALERT_CREATED = "alert-created";
    public static final String TOPIC_KNOWLEDGE_UPDATED = "knowledge-updated";
    public static final String TOPIC_FEATURE_COMPUTED = "feature-computed";

    // ===== 风险等级阈值 =====
    public static final double HIGH_RISK_THRESHOLD = 80.0;
    public static final double MEDIUM_RISK_THRESHOLD = 50.0;
    public static final double LOW_RISK_THRESHOLD = 0.0;

    // ===== 决策融合权重 (可配置) =====
    public static final double RULE_WEIGHT = 0.4;
    public static final double ML_WEIGHT = 0.6;

    // ===== 交易金额阈值 =====
    public static final double LARGE_AMOUNT_THRESHOLD = 500000.00;  // 50万
    public static final double MAX_TRANSACTION_AMOUNT = 10000000.00; // 1000万
    public static final double MIN_TRANSACTION_AMOUNT = 0.01;

    // ===== 特征计算窗口 =====
    public static final long WINDOW_1H_MS = 3600000L;
    public static final long WINDOW_24H_MS = 86400000L;
    public static final long WINDOW_7D_MS = 604800000L;
    public static final long WINDOW_30D_MS = 2592000000L;

    // ===== 风险等级编码 =====
    public static final String RISK_LEVEL_LOW = "LOW";
    public static final String RISK_LEVEL_MEDIUM = "MEDIUM";
    public static final String RISK_LEVEL_HIGH = "HIGH";

    // ===== 决策编码 =====
    public static final String DECISION_PASS = "PASS";
    public static final String DECISION_REVIEW = "REVIEW";
    public static final String DECISION_BLOCK = "BLOCK";

    // ===== 预警类型 =====
    public static final String ALERT_TYPE_FRAUD = "FRAUD";
    public static final String ALERT_TYPE_AML = "AML";
    public static final String ALERT_TYPE_CTF = "CTF";
    public static final String ALERT_TYPE_SANCTION = "SANCTION";

    // ===== 预警状态 =====
    public static final String ALERT_STATUS_NEW = "NEW";
    public static final String ALERT_STATUS_PENDING = "PENDING";
    public static final String ALERT_STATUS_PROCESSING = "PROCESSING";
    public static final String ALERT_STATUS_RESOLVED = "RESOLVED";
    public static final String ALERT_STATUS_CLOSED = "CLOSED";

    // ===== Redis Key前缀 =====
    public static final String REDIS_PREFIX_FEATURE = "risk:feature:";
    public static final String REDIS_PREFIX_BLACKLIST = "risk:blacklist:";
    public static final String REDIS_PREFIX_RULES = "risk:rules:";
    public static final String REDIS_PREFIX_CUSTOMER = "risk:customer:";
    public static final String REDIS_PREFIX_RATE_LIMIT = "risk:ratelimit:";

    // ===== 安全 =====
    public static final String API_KEY_HEADER = "X-API-Key";
    public static final String REQUEST_ID_HEADER = "X-Request-Id";

    // ===== gRPC服务端口 =====
    public static final int GRPC_AGENT_PORT = 9090;
    public static final int GRPC_KNOWLEDGE_PORT = 9091;
    public static final int GRPC_ML_PORT = 9092;
}
