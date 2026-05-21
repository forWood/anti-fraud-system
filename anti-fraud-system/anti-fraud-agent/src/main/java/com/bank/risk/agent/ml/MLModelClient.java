package com.bank.risk.agent.ml;

import com.bank.risk.common.model.RiskAssessment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.*;
import java.util.stream.Collectors;

/**
 * TensorFlow Serving 模型推理客户端
 *
 * 通过REST API调用部署在Kubernetes上的TensorFlow Serving服务，
 * 获取欺诈概率和特征贡献值。
 *
 * 模型:
 * - 名称: fraud_detection
 * - 输入: 特征向量 (32维)
 * - 输出: fraud_probability (0-1), shap_values
 *
 * @author 银行科技部
 */
@Service
public class MLModelClient {

    private static final Logger log = LoggerFactory.getLogger(MLModelClient.class);

    @Value("${ml.model.service.url:http://localhost:8501}")
    private String modelServiceUrl;

    @Value("${ml.model.name:fraud_detection}")
    private String modelName;

    @Value("${ml.model.timeout:200}")
    private int timeoutMs;

    private final WebClient webClient;

    public MLModelClient() {
        this.webClient = WebClient.builder()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
            .build();
    }

    /**
     * 调用ML模型进行欺诈预测
     *
     * @param features 交易特征Map
     * @return 预测结果
     */
    public MLPrediction predict(Map<String, Object> features) {
        long startTime = System.currentTimeMillis();

        try {
            // Step 1: 构建特征向量 (32维)
            float[] featureVector = buildFeatureVector(features);

            // Step 2: 构建TF Serving请求
            Map<String, Object> requestBody = buildTFRequest(featureVector);

            // Step 3: 发送推理请求
            Map<String, Object> response = webClient.post()
                .uri(modelServiceUrl + "/v1/models/" + modelName + ":predict")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(Map.class)
                .block();

            // Step 4: 解析响应
            MLPrediction prediction = parsePrediction(response, featureVector);

            long elapsedMs = System.currentTimeMillis() - startTime;
            log.debug("ML prediction completed: prob={}, time={}ms",
                prediction.getFraudProbability(), elapsedMs);

            return prediction;

        } catch (Exception e) {
            log.error("ML prediction failed: {}", e.getMessage());
            // 降级策略: 返回默认值
            long elapsedMs = System.currentTimeMillis() - startTime;
            return MLPrediction.fallback(elapsedMs);
        }
    }

    /**
     * 构建特征向量 (32维)
     *
     * 特征顺序:
     * [0] 交易金额(标准化)
     * [1] 1小时交易次数
     * [2] 24小时交易次数
     * [3] 30天设备数量
     * [4] 30天城市数量
     * [5] 30天交易对手数量
     * [6] 是否夜间 (0/1)
     * [7] 是否跨境 (0/1)
     * [8] 是否新设备 (0/1)
     * [9] IP风险等级编码
     * [10] 金额偏离度
     * [11] 是否是模拟器 (0/1)
     * [12] 是否ROOT (0/1)
     * [13-18] 渠道one-hot编码 (EBANK/MBANK/COUNTER/ATM/POS/OTHER)
     * [19-23] 交易类型one-hot (TRANSFER/PAYMENT/WITHDRAW/DEPOSIT/OTHER)
     * [24-27] 金额分段 (小额/中额/大额/超大额)
     * [28] 周末交易 (0/1)
     * [29] 是否节假日 (0/1)
     * [30] amount_1h / amount (比例)
     * [31] count_24h / device_count_30d (密度)
     */
    private float[] buildFeatureVector(Map<String, Object> features) {
        float[] vec = new float[32];

        // [0] 交易金额标准化 (log10)
        double amount = toDouble(features.get("amount"));
        vec[0] = (float) Math.log10(Math.max(amount, 1.0)) / 8.0f;

        // [1-2] 交易频率
        vec[1] = toFloat(features.get("count_1h")) / 100.0f;
        vec[2] = toFloat(features.get("count_24h")) / 1000.0f;

        // [3-5] 多样性
        vec[3] = toFloat(features.get("device_count_30d")) / 50.0f;
        vec[4] = toFloat(features.get("city_count_30d")) / 20.0f;
        vec[5] = toFloat(features.get("counterparty_count_30d")) / 100.0f;

        // [6-8] 布尔特征
        vec[6] = toBooleanFloat(features.get("is_night_time"));
        vec[7] = toBooleanFloat(features.get("is_cross_border"));
        vec[8] = toBooleanFloat(features.get("is_first_device"));

        // [9] IP风险编码
        String ipRisk = String.valueOf(features.getOrDefault("ip_risk_level", "NORMAL"));
        vec[9] = ipRiskLevelToFloat(ipRisk);

        // [10] 金额偏离度
        vec[10] = Math.min(1.0f, toFloat(features.get("amount_deviation")) / 10.0f);

        // [11-12] 设备安全
        vec[11] = toBooleanFloat(features.get("is_emulator"));
        vec[12] = toBooleanFloat(features.get("is_rooted"));

        // [13-18] 渠道one-hot
        String channel = String.valueOf(features.getOrDefault("channel_code", "OTHER"));
        channelOneHot(channel, vec, 13);

        // [19-23] 交易类型one-hot
        String txType = String.valueOf(features.getOrDefault("transaction_type", "OTHER"));
        txTypeOneHot(txType, vec, 19);

        // [24-27] 金额分段
        amountBins(amount, vec, 24);

        // [28-29] 时间特征
        vec[28] = toBooleanFloat(features.get("is_weekend"));
        vec[29] = toBooleanFloat(features.get("is_holiday"));

        // [30] amount_1h / amount
        double amount1h = toDouble(features.get("amount_1h"));
        vec[30] = amount > 0 ? (float) (amount1h / amount) : 0.0f;

        // [31] 密度
        int count24h = toInt(features.get("count_24h"));
        int deviceCount = toInt(features.get("device_count_30d"));
        vec[31] = deviceCount > 0 ? (float) count24h / deviceCount : 0.0f;

        return vec;
    }

    /**
     * 构建TensorFlow Serving请求体
     */
    private Map<String, Object> buildTFRequest(float[] featureVector) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("signature_name", "serving_default");

        List<Object> instances = new ArrayList<>();
        List<Float> instance = new ArrayList<>();
        for (float v : featureVector) {
            instance.add(v);
        }
        instances.add(instance);
        request.put("instances", instances);

        return request;
    }

    /**
     * 解析TF Serving预测结果
     */
    @SuppressWarnings("unchecked")
    private MLPrediction parsePrediction(Map<String, Object> response, float[] featureVector) {
        double fraudProb = 0.0;
        List<RiskAssessment.FeatureContribution> topFeatures = new ArrayList<>();

        try {
            List<Object> predictions = (List<Object>) response.get("predictions");
            if (predictions != null && !predictions.isEmpty()) {
                Object pred = predictions.get(0);
                if (pred instanceof List) {
                    List<Double> probs = (List<Double>) pred;
                    if (probs.size() >= 2) {
                        fraudProb = probs.get(1); // 二分类: [normal_prob, fraud_prob]
                    }
                } else if (pred instanceof Map) {
                    Map<String, Double> predMap = (Map<String, Double>) pred;
                    fraudProb = predMap.getOrDefault("fraud_probability", 0.0);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse ML response: {}", e.getMessage());
        }

        // 构建特征贡献 (简化版, 实际应使用SHAP值)
        topFeatures = buildSimplifiedFeatureContributions(featureVector, fraudProb);

        return MLPrediction.builder()
            .fraudProbability(fraudProb)
            .topFeatures(topFeatures)
            .inferenceTimeMs(0)
            .modelVersion("2.0.0")
            .build();
    }

    /**
     * 构建简化的特征贡献列表
     * (实际生产环境应使用模型返回的SHAP值)
     */
    private List<RiskAssessment.FeatureContribution> buildSimplifiedFeatureContributions(
            float[] featureVector, double fraudProb) {

        List<RiskAssessment.FeatureContribution> contributions = new ArrayList<>();

        // 简化的特征重要性 (基于特征敏感度分析)
        String[] featureNames = {
            "交易金额", "1h交易频率", "24h交易频率",
            "30天设备数", "30天城市数", "30天对手数",
            "夜间交易", "跨境交易", "新设备",
            "IP风险等级", "金额偏离度",
            "模拟器检测", "ROOT检测"
        };

        double[] importance = {
            0.15, 0.12, 0.10, 0.08, 0.07, 0.06,
            0.05, 0.08, 0.07, 0.06, 0.05,
            0.06, 0.05
        };

        // Top-5
        List<Map.Entry<String, Double>> sorted = new ArrayList<>();
        for (int i = 0; i < Math.min(featureNames.length, importance.length); i++) {
            sorted.add(new AbstractMap.SimpleEntry<>(featureNames[i], importance[i]));
        }
        sorted.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        for (int i = 0; i < Math.min(5, sorted.size()); i++) {
            contributions.add(RiskAssessment.FeatureContribution.builder()
                .featureName(sorted.get(i).getKey())
                .shapValue(sorted.get(i).getValue())
                .featureDescription(sorted.get(i).getKey())
                .build());
        }

        return contributions;
    }

    // ===== 辅助方法 =====

    private double toDouble(Object value) {
        if (value instanceof Number) return ((Number) value).doubleValue();
        if (value instanceof String) return Double.parseDouble((String) value);
        return 0.0;
    }

    private float toFloat(Object value) {
        if (value instanceof Number) return ((Number) value).floatValue();
        if (value instanceof String) return Float.parseFloat((String) value);
        return 0.0f;
    }

    private int toInt(Object value) {
        if (value instanceof Number) return ((Number) value).intValue();
        if (value instanceof String) return Integer.parseInt((String) value);
        return 0;
    }

    private float toBooleanFloat(Object value) {
        if (value instanceof Boolean) return (Boolean) value ? 1.0f : 0.0f;
        if (value instanceof String) return "true".equalsIgnoreCase((String) value) ? 1.0f : 0.0f;
        return 0.0f;
    }

    private float ipRiskLevelToFloat(String level) {
        switch (level.toUpperCase()) {
            case "HIGH_RISK": return 0.9f;
            case "MEDIUM_RISK": return 0.5f;
            case "LOW_RISK": return 0.2f;
            default: return 0.0f;
        }
    }

    private void channelOneHot(String channel, float[] vec, int startIdx) {
        Arrays.fill(vec, startIdx, startIdx + 6, 0.0f);
        switch (channel.toUpperCase()) {
            case "EBANK": vec[startIdx] = 1.0f; break;
            case "MBANK": vec[startIdx + 1] = 1.0f; break;
            case "COUNTER": vec[startIdx + 2] = 1.0f; break;
            case "ATM": vec[startIdx + 3] = 1.0f; break;
            case "POS": vec[startIdx + 4] = 1.0f; break;
            default: vec[startIdx + 5] = 1.0f; break; // OTHER
        }
    }

    private void txTypeOneHot(String txType, float[] vec, int startIdx) {
        Arrays.fill(vec, startIdx, startIdx + 5, 0.0f);
        switch (txType.toUpperCase()) {
            case "TRANSFER": vec[startIdx] = 1.0f; break;
            case "PAYMENT": vec[startIdx + 1] = 1.0f; break;
            case "WITHDRAW": vec[startIdx + 2] = 1.0f; break;
            case "DEPOSIT": vec[startIdx + 3] = 1.0f; break;
            default: vec[startIdx + 4] = 1.0f; break;
        }
    }

    private void amountBins(double amount, float[] vec, int startIdx) {
        Arrays.fill(vec, startIdx, startIdx + 4, 0.0f);
        if (amount <= 10000) vec[startIdx] = 1.0f;           // 小额
        else if (amount <= 100000) vec[startIdx + 1] = 1.0f; // 中额
        else if (amount <= 500000) vec[startIdx + 2] = 1.0f; // 大额
        else vec[startIdx + 3] = 1.0f;                        // 超大额
    }

    // ===== 预测结果模型 =====

    @Data
    @Builder
    public static class MLPrediction {
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
}
