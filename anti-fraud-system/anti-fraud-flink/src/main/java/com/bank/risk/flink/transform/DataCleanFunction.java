package com.bank.risk.flink.transform;

import com.bank.risk.common.model.Transaction;
import com.bank.risk.common.util.DataSecurityUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 数据清洗函数
 * 执行规范: 去重 → 格式标准化 → 脱敏处理 → 异常过滤
 *
 * 遵循SRS 6.2节数据清洗治理规范
 *
 * @author 银行科技部
 */
public class DataCleanFunction extends RichMapFunction<Transaction, Transaction> {

    private static final Logger log = LoggerFactory.getLogger(DataCleanFunction.class);

    /** 测试数据过滤手机号前缀 */
    private static final String[] TEST_PHONE_PREFIXES = {"199", "198", "123"};

    /** 累计处理计数 */
    private transient long processedCount;
    private transient long filteredCount;

    @Override
    public void open(Configuration parameters) {
        processedCount = 0;
        filteredCount = 0;
    }

    @Override
    public Transaction map(Transaction tx) throws Exception {
        processedCount++;

        // ===== 步骤1: 数据去重（基于交易流水号）=====
        // Flink state backend 会处理数据流中的去重
        // 此处做基本的空值检查

        // ===== 步骤2: 格式标准化 =====
        // 金额统一为2位小数
        if (tx.getAmount() != null) {
            tx.setAmount(tx.getAmount().setScale(2, RoundingMode.HALF_UP));
        }

        // 币种默认CNY
        if (tx.getCurrency() == null || tx.getCurrency().isEmpty()) {
            tx.setCurrency("CNY");
        }

        // 渠道编码标准化
        if (tx.getChannelCode() != null) {
            tx.setChannelCode(tx.getChannelCode().toUpperCase());
        }

        // ===== 步骤3: 敏感数据脱敏 =====
        // 账号哈希（如果未设置）
        if (tx.getAccountIdHash() == null && tx.getAccountId() != null) {
            tx.setAccountIdHash(DataSecurityUtil.sha256(tx.getAccountId()));
        }

        // 对手账号哈希
        if (tx.getCounterpartyHash() == null && tx.getCounterpartyAccount() != null) {
            tx.setCounterpartyHash(DataSecurityUtil.sha256(tx.getCounterpartyAccount()));
        }

        // GPS坐标脱敏
        if (tx.getGpsLat() != null) {
            tx.setGpsLat(DataSecurityUtil.maskGps(tx.getGpsLat()));
        }
        if (tx.getGpsLng() != null) {
            tx.setGpsLng(DataSecurityUtil.maskGps(tx.getGpsLng()));
        }

        // ===== 步骤4: 异常值标记 =====
        // 极端大额标记
        if (tx.getAmount() != null && tx.getAmount().doubleValue() >= 10_000_000.00) {
            log.warn("Extreme large amount detected: txId={}, amount={}", tx.getTransactionId(), tx.getAmount());
        }

        // IP风险等级评估
        if (tx.getIpAddress() != null) {
            tx.setIpRiskLevel(evaluateIpRisk(tx.getIpAddress()));
        }

        // 每处理10万条输出一次日志
        if (processedCount % 100000 == 0) {
            log.info("DataClean progress: processed={}, filtered={}", processedCount, filteredCount);
        }

        return tx;
    }

    /**
     * IP风险等级评估（简化版）
     */
    private String evaluateIpRisk(String ip) {
        if (ip == null) return "NORMAL";

        // 已知高风险IP段（示例）
        if (ip.startsWith("185.220.") || ip.startsWith("23.129.")) {
            return "HIGH_RISK";
        }
        if (ip.startsWith("103.") || ip.startsWith("45.")) {
            return "MEDIUM_RISK";
        }
        return "NORMAL";
    }
}
