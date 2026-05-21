package com.bank.risk.flink.transform;

import com.bank.risk.common.model.EnrichedTransaction;
import com.bank.risk.common.model.Transaction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 实时特征提取ProcessFunction
 *
 * 提取特征：
 * - 1小时交易频率/总额
 * - 24小时交易频率/总额
 * - 30天设备数量
 * - 30天涉及城市数量
 * - 是否新设备
 * - 跨境标记
 * - IP风险等级
 * - 交易金额偏离度
 *
 * 使用Flink KeyedState针对每个账户独立计算
 *
 * @author 银行科技部
 */
public class FeatureExtractionProcess
        extends KeyedProcessFunction<String, Transaction, EnrichedTransaction> {

    private static final Logger log = LoggerFactory.getLogger(FeatureExtractionProcess.class);

    // ===== Flink State =====

    /** 最近24小时交易列表 */
    private transient ListState<Transaction> recentTransactionState;

    /** 行为基线 */
    private transient ValueState<BehaviorBaseline> behaviorBaselineState;

    /** 30天设备集合 */
    private transient MapState<String, Long> deviceState;

    /** 30天城市集合 */
    private transient MapState<String, Long> cityState;

    /** 30天交易对手集合 */
    private transient MapState<String, Long> counterpartyState;

    /** 24小时交易计数滚动窗口 */
    private transient ValueState<RollingWindow> rollingWindowState;

    @Override
    public void open(Configuration parameters) {
        recentTransactionState = getRuntimeContext().getListState(
            new ListStateDescriptor<>("recent-transactions", Transaction.class));

        behaviorBaselineState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("behavior-baseline", BehaviorBaseline.class));

        MapStateDescriptor<String, Long> deviceDesc = new MapStateDescriptor<>(
            "device-set", String.class, Long.class);
        deviceState = getRuntimeContext().getMapState(deviceDesc);

        MapStateDescriptor<String, Long> cityDesc = new MapStateDescriptor<>(
            "city-set", String.class, Long.class);
        cityState = getRuntimeContext().getMapState(cityDesc);

        MapStateDescriptor<String, Long> counterpartyDesc = new MapStateDescriptor<>(
            "counterparty-set", String.class, Long.class);
        counterpartyState = getRuntimeContext().getMapState(counterpartyDesc);

        rollingWindowState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("rolling-window", RollingWindow.class));
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<EnrichedTransaction> out)
            throws Exception {

        long eventTime = tx.getTransactionTime() != null
            ? tx.getTransactionTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
            : System.currentTimeMillis();

        // 注册清理定时器（24小时后）
        ctx.timerService().registerEventTimeTimer(eventTime + 86_400_000L);

        // ===== 更新状态 =====

        // 1. 将当前交易加入最近交易列表
        recentTransactionState.add(tx);

        // 2. 更新设备和城市计数
        if (tx.getDeviceId() != null) {
            deviceState.put(tx.getDeviceId(), eventTime);
        }
        if (tx.getIpCity() != null) {
            cityState.put(tx.getIpCity(), eventTime);
        }
        if (tx.getCounterpartyHash() != null) {
            counterpartyState.put(tx.getCounterpartyHash(), eventTime);
        }

        // 3. 更新滚动窗口
        RollingWindow window = rollingWindowState.value();
        if (window == null) {
            window = new RollingWindow();
        }
        window.add(eventTime, tx.getAmount() != null ? tx.getAmount() : BigDecimal.ZERO);
        rollingWindowState.update(window);

        // ===== 计算实时特征 =====

        List<Transaction> recentTxs = new ArrayList<>();
        recentTransactionState.get().forEach(recentTxs::add);

        // 1小时窗口特征
        long cutoff1h = eventTime - 3_600_000L;
        BigDecimal amount1h = BigDecimal.ZERO;
        int count1h = 0;
        for (Transaction t : recentTxs) {
            long tTime = t.getTransactionTime() != null
                ? t.getTransactionTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
                : 0;
            if (tTime >= cutoff1h) {
                count1h++;
                if (t.getAmount() != null) {
                    amount1h = amount1h.add(t.getAmount());
                }
            }
        }

        // 24小时窗口特征
        long cutoff24h = eventTime - 86_400_000L;
        BigDecimal amount24h = BigDecimal.ZERO;
        int count24h = 0;
        for (Transaction t : recentTxs) {
            long tTime = t.getTransactionTime() != null
                ? t.getTransactionTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
                : 0;
            if (tTime >= cutoff24h) {
                count24h++;
                if (t.getAmount() != null) {
                    amount24h = amount24h.add(t.getAmount());
                }
            }
        }

        // 设备/城市/对手统计
        int deviceCount = countMapEntries(deviceState, eventTime, 30L * 86_400_000L);
        int cityCount = countMapEntries(cityState, eventTime, 30L * 86_400_000L);
        int counterpartyCount = countMapEntries(counterpartyState, eventTime, 30L * 86_400_000L);

        // 是否新设备
        String deviceId = tx.getDeviceId();
        boolean isFirstDevice = deviceId != null &&
            (!deviceState.contains(deviceId) || deviceCount == 1);

        // 金额偏离度
        Double amountDeviation = calculateDeviation(tx.getAmount(), window);

        // ===== 构建EnrichedTransaction =====

        EnrichedTransaction enriched = EnrichedTransaction.builder()
            .transaction(tx)
            .amount1h(amount1h.setScale(2, RoundingMode.HALF_UP))
            .count1h(count1h)
            .amount24h(amount24h.setScale(2, RoundingMode.HALF_UP))
            .count24h(count24h)
            .deviceCount30d(deviceCount)
            .cityCount30d(cityCount)
            .counterpartyCount30d(counterpartyCount)
            .isNightTime(tx.isNightTime())
            .isCrossBorder(tx.getIsCrossBorder() != null && tx.getIsCrossBorder())
            .isFirstDevice(isFirstDevice)
            .ipRiskLevel(tx.getIpRiskLevel())
            .amountDeviation(amountDeviation)
            .build();

        out.collect(enriched);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedTransaction> out)
            throws Exception {
        // 清理24小时前的过期数据
        long cutoff = timestamp - 86_400_000L;
        List<Transaction> currentTxs = new ArrayList<>();
        recentTransactionState.get().forEach(currentTxs);

        List<Transaction> validTxs = new ArrayList<>();
        for (Transaction t : currentTxs) {
            long tTime = t.getTransactionTime() != null
                ? t.getTransactionTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
                : 0;
            if (tTime >= cutoff) {
                validTxs.add(t);
            }
        }
        recentTransactionState.update(validTxs);
    }

    // ===== 辅助方法 =====

    private int countMapEntries(MapState<String, Long> state, long currentTime, long windowMs)
            throws Exception {
        int count = 0;
        for (String key : state.keys()) {
            Long ts = state.get(key);
            if (ts != null && (currentTime - ts) <= windowMs) {
                count++;
            }
        }
        return count;
    }

    private Double calculateDeviation(BigDecimal amount, RollingWindow window) {
        if (amount == null || window == null || window.getCount() < 2) {
            return 0.0;
        }
        double mean = window.getSum().doubleValue() / window.getCount();
        if (mean == 0) return 0.0;
        return Math.abs(amount.doubleValue() - mean) / mean;
    }

    // ===== 内部类 =====

    /**
     * 行为基线
     */
    private static class BehaviorBaseline {
        private double avgAmount;
        private double stdAmount;
        private double avgCount;

        public double getAvgAmount() { return avgAmount; }
        public void setAvgAmount(double avgAmount) { this.avgAmount = avgAmount; }
        public double getStdAmount() { return stdAmount; }
        public void setStdAmount(double stdAmount) { this.stdAmount = stdAmount; }
    }

    /**
     * 滚动窗口统计
     */
    private static class RollingWindow {
        private long count;
        private BigDecimal sum;

        public RollingWindow() {
            this.count = 0;
            this.sum = BigDecimal.ZERO;
        }

        public void add(long timestamp, BigDecimal amount) {
            count++;
            sum = sum.add(amount);
        }

        public long getCount() { return count; }
        public BigDecimal getSum() { return sum; }
    }
}
