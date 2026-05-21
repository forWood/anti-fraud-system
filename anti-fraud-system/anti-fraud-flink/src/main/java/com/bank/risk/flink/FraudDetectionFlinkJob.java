package com.bank.risk.flink;

import com.bank.risk.flink.sink.AlertKafkaSink;
import com.bank.risk.flink.sink.ClickHouseSink;
import com.bank.risk.flink.sink.EnrichedKafkaSink;
import com.bank.risk.flink.sink.RedisFeatureSink;
import com.bank.risk.flink.source.TransactionKafkaSource;
import com.bank.risk.flink.transform.DataCleanFunction;
import com.bank.risk.flink.transform.FeatureExtractionProcess;
import com.bank.risk.common.model.EnrichedTransaction;
import com.bank.risk.common.model.AlertEvent;
import com.bank.risk.common.model.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 反欺诈Flink实时计算主程序
 *
 * 数据流: Kafka(raw) → 数据清洗 → 特征提取 → 风险评估 → Kafka(assessed) + Redis + ClickHouse
 *
 * 架构说明:
 * 1. 从Kafka读取原始交易数据
 * 2. 数据清洗: 去重、格式标准化、敏感数据脱敏
 * 3. 实时特征: 1小时/24小时交易频次、设备指纹、IP风险等级
 * 4. 输出:
 *    - 清洁数据 → Kafka transaction-clean
 *    - 特征数据 → Redis (在线服务)
 *    - 特征数据 → ClickHouse (离线分析)
 *
 * @author 银行科技部
 */
public class FraudDetectionFlinkJob {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionFlinkJob.class);

    public static void main(String[] args) throws Exception {
        log.info("========================================");
        log.info(" 反欺诈Flink实时计算作业启动");
        log.info("========================================");

        // ===== 1. 创建Flink执行环境 =====
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置Checkpoint (精准一次语义)
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setMinPauseBetweenCheckpoints(30_000L);
        cpConfig.setCheckpointTimeout(120_000L);
        cpConfig.setMaxConcurrentCheckpoints(1);
        cpConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 配置并行度 (从环境变量读取或使用默认值)
        int parallelism = Integer.parseInt(System.getProperty("flink.parallelism", "4"));
        env.setParallelism(parallelism);

        // ===== 2. 数据源: 从Kafka读取原始交易 =====
        String kafkaBootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String rawTopic = System.getProperty("kafka.topic.raw", "transaction-raw");
        String cleanTopic = System.getProperty("kafka.topic.clean", "transaction-clean");
        String assessedTopic = System.getProperty("kafka.topic.assessed", "transaction-assessed");
        String alertTopic = System.getProperty("kafka.topic.alert", "alert-created");

        // 配置Watermark策略: 允许10秒乱序
        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((tx, timestamp) ->
                tx.getTransactionTime() != null
                    ? tx.getTransactionTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
                    : System.currentTimeMillis())
            .withIdleness(Duration.ofMinutes(1));

        DataStream<Transaction> rawTransactionStream = env
            .addSource(new TransactionKafkaSource(kafkaBootstrapServers, rawTopic, "fraud-detection-group"))
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .name("kafka-transaction-source")
            .uid("kafka-transaction-source");

        // ===== 3. 数据清洗: 去重、脱敏、异常过滤 =====
        DataStream<Transaction> cleanTransactionStream = rawTransactionStream
            .filter(tx -> tx.getAmount() != null
                && tx.getAmount().doubleValue() >= 0.01
                && tx.getAmount().doubleValue() <= 10_000_000.00)
            .map(new DataCleanFunction())
            .name("data-clean")
            .uid("data-clean");

        // 输出清洁数据到Kafka
        cleanTransactionStream
            .map(tx -> com.bank.risk.common.util.JsonUtil.toJson(tx))
            .addSink(new EnrichedKafkaSink(kafkaBootstrapServers, cleanTopic))
            .name("kafka-clean-sink")
            .uid("kafka-clean-sink");

        // ===== 4. 实时特征提取 =====
        DataStream<EnrichedTransaction> enrichedStream = cleanTransactionStream
            .keyBy(tx -> tx.getAccountIdHash() != null ? tx.getAccountIdHash() : "unknown")
            .process(new FeatureExtractionProcess())
            .name("feature-extraction")
            .uid("feature-extraction");

        // ===== 5. 输出: Redis (在线特征) =====
        String redisHost = System.getProperty("redis.host", "localhost");
        int redisPort = Integer.parseInt(System.getProperty("redis.port", "6379"));
        enrichedStream
            .addSink(new RedisFeatureSink(redisHost, redisPort))
            .name("redis-feature-sink")
            .uid("redis-feature-sink");

        // ===== 6. 输出: ClickHouse (离线分析) =====
        String clickhouseUrl = System.getProperty("clickhouse.url", "jdbc:clickhouse://localhost:8123/risk_db");
        enrichedStream
            .addSink(new ClickHouseSink(clickhouseUrl))
            .name("clickhouse-sink")
            .uid("clickhouse-sink");

        // ===== 7. 输出: 已评估交易到Kafka (下游智能体消费) =====
        enrichedStream
            .map(tx -> com.bank.risk.common.util.JsonUtil.toJson(tx))
            .addSink(new EnrichedKafkaSink(kafkaBootstrapServers, assessedTopic))
            .name("kafka-assessed-sink")
            .uid("kafka-assessed-sink");

        // ===== 8. 执行 =====
        env.execute("FraudDetection-Flink-Job");
    }
}
