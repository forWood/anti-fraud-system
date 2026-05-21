package com.bank.risk.flink.source;

import com.bank.risk.common.model.Transaction;
import com.bank.risk.common.util.JsonUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Kafka交易数据源
 *
 * @author 银行科技部
 */
public class TransactionKafkaSource extends FlinkKafkaConsumer<Transaction> {

    /**
     * 创建Kafka数据源
     *
     * @param bootstrapServers Kafka服务地址
     * @param topic 消费Topic
     * @param groupId 消费组ID
     */
    public TransactionKafkaSource(String bootstrapServers, String topic, String groupId) {
        super(
            topic,
            new TransactionDeserializationSchema(),
            createKafkaProperties(bootstrapServers, groupId)
        );
    }

    private static Properties createKafkaProperties(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", groupId);
        // 从最新数据开始消费
        props.setProperty("auto.offset.reset", "latest");
        // 禁用自动提交（由Flink Checkpoint管理）
        props.setProperty("enable.auto.commit", "false");
        // 性能配置
        props.setProperty("max.partition.fetch.bytes", String.valueOf(10 * 1024 * 1024)); // 10MB
        props.setProperty("fetch.max.bytes", String.valueOf(50 * 1024 * 1024));            // 50MB
        props.setProperty("session.timeout.ms", "30000");
        props.setProperty("heartbeat.interval.ms", "10000");
        return props;
    }
}
