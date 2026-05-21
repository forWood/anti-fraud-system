package com.bank.risk.flink.sink;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Kafka输出Sink（用于清洁数据和特征数据输出）
 *
 * @author 银行科技部
 */
public class EnrichedKafkaSink extends FlinkKafkaProducer<String> {

    public EnrichedKafkaSink(String bootstrapServers, String topic) {
        super(
            topic,
            new SimpleStringSchema(),
            createProperties(bootstrapServers)
        );
    }

    private static Properties createProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("acks", "1");
        props.setProperty("compression.type", "snappy");
        return props;
    }
}
