package com.bank.risk.collection.producer;

import com.bank.risk.common.model.Transaction;
import com.bank.risk.common.util.DataSecurityUtil;
import com.bank.risk.common.util.JsonUtil;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Kafka交易流水模拟生产者
 * 模拟生成真实的银行交易数据，发送到 transaction-raw topic
 *
 * 字段覆盖：交易流水号、账户ID、金额、时间、渠道、设备指纹、IP地址
 * 符合SRS 6.2.1节交易记录字段规范
 *
 * @author 银行科技部
 */
public class TransactionKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionKafkaProducer.class);
    private static final Random RANDOM = new SecureRandom();

    // ===== 模拟数据池 =====
    private static final String[] ACCOUNT_IDS = {
        "6222021234567890", "6222039876543210", "6225881234567800",
        "6217001234567801", "6228489876543202", "6227001234567890"
    };
    private static final String[] CHANNELS = {"EBANK", "MBANK", "COUNTER", "ATM", "POS"};
    private static final String[] TX_TYPES = {"TRANSFER", "PAYMENT", "WITHDRAW", "DEPOSIT"};
    private static final String[] DEVICE_TYPES = {"MOBILE", "PC", "PAD"};
    private static final String[] DEVICE_OS = {"Android 14", "iOS 18", "Windows 11", "macOS 14"};
    private static final String[] IP_ADDRESSES = {
        "114.114.114.114", "8.8.8.8", "192.168.1.100",
        "220.181.38.148", "59.82.9.100", "1.1.1.1",
        "185.220.101.50", "23.129.64.210"  // 高风险IP
    };
    private static final String[] IP_CITIES = {
        "北京", "上海", "广州", "深圳", "杭州", "成都",
        "New York", "London", "Singapore", "Moscow"
    };
    private static final String[] REMARKS = {
        "货款", "工资", "还款", "转账", "购物", "",
        "投资款", "借款", "租金", "分红"
    };

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private volatile boolean running = true;

    /**
     * 构建Kafka生产者
     * @param bootstrapServers Kafka服务地址
     * @param topic 目标topic名称
     */
    public TransactionKafkaProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 生产优化配置
        props.put(ProducerConfig.ACKS_CONFIG, "1");                     // 至少leader确认
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");    // 压缩传输
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);             // 16KB批次
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);                  // 5ms发送延迟
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);                    // 重试3次

        this.producer = new KafkaProducer<>(props);
        log.info("KafkaProducer initialized: servers={}, topic={}", bootstrapServers, topic);
    }

    /**
     * 启动模拟数据生产
     * @param ratePerSec 每秒生成交易数 (建议 100-5000)
     * @param durationSeconds 持续时长(秒), -1表示持续运行
     */
    public void start(int ratePerSec, int durationSeconds) {
        log.info("Starting transaction simulation: rate={}/s, duration={}s", ratePerSec, durationSeconds);
        long startTime = System.currentTimeMillis();
        long targetDuration = durationSeconds > 0 ? durationSeconds * 1000L : Long.MAX_VALUE;
        long produced = 0;

        // 计算每次发送间隔（纳秒）
        long intervalNs = 1_000_000_000L / Math.max(ratePerSec, 1);

        while (running && (System.currentTimeMillis() - startTime) < targetDuration) {
            long batchStart = System.nanoTime();

            Transaction tx = generateTransaction();
            sendTransaction(tx);
            produced++;

            // 每1000条输出一次日志
            if (produced % 1000 == 0) {
                log.info("Produced {} transactions, latest: {}", produced, tx.getTransactionId());
            }

            // 控制发送速率
            long elapsed = System.nanoTime() - batchStart;
            if (elapsed < intervalNs) {
                try {
                    Thread.sleep((intervalNs - elapsed) / 1_000_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Simulation ended. Total produced: {} transactions", produced);
    }

    /**
     * 生成一条模拟交易数据
     */
    private Transaction generateTransaction() {
        String accountId = ACCOUNT_IDS[RANDOM.nextInt(ACCOUNT_IDS.length)];
        String counterparty = ACCOUNT_IDS[RANDOM.nextInt(ACCOUNT_IDS.length)];

        // 确保对手账号不同
        while (counterparty.equals(accountId)) {
            counterparty = ACCOUNT_IDS[RANDOM.nextInt(ACCOUNT_IDS.length)];
        }

        String channel = CHANNELS[RANDOM.nextInt(CHANNELS.length)];
        String ipAddr = IP_ADDRESSES[RANDOM.nextInt(IP_ADDRESSES.length)];
        String ipCity = IP_CITIES[RANDOM.nextInt(IP_CITIES.length)];

        // 金额分布：大部分小额，少量大额模拟欺诈场景
        BigDecimal amount = generateRealisticAmount();
        String txType = TX_TYPES[RANDOM.nextInt(TX_TYPES.length)];

        // 10%概率模拟风险交易（新设备、异常时间等）
        boolean isRiskScenario = RANDOM.nextDouble() < 0.10;
        boolean isEmulator = isRiskScenario && RANDOM.nextDouble() < 0.3;
        boolean isRooted = isRiskScenario && RANDOM.nextDouble() < 0.2;

        // 夜间交易概率
        boolean isNightTime = isRiskScenario && RANDOM.nextDouble() < 0.4;

        return Transaction.builder()
            .transactionId("TX" + DataSecurityUtil.generateId(""))
            .accountId(accountId)
            .accountIdHash(DataSecurityUtil.sha256(accountId))
            .counterpartyAccount(counterparty)
            .counterpartyHash(DataSecurityUtil.sha256(counterparty))
            .transactionType(txType)
            .channelCode(channel)
            .amount(amount)
            .currency("CNY")
            .transactionTime(generateTransactionTime(isNightTime))
            .ipAddress(ipAddr)
            .ipCountry("CN")
            .ipCity(ipCity)
            .ipRiskLevel(determineIpRisk(ipAddr, isRiskScenario))
            .deviceId("DEV-" + UUID.randomUUID().toString().substring(0, 12))
            .deviceType(DEVICE_TYPES[RANDOM.nextInt(DEVICE_TYPES.length)])
            .deviceOs(DEVICE_OS[RANDOM.nextInt(DEVICE_OS.length)])
            .isEmulator(isEmulator)
            .isRooted(isRooted)
            .isFirstDeviceBinding(isRiskScenario && RANDOM.nextDouble() < 0.5)
            .gpsLat(BigDecimal.valueOf(39.9042 + RANDOM.nextDouble() * 5 - 2.5).setScale(7, RoundingMode.HALF_UP))
            .gpsLng(BigDecimal.valueOf(116.4074 + RANDOM.nextDouble() * 5 - 2.5).setScale(7, RoundingMode.HALF_UP))
            .remark(REMARKS[RANDOM.nextInt(REMARKS.length)])
            .dataSource("CORE")
            .isCrossBorder(isRiskScenario && RANDOM.nextDouble() < 0.3)
            .counterpartyName("对手方" + RANDOM.nextInt(1000))
            .counterpartyCountry("CN")
            .createdAt(LocalDateTime.now())
            .build();
    }

    /**
     * 生成符合真实分布的金额
     * 70%小额(100-10000), 20%中等(10000-100000), 8%大额(100000-500000), 2%超大额(>500000)
     */
    private BigDecimal generateRealisticAmount() {
        double rand = RANDOM.nextDouble();
        double amount;
        if (rand < 0.70) {
            amount = 100 + RANDOM.nextDouble() * 9900;           // 100-10000
        } else if (rand < 0.90) {
            amount = 10000 + RANDOM.nextDouble() * 90000;        // 10000-100000
        } else if (rand < 0.98) {
            amount = 100000 + RANDOM.nextDouble() * 400000;      // 100000-500000
        } else {
            amount = 500000 + RANDOM.nextDouble() * 500000;      // 500000-1000000
        }
        return BigDecimal.valueOf(amount).setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * 生成交易时间
     */
    private LocalDateTime generateTransactionTime(boolean isNightTime) {
        LocalDateTime now = LocalDateTime.now();
        if (isNightTime) {
            return now.withHour(RANDOM.nextInt(5)).withMinute(RANDOM.nextInt(60));
        }
        return now.minusMinutes(RANDOM.nextInt(10));
    }

    /**
     * 根据IP和场景确定IP风险等级
     */
    private String determineIpRisk(String ipAddr, boolean isRiskScenario) {
        if (ipAddr.equals("185.220.101.50")) return "TOR";
        if (ipAddr.equals("23.129.64.210")) return "PROXY";
        if (isRiskScenario && RANDOM.nextDouble() < 0.2) return "VPN";
        return "NORMAL";
    }

    /**
     * 发送交易记录到Kafka
     */
    private void sendTransaction(Transaction tx) {
        String key = tx.getAccountIdHash();
        String value = JsonUtil.toJson(tx);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send transaction {}: {}", tx.getTransactionId(), exception.getMessage());
            }
        });
    }

    /**
     * 同步发送（用于测试）
     */
    public void sendSync(Transaction tx) throws ExecutionException, InterruptedException {
        String key = tx.getAccountIdHash();
        String value = JsonUtil.toJson(tx);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        log.debug("Sent: partition={}, offset={}", metadata.partition(), metadata.offset());
    }

    /**
     * 停止生产
     */
    public void stop() {
        running = false;
    }

    /**
     * 关闭资源
     */
    public void close() {
        running = false;
        if (producer != null) {
            producer.flush();
            producer.close();
        }
        log.info("KafkaProducer closed");
    }

    // ===== 主入口 =====

    public static void main(String[] args) {
        String bootstrapServers = System.getProperty("kafka.servers", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "transaction-raw");
        int rate = Integer.parseInt(System.getProperty("producer.rate", "100"));
        int duration = Integer.parseInt(System.getProperty("producer.duration", "60"));

        TransactionKafkaProducer producer = new TransactionKafkaProducer(bootstrapServers, topic);

        // 优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down producer...");
            producer.close();
        }));

        producer.start(rate, duration);
        producer.close();
    }
}
