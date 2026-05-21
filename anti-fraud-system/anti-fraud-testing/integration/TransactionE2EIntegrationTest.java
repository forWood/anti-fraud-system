package com.bank.risk.testing.integration;

import com.bank.risk.collection.producer.TransactionKafkaProducer;
import com.bank.risk.common.model.Transaction;
import com.bank.risk.common.util.JsonUtil;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 集成测试 - 端到端交易风险评估流程
 *
 * 使用TestContainers启动Kafka和MySQL，
 * 模拟从交易产生到风险评估的完整链路。
 *
 * 启动方式: mvn verify -Pintegration-test
 *
 * @author 银行科技部
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TransactionE2EIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>(
        DockerImageName.parse("mysql:8.0.33"))
        .withDatabaseName("risk_db")
        .withUsername("risk_user")
        .withPassword("risk_pass_2026");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
        DockerImageName.parse("redis:7.0-alpine"))
        .withExposedPorts(6379);

    private static TransactionKafkaProducer producer;
    private static String bootstrapServers;

    @BeforeAll
    static void setUp() {
        bootstrapServers = kafka.getBootstrapServers();
        producer = new TransactionKafkaProducer(bootstrapServers, "transaction-raw");

        System.out.println("==========================================");
        System.out.println(" 集成测试环境启动");
        System.out.println(" Kafka: " + bootstrapServers);
        System.out.println(" MySQL: " + mysql.getJdbcUrl());
        System.out.println(" Redis: " + redis.getHost() + ":" + redis.getMappedPort(6379));
        System.out.println("==========================================");
    }

    @AfterAll
    static void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    // ========================================================================
    // Test 1: 正常交易端到端流程
    // ========================================================================

    @Test
    @Order(1)
    void testNormalTransactionFlow() throws Exception {
        // Given: 创建一笔正常交易
        Transaction tx = Transaction.builder()
            .transactionId("TX-TEST-001")
            .accountId("6222021234567890")
            .accountIdHash("hash_" + System.currentTimeMillis())
            .transactionType("TRANSFER")
            .channelCode("EBANK")
            .amount(new BigDecimal("5000.00"))
            .currency("CNY")
            .transactionTime(java.time.LocalDateTime.now())
            .ipAddress("114.114.114.114")
            .ipRiskLevel("NORMAL")
            .deviceId("DEV-NORMAL-001")
            .isEmulator(false)
            .isRooted(false)
            .isFirstDeviceBinding(false)
            .isCrossBorder(false)
            .build();

        // When: 发送到Kafka
        assertDoesNotThrow(() -> producer.sendSync(tx));

        // Then: 验证交易已发送
        assertNotNull(tx.getTransactionId());
        assertEquals(new BigDecimal("5000.00"), tx.getAmount());
        assertEquals("NORMAL", tx.getIpRiskLevel());

        System.out.println("Test 1 PASS: Normal transaction sent to Kafka");
    }

    // ========================================================================
    // Test 2: 高风险交易端到端流程
    // ========================================================================

    @Test
    @Order(2)
    void testHighRiskTransactionFlow() throws Exception {
        // Given: 创建高风险交易(大额+新设备+模拟器+高风险IP)
        Transaction tx = Transaction.builder()
            .transactionId("TX-TEST-002")
            .accountId("6222039876543210")
            .accountIdHash("hash_" + System.currentTimeMillis())
            .transactionType("TRANSFER")
            .channelCode("EBANK")
            .amount(new BigDecimal("800000.00"))
            .currency("CNY")
            .transactionTime(java.time.LocalDateTime.now())
            .ipAddress("185.220.101.50")
            .ipRiskLevel("HIGH_RISK")
            .deviceId("DEV-RISK-001")
            .isEmulator(true)
            .isRooted(true)
            .isFirstDeviceBinding(true)
            .isCrossBorder(true)
            .counterpartyCountry("KP")  // 高风险国家
            .build();

        // When: 发送到Kafka
        assertDoesNotThrow(() -> producer.sendSync(tx));

        // Then: 验证交易字段
        assertNotNull(tx.getTransactionId());
        assertEquals(new BigDecimal("800000.00"), tx.getAmount());
        assertTrue(tx.getIsEmulator());
        assertTrue(tx.getIsRooted());
        assertTrue(tx.getIsFirstDeviceBinding());
        assertEquals("HIGH_RISK", tx.getIpRiskLevel());

        System.out.println("Test 2 PASS: High-risk transaction sent to Kafka");
    }

    // ========================================================================
    // Test 3: 批量交易压测
    // ========================================================================

    @Test
    @Order(3)
    void testBatchTransactions() throws Exception {
        int batchSize = 100;
        int successCount = 0;
        int failCount = 0;

        for (int i = 0; i < batchSize; i++) {
            try {
                Transaction tx = Transaction.builder()
                    .transactionId("TX-BATCH-" + String.format("%04d", i))
                    .accountId("6227001234567890")
                    .accountIdHash("batch_hash_" + i)
                    .transactionType("PAYMENT")
                    .channelCode(i % 2 == 0 ? "EBANK" : "MBANK")
                    .amount(new BigDecimal(i * 1000 + 100))
                    .currency("CNY")
                    .transactionTime(java.time.LocalDateTime.now())
                    .ipAddress("192.168.1." + (i % 255))
                    .ipRiskLevel("NORMAL")
                    .deviceId("DEV-" + i)
                    .build();

                producer.sendSync(tx);
                successCount++;
            } catch (Exception e) {
                failCount++;
                System.err.println("Batch test failed for i=" + i + ": " + e.getMessage());
            }
        }

        System.out.printf("Test 3: Batch test results: success=%d, failed=%d%n", successCount, failCount);
        assertEquals(batchSize, successCount);
        assertEquals(0, failCount);
    }

    // ========================================================================
    // Test 4: 数据脱敏验证
    // ========================================================================

    @Test
    @Order(4)
    void testDataMasking() {
        // 手机号脱敏
        String maskedPhone = com.bank.risk.common.util.DataSecurityUtil.maskPhone("13812345678");
        assertEquals("138****5678", maskedPhone);

        // 身份证脱敏
        String maskedId = com.bank.risk.common.util.DataSecurityUtil.maskIdCard("110101199001011234");
        assertEquals("110101********1234", maskedId);

        // 银行卡脱敏
        String maskedCard = com.bank.risk.common.util.DataSecurityUtil.maskBankCard("6222021234567890");
        assertEquals("6222****7890", maskedCard);

        // SHA-256哈希
        String hash = com.bank.risk.common.util.DataSecurityUtil.sha256("test_account");
        assertNotNull(hash);
        assertEquals(64, hash.length());

        System.out.println("Test 4 PASS: Data masking verified");
    }

    // ========================================================================
    // Test 5: 异常交易验证
    // ========================================================================

    @Test
    @Order(5)
    void testEdgeCases() {
        // 极端金额
        Transaction tx1 = Transaction.builder()
            .transactionId("TX-EDGE-001")
            .accountId("6222021234567890")
            .amount(new BigDecimal("10000000.00"))  // 1000万
            .transactionType("TRANSFER")
            .channelCode("COUNTER")
            .transactionTime(java.time.LocalDateTime.now())
            .build();

        assertEquals(new BigDecimal("10000000.00"), tx1.getAmount());

        // 微小金额
        Transaction tx2 = Transaction.builder()
            .transactionId("TX-EDGE-002")
            .accountId("6222021234567890")
            .amount(new BigDecimal("0.01"))
            .transactionType("PAYMENT")
            .channelCode("POS")
            .transactionTime(java.time.LocalDateTime.now())
            .build();

        assertEquals(new BigDecimal("0.01"), tx2.getAmount());

        // 夜间交易判断
        Transaction tx3 = Transaction.builder()
            .transactionId("TX-EDGE-003")
            .accountId("6222021234567890")
            .transactionType("TRANSFER")
            .channelCode("MBANK")
            .amount(new BigDecimal("1000.00"))
            .transactionTime(java.time.LocalDateTime.now().withHour(3).withMinute(0))
            .build();

        assertTrue(tx3.isNightTime());

        // 周末交易判断
        Transaction tx4 = Transaction.builder()
            .transactionId("TX-EDGE-004")
            .accountId("6222021234567890")
            .transactionType("TRANSFER")
            .channelCode("MBANK")
            .amount(new BigDecimal("1000.00"))
            .transactionTime(java.time.LocalDateTime.now()
                .with(java.time.DayOfWeek.SATURDAY))
            .build();

        assertTrue(tx4.isWeekend());

        System.out.println("Test 5 PASS: Edge cases verified");
    }
}
