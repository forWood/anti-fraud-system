package com.bank.risk.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 批量客户风险评分作业
 *
 * 功能:
 * - 从 MySQL/ClickHouse 读取客户交易数据
 * - 计算多维度风险指标
 * - 生成客户风险评分 (0-100)
 * - 写入风险评分结果表
 *
 * 运行方式:
 *   spark-submit --master yarn --class com.bank.risk.spark.BatchRiskScoringJob \
 *     anti-fraud-spark-2.0.0.jar --date 2026-05-17
 */
public class BatchRiskScoringJob {

    private static final Logger log = LoggerFactory.getLogger(BatchRiskScoringJob.class);

    public static void main(String[] args) {
        String batchDate = LocalDate.now().toString();
        for (int i = 0; i < args.length - 1; i++) {
            if ("--date".equals(args[i])) {
                batchDate = args[i + 1];
            }
        }

        log.info("Starting BatchRiskScoringJob for date: {}", batchDate);

        SparkConf conf = new SparkConf()
            .setAppName("BatchRiskScoringJob")
            .setMaster("yarn")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.sql.adaptive.enabled", "true")
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true");

        try (SparkSession spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()) {

            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            jsc.setLogLevel("WARN");

            // 1. 读取交易数据 (MySQL)
            Dataset<Row> transactions = loadTransactions(spark, batchDate);
            log.info("Loaded {} transactions for date {}", transactions.count(), batchDate);

            // 2. 读取客户数据
            Dataset<Row> customers = loadCustomers(spark);
            log.info("Loaded {} customers", customers.count());

            // 3. 计算风险指标
            Dataset<Row> riskFeatures = computeRiskFeatures(spark, transactions, customers);

            // 4. 计算风险评分 (简化的评分规则)
            Dataset<Row> riskScores = computeRiskScores(spark, riskFeatures);

            // 5. 写入结果 (ClickHouse)
            writeRiskScores(riskScores);

            log.info("BatchRiskScoringJob completed successfully for date {}", batchDate);

        } catch (Exception e) {
            log.error("BatchRiskScoringJob failed", e);
            System.exit(1);
        }
    }

    /**
     * 从 MySQL 加载交易数据
     */
    private static Dataset<Row> loadTransactions(SparkSession spark, String date) {
        String jdbcUrl = System.getenv("MYSQL_JDBC_URL");
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            jdbcUrl = "jdbc:mysql://localhost:3306/risk_db";
        }

        Map<String, String> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("dbtable", "transaction_records");
        options.put("user", System.getenv("MYSQL_USER") != null ? System.getenv("MYSQL_USER") : "risk_user");
        options.put("password", System.getenv("MYSQL_PASSWORD") != null ? System.getenv("MYSQL_PASSWORD") : "risk_pass_2026");
        options.put("driver", "com.mysql.cj.jdbc.Driver");

        return spark.read().format("jdbc").options(options).load()
            .filter("DATE(trans_time) = '" + date + "'");
    }

    /**
     * 从 ClickHouse 加载客户数据
     */
    private static Dataset<Row> loadCustomers(SparkSession spark) {
        String jdbcUrl = System.getenv("CLICKHOUSE_JDBC_URL");
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            jdbcUrl = "jdbc:clickhouse://localhost:8123/default";
        }

        Map<String, String> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("dbtable", "customer_profiles");
        options.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");

        return spark.read().format("jdbc").options(options).load();
    }

    /**
     * 计算风险特征
     */
    private static Dataset<Row> computeRiskFeatures(SparkSession spark,
            Dataset<Row> transactions, Dataset<Row> customers) {

        // 计算每个客户的交易统计特征
        Dataset<Row> txnStats = transactions.groupBy("customer_id")
            .agg(
                org.apache.spark.sql.functions.count("*").as("txn_count"),
                org.apache.spark.sql.functions.sum("amount").as("total_amount"),
                org.apache.spark.sql.functions.avg("amount").as("avg_amount"),
                org.apache.spark.sql.functions.max("amount").as("max_amount"),
                org.apache.spark.sql.functions.min("amount").as("min_amount"),
                org.apache.spark.sql.functions.stddev("amount").as("stddev_amount"),
                org.apache.spark.sql.functions.countDistinct("target_account").as("unique_targets")
            );

        // 关联客户基本信息
        return txnStats.join(
            customers.select("customer_id", "customer_name", "account_type", "risk_level"),
            "customer_id", "left"
        );
    }

    /**
     * 计算综合风险评分 (0-100)
     *
     * 评分维度:
     * - 交易频率异常 (权重 25%)
     * - 大额交易占比 (权重 25%)
     * - 目标账户分散度 (权重 20%)
     * - 金额波动度 (权重 15%)
     * - 历史风险记录 (权重 15%)
     */
    private static Dataset<Row> computeRiskScores(SparkSession spark, Dataset<Row> features) {
        Dataset<Row> scores = features.withColumn("score_txn_freq",
                org.apache.spark.sql.functions.when(
                    org.apache.spark.sql.functions.col("txn_count").gt(50), 40.0
                ).when(
                    org.apache.spark.sql.functions.col("txn_count").gt(20), 25.0
                ).when(
                    org.apache.spark.sql.functions.col("txn_count").gt(10), 10.0
                ).otherwise(0.0)
            )
            .withColumn("score_large_txn",
                org.apache.spark.sql.functions.when(
                    org.apache.spark.sql.functions.col("avg_amount").gt(50000), 35.0
                ).when(
                    org.apache.spark.sql.functions.col("avg_amount").gt(20000), 20.0
                ).when(
                    org.apache.spark.sql.functions.col("avg_amount").gt(10000), 10.0
                ).otherwise(0.0)
            )
            .withColumn("score_target_dispersion",
                org.apache.spark.sql.functions.when(
                    org.apache.spark.sql.functions.col("unique_targets").gt(20), 30.0
                ).when(
                    org.apache.spark.sql.functions.col("unique_targets").gt(10), 20.0
                ).when(
                    org.apache.spark.sql.functions.col("unique_targets").gt(5), 10.0
                ).otherwise(0.0)
            )
            .withColumn("score_volatility",
                org.apache.spark.sql.functions.coalesce(
                    org.apache.spark.sql.functions.when(
                        org.apache.spark.sql.functions.col("stddev_amount").divide(
                            org.apache.spark.sql.functions.col("avg_amount")
                        ).gt(2.0), 20.0
                    ).otherwise(0.0),
                    org.apache.spark.sql.functions.lit(0.0)
                )
            )
            .withColumn("score_history",
                org.apache.spark.sql.functions.when(
                    org.apache.spark.sql.functions.col("risk_level").equalTo("HIGH"), 30.0
                ).when(
                    org.apache.spark.sql.functions.col("risk_level").equalTo("MEDIUM"), 15.0
                ).otherwise(0.0)
            );

        // 综合评分
        return scores.withColumn("risk_score",
                org.apache.spark.sql.functions.col("score_txn_freq")
                .plus(org.apache.spark.sql.functions.col("score_large_txn"))
                .plus(org.apache.spark.sql.functions.col("score_target_dispersion"))
                .plus(org.apache.spark.sql.functions.col("score_volatility"))
                .plus(org.apache.spark.sql.functions.col("score_history"))
            )
            .withColumn("risk_level",
                org.apache.spark.sql.functions.when(
                    org.apache.spark.sql.functions.col("risk_score").geq(70), "CRITICAL"
                ).when(
                    org.apache.spark.sql.functions.col("risk_score").geq(50), "HIGH"
                ).when(
                    org.apache.spark.sql.functions.col("risk_score").geq(30), "MEDIUM"
                ).otherwise("LOW")
            )
            .withColumn("scored_at",
                org.apache.spark.sql.functions.lit(LocalDateTime.now().toString())
            )
            .withColumn("batch_date",
                org.apache.spark.sql.functions.lit(LocalDate.now().toString())
            )
            .drop("score_txn_freq", "score_large_txn", "score_target_dispersion",
                  "score_volatility", "score_history");
    }

    /**
     * 写入风险评分结果到 ClickHouse
     */
    private static void writeRiskScores(Dataset<Row> scores) {
        String jdbcUrl = System.getenv("CLICKHOUSE_JDBC_URL");
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            jdbcUrl = "jdbc:clickhouse://localhost:8123/default";
        }

        Map<String, String> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("dbtable", "customer_risk_scores");
        options.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");
        options.put("batchsize", "1000");
        options.put("isolationLevel", "NONE");

        scores.write()
            .format("jdbc")
            .mode(SaveMode.Overwrite)
            .options(options)
            .save();

        log.info("Risk scores written to ClickHouse: customer_risk_scores");
    }
}
