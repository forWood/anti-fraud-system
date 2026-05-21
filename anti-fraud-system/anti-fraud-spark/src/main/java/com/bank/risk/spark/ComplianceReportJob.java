package com.bank.risk.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 可疑交易合规报告生成作业 (FATF合规要求)
 *
 * 功能:
 * - 识别大额可疑交易 (单笔 >= 50000 CNY)
 * - 识别拆分交易 (同一客户同一天多笔交易总额 >= 50000 CNY)
 * - 识别跨境异常交易
 * - 生成合规报告写入 ClickHouse
 */
public class ComplianceReportJob {

    private static final Logger log = LoggerFactory.getLogger(ComplianceReportJob.class);
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        String batchDate = LocalDate.now().toString();
        for (int i = 0; i < args.length - 1; i++) {
            if ("--date".equals(args[i])) {
                batchDate = args[i + 1];
            }
        }

        log.info("Starting ComplianceReportJob for date: {}", batchDate);

        SparkConf conf = new SparkConf()
            .setAppName("ComplianceReportJob")
            .setMaster("yarn")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        try (SparkSession spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()) {

            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            jsc.setLogLevel("WARN");

            // 加载交易数据
            Dataset<Row> transactions = loadTransactions(spark, batchDate);

            // 大额可疑交易
            Dataset<Row> largeTransactions = detectLargeTransactions(transactions);
            log.info("Detected {} large suspicious transactions", largeTransactions.count());

            // 拆分交易 (同一客户同一天多笔，总额 >= 50000)
            Dataset<Row> splitTransactions = detectSplitTransactions(spark, transactions);
            log.info("Detected {} split transaction patterns", splitTransactions.count());

            // 跨境异常
            Dataset<Row> crossBorder = detectCrossBorder(transactions);
            log.info("Detected {} cross-border anomalies", crossBorder.count());

            // 合并所有可疑交易
            Dataset<Row> allSuspicious = largeTransactions
                .union(splitTransactions)
                .union(crossBorder)
                .distinct()
                .withColumn("report_date", org.apache.spark.sql.functions.lit(batchDate))
                .withColumn("generated_at", org.apache.spark.sql.functions.lit(LocalDateTime.now().format(FMT)));

            // 写入 ClickHouse
            writeToClickHouse(allSuspicious, "suspicious_transaction_reports");

            // 生成统计摘要
            generateSummary(spark, allSuspicious, batchDate);

            log.info("ComplianceReportJob completed for date {}", batchDate);

        } catch (Exception e) {
            log.error("ComplianceReportJob failed", e);
            System.exit(1);
        }
    }

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

    /** 检测大额可疑交易 (>= 50000 CNY) */
    private static Dataset<Row> detectLargeTransactions(Dataset<Row> transactions) {
        return transactions
            .filter(org.apache.spark.sql.functions.col("amount").geq(50000))
            .withColumn("suspicious_type", org.apache.spark.sql.functions.lit("LARGE_AMOUNT"))
            .withColumn("suspicious_level",
                org.apache.spark.sql.functions.when(
                    org.apache.spark.sql.functions.col("amount").geq(100000), "HIGH"
                ).otherwise("MEDIUM")
            )
            .withColumn("reason", org.apache.spark.sql.functions.concat(
                org.apache.spark.sql.functions.lit("大额交易: "),
                org.apache.spark.sql.functions.col("amount")
            ));
    }

    /** 检测拆分交易 (同一客户同一天多笔，总额 >= 50000) */
    private static Dataset<Row> detectSplitTransactions(SparkSession spark, Dataset<Row> transactions) {
        // 按客户+日期分组，计算总额
        Dataset<Row> dailyTotals = transactions.groupBy("customer_id",
                org.apache.spark.sql.functions.to_date(transactions.col("trans_time")).as("trans_date"))
            .agg(
                org.apache.spark.sql.functions.sum("amount").as("daily_total"),
                org.apache.spark.sql.functions.count("*").as("txn_count"),
                org.apache.spark.sql.functions.min(transactions.col("amount")).as("min_amount"),
                org.apache.spark.sql.functions.max(transactions.col("amount")).as("max_amount")
            )
            .filter(org.apache.spark.sql.functions.col("daily_total").geq(50000))
            .filter(org.apache.spark.sql.functions.col("txn_count").geq(2))
            .filter(
                // 确实存在拆分特征：每笔都低于阈值但总额高
                org.apache.spark.sql.functions.col("max_amount").lt(50000)
            );

        // 关联回原始交易
        return transactions.join(
            dailyTotals.select("customer_id", "trans_date", "daily_total", "txn_count"),
            org.apache.spark.sql.functions.to_date(transactions.col("trans_time"))
                .equalTo(dailyTotals.col("trans_date"))
                .and(transactions.col("customer_id").equalTo(dailyTotals.col("customer_id")))
        )
        .withColumn("suspicious_type", org.apache.spark.sql.functions.lit("SPLIT_TRANSACTION"))
        .withColumn("suspicious_level", org.apache.spark.sql.functions.lit("HIGH"))
        .withColumn("reason", org.apache.spark.sql.functions.concat(
            org.apache.spark.sql.functions.lit("拆分交易: 日总额="),
            org.apache.spark.sql.functions.col("daily_total"),
            org.apache.spark.sql.functions.lit(", 笔数="),
            org.apache.spark.sql.functions.col("txn_count")
        ));
    }

    /** 检测跨境异常 */
    private static Dataset<Row> detectCrossBorder(Dataset<Row> transactions) {
        return transactions
            .filter(org.apache.spark.sql.functions.col("is_cross_border").equalTo(true))
            .filter(org.apache.spark.sql.functions.col("amount").geq(10000))
            .withColumn("suspicious_type", org.apache.spark.sql.functions.lit("CROSS_BORDER"))
            .withColumn("suspicious_level",
                org.apache.spark.sql.functions.when(
                    org.apache.spark.sql.functions.col("amount").geq(50000), "HIGH"
                ).otherwise("MEDIUM")
            )
            .withColumn("reason", org.apache.spark.sql.functions.concat(
                org.apache.spark.sql.functions.lit("跨境交易: "),
                org.apache.spark.sql.functions.col("amount"),
                org.apache.spark.sql.functions.lit(" "),
                org.apache.spark.sql.functions.col("country")
            ));
    }

    /** 生成统计摘要 */
    private static void generateSummary(SparkSession spark, Dataset<Row> suspicious, String batchDate) {
        Dataset<Row> summary = suspicious.groupBy("suspicious_type", "suspicious_level")
            .agg(
                org.apache.spark.sql.functions.count("*").as("count"),
                org.apache.spark.sql.functions.sum("amount").as("total_amount")
            )
            .withColumn("report_date", org.apache.spark.sql.functions.lit(batchDate))
            .withColumn("generated_at", org.apache.spark.sql.functions.lit(LocalDateTime.now().format(FMT)));

        writeToClickHouse(summary, "compliance_summary");
        log.info("Compliance summary generated");
    }

    private static void writeToClickHouse(Dataset<Row> df, String tableName) {
        String jdbcUrl = System.getenv("CLICKHOUSE_JDBC_URL");
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            jdbcUrl = "jdbc:clickhouse://localhost:8123/default";
        }

        Map<String, String> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("dbtable", tableName);
        options.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");

        df.write()
            .format("jdbc")
            .mode(SaveMode.Overwrite)
            .options(options)
            .save();

        log.info("Data written to ClickHouse table: {}", tableName);
    }
}
