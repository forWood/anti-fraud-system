package com.bank.risk.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * 客户关联关系分析作业
 *
 * 功能:
 * - 分析客户之间的关联关系 (同一IP、设备指纹、联系电话等)
 * - 构建客户关系图谱
 * - 识别可疑关联团体
 */
public class CustomerGraphAnalysisJob {

    private static final Logger log = LoggerFactory.getLogger(CustomerGraphAnalysisJob.class);

    public static void main(String[] args) {
        String batchDate = LocalDate.now().toString();
        for (int i = 0; i < args.length - 1; i++) {
            if ("--date".equals(args[i])) {
                batchDate = args[i + 1];
            }
        }

        log.info("Starting CustomerGraphAnalysisJob for date: {}", batchDate);

        SparkConf conf = new SparkConf()
            .setAppName("CustomerGraphAnalysisJob")
            .setMaster("yarn")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        try (SparkSession spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()) {

            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            jsc.setLogLevel("WARN");

            // 加载交易数据
            Dataset<Row> transactions = loadTransactions(spark, batchDate);

            // 关联分析 1: 同一IP地址
            Dataset<Row> sameIpGroup = analyzeByIp(spark, transactions);

            // 关联分析 2: 同一设备指纹
            Dataset<Row> sameDeviceGroup = analyzeByDevice(spark, transactions);

            // 关联分析 3: 短时间内密集交易
            Dataset<Row> burstGroup = analyzeBurstTransactions(spark, transactions);

            // 合并关联团体
            Dataset<Row> allGroups = sameIpGroup
                .union(sameDeviceGroup)
                .union(burstGroup)
                .withColumn("batch_date", lit(batchDate))
                .withColumn("analyzed_at", lit(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

            // 写入 ClickHouse
            writeToClickHouse(allGroups, "customer_relationship_groups");

            log.info("CustomerGraphAnalysisJob completed. Groups found: {}", allGroups.count());

        } catch (Exception e) {
            log.error("CustomerGraphAnalysisJob failed", e);
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

    /** 同一IP地址关联分析 */
    private static Dataset<Row> analyzeByIp(SparkSession spark, Dataset<Row> transactions) {
        return transactions
            .filter(col("ip_address").isNotNull().and(length(col("ip_address")).gt(0)))
            .groupBy("ip_address")
            .agg(
                collect_set("customer_id").as("customer_ids"),
                count("*").as("txn_count"),
                sum("amount").as("total_amount")
            )
            .filter(size(col("customer_ids")).gt(1)) // 至少2个不同客户
            .withColumn("relationship_type", lit("SAME_IP"))
            .withColumn("risk_level",
                when(size(col("customer_ids")).geq(5), lit("HIGH"))
                .otherwise(lit("MEDIUM"))
            );
    }

    /** 同一设备指纹关联分析 */
    private static Dataset<Row> analyzeByDevice(SparkSession spark, Dataset<Row> transactions) {
        return transactions
            .filter(col("device_fingerprint").isNotNull().and(length(col("device_fingerprint")).gt(0)))
            .groupBy("device_fingerprint")
            .agg(
                collect_set("customer_id").as("customer_ids"),
                count("*").as("txn_count")
            )
            .filter(size(col("customer_ids")).gt(1))
            .withColumn("relationship_type", lit("SAME_DEVICE"))
            .withColumn("risk_level",
                when(size(col("customer_ids")).geq(3), lit("HIGH"))
                .otherwise(lit("MEDIUM"))
            );
    }

    /** 短时间内密集交易分析 */
    private static Dataset<Row> analyzeBurstTransactions(SparkSession spark, Dataset<Row> transactions) {
        return transactions
            .withColumn("time_window",
                date_format(col("trans_time"), "yyyy-MM-dd HH:mm")
            )
            .groupBy("time_window", "customer_id")
            .agg(count("*").as("txn_count"), sum("amount").as("total_amount"))
            .filter(col("txn_count").geq(5)) // 同一分钟内>=5笔
            .groupBy("time_window")
            .agg(
                collect_set("customer_id").as("customer_ids"),
                sum("txn_count").as("total_count"),
                sum("total_amount").as("total_amount")
            )
            .filter(size(col("customer_ids")).gt(1))
            .withColumn("relationship_type", lit("BURST_TXN"))
            .withColumn("risk_level", lit("HIGH"));
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

        log.info("Written to ClickHouse: {}", tableName);
    }
}
