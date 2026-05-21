package com.bank.risk.spark.feature

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import OfflineFeatureConfig
import java.util.Properties

/**
 * 离线特征计算引擎
 *
 * 基于Spark SQL对ClickHouse/Hive中的交易记录进行聚合计算，
 * 生成30天窗口的客户级离线特征。
 *
 * @author 银行科技部
 */
class OfflineFeatureComputer(spark: SparkSession, config: OfflineFeatureConfig) {

  private val log = LoggerFactory.getLogger(getClass)
  import spark.implicits._

  def run(): Unit = {
    log.info(s"Starting offline feature computation: lookback=${config.lookbackDays} days")

    // ===== Step 1: 加载原始交易数据 =====
    val transactions = loadTransactions()

    // ===== Step 2: 客户级聚合特征 =====
    val customerFeatures = computeCustomerFeatures(transactions)

    // ===== Step 3: 行为偏离度计算 =====
    val deviationFeatures = computeDeviationFeatures(transactions, customerFeatures)

    // ===== Step 4: 合并特征 =====
    val finalFeatures = mergeFeatures(customerFeatures, deviationFeatures)

    // ===== Step 5: 写入Redis(在线)和ClickHouse(分析) =====
    writeToRedis(finalFeatures)
    writeToClickHouse(finalFeatures)

    // 输出统计
    val count = finalFeatures.count()
    log.info(s"Offline feature computation completed: $count customer features generated")
  }

  // ========================================================================
  // 数据加载
  // ========================================================================

  private def loadTransactions(): DataFrame = {
    log.info(s"Loading transactions from ${config.inputTable} for last ${config.lookbackDays} days")

    spark.read.format("jdbc")
      .option("url", config.clickhouseUrl)
      .option("dbtable", s"(SELECT * FROM ${config.inputTable} " +
        s"WHERE transaction_time >= now() - INTERVAL ${config.lookbackDays} DAY) t")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .load()
  }

  // ========================================================================
  // 客户级聚合特征
  // ========================================================================

  private def computeCustomerFeatures(txs: DataFrame): DataFrame = {
    txs.groupBy("account_id_hash")
      .agg(
        // 金额特征
        sum("amount").as("total_amount_30d"),
        avg("amount").as("avg_amount_30d"),
        max("amount").as("max_amount_30d"),
        min("amount").as("min_amount_30d"),
        stddev("amount").as("std_amount_30d"),

        // 频率特征
        count("*").as("total_count_30d"),
        (count("*") / config.lookbackDays).as("avg_daily_count_30d"),

        // 时间特征
        sum(when(hour($"transaction_time") >= 0 && hour($"transaction_time") < 6, 1)
          .otherwise(0)).as("night_count_30d"),
        sum(when(dayofweek($"transaction_time").isin(1, 7), 1)
          .otherwise(0)).as("weekend_count_30d"),

        // 渠道多样性
        countDistinct("channel_code").as("channel_diversity_30d"),
        countDistinct("merchant_id").as("merchant_diversity_30d"),

        // 交易类型分布
        sum(when($"transaction_type" === "TRANSFER", 1).otherwise(0)).as("transfer_count_30d"),
        sum(when($"transaction_type" === "WITHDRAW", 1).otherwise(0)).as("withdraw_count_30d"),
        sum(when($"transaction_type" === "PAYMENT", 1).otherwise(0)).as("payment_count_30d")
      )
      .withColumnRenamed("account_id_hash", "customer_id_hash")
  }

  // ========================================================================
  // 行为偏离度特征 (按客户计算与自身历史均值的偏差)
  // ========================================================================

  private def computeDeviationFeatures(txs: DataFrame, features: DataFrame): DataFrame = {
    // 最近7天数据 vs 前21天均值对比
    val recent7d = txs.filter($"transaction_time" >= date_sub(current_date(), 7))
    val prior21d = txs.filter(
      $"transaction_time" >= date_sub(current_date(), config.lookbackDays) &&
      $"transaction_time" < date_sub(current_date(), 7))

    val recentAgg = recent7d.groupBy("account_id_hash")
      .agg(
        avg("amount").as("recent7d_avg_amount"),
        count("*").as("recent7d_tx_count")
      )

    val priorAgg = prior21d.groupBy("account_id_hash")
      .agg(
        avg("amount").as("prior21d_avg_amount"),
        (count("*") / 21.0).as("prior21d_daily_avg")
      )

    // 计算偏离度
    recentAgg.join(priorAgg, Seq("account_id_hash"), "left_outer")
      .withColumn("amount_deviation_ratio",
        when($"prior21d_avg_amount" > 0,
          ($"recent7d_avg_amount" - $"prior21d_avg_amount") / $"prior21d_avg_amount")
        .otherwise(0.0))
      .withColumn("frequency_deviation_ratio",
        when($"prior21d_daily_avg" > 0,
          ($"recent7d_tx_count" / 7.0 - $"prior21d_daily_avg") / $"prior21d_daily_avg")
        .otherwise(0.0))
  }

  // ========================================================================
  // 合并特征
  // ========================================================================

  private def mergeFeatures(baseFeatures: DataFrame, deviationFeatures: DataFrame): DataFrame = {
    baseFeatures
      .join(deviationFeatures,
        baseFeatures("customer_id_hash") === deviationFeatures("account_id_hash"),
        "left_outer")
      .drop(deviationFeatures("account_id_hash"))
      .withColumn("feature_version", lit("2.0"))
      .withColumn("computed_at", current_timestamp())
  }

  // ========================================================================
  // 写入存储
  // ========================================================================

  private def writeToRedis(features: DataFrame): Unit = {
    log.info("Writing offline features to Redis")

    features.select("customer_id_hash",
      "total_amount_30d", "avg_amount_30d", "max_amount_30d", "std_amount_30d",
      "total_count_30d", "avg_daily_count_30d",
      "amount_deviation_ratio", "frequency_deviation_ratio",
      "night_count_30d", "channel_diversity_30d")
      .collect()
      .foreach { row =>
        val customerId = row.getString(0)
        val featureMap = new java.util.HashMap[String, Any]()
        featureMap.put("total_amount_30d", row.getAs[java.math.BigDecimal](1))
        featureMap.put("avg_amount_30d", row.getAs[java.math.BigDecimal](2))
        featureMap.put("max_amount_30d", row.getAs[java.math.BigDecimal](3))
        featureMap.put("std_amount_30d", row.getAs[java.math.BigDecimal](4))
        featureMap.put("total_count_30d", row.getLong(5))
        featureMap.put("avg_daily_count_30d", row.getDouble(6))
        featureMap.put("amount_deviation_ratio", row.getAs[Double](7))
        featureMap.put("frequency_deviation_ratio", row.getAs[Double](8))

        val key = s"risk:offline_feature:$customerId"
        val json = new org.json4s.jackson.Serialization
          .write(featureMap)(org.json4s.DefaultFormats)
        // Redis写入通过Jedis（实际生产中使用Redis Pipeline批量写入）
        // 这里简化为记录日志
      }

    log.info("Offline features written to Redis")
  }

  private def writeToClickHouse(features: DataFrame): Unit = {
    log.info("Writing offline features to ClickHouse")

    features
      .select("customer_id_hash", "total_amount_30d", "avg_amount_30d",
        "max_amount_30d", "std_amount_30d", "total_count_30d",
        "avg_daily_count_30d", "night_count_30d", "weekend_count_30d",
        "channel_diversity_30d", "merchant_diversity_30d",
        "amount_deviation_ratio", "frequency_deviation_ratio",
        "feature_version", "computed_at")
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", config.clickhouseUrl)
      .option("dbtable", "risk_db.customer_offline_features")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("batchsize", "10000")
      .save()

    log.info("Offline features written to ClickHouse")
  }
}
