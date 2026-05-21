package com.bank.risk.spark

import com.bank.risk.spark.feature.OfflineFeatureComputer
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Spark离线特征计算主程序
 *
 * 计算特征:
 * - 30天交易总额、笔均、最大金额
 * - 30天交易对手数量
 * - 30天使用设备数量
 * - 30天涉及城市数量
 * - 行为偏离度
 *
 * 数据流: ClickHouse/Hive → Spark → Redis + ClickHouse
 *
 * @author 银行科技部
 */
object OfflineFeatureJob {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    log.info("========================================")
    log.info(" Spark离线特征计算作业启动")
    log.info("========================================")

    val spark = SparkSession.builder()
      .appName("AntiFraud-OfflineFeatureJob")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {
      val config = OfflineFeatureConfig.fromArgs(args)
      val computer = new OfflineFeatureComputer(spark, config)
      computer.run()
    } finally {
      spark.stop()
      log.info("Spark job finished")
    }
  }
}

case class OfflineFeatureConfig(
  inputTable: String = "risk_db.transaction_record",
  outputRedisHost: String = "localhost",
  outputRedisPort: Int = 6379,
  clickhouseUrl: String = "jdbc:clickhouse://localhost:8123/risk_db",
  lookbackDays: Int = 30
)

object OfflineFeatureConfig {
  def fromArgs(args: Array[String]): OfflineFeatureConfig = {
    val parser = new scopt.OptionParser[OfflineFeatureConfig]("offline-feature-job") {
      opt[String]("input-table").action((x, c) => c.copy(inputTable = x))
      opt[String]("redis-host").action((x, c) => c.copy(outputRedisHost = x))
      opt[Int]("redis-port").action((x, c) => c.copy(outputRedisPort = x))
      opt[String]("clickhouse-url").action((x, c) => c.copy(clickhouseUrl = x))
      opt[Int]("lookback-days").action((x, c) => c.copy(lookbackDays = x))
    }
    parser.parse(args, OfflineFeatureConfig()) match {
      case Some(c) => c
      case None => throw new RuntimeException("Invalid arguments")
    }
  }
}
