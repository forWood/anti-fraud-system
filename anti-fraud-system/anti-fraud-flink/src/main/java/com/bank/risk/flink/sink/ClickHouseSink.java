package com.bank.risk.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * ClickHouse离线特征存储Sink
 *
 * 将特征数据批量写入ClickHouse用于离线分析和报表
 *
 * @author 银行科技部
 */
public class ClickHouseSink extends RichSinkFunction<com.bank.risk.common.model.EnrichedTransaction> {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseSink.class);

    private final String jdbcUrl;
    private transient Connection connection;
    private transient PreparedStatement preparedStatement;
    private transient int batchCount;

    private static final int BATCH_SIZE = 100;

    private static final String INSERT_SQL =
        "INSERT INTO risk_db.transaction_features " +
        "(transaction_id, account_id_hash, amount, amount_1h, count_1h, " +
        " amount_24h, count_24h, device_count_30d, city_count_30d, " +
        " is_night_time, is_cross_border, is_first_device, ip_risk_level, " +
        " amount_deviation, create_time) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())";

    public ClickHouseSink(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        connection = DriverManager.getConnection(jdbcUrl);
        preparedStatement = connection.prepareStatement(INSERT_SQL);
        batchCount = 0;
        log.info("ClickHouseSink initialized: {}", jdbcUrl);
    }

    @Override
    public void invoke(com.bank.risk.common.model.EnrichedTransaction enriched, Context context) {
        try {
            com.bank.risk.common.model.Transaction tx = enriched.getTransaction();
            if (tx == null) return;

            preparedStatement.setString(1, tx.getTransactionId());
            preparedStatement.setString(2, tx.getAccountIdHash());
            preparedStatement.setBigDecimal(3, tx.getAmount());
            preparedStatement.setBigDecimal(4, enriched.getAmount1h());
            preparedStatement.setInt(5, enriched.getCount1h());
            preparedStatement.setBigDecimal(6, enriched.getAmount24h());
            preparedStatement.setInt(7, enriched.getCount24h());
            preparedStatement.setInt(8, enriched.getDeviceCount30d());
            preparedStatement.setInt(9, enriched.getCityCount30d());
            preparedStatement.setBoolean(10, enriched.getIsNightTime());
            preparedStatement.setBoolean(11, enriched.getIsCrossBorder());
            preparedStatement.setBoolean(12, enriched.getIsFirstDevice());
            preparedStatement.setString(13, enriched.getIpRiskLevel());
            preparedStatement.setDouble(14, enriched.getAmountDeviation() != null ? enriched.getAmountDeviation() : 0.0);
            preparedStatement.addBatch();

            batchCount++;

            if (batchCount >= BATCH_SIZE) {
                preparedStatement.executeBatch();
                batchCount = 0;
            }
        } catch (Exception e) {
            log.error("Failed to write to ClickHouse: {}", e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if (batchCount > 0 && preparedStatement != null) {
            preparedStatement.executeBatch();
        }
        if (preparedStatement != null) preparedStatement.close();
        if (connection != null) connection.close();
        log.info("ClickHouseSink closed");
    }
}
