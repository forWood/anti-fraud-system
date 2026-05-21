package com.bank.risk.flink.sink;

import com.bank.risk.common.model.EnrichedTransaction;
import com.bank.risk.common.util.JsonUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * Redis特征存储Sink
 *
 * 将实时特征写入Redis用于在线服务
 * Key格式: risk:feature:{accountIdHash}
 * Value: JSON格式的特征数据
 *
 * @author 银行科技部
 */
public class RedisFeatureSink extends RichSinkFunction<EnrichedTransaction> {

    private static final Logger log = LoggerFactory.getLogger(RedisFeatureSink.class);

    private final String host;
    private final int port;
    private final String password;
    private transient JedisPool jedisPool;

    public RedisFeatureSink(String host, int port) {
        this(host, port, null);
    }

    public RedisFeatureSink(String host, int port, String password) {
        this.host = host;
        this.port = port;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(50);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(2);
        poolConfig.setMaxWait(Duration.ofSeconds(3));

        if (password != null && !password.isEmpty()) {
            jedisPool = new JedisPool(poolConfig, host, port, 3000, password);
        } else {
            jedisPool = new JedisPool(poolConfig, host, port, 3000);
        }
        log.info("RedisFeatureSink initialized: {}:{}", host, port);
    }

    @Override
    public void invoke(EnrichedTransaction enriched, Context context) {
        if (enriched == null || enriched.getTransaction() == null) return;

        String accountIdHash = enriched.getTransaction().getAccountIdHash();
        if (accountIdHash == null) return;

        String key = "risk:feature:" + accountIdHash;
        // 只存储特征部分（不包含完整交易记录），降低存储成本
        String featureJson = JsonUtil.toJson(buildFeatureMap(enriched));

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, 86400, featureJson); // 24小时过期
        } catch (Exception e) {
            log.error("Failed to write feature to Redis: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
        log.info("RedisFeatureSink closed");
    }

    private java.util.Map<String, Object> buildFeatureMap(EnrichedTransaction enriched) {
        java.util.Map<String, Object> map = new java.util.HashMap<>();
        map.put("amount1h", enriched.getAmount1h());
        map.put("count1h", enriched.getCount1h());
        map.put("amount24h", enriched.getAmount24h());
        map.put("count24h", enriched.getCount24h());
        map.put("deviceCount30d", enriched.getDeviceCount30d());
        map.put("cityCount30d", enriched.getCityCount30d());
        map.put("counterpartyCount30d", enriched.getCounterpartyCount30d());
        map.put("isNightTime", enriched.getIsNightTime());
        map.put("isCrossBorder", enriched.getIsCrossBorder());
        map.put("isFirstDevice", enriched.getIsFirstDevice());
        map.put("ipRiskLevel", enriched.getIpRiskLevel());
        map.put("amountDeviation", enriched.getAmountDeviation());
        map.put("timestamp", System.currentTimeMillis());
        return map;
    }
}
