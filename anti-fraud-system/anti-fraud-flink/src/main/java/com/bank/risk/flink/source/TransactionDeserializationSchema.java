package com.bank.risk.flink.source;

import com.bank.risk.common.model.Transaction;
import com.bank.risk.common.util.JsonUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * 交易数据反序列化器
 * 从Kafka消息字节数组中解析Transaction JSON
 *
 * @author 银行科技部
 */
public class TransactionDeserializationSchema implements DeserializationSchema<Transaction> {

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            return JsonUtil.fromJson(message, Transaction.class);
        } catch (Exception e) {
            // 记录解析失败，返回null由Flink过滤
            System.err.println("Failed to deserialize transaction: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeExtractor.getForClass(Transaction.class);
    }
}
