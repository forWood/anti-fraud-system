package com.bank.risk.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 交易记录模型 - 对应transaction_record表
 * 字段规范遵循SRS 6.2.1节定义
 *
 * @author 银行科技部
 * @version 2.0.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    /** 交易流水号(全局唯一) */
    @NotBlank(message = "交易流水号不能为空")
    private String transactionId;

    /** 发起方账号(脱敏后) */
    @NotBlank(message = "账户ID不能为空")
    private String accountId;

    /** 账号SHA-256哈希 */
    private String accountIdHash;

    /** 交易对手账号(脱敏后) */
    private String counterpartyAccount;

    /** 对手账号哈希 */
    private String counterpartyHash;

    /** 交易类型: TRANSFER/PAYMENT/WITHDRAW/DEPOSIT/EXCHANGE */
    @NotBlank(message = "交易类型不能为空")
    private String transactionType;

    /** 渠道编码: EBANK/MBANK/COUNTER/ATM/POS/THIRD_PARTY */
    @NotBlank(message = "渠道编码不能为空")
    private String channelCode;

    /** 交易金额(元, 保留2位小数) */
    @NotNull(message = "交易金额不能为空")
    @DecimalMin(value = "0.01", message = "交易金额必须≥0.01元")
    @DecimalMax(value = "10000000.00", message = "交易金额必须≤1000万元")
    private BigDecimal amount;

    /** 币种ISO代码 */
    @Builder.Default
    private String currency = "CNY";

    /** 交易发起时间(ISO8601) */
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private LocalDateTime transactionTime;

    /** 交易IP地址 */
    @Pattern(regexp = "^$|^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$", message = "IP地址格式无效")
    private String ipAddress;

    /** IP归属国家 */
    private String ipCountry;

    /** IP归属城市 */
    private String ipCity;

    /** IP风险等级: NORMAL/VPN/PROXY/TOR */
    private String ipRiskLevel;

    /** 设备指纹ID */
    private String deviceId;

    /** 设备类型: MOBILE/PC/PAD */
    private String deviceType;

    /** 设备操作系统 */
    private String deviceOs;

    /** 是否模拟器 */
    private Boolean isEmulator;

    /** 是否ROOT/越狱 */
    private Boolean isRooted;

    /** 是否为首次绑定设备 */
    private Boolean isFirstDeviceBinding;

    /** GPS纬度(脱敏到0.01°) */
    private BigDecimal gpsLat;

    /** GPS经度(脱敏到0.01°) */
    private BigDecimal gpsLng;

    /** 商户编号 */
    private String merchantId;

    /** 交易附言/备注 */
    private String remark;

    /** 上游系统预标记 */
    private String riskTag;

    /** 数据来源: CORE/MBS/APP/THIRD */
    private String dataSource;

    /** 是否为跨境交易 */
    @Builder.Default
    private Boolean isCrossBorder = false;

    /** 交易对手姓名 */
    private String counterpartyName;

    /** 交易对手国家 */
    private String counterpartyCountry;

    /** 创建时间 */
    private LocalDateTime createdAt;

    // ===== 风险相关 =====

    /** 风险评分(0-100) */
    private BigDecimal riskScore;

    /** 风险等级: LOW/MEDIUM/HIGH */
    private String riskLevel;

    /** 处理状态: PENDING/PROCESSING/RESOLVED */
    private String processingStatus;

    // ===== 辅助方法 =====

    /**
     * 判断是否为夜间交易(00:00-06:00)
     */
    public boolean isNightTime() {
        if (transactionTime == null) return false;
        int hour = transactionTime.getHour();
        return hour >= 0 && hour < 6;
    }

    /**
     * 判断是否为节假日(简化判断: 周末)
     */
    public boolean isWeekend() {
        if (transactionTime == null) return false;
        java.time.DayOfWeek dayOfWeek = transactionTime.getDayOfWeek();
        return dayOfWeek == java.time.DayOfWeek.SATURDAY
            || dayOfWeek == java.time.DayOfWeek.SUNDAY;
    }
}
