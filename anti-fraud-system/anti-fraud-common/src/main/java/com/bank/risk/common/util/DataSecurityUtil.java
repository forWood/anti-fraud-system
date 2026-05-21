package com.bank.risk.common.util;

import com.google.common.hash.Hashing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.UUID;

/**
 * 数据安全与脱敏工具类
 * 遵循SRS 7.1节安全需求
 *
 * @author 银行科技部
 */
public final class DataSecurityUtil {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

    private DataSecurityUtil() {}

    /**
     * SHA-256哈希
     */
    public static String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * 手机号脱敏: 138****5678
     */
    public static String maskPhone(String phone) {
        if (phone == null || phone.length() != 11) return phone;
        return phone.substring(0, 3) + "****" + phone.substring(7);
    }

    /**
     * 身份证脱敏: 110101********1234
     */
    public static String maskIdCard(String idCard) {
        if (idCard == null || idCard.length() != 18) return idCard;
        return idCard.substring(0, 6) + "********" + idCard.substring(14);
    }

    /**
     * 银行卡号脱敏: 6222****8900
     */
    public static String maskBankCard(String cardNo) {
        if (cardNo == null || cardNo.length() < 8) return cardNo;
        return cardNo.substring(0, 4) + "****" + cardNo.substring(cardNo.length() - 4);
    }

    /**
     * GPS坐标脱敏(精确到0.01°)
     */
    public static BigDecimal maskGps(BigDecimal coord) {
        if (coord == null) return null;
        return coord.setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * 生成全局唯一ID
     */
    public static String generateId(String prefix) {
        String timestamp = LocalDateTime.now().format(DT_FORMATTER);
        String random = String.format("%06d", SECURE_RANDOM.nextInt(999999));
        return prefix + timestamp + random;
    }

    /**
     * 生成UUID
     */
    public static String generateUuid() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 生成API Key
     */
    public static String generateApiKey() {
        byte[] key = new byte[32];
        SECURE_RANDOM.nextBytes(key);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(key);
    }
}
