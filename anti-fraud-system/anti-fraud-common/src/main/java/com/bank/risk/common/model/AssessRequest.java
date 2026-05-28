package com.bank.risk.common.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 风险评估请求 - POJO替代gRPC stub
 *
 * @author 银行科技部
 */
public class AssessRequest {
    private String transactionId;
    private String accountId;
    private String accountIdHash;
    private double amount;
    private String currency;
    private String transactionType;
    private String channelCode;
    private long transactionTime;
    private String counterpartyAccount;
    private String counterpartyHash;
    private String counterpartyName;
    private String counterpartyCountry;
    private String deviceId;
    private String deviceType;
    private String deviceOs;
    private boolean isEmulator;
    private boolean isRooted;
    private boolean isFirstDevice;
    private String ipAddress;
    private String ipCountry;
    private String ipCity;
    private String ipRiskLevel;
    private double gpsLat;
    private double gpsLng;
    private double amount1h;
    private int count1h;
    private double amount24h;
    private int count24h;
    private int deviceCount30d;
    private int cityCount30d;
    private int counterpartyCount30d;
    private boolean isNightTime;
    private boolean isCrossBorder;
    private double amountDeviation;
    private String merchantId;
    private String remark;
    private boolean enableKnowledgeSearch = true;
    private int knowledgeTopK = 5;
    private boolean enableDetailExplain = true;

    // Getters and Setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String v) { this.transactionId = v; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String v) { this.accountId = v; }
    public String getAccountIdHash() { return accountIdHash; }
    public void setAccountIdHash(String v) { this.accountIdHash = v; }
    public double getAmount() { return amount; }
    public void setAmount(double v) { this.amount = v; }
    public String getCurrency() { return currency; }
    public void setCurrency(String v) { this.currency = v; }
    public String getTransactionType() { return transactionType; }
    public void setTransactionType(String v) { this.transactionType = v; }
    public String getChannelCode() { return channelCode; }
    public void setChannelCode(String v) { this.channelCode = v; }
    public long getTransactionTime() { return transactionTime; }
    public void setTransactionTime(long v) { this.transactionTime = v; }
    public String getCounterpartyAccount() { return counterpartyAccount; }
    public void setCounterpartyAccount(String v) { this.counterpartyAccount = v; }
    public String getCounterpartyHash() { return counterpartyHash; }
    public void setCounterpartyHash(String v) { this.counterpartyHash = v; }
    public String getCounterpartyName() { return counterpartyName; }
    public void setCounterpartyName(String v) { this.counterpartyName = v; }
    public String getCounterpartyCountry() { return counterpartyCountry; }
    public void setCounterpartyCountry(String v) { this.counterpartyCountry = v; }
    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String v) { this.deviceId = v; }
    public String getDeviceType() { return deviceType; }
    public void setDeviceType(String v) { this.deviceType = v; }
    public String getDeviceOs() { return deviceOs; }
    public void setDeviceOs(String v) { this.deviceOs = v; }
    public boolean isEmulator() { return isEmulator; }
    public void setIsEmulator(boolean v) { this.isEmulator = v; }
    public boolean isRooted() { return isRooted; }
    public void setIsRooted(boolean v) { this.isRooted = v; }
    public boolean isFirstDevice() { return isFirstDevice; }
    public void setIsFirstDevice(boolean v) { this.isFirstDevice = v; }
    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String v) { this.ipAddress = v; }
    public String getIpCountry() { return ipCountry; }
    public void setIpCountry(String v) { this.ipCountry = v; }
    public String getIpCity() { return ipCity; }
    public void setIpCity(String v) { this.ipCity = v; }
    public String getIpRiskLevel() { return ipRiskLevel; }
    public void setIpRiskLevel(String v) { this.ipRiskLevel = v; }
    public double getGpsLat() { return gpsLat; }
    public void setGpsLat(double v) { this.gpsLat = v; }
    public double getGpsLng() { return gpsLng; }
    public void setGpsLng(double v) { this.gpsLng = v; }
    public double getAmount1h() { return amount1h; }
    public void setAmount1h(double v) { this.amount1h = v; }
    public int getCount1h() { return count1h; }
    public void setCount1h(int v) { this.count1h = v; }
    public double getAmount24h() { return amount24h; }
    public void setAmount24h(double v) { this.amount24h = v; }
    public int getCount24h() { return count24h; }
    public void setCount24h(int v) { this.count24h = v; }
    public int getDeviceCount30d() { return deviceCount30d; }
    public void setDeviceCount30d(int v) { this.deviceCount30d = v; }
    public int getCityCount30d() { return cityCount30d; }
    public void setCityCount30d(int v) { this.cityCount30d = v; }
    public int getCounterpartyCount30d() { return counterpartyCount30d; }
    public void setCounterpartyCount30d(int v) { this.counterpartyCount30d = v; }
    public boolean isNightTime() { return isNightTime; }
    public void setIsNightTime(boolean v) { this.isNightTime = v; }
    public boolean isCrossBorder() { return isCrossBorder; }
    public void setIsCrossBorder(boolean v) { this.isCrossBorder = v; }
    public double getAmountDeviation() { return amountDeviation; }
    public void setAmountDeviation(double v) { this.amountDeviation = v; }
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String v) { this.merchantId = v; }
    public String getRemark() { return remark; }
    public void setRemark(String v) { this.remark = v; }
    public boolean isEnableKnowledgeSearch() { return enableKnowledgeSearch; }
    public void setEnableKnowledgeSearch(boolean v) { this.enableKnowledgeSearch = v; }
    public int getKnowledgeTopK() { return knowledgeTopK; }
    public void setKnowledgeTopK(int v) { this.knowledgeTopK = v; }
    public boolean isEnableDetailExplain() { return enableDetailExplain; }
    public void setEnableDetailExplain(boolean v) { this.enableDetailExplain = v; }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("transaction_id", transactionId);
        map.put("account_id", accountId);
        map.put("account_id_hash", accountIdHash);
        map.put("amount", amount);
        map.put("currency", currency);
        map.put("transaction_type", transactionType);
        map.put("channel_code", channelCode);
        map.put("transaction_time", transactionTime);
        map.put("counterparty_account", counterpartyAccount);
        map.put("counterparty_hash", counterpartyHash);
        map.put("counterparty_name", counterpartyName);
        map.put("counterparty_country", counterpartyCountry);
        map.put("device_id", deviceId);
        map.put("device_type", deviceType);
        map.put("device_os", deviceOs);
        map.put("is_emulator", isEmulator);
        map.put("is_rooted", isRooted);
        map.put("is_first_device", isFirstDevice);
        map.put("ip_address", ipAddress);
        map.put("ip_country", ipCountry);
        map.put("ip_city", ipCity);
        map.put("ip_risk_level", ipRiskLevel);
        map.put("gps_lat", gpsLat);
        map.put("gps_lng", gpsLng);
        map.put("amount_1h", amount1h);
        map.put("count_1h", count1h);
        map.put("amount_24h", amount24h);
        map.put("count_24h", count24h);
        map.put("device_count_30d", deviceCount30d);
        map.put("city_count_30d", cityCount30d);
        map.put("counterparty_count_30d", counterpartyCount30d);
        map.put("is_night_time", isNightTime);
        map.put("is_cross_border", isCrossBorder);
        map.put("amount_deviation", amountDeviation);
        map.put("merchant_id", merchantId);
        map.put("remark", remark);
        map.put("enable_knowledge_search", enableKnowledgeSearch);
        map.put("knowledge_top_k", knowledgeTopK);
        map.put("enable_detail_explain", enableDetailExplain);
        return map;
    }
}
