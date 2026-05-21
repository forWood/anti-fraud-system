package com.bank.risk.am.service;

import com.bank.risk.common.model.AlertEvent;
import com.bank.risk.common.util.MapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

/**
 * 预警管理服务
 *
 * 功能:
 * - 创建/更新/查询预警
 * - 预警升级机制 (高风险24h未处理→自动升级)
 * - 邮件/短信通知风控专员
 *
 * @author 银行科技部
 */
@Service
public class AlertManagementService {

    private static final Logger log = LoggerFactory.getLogger(AlertManagementService.class);

    /** 模拟存储 (实际生产使用 MySQL + MyBatis) */
    private final Map<String, AlertEvent> alertStore = new LinkedHashMap<>();

    @Autowired(required = false)
    private JavaMailSender mailSender;

    /** 创建预警 */
    public AlertEvent createAlert(Map<String, Object> data) {
        String alertId = "ALT" + UUID.randomUUID().toString().substring(0, 12);

        AlertEvent alert = AlertEvent.builder()
            .alertId(alertId)
            .transactionId(getString(data, "transactionId"))
            .customerId(getString(data, "customerId"))
            .alertType(getString(data, "alertType", "FRAUD"))
            .riskScore(new BigDecimal(getString(data, "riskScore", "0")))
            .riskLevel(getString(data, "riskLevel", "MEDIUM"))
            .alertStatus("NEW")
            .processingDeadline(calculateDeadline(getString(data, "riskLevel", "MEDIUM")))
            .createdAt(LocalDateTime.now())
            .build();

        alertStore.put(alertId, alert);

        // 高风险立即推送
        if ("HIGH".equals(alert.getRiskLevel())) {
            sendNotification(alertId);
        }

        log.info("Alert created: id={}, score={}, level={}", alertId, alert.getRiskScore(), alert.getRiskLevel());
        return alert;
    }

    /** 预警列表 */
    public Map<String, Object> listAlerts(int page, int size, String status) {
        List<AlertEvent> alerts = new ArrayList<>(alertStore.values());
        List<AlertEvent> filtered = new ArrayList<>();

        for (AlertEvent a : alerts) {
            if (status.equals(a.getAlertStatus()) || "ALL".equals(status)) {
                filtered.add(a);
            }
        }

        // 分页
        int start = page * size;
        int end = Math.min(start + size, filtered.size());
        if (start >= filtered.size()) {
            filtered = filtered.isEmpty() ? filtered : filtered.subList(0, Math.min(size, filtered.size()));
        } else {
            filtered = filtered.subList(start, end);
        }

        return MapBuilder.of(
            "total", alertStore.values().stream().filter(a -> status.equals(a.getAlertStatus())).count(),
            "page", page,
            "size", size,
            "items", filtered
        );
    }

    /** 处理预警 */
    public AlertEvent processAlert(String alertId, Map<String, Object> data) {
        AlertEvent alert = alertStore.get(alertId);
        if (alert == null) throw new RuntimeException("Alert not found: " + alertId);

        String action = getString(data, "action", "PROCESSING");
        alert.setAlertStatus("PROCESSING".equals(action) ? "PROCESSING" :
                           "RESOLVED".equals(action) ? "RESOLVED" : "CLOSED");
        alert.setAssignee(getString(data, "assignee"));
        alert.setUpdatedAt(LocalDateTime.now());

        log.info("Alert processed: id={}, action={}", alertId, action);
        return alert;
    }

    /** 升级预警 */
    public AlertEvent escalateAlert(String alertId, Map<String, Object> data) {
        AlertEvent alert = alertStore.get(alertId);
        if (alert == null) throw new RuntimeException("Alert not found: " + alertId);

        int newLevel = alert.getEscalationLevel() + 1;
        alert.setEscalationLevel(newLevel);
        alert.setAlertStatus("PENDING");
        alert.setUpdatedAt(LocalDateTime.now());

        log.info("Alert escalated: id={}, level={}", alertId, newLevel);

        // 升级后通知更高级别
        sendEscalationNotification(alert, newLevel);
        return alert;
    }

    /** 发送通知 */
    public void sendNotification(String alertId) {
        AlertEvent alert = alertStore.get(alertId);
        if (alert == null) return;

        try {
            if (mailSender != null) {
                SimpleMailMessage message = new SimpleMailMessage();
                message.setTo("risk-officer@bank.com");
                message.setSubject(String.format("[风险预警] 新预警 %s - %s风险",
                    alertId, alert.getRiskLevel()));
                message.setText(String.format(
                    "预警编号: %s\n交易编号: %s\n风险评分: %s\n风险等级: %s\n创建时间: %s\n处理截止: %s",
                    alertId, alert.getTransactionId(), alert.getRiskScore(),
                    alert.getRiskLevel(), alert.getCreatedAt(), alert.getProcessingDeadline()
                ));
                mailSender.send(message);
                log.info("Email notification sent for alert: {}", alertId);
            }
        } catch (Exception e) {
            log.error("Failed to send notification for alert {}: {}", alertId, e.getMessage());
        }
    }

    private void sendEscalationNotification(AlertEvent alert, int level) {
        log.warn("ESCALATION: Alert {} escalated to level {}, assigned to risk-manager",
            alert.getAlertId(), level);
    }

    private LocalDateTime calculateDeadline(String riskLevel) {
        LocalDateTime now = LocalDateTime.now();
        switch (riskLevel) {
            case "HIGH": return now.plusHours(24);
            case "MEDIUM": return now.plusHours(72);
            default: return now.plusDays(7);
        }
    }

    private String getString(Map<String, Object> map, String key) {
        return getString(map, key, "");
    }

    private String getString(Map<String, Object> map, String key, String defaultValue) {
        Object val = map.get(key);
        return val != null ? val.toString() : defaultValue;
    }
}
