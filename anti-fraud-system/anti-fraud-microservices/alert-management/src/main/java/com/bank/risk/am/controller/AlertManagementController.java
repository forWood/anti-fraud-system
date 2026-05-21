package com.bank.risk.am.controller;

import com.bank.risk.am.service.AlertManagementService;
import com.bank.risk.common.model.AlertEvent;
import com.bank.risk.common.util.MapBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

/**
 * 预警管理REST接口
 */
@RestController
@RequestMapping("/api/v1/alerts")
public class AlertManagementController {

    @Autowired
    private AlertManagementService service;

    /** 创建预警 */
    @PostMapping
    public AlertEvent createAlert(@RequestBody Map<String, Object> alertData) {
        return service.createAlert(alertData);
    }

    /** 查询预警列表 */
    @GetMapping
    public Map<String, Object> listAlerts(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "NEW") String status) {
        return service.listAlerts(page, size, status);
    }

    /** 处理预警 */
    @PutMapping("/{alertId}/process")
    public AlertEvent processAlert(@PathVariable String alertId, @RequestBody Map<String, Object> processingData) {
        return service.processAlert(alertId, processingData);
    }

    /** 升级预警 */
    @PutMapping("/{alertId}/escalate")
    public AlertEvent escalateAlert(@PathVariable String alertId, @RequestBody Map<String, Object> escalationData) {
        return service.escalateAlert(alertId, escalationData);
    }

    /** 推送给风控专员 */
    @PostMapping("/{alertId}/notify")
    public Map<String, String> sendNotification(@PathVariable String alertId) {
        service.sendNotification(alertId);
        return MapBuilder.of("status", "OK", "message", "通知已发送");
    }
}
