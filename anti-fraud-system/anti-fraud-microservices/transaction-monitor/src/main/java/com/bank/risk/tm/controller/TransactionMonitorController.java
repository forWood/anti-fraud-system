package com.bank.risk.tm.controller;

import com.bank.risk.tm.service.TransactionAssessService;
import com.bank.risk.common.model.RiskAssessment;
import com.bank.risk.common.util.MapBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.Map;

/**
 * 交易监控REST接口
 *
 * @author 银行科技部
 */
@RestController
@RequestMapping("/api/v1/monitor")
public class TransactionMonitorController {

    @Autowired
    private TransactionAssessService assessService;

    /**
     * 单笔交易风险评估
     *
     * POST /api/v1/monitor/assess
     * Body: 交易特征JSON
     * Response: 风险评估结果
     */
    @PostMapping("/assess")
    public RiskAssessment assessTransaction(@Valid @RequestBody Map<String, Object> transactionRequest) {
        return assessService.assess(transactionRequest);
    }

    /**
     * 批量交易风险评估
     */
    @PostMapping("/batch-assess")
    public java.util.List<RiskAssessment> batchAssess(@RequestBody java.util.List<Map<String, Object>> requests) {
        return assessService.batchAssess(requests);
    }

    /**
     * 健康检查
     */
    @GetMapping("/health")
    public Map<String, String> health() {
        return MapBuilder.of("status", "UP", "service", "transaction-monitor");
    }
}
