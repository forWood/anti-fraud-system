package com.bank.risk.rg.controller;

import com.bank.risk.rg.service.ReportGenerationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 报告生成 REST API
 *
 * 支持报表类型:
 * - DAILY: 每日风险统计报告
 * - WEEKLY: 周度合规报告
 * - MONTHLY: 月度风控报告
 * - COMPLIANCE: 可疑交易报告 (FATF合规要求)
 * - CUSTOMER_RISK: 客户风险评估报告
 */
@RestController
@RequestMapping("/api/reports")
public class ReportGenerationController {

    private static final Logger log = LoggerFactory.getLogger(ReportGenerationController.class);

    @Autowired
    private ReportGenerationService reportService;

    /** 创建报告任务 */
    @PostMapping
    public Map<String, Object> createReport(@RequestBody Map<String, Object> data) {
        log.info("Creating report: type={}", data.get("reportType"));
        return reportService.createReport(data);
    }

    /** 报告列表 */
    @GetMapping
    public Map<String, Object> listReports(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String reportType) {
        return reportService.listReports(page, size, reportType);
    }

    /** 下载报告 */
    @GetMapping("/{reportId}/download")
    public Map<String, Object> downloadReport(@PathVariable String reportId) {
        return reportService.downloadReport(reportId);
    }

    /** 查询报告详情 */
    @GetMapping("/{reportId}")
    public Map<String, Object> getReport(@PathVariable String reportId) {
        return reportService.getReport(reportId);
    }

    /** 删除报告 */
    @DeleteMapping("/{reportId}")
    public Map<String, Object> deleteReport(@PathVariable String reportId) {
        return reportService.deleteReport(reportId);
    }

    /** 手动触发定时报告生成 */
    @PostMapping("/trigger")
    public Map<String, Object> triggerReport(@RequestBody Map<String, Object> data) {
        return reportService.triggerReport(data);
    }
}
