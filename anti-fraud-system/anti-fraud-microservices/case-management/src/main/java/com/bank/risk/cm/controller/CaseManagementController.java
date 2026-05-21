package com.bank.risk.cm.controller;

import com.bank.risk.cm.service.CaseManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 案件管理 REST API
 *
 * 案件生命周期: 创建 -> 分配 -> 调查中 -> 等待反馈 -> 结案
 */
@RestController
@RequestMapping("/api/cases")
public class CaseManagementController {

    private static final Logger log = LoggerFactory.getLogger(CaseManagementController.class);

    @Autowired
    private CaseManagementService caseService;

    /** 创建案件 */
    @PostMapping
    public Map<String, Object> createCase(@RequestBody Map<String, Object> data) {
        log.info("Creating case with data: {}", data);
        return caseService.createCase(data);
    }

    /** 案件列表 */
    @GetMapping
    public Map<String, Object> listCases(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "ALL") String status) {
        return caseService.listCases(page, size, status);
    }

    /** 查询单个案件 */
    @GetMapping("/{caseId}")
    public Map<String, Object> getCase(@PathVariable String caseId) {
        return caseService.getCase(caseId);
    }

    /** 分配案件 */
    @PostMapping("/{caseId}/assign")
    public Map<String, Object> assignCase(
            @PathVariable String caseId,
            @RequestBody Map<String, Object> data) {
        return caseService.assignCase(caseId, data);
    }

    /** 处理/更新案件 */
    @PostMapping("/{caseId}/process")
    public Map<String, Object> processCase(
            @PathVariable String caseId,
            @RequestBody Map<String, Object> data) {
        return caseService.processCase(caseId, data);
    }

    /** 结案 */
    @PostMapping("/{caseId}/close")
    public Map<String, Object> closeCase(
            @PathVariable String caseId,
            @RequestBody Map<String, Object> data) {
        return caseService.closeCase(caseId, data);
    }

    /** 获取案件统计 */
    @GetMapping("/stats")
    public Map<String, Object> getStats() {
        return caseService.getStats();
    }
}
