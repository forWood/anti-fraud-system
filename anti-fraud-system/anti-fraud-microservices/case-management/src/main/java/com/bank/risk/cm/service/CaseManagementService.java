package com.bank.risk.cm.service;

import com.bank.risk.common.util.MapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 案件管理服务
 *
 * 功能:
 * - 创建/分配/处理/结案案件
 * - 案件生命周期状态流转
 * - 与预警管理、智能体服务联动
 */
@Service
public class CaseManagementService {

    private static final Logger log = LoggerFactory.getLogger(CaseManagementService.class);

    /** 模拟存储 (实际生产使用 MySQL + MyBatis) */
    private final Map<String, CaseInfo> caseStore = new ConcurrentHashMap<>();

    /** 案件状态枚举 */
    private static final String STATUS_NEW = "NEW";
    private static final String STATUS_ASSIGNED = "ASSIGNED";
    private static final String STATUS_INVESTIGATING = "INVESTIGATING";
    private static final String STATUS_PENDING_FEEDBACK = "PENDING_FEEDBACK";
    private static final String STATUS_CLOSED = "CLOSED";
    private static final String STATUS_ARCHIVED = "ARCHIVED";

    /** 创建案件 */
    public Map<String, Object> createCase(Map<String, Object> data) {
        String caseId = "CASE" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();

        CaseInfo c = new CaseInfo();
        c.setCaseId(caseId);
        c.setAlertId(getString(data, "alertId"));
        c.setTransactionId(getString(data, "transactionId"));
        c.setCustomerId(getString(data, "customerId"));
        c.setCaseType(getString(data, "caseType", "FRAUD"));
        c.setRiskLevel(getString(data, "riskLevel", "MEDIUM"));
        c.setCaseStatus(STATUS_NEW);
        c.setDescription(getString(data, "description"));
        c.setCreatedAt(LocalDateTime.now());
        c.setPriority(calculatePriority(c.getRiskLevel()));

        caseStore.put(caseId, c);

        log.info("Case created: id={}, type={}, level={}", caseId, c.getCaseType(), c.getRiskLevel());

        return MapBuilder.of(
            "code", 0,
            "message", "案件创建成功",
            "data", MapBuilder.of(
                "caseId", caseId,
                "caseStatus", STATUS_NEW,
                "createdAt", c.getCreatedAt()
            )
        );
    }

    /** 案件列表 */
    public Map<String, Object> listCases(int page, int size, String status) {
        List<CaseInfo> filtered;
        if ("ALL".equals(status)) {
            filtered = new ArrayList<>(caseStore.values());
        } else {
            filtered = caseStore.values().stream()
                .filter(c -> status.equals(c.getCaseStatus()))
                .collect(Collectors.toList());
        }

        // 按创建时间倒序
        filtered.sort((a, b) -> b.getCreatedAt().compareTo(a.getCreatedAt()));

        // 分页
        int start = page * size;
        int end = Math.min(start + size, filtered.size());
        List<CaseInfo> pageData = (start < filtered.size()) ? filtered.subList(start, end) : Collections.emptyList();

        return MapBuilder.of(
            "code", 0,
            "data", MapBuilder.of(
                "total", (long) filtered.size(),
                "page", page,
                "size", size,
                "items", pageData
            )
        );
    }

    /** 查询案件 */
    public Map<String, Object> getCase(String caseId) {
        CaseInfo c = caseStore.get(caseId);
        if (c == null) {
            return MapBuilder.of("code", 404, "message", "案件不存在: " + caseId);
        }
        return MapBuilder.of("code", 0, "data", c);
    }

    /** 分配案件 */
    public Map<String, Object> assignCase(String caseId, Map<String, Object> data) {
        CaseInfo c = caseStore.get(caseId);
        if (c == null) {
            return MapBuilder.of("code", 404, "message", "案件不存在: " + caseId);
        }

        c.setAssignee(getString(data, "assignee"));
        c.setDepartment(getString(data, "department"));
        c.setCaseStatus(STATUS_ASSIGNED);
        c.setAssignedAt(LocalDateTime.now());
        c.setUpdatedAt(LocalDateTime.now());

        log.info("Case assigned: id={}, assignee={}", caseId, c.getAssignee());
        return MapBuilder.of("code", 0, "message", "案件分配成功", "data", c);
    }

    /** 处理案件 */
    public Map<String, Object> processCase(String caseId, Map<String, Object> data) {
        CaseInfo c = caseStore.get(caseId);
        if (c == null) {
            return MapBuilder.of("code", 404, "message", "案件不存在: " + caseId);
        }

        String action = getString(data, "action");
        c.setAction(action);
        c.setInvestigationNotes(getString(data, "notes"));
        c.setCaseStatus(STATUS_INVESTIGATING);
        c.setUpdatedAt(LocalDateTime.now());

        log.info("Case processed: id={}, action={}", caseId, action);
        return MapBuilder.of("code", 0, "message", "案件处理更新成功", "data", c);
    }

    /** 结案 */
    public Map<String, Object> closeCase(String caseId, Map<String, Object> data) {
        CaseInfo c = caseStore.get(caseId);
        if (c == null) {
            return MapBuilder.of("code", 404, "message", "案件不存在: " + caseId);
        }

        c.setCaseStatus(STATUS_CLOSED);
        c.setConclusion(getString(data, "conclusion"));
        c.setResolution(getString(data, "resolution"));
        c.setClosedAt(LocalDateTime.now());
        c.setUpdatedAt(LocalDateTime.now());

        log.info("Case closed: id={}, conclusion={}", caseId, c.getConclusion());
        return MapBuilder.of("code", 0, "message", "案件结案成功", "data", c);
    }

    /** 案件统计 */
    public Map<String, Object> getStats() {
        long total = caseStore.size();
        long byStatus = caseStore.values().stream()
            .filter(c -> STATUS_NEW.equals(c.getCaseStatus())).count();
        long inProgress = caseStore.values().stream()
            .filter(c -> STATUS_ASSIGNED.equals(c.getCaseStatus())
                      || STATUS_INVESTIGATING.equals(c.getCaseStatus())).count();
        long closed = caseStore.values().stream()
            .filter(c -> STATUS_CLOSED.equals(c.getCaseStatus())
                      || STATUS_ARCHIVED.equals(c.getCaseStatus())).count();

        return MapBuilder.of(
            "code", 0,
            "data", MapBuilder.of(
                "totalCases", total,
                "newCases", byStatus,
                "inProgress", inProgress,
                "closedCases", closed
            )
        );
    }

    private int calculatePriority(String riskLevel) {
        switch (riskLevel) {
            case "CRITICAL": return 1;
            case "HIGH": return 2;
            case "MEDIUM": return 3;
            default: return 4;
        }
    }

    private String getString(Map<String, Object> map, String key) {
        return getString(map, key, "");
    }

    private String getString(Map<String, Object> map, String key, String defaultValue) {
        Object val = map.get(key);
        return val != null ? val.toString() : defaultValue;
    }

    /** 案件信息内部类 */
    public static class CaseInfo {
        private String caseId;
        private String alertId;
        private String transactionId;
        private String customerId;
        private String caseType;
        private String riskLevel;
        private String caseStatus;
        private String description;
        private String assignee;
        private String department;
        private String action;
        private String investigationNotes;
        private String conclusion;
        private String resolution;
        private int priority;
        private LocalDateTime createdAt;
        private LocalDateTime assignedAt;
        private LocalDateTime closedAt;
        private LocalDateTime updatedAt;

        public String getCaseId() { return caseId; }
        public void setCaseId(String caseId) { this.caseId = caseId; }
        public String getAlertId() { return alertId; }
        public void setAlertId(String alertId) { this.alertId = alertId; }
        public String getTransactionId() { return transactionId; }
        public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getCaseType() { return caseType; }
        public void setCaseType(String caseType) { this.caseType = caseType; }
        public String getRiskLevel() { return riskLevel; }
        public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }
        public String getCaseStatus() { return caseStatus; }
        public void setCaseStatus(String caseStatus) { this.caseStatus = caseStatus; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public String getAssignee() { return assignee; }
        public void setAssignee(String assignee) { this.assignee = assignee; }
        public String getDepartment() { return department; }
        public void setDepartment(String department) { this.department = department; }
        public String getAction() { return action; }
        public void setAction(String action) { this.action = action; }
        public String getInvestigationNotes() { return investigationNotes; }
        public void setInvestigationNotes(String investigationNotes) { this.investigationNotes = investigationNotes; }
        public String getConclusion() { return conclusion; }
        public void setConclusion(String conclusion) { this.conclusion = conclusion; }
        public String getResolution() { return resolution; }
        public void setResolution(String resolution) { this.resolution = resolution; }
        public int getPriority() { return priority; }
        public void setPriority(int priority) { this.priority = priority; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        public LocalDateTime getAssignedAt() { return assignedAt; }
        public void setAssignedAt(LocalDateTime assignedAt) { this.assignedAt = assignedAt; }
        public LocalDateTime getClosedAt() { return closedAt; }
        public void setClosedAt(LocalDateTime closedAt) { this.closedAt = closedAt; }
        public LocalDateTime getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
    }
}
