package com.bank.risk.rg.service;

import com.bank.risk.common.util.MapBuilder;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 报告生成服务
 *
 * 功能:
 * - 支持多种报告类型 (日报/周报/月报/合规报告/客户风险报告)
 * - 生成 Excel 格式报告
 * - 定时自动生成 (FATF合规要求)
 */
@Service
public class ReportGenerationService {

    private static final Logger log = LoggerFactory.getLogger(ReportGenerationService.class);

    private final Map<String, ReportInfo> reportStore = new ConcurrentHashMap<>();
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** 创建报告任务 */
    public Map<String, Object> createReport(Map<String, Object> data) {
        String reportId = "RPT" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
        String reportType = getString(data, "reportType", "DAILY");
        String dateRange = getString(data, "dateRange", LocalDate.now().toString());

        ReportInfo r = new ReportInfo();
        r.setReportId(reportId);
        r.setReportType(reportType);
        r.setReportName(buildReportName(reportType, dateRange));
        r.setDateRange(dateRange);
        r.setStatus("GENERATING");
        r.setFormat("EXCEL");
        r.setCreatedAt(LocalDateTime.now());

        reportStore.put(reportId, r);

        // 异步生成 (这里同步模拟)
        generateReportAsync(reportId);

        log.info("Report task created: id={}, type={}", reportId, reportType);
        return MapBuilder.of(
            "code", 0,
            "message", "报告生成任务已创建",
            "data", MapBuilder.of(
                "reportId", reportId,
                "status", "GENERATING"
            )
        );
    }

    /** 报告列表 */
    public Map<String, Object> listReports(int page, int size, String reportType) {
        List<ReportInfo> filtered = new ArrayList<>(reportStore.values());
        if (reportType != null && !reportType.isEmpty()) {
            filtered.removeIf(r -> !reportType.equals(r.getReportType()));
        }

        filtered.sort((a, b) -> b.getCreatedAt().compareTo(a.getCreatedAt()));

        int start = page * size;
        int end = Math.min(start + size, filtered.size());
        List<ReportInfo> pageData = (start < filtered.size()) ? filtered.subList(start, end) : Collections.emptyList();

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

    /** 查询报告 */
    public Map<String, Object> getReport(String reportId) {
        ReportInfo r = reportStore.get(reportId);
        if (r == null) {
            return MapBuilder.of("code", 404, "message", "报告不存在: " + reportId);
        }
        return MapBuilder.of("code", 0, "data", r);
    }

    /** 下载报告 */
    public Map<String, Object> downloadReport(String reportId) {
        ReportInfo r = reportStore.get(reportId);
        if (r == null) {
            return MapBuilder.of("code", 404, "message", "报告不存在: " + reportId);
        }
        if (!"COMPLETED".equals(r.getStatus())) {
            return MapBuilder.of("code", 400, "message", "报告尚未生成完成: " + r.getStatus());
        }

        return MapBuilder.of(
            "code", 0,
            "message", "报告下载链接已生成",
            "data", MapBuilder.of(
                "reportId", reportId,
                "downloadUrl", "/api/reports/files/" + reportId + ".xlsx",
                "fileSize", r.getFileSize()
            )
        );
    }

    /** 删除报告 */
    public Map<String, Object> deleteReport(String reportId) {
        if (reportStore.remove(reportId) != null) {
            return MapBuilder.of("code", 0, "message", "报告已删除");
        }
        return MapBuilder.of("code", 404, "message", "报告不存在: " + reportId);
    }

    /** 触发报告 */
    public Map<String, Object> triggerReport(Map<String, Object> data) {
        return createReport(data);
    }

    /** 定时生成合规报告 - 每天凌晨2点执行 */
    @Scheduled(cron = "0 0 2 * * ?")
    public void scheduledComplianceReport() {
        log.info("Scheduled compliance report generation started");
        Map<String, Object> data = MapBuilder.of(
            "reportType", "COMPLIANCE",
            "dateRange", LocalDate.now().minusDays(1).toString()
        );
        createReport(data);
    }

    /** 定时生成日报 - 每天早上8点执行 */
    @Scheduled(cron = "0 0 8 * * ?")
    public void scheduledDailyReport() {
        log.info("Scheduled daily report generation started");
        Map<String, Object> data = MapBuilder.of(
            "reportType", "DAILY",
            "dateRange", LocalDate.now().toString()
        );
        createReport(data);
    }

    private void generateReportAsync(String reportId) {
        try {
            Thread.sleep(500); // 模拟生成耗时
            ReportInfo r = reportStore.get(reportId);
            if (r != null) {
                r.setStatus("COMPLETED");
                r.setGeneratedAt(LocalDateTime.now());
                r.setFileSize((long) generateExcelReport(r).length);
                r.setDownloadUrl("/api/reports/files/" + reportId + ".xlsx");
                log.info("Report generated: id={}, size={}KB", reportId, r.getFileSize() / 1024);
            }
        } catch (Exception e) {
            log.error("Report generation failed: id={}", reportId, e);
            ReportInfo r = reportStore.get(reportId);
            if (r != null) r.setStatus("FAILED");
        }
    }

    private byte[] generateExcelReport(ReportInfo r) {
        try (Workbook wb = new XSSFWorkbook();
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            Sheet sheet = wb.createSheet(r.getReportName());

            // Header style
            CellStyle headerStyle = wb.createCellStyle();
            Font headerFont = wb.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            String[] headers = getHeadersForType(r.getReportType());
            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }

            // Sample data rows
            String[][] sampleData = getSampleDataForType(r.getReportType());
            for (int i = 0; i < sampleData.length; i++) {
                Row row = sheet.createRow(i + 1);
                for (int j = 0; j < sampleData[i].length; j++) {
                    row.createCell(j).setCellValue(sampleData[i][j]);
                }
            }

            // Auto-size columns
            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            wb.write(baos);
            return baos.toByteArray();
        } catch (Exception e) {
            log.error("Excel generation failed", e);
            return new byte[0];
        }
    }

    private String[] getHeadersForType(String reportType) {
        switch (reportType) {
            case "COMPLIANCE":
                return new String[]{"报告日期", "交易编号", "客户编号", "交易金额", "风险类型", "可疑等级", "处理状态"};
            case "CUSTOMER_RISK":
                return new String[]{"客户编号", "客户姓名", "风险等级", "关联案件数", "交易异常次数", "最后更新时间"};
            default:
                return new String[]{"日期", "指标名称", "数值", "环比变化", "风险趋势"};
        }
    }

    private String[][] getSampleDataForType(String reportType) {
        switch (reportType) {
            case "COMPLIANCE":
                return new String[][]{
                    {LocalDate.now().toString(), "TXN001", "C001", "50000", "大额可疑", "高", "待处理"},
                    {LocalDate.now().toString(), "TXN002", "C002", "98000", "拆分交易", "中", "调查中"},
                };
            case "CUSTOMER_RISK":
                return new String[][]{
                    {"C001", "张三", "高风险", "3", "15", LocalDateTime.now().format(FMT)},
                    {"C002", "李四", "中风险", "1", "4", LocalDateTime.now().format(FMT)},
                };
            default:
                return new String[][]{
                    {LocalDate.now().toString(), "新增预警数", "23", "+12%", "上升"},
                    {LocalDate.now().toString(), "处理中案件", "8", "-5%", "下降"},
                    {LocalDate.now().toString(), "结案数", "15", "+20%", "良好"},
                };
        }
    }

    private String buildReportName(String reportType, String dateRange) {
        String prefix;
        switch (reportType) {
            case "COMPLIANCE": prefix = "可疑交易合规报告"; break;
            case "CUSTOMER_RISK": prefix = "客户风险评估报告"; break;
            case "WEEKLY": prefix = "周度风控报告"; break;
            case "MONTHLY": prefix = "月度风控报告"; break;
            default: prefix = "日报";
        }
        return prefix + "_" + dateRange;
    }

    private String getString(Map<String, Object> map, String key, String defaultValue) {
        Object val = map.get(key);
        return val != null ? val.toString() : defaultValue;
    }

    /** 报告信息 */
    public static class ReportInfo {
        private String reportId;
        private String reportType;
        private String reportName;
        private String dateRange;
        private String status;
        private String format;
        private String downloadUrl;
        private long fileSize;
        private LocalDateTime createdAt;
        private LocalDateTime generatedAt;

        public String getReportId() { return reportId; }
        public void setReportId(String reportId) { this.reportId = reportId; }
        public String getReportType() { return reportType; }
        public void setReportType(String reportType) { this.reportType = reportType; }
        public String getReportName() { return reportName; }
        public void setReportName(String reportName) { this.reportName = reportName; }
        public String getDateRange() { return dateRange; }
        public void setDateRange(String dateRange) { this.dateRange = dateRange; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
        public String getDownloadUrl() { return downloadUrl; }
        public void setDownloadUrl(String downloadUrl) { this.downloadUrl = downloadUrl; }
        public long getFileSize() { return fileSize; }
        public void setFileSize(long fileSize) { this.fileSize = fileSize; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        public LocalDateTime getGeneratedAt() { return generatedAt; }
        public void setGeneratedAt(LocalDateTime generatedAt) { this.generatedAt = generatedAt; }
    }
}
