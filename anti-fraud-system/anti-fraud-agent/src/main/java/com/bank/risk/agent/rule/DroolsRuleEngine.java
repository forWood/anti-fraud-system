package com.bank.risk.agent.rule;

import com.bank.risk.common.model.RuleResult;
import org.drools.core.impl.KnowledgeBaseFactory;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.*;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Drools规则引擎服务
 *
 * 功能:
 * 1. 加载 fraud-rules.drl 规则文件
 * 2. 创建 KieSession 执行规则匹配
 * 3. 支持动态刷新规则 (热加载)
 * 4. 收集规则执行指标
 *
 * @author 银行科技部
 */
@Service
public class DroolsRuleEngine {

    private static final Logger log = LoggerFactory.getLogger(DroolsRuleEngine.class);

    @Value("${drools.rules.path:src/main/resources/rules}")
    private String rulesPath;

    @Value("${drools.enable-dynamic-refresh:true}")
    private boolean enableDynamicRefresh;

    private volatile KieContainer kieContainer;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private String currentKieBaseHash;

    @PostConstruct
    public void init() {
        try {
            loadRules();
            log.info("DroolsRuleEngine initialized successfully. Rules path: {}", rulesPath);
        } catch (Exception e) {
            log.error("Failed to initialize DroolsRuleEngine", e);
        }
    }

    /**
     * 加载/重载规则文件
     */
    public void loadRules() {
        lock.writeLock().lock();
        try {
            KieServices kieServices = KieServices.Factory.get();
            KieFileSystem kieFileSystem = kieServices.newKieFileSystem();

            // 加载所有DRL文件
            File rulesDir = new File(rulesPath);
            if (rulesDir.exists() && rulesDir.isDirectory()) {
                File[] drlFiles = rulesDir.listFiles((dir, name) ->
                    name.endsWith(".drl") || name.endsWith(".xls"));
                if (drlFiles != null) {
                    for (File file : drlFiles) {
                        log.info("Loading rule file: {}", file.getName());
                        kieFileSystem.write(ResourceFactory.newFileResource(file)
                            .setResourceType(ResourceType.DRL));
                    }
                }
            } else {
                // 从 classpath 加载默认规则
                log.info("Loading default rules from classpath");
                kieFileSystem.write(ResourceFactory.newClassPathResource("rules/fraud-rules.drl")
                    .setResourceType(ResourceType.DRL));
            }

            KieBuilder kieBuilder = kieServices.newKieBuilder(kieFileSystem);
            kieBuilder.buildAll();

            Results results = kieBuilder.getResults();
            if (results.hasMessages(Message.Level.ERROR)) {
                String errors = results.getMessages(Message.Level.ERROR).stream()
                    .map(Message::getText)
                    .reduce("", (a, b) -> a + "; " + b);
                log.error("Rule compilation errors: {}", errors);
                throw new RuntimeException("Rule compilation failed: " + errors);
            }

            kieContainer = kieServices.newKieContainer(kieBuilder.getKieModule().getReleaseId());
            currentKieBaseHash = String.valueOf(kieContainer.hashCode());
            log.info("Rules loaded successfully: {} files", kieContainer.getKieBase().getKiePackages().size());

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 动态刷新规则 (每5分钟检查一次)
     */
    @Scheduled(fixedDelay = 300000) // 5分钟
    public void refreshRulesIfNeeded() {
        if (!enableDynamicRefresh) return;
        try {
            // 检查规则文件是否有变化 (通过文件修改时间)
            File rulesDir = new File(rulesPath);
            if (rulesDir.exists() && rulesDir.isDirectory()) {
                File[] drlFiles = rulesDir.listFiles((dir, name) -> name.endsWith(".drl"));
                if (drlFiles != null) {
                    long latestModified = Arrays.stream(drlFiles)
                        .mapToLong(File::lastModified)
                        .max()
                        .orElse(0);
                    String newHash = String.valueOf(latestModified);
                    if (!newHash.equals(currentKieBaseHash)) {
                        log.info("Rule files changed, reloading...");
                        loadRules();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to refresh rules", e);
        }
    }

    /**
     * 执行规则评估
     *
     * @param features 交易特征Map
     * @return 规则引擎评估结果
     */
    public RuleEngineResult evaluate(Map<String, Object> features) {
        lock.readLock().lock();
        try {
            if (kieContainer == null) {
                log.error("KieContainer not initialized");
                return RuleEngineResult.empty();
            }

            KieSession kieSession = kieContainer.newKieSession();
            List<RuleResult> matchedRules = new ArrayList<>();

            try {
                // 设置全局变量
                kieSession.setGlobal("matchedRules", matchedRules);
                kieSession.setGlobal("logger", log);

                // 插入事实对象
                kieSession.insert(features);

                // 插入设备信息
                Map<String, Object> deviceInfo = new HashMap<>();
                deviceInfo.put("deviceId", features.get("device_id"));
                deviceInfo.put("isEmulator", features.getOrDefault("is_emulator", false));
                deviceInfo.put("isRooted", features.getOrDefault("is_rooted", false));
                deviceInfo.put("isFirstBinding", features.getOrDefault("is_first_device", false));
                kieSession.insert(deviceInfo);

                // 插入客户统计信息
                Map<String, Object> customerStats = new HashMap<>();
                customerStats.put("dailyTransferInCount", features.getOrDefault("count_24h", 0));
                customerStats.put("dailyTransferOutAmount", features.getOrDefault("amount_24h", 0.0));
                customerStats.put("todayTransactionCount", features.getOrDefault("count_24h", 0));
                customerStats.put("deviceCount30d", features.getOrDefault("device_count_30d", 0));
                customerStats.put("cityCount30d", features.getOrDefault("city_count_30d", 0));
                kieSession.insert(customerStats);

                // 执行规则
                int rulesFired = kieSession.fireAllRules();

                log.debug("Rule engine executed: {} rules fired", rulesFired);

                return aggregateResults(features, matchedRules);

            } finally {
                kieSession.dispose();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 聚合规则结果
     */
    private RuleEngineResult aggregateResults(Map<String, Object> features,
                                                List<RuleResult> matchedRules) {
        if (matchedRules == null || matchedRules.isEmpty()) {
            return RuleEngineResult.empty();
        }

        // 计算加权规则得分
        double totalWeight = matchedRules.stream()
            .mapToDouble(r -> r.getRiskWeight() != null ? r.getRiskWeight() : 0)
            .sum();
        double weightedSum = matchedRules.stream()
            .mapToDouble(r -> (r.getRiskWeight() != null ? r.getRiskWeight() : 0)
                * (r.getRiskScore() != null ? r.getRiskScore() : 0))
            .sum();

        // 规则得分 = 加权和 / 总权重 * 系数
        double ruleScore = totalWeight > 0
            ? Math.min(100, (weightedSum / totalWeight) * 1.5)
            : 0.0;

        // 最高风险等级
        String highestRiskLevel = matchedRules.stream()
            .map(r -> r.getRiskLevel())
            .filter(Objects::nonNull)
            .max(Comparator.comparingInt(this::riskLevelToInt))
            .orElse("LOW");

        return RuleEngineResult.builder()
            .ruleScore(Math.round(ruleScore * 100.0) / 100.0)
            .matchedRules(matchedRules)
            .highestRiskLevel(highestRiskLevel)
            .ruleCount(matchedRules.size())
            .totalWeight(totalWeight)
            .build();
    }

    private int riskLevelToInt(String level) {
        switch (level) {
            case "HIGH": return 3;
            case "MEDIUM": return 2;
            case "LOW": return 1;
            default: return 0;
        }
    }

    /**
     * 获取当前规则数量
     */
    public int getRuleCount() {
        lock.readLock().lock();
        try {
            if (kieContainer != null) {
                return (int) kieContainer.getKieBase().getKiePackages().stream()
                    .flatMap(pkg -> pkg.getRules().stream())
                    .count();
            }
            return 0;
        } finally {
            lock.readLock().unlock();
        }
    }
}
