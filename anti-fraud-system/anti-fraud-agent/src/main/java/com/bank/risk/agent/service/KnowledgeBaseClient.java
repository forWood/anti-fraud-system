package com.bank.risk.agent.service;

import com.bank.risk.agent.grpc.SimilarCase;
import com.bank.risk.common.util.MapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 知识库客户端
 *
 * 通过HTTP/WebClient调用Python知识库服务的RAG检索接口
 * 在生产环境中通过gRPC调用knowledge-base-service
 *
 * @author 银行科技部
 */
@Service
public class KnowledgeBaseClient {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeBaseClient.class);

    @Value("${knowledge.service.url:http://localhost:8088}")
    private String knowledgeServiceUrl;

    @Value("${knowledge.service.timeout:2000}")
    private int timeoutMs;

    private final org.springframework.web.reactive.function.client.WebClient webClient;

    public KnowledgeBaseClient() {
        this.webClient = org.springframework.web.reactive.function.client.WebClient.builder()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
            .build();
    }

    /**
     * 检索相似案例
     */
    public KBSearchResult searchSimilar(Map<String, Object> features, int topK) {
        long startTime = System.currentTimeMillis();

        try {
            Map<String, Object> requestBody = MapBuilder.of(
                "features", features,
                "top_k", Math.max(1, Math.min(topK, 20))
            );

            @SuppressWarnings("unchecked")
            Map<String, Object> response = webClient.post()
                .uri(knowledgeServiceUrl + "/api/v1/knowledge/search")
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(Map.class)
                .block();

            long elapsedMs = System.currentTimeMillis() - startTime;

            if (response == null) {
                return KBSearchResult.empty(elapsedMs);
            }

            return parseSearchResult(response, elapsedMs);

        } catch (Exception e) {
            log.warn("Knowledge search failed: {}", e.getMessage());
            long elapsedMs = System.currentTimeMillis() - startTime;
            return KBSearchResult.empty(elapsedMs);
        }
    }

    @SuppressWarnings("unchecked")
    private KBSearchResult parseSearchResult(Map<String, Object> response, long elapsedMs) {
        List<SimilarCase> cases = Collections.emptyList();
        double relevanceScore = 0.0;

        try {
            List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("results");
            if (results != null && !results.isEmpty()) {
                cases = results.stream()
                    .limit(5)
                    .map(this::mapToSimilarCase)
                    .collect(java.util.stream.Collectors.toList());

                relevanceScore = results.stream()
                    .mapToDouble(r -> ((Number) r.getOrDefault("similarity", 0.0)).doubleValue())
                    .average()
                    .orElse(0.0);
            }
        } catch (Exception e) {
            log.warn("Failed to parse search results: {}", e.getMessage());
        }

        return new KBSearchResult(cases, relevanceScore * 100, elapsedMs);
    }

    private SimilarCase mapToSimilarCase(Map<String, Object> map) {
        return SimilarCase.newBuilder()
            .setCaseId(String.valueOf(map.getOrDefault("case_id", "")))
            .setCaseTitle(String.valueOf(map.getOrDefault("case_title", "")))
            .setCaseType(String.valueOf(map.getOrDefault("case_type", "")))
            .setFraudPattern(String.valueOf(map.getOrDefault("fraud_pattern", "")))
            .setModusOperandi(String.valueOf(map.getOrDefault("modus_operandi", "")))
            .setResolution(String.valueOf(map.getOrDefault("resolution", "")))
            .setSimilarity(((Number) map.getOrDefault("similarity", 0.0)).doubleValue())
            .build();
    }

    /**
     * 知识库检索结果
     */
    public static class KBSearchResult {
        private final List<SimilarCase> cases;
        private final double relevanceScore;
        private final long searchTimeMs;

        public KBSearchResult(List<SimilarCase> cases, double relevanceScore, long searchTimeMs) {
            this.cases = cases;
            this.relevanceScore = relevanceScore;
            this.searchTimeMs = searchTimeMs;
        }

        public List<SimilarCase> getCases() { return cases; }
        public double getRelevanceScore() { return relevanceScore; }
        public long getSearchTimeMs() { return searchTimeMs; }

        public static KBSearchResult empty(long elapsedMs) {
            return new KBSearchResult(Collections.emptyList(), 0.0, elapsedMs);
        }
    }
}
