# 反欺诈与反恐融资风险识别智能体系统
# 系统设计说明书 (SDS)

---

**文档版本：** V2.0  
**编写日期：** 2026年5月3日  
**文档状态：** 初稿  
**编制人：** 银行科技部  

---

## 目录

1. [系统架构设计](#1-系统架构设计)
2. [技术架构设计](#2-技术架构设计)
3. [大数据采集与处理设计](#3-大数据采集与处理设计)
4. [风控知识库设计](#4-风控知识库设计)
5. [AI智能体设计](#5-ai智能体设计)
6. [核心模块设计](#6-核心模块设计)
7. [数据架构设计](#7-数据架构设计)
8. [接口设计](#8-接口设计)
9. [安全架构设计](#9-安全架构设计)
10. [部署架构设计](#10-部署架构设计)
11. [高可用与容灾设计](#11-高可用与容灾设计)
12. [监控运维设计](#12-监控运维设计)

---

## 1. 系统架构设计

### 1.1 完整架构设计

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         大数据驱动的反欺诈智能体系统完整架构                           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ════════════════════════════════════════════════════════════════════════════════  │
│  ║                            第一层：大数据采集层                                   ║  │
│  ════════════════════════════════════════════════════════════════════════════════  │
│  │                                                                                 │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │  │
│  │  │  交易系统   │  │ 埋点日志    │  │ 用户行为    │  │  设备指纹   │          │  │
│  │  │  核心银行   │  │  App/Web    │  │  登录登出   │  │  SDK采集    │          │  │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘          │  │
│  │         └───────────────┼─────────────────┼─────────────────┘                 │  │
│  ╚══════════════════════════╪═════════════════╪══════════════════════════════════════╝  │
│                           ▼                                   │                     │
│  ═════════════════════════════════════════════════════════════╪═════════════════════  │
│  ║                            数据接入层                      ║                      │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  │                   │
│  │  │  Kafka Producer│  │  Flume Agent   │  │  Database CDC  │  │                   │
│  │  │  (实时流)      │  │  (日志文件)    │  │  (binlog)     │  │                   │
│  │  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘  │                   │
│  ╚══════════╪═══════════════════╪══════════════════╪═════════════════════════════╝  │
│             └───────────────────┼─────────────────┼──────────────────┘                │
│                                 ▼                                 │                 │
│  ═════════════════════════════════════════════════════════════════╪═════════════════  │
│  ║                           大数据处理层                         ║                   │
│  │                                                                                 │  │
│  │  ┌─────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                        Flink 流处理引擎                                  │  │  │
│  │  │                                                                          │  │  │
│  │  │   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐               │  │  │
│  │  │   │ 数据接入 │ → │ 数据校验 │ → │ 实时清洗 │ → │ 实时特征 │               │  │  │
│  │  │   └─────────┘    └─────────┘    └─────────┘    └─────────┘               │  │  │
│  │  │                                                  │                        │  │  │
│  │  │                              ┌──────────────────┘                        │  │  │
│  │  │                              ▼                                          │  │  │
│  │  │   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐               │  │  │
│  │  │   │ 名单匹配 │    │ 风险评分│    │ 预警生成│    │ 结果输出│               │  │  │
│  │  │   └─────────┘    └─────────┘    └─────────┘    └─────────┘               │  │  │
│  │  └─────────────────────────────────────────────────────────────────────────┘  │  │
│  ╚═══════════════════════════════════════════════════════════════════════════════  │
│                                     │                                              │
│                                     ▼                                              │
│  ════════════════════════════════════════════════════════════════════════════════  │
│  ║                           风控知识库层                                         ║      │
│  │                                                                                 │  │
│  │  ┌─────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                         知识库管理系统                                    │  │  │
│  │  │                                                                          │  │  │
│  │  │   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐                │  │  │
│  │  │   │ 规则库  │  │ 黑名单库 │  │ 案例库  │  │ 画像库  │                │  │  │
│  │  │   └─────────┘  └─────────┘  └─────────┘  └─────────┘                │  │  │
│  │  │                                                                          │  │  │
│  │  │   ┌─────────────────────────────────────────────────────────────────┐   │  │  │
│  │  │   │                      向量化处理层                                │   │  │  │
│  │  │   │   Embedding生成  →  向量存储(FAISS/Milvus)  →  相似度检索      │   │  │  │
│  │  │   └─────────────────────────────────────────────────────────────────┘   │  │  │
│  │  └─────────────────────────────────────────────────────────────────────────┘  │  │
│  ╚═══════════════════════════════════════════════════════════════════════════════  │
│                                     │                                              │
│                                     ▼                                              │
│  ════════════════════════════════════════════════════════════════════════════════  │
│  ║                        AI智能体层                                    ║      │
│  │                                                                                 │  │
│  │  ┌─────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                      AI智能体核心                                       │  │  │
│  │  │                                                                          │  │  │
│  │  │   ┌────────────────────────────────────────────────────────────────┐    │  │  │
│  │  │   │                     智能体推理流程                                │    │  │  │
│  │  │   │                                                                  │    │  │  │
│  │  │   │   [交易请求] → [上下文构建] → [知识检索] → [规则匹配] → [模型推理] │    │  │  │
│  │  │   │        │            │            │            │            │        │    │  │  │
│  │  │   │        ▼            ▼            ▼            ▼            ▼        │    │  │  │
│  │  │   │   [响应决策] ← [综合评分] ← [RAG检索] ← [规则执行] ← [特征提取]  │    │  │  │
│  │  │   └────────────────────────────────────────────────────────────────┘    │  │  │
│  │  │                                                                          │  │  │
│  │  │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │  │  │
│  │  │   │  本地挂载   │  │  向量检索   │  │  规则引擎   │  │  ML模型    │    │  │  │
│  │  │   │  文档检索   │  │  相似案例   │  │  Drools     │  │  TensorFlow │    │  │  │
│  │  │   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │  │  │
│  │  └─────────────────────────────────────────────────────────────────────────┘  │  │
│  ╚═══════════════════════════════════════════════════════════════════════════════  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 技术选型

#### 1.2.1 轻量级方案（本地运行）

| 组件类别 | 技术选型 | 版本要求 | 说明 |
|----------|----------|----------|------|
| 数据采集 | Python脚本/日志读取 | 3.9+ | 轻量数据接入 |
| 数据清洗 | Pandas | 1.5+ | 中小量级数据处理 |
| 知识库存储 | SQLite + 本地文件 | - | 简单部署 |
| 向量库 | FAISS | 1.7+ | Facebook开源向量库 |
| 全文检索 | Whoosh | 2.7+ | 轻量搜索引擎 |
| AI智能体 | LangChain/LlamaIndex | 最新版 | 文档检索RAG |
| 机器学习 | scikit-learn | 1.2+ | 模型训练 |
| 规则引擎 | Drools | 7.x | 复杂规则执行 |

#### 1.2.2 企业级方案

| 组件类别 | 技术选型 | 版本要求 | 说明 |
|----------|----------|----------|------|
| 数据采集 | Flume/Kafka Connect | 1.x/2.x | 高吞吐数据采集 |
| 实时计算 | Flink | 1.16+ | 实时交易特征计算 |
| 离线计算 | Spark | 3.3+ | 批量特征计算 |
| 规则引擎 | Drools | 7.x | 复杂业务规则执行 |
| 机器学习 | Python + TensorFlow/PyTorch | 3.9+/2.x | 模型训练与推理 |
| 模型服务 | TensorFlow Serving | 2.x | 模型在线推理 |
| 消息队列 | Apache Kafka | 3.x | 异步消息处理 |
| 缓存 | Redis Cluster | 6.x | 高性能缓存 |
| 数据库 | MySQL 8.0 / PolarDB | 最新版 | 主从架构 |
| OLAP | ClickHouse | 22.x | 高性能分析 |
| 搜索引擎 | Elasticsearch | 7.x | 日志搜索、分析 |
| 图数据库 | Neo4j | 5.x | 关系图谱 |
| 向量库 | Milvus | 2.x | 企业级向量检索 |
| 容器化 | Docker + Kubernetes | 1.24+ | 容器编排 |
| AI智能体 | LangChain/LlamaIndex | 最新版 | 文档检索RAG |

---

## 2. 技术架构设计

### 2.1 微服务划分

| 服务名称 | 服务代码 | 主要职责 | 服务数量建议 |
|----------|----------|----------|--------------|
| 数据采集服务 | data-collection | 数据接入、格式转换、路由分发 | 2-4 |
| 实时计算服务 | flink-stream | 实时特征计算、实时评分 | 4-8 |
| 离线计算服务 | spark-batch | 离线特征计算、批次处理 | 2-4 |
| 交易监控服务 | transaction-monitor | 交易接入、预处理、规则预检 | 4-8 |
| 风险评估服务 | risk-assessment | 风险评分、模型推理、决策引擎 | 4-8 |
| 预警管理服务 | alert-management | 预警生成、推送、升级 | 2-4 |
| 案例管理服务 | case-management | 案例创建、调查、工作流 | 2-4 |
| 名单扫描服务 | list-scan | 名单管理、实时匹配、模糊匹配 | 2-4 |
| 报告生成服务 | report-generation | 可疑报告生成、上报管理 | 2-4 |
| 统计报表服务 | report-statistics | 数据统计、报表生成 | 2-4 |
| 知识库服务 | knowledge-base | 知识管理、检索、向量化 | 2-4 |
| 智能体服务 | ai-agent | 智能体推理、RAG检索 | 2-4 |
| 系统管理服务 | system-admin | 用户管理、权限配置、参数管理 | 2 |

---

## 3. 大数据采集与处理设计

### 3.1 数据采集模块

```python
# 数据采集配置示例
class DataSourceConfig:
    """数据源接入配置"""
    
    # 交易流水数据源
    TRANSACTION_SOURCE = {
        "type": "kafka",
        "bootstrap_servers": "kafka:9092",
        "topic": "transaction-raw",
        "group_id": "risk-system",
        "format": "json"
    }
    
    # 埋点日志数据源
    LOG_SOURCE = {
        "type": "flume",
        "host": "flume-agent:9092",
        "format": "avro"
    }
```

### 3.2 数据清洗流水线

```python
# 数据清洗流水线
class DataCleaningPipeline:
    """数据清洗处理流水线"""
    
    def __init__(self):
        self.steps = [
            DeduplicationStep(),        # 去重
            FormatStandardizeStep(),    # 格式标准化
            DataMaskingStep(),          # 脱敏处理
            AnomalyFilterStep(),        # 异常值过滤
            FeatureAggregationStep()    # 特征聚合
        ]
        
    def process(self, raw_data: dict) -> dict:
        """执行清洗流水线"""
        data = raw_data.copy()
        for step in self.steps:
            data = step.execute(data)
        return data

class DataMaskingStep:
    """数据脱敏步骤"""
    
    @staticmethod
    def mask_phone(phone: str) -> str:
        """手机号脱敏: 138****5678"""
        if len(phone) == 11:
            return f"{phone[:3]}****{phone[-4:]}"
        return phone
    
    @staticmethod
    def mask_id_card(id_card: str) -> str:
        """身份证脱敏: 110101****12345678"""
        if len(id_card) == 18:
            return f"{id_card[:6]}********{id_card[-4:]}"
        return id_card
```

---

## 4. 风控知识库设计

### 4.1 知识库体系架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          风控知识库体系架构                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        知识库类型                                    │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │                                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │   │
│  │  │   规则库     │  │   黑名单库   │  │   欺诈案例库  │               │   │
│  │  ├─────────────┤  ├─────────────┤  ├─────────────┤               │   │
│  │  │ 历史风控规则 │  │ 制裁名单    │  │ 历史欺诈样本  │               │   │
│  │  │ 欺诈团伙特征 │  │ 内部黑名单  │  │ 风险处置方案  │               │   │
│  │  │ 作案模式文档 │  │ 高风险名单  │  │ 误判复盘记录  │               │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘               │   │
│  │                                                                     │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │                    用户行为画像库                            │   │   │
│  │  ├─────────────────────────────────────────────────────────────┤   │   │
│  │  │ 正常行为基线 │ 风险阈值表 │ 地域风险等级 │ 设备风险清单 │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                       │
│                                     ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        向量化处理层                                   │   │
│  │                                                                      │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │              Embedding生成                                    │   │   │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐        │   │   │
│  │  │  │ 文本分词 │ → │ 向量化  │ → │ 向量存储 │ → │ 索引构建 │        │   │   │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘        │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 知识库向量化设计

```python
# 知识库向量化处理
import faiss
import numpy as np

class KnowledgeBaseVectorizer:
    """知识库向量化处理器"""
    
    def __init__(self, model_name: str = 'text2vec-chinese'):
        self.model = SentenceTransformer(model_name)
        self.dimension = 256  # 向量维度
        self.index = faiss.IndexFlatIP(self.dimension)
        self.metadata = []
        
    def vectorize_document(self, document: str) -> np.ndarray:
        """将文档向量化"""
        embedding = self.model.encode(document)
        return embedding / np.linalg.norm(embedding)
        
    def add_knowledge(self, knowledge_id: str, content: str, category: str):
        """添加知识条目到向量库"""
        vector = self.vectorize_document(content)
        self.index.add(np.array([vector]).astype('float32'))
        self.metadata.append({
            'id': knowledge_id,
            'content': content,
            'category': category
        })
        
    def search_similar(self, query: str, top_k: int = 5) -> List[dict]:
        """检索相似知识条目"""
        query_vector = self.vectorize_document(query)
        distances, indices = self.index.search(
            np.array([query_vector]).astype('float32'), 
            top_k
        )
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx >= 0:
                results.append({
                    **self.metadata[idx],
                    'similarity': float(dist)
                })
        return results
```

---

## 5. AI智能体设计



### 5.1 智能体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AI智能体架构                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      智能体核心 (AI Agent)                             │   │
│  │                                                                      │   │
│  │  ┌─────────────────────────────────────────────────────────────┐    │   │
│  │  │                     上下文构建器                              │    │   │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐        │    │   │
│  │  │  │ 交易特征 │  │ 客户画像 │  │ 历史记录 │  │ 实时特征 │        │    │   │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘        │    │   │
│  │  └─────────────────────────────────────────────────────────────┘    │   │
│  │                                    │                                  │   │
│  │                                    ▼                                  │   │
│  │  ┌─────────────────────────────────────────────────────────────┐    │   │
│  │  │                     RAG检索增强                              │    │   │
│  │  │   ┌─────────┐  ┌─────────┐  ┌─────────┐                    │    │   │
│  │  │   │ 规则检索 │→│ 案例检索 │→│ 知识检索 │                    │    │   │
│  │  │   │         │  │         │  │         │                    │    │   │
│  │  │   │ Drools  │  │ FAISS   │  │ ES检索   │                    │    │   │
│  │  │   └─────────┘  └─────────┘  └─────────┘                    │    │   │
│  │  └─────────────────────────────────────────────────────────────┘    │   │
│  │                                    │                                  │   │
│  │                                    ▼                                  │   │
│  │  ┌─────────────────────────────────────────────────────────────┐    │   │
│  │  │                    决策推理引擎                              │    │   │
│  │  │   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐      │    │   │
│  │  │   │ 规则匹配 │→│ 模型评分 │→│ 综合评分 │→│ 决策输出 │      │    │   │
│  │  │   │ Drools  │  │TensorFlow│ │ 加权融合 │ │ 风险决策 │      │    │   │
│  │  │   └─────────┘  └─────────┘  └─────────┘  └─────────┘      │    │   │
│  │  └─────────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 智能体核心实现

```python
# 反欺诈智能体核心
class AntiFraudAgent:
    """
    反欺诈AI智能体
    融合知识库检索 + 规则引擎 + 机器学习模型
    """
    
    def __init__(self):
        self.knowledge_manager = KnowledgeBaseManager()
        self.rule_engine = RuleEngine()
        self.fraud_model = FraudDetectionModel()
        self.anomaly_model = AnomalyDetectionModel()
        self.feature_store = FeatureStore()
        
    async def assess_transaction(self, transaction: Transaction) -> AssessmentResult:
        """评估交易风险"""
        # 1. 构建上下文
        context = await self._build_context(transaction)
        
        # 2. RAG检索
        rag_context = await self._rag_retrieve(context)
        
        # 3. 规则匹配
        rule_results = self.rule_engine.evaluate(transaction, context)
        
        # 4. 机器学习模型评分
        ml_score = self._ml_score(context, rag_context)
        
        # 5. 综合评分
        final_score = self._aggregate_score(rule_results, ml_score)
        
        # 6. 生成决策和解释
        decision = self._generate_decision(final_score)
        explanation = self._generate_explanation(
            transaction, rule_results, rag_context, ml_score
        )
        
        return AssessmentResult(
            transaction_id=transaction.transaction_id,
            risk_score=final_score,
            risk_level=self._score_to_level(final_score),
            decision=decision,
            confidence=self._calculate_confidence(rule_results, ml_score),
            explanation=explanation,
            matched_rules=rule_results.matched_rules,
            similar_cases=rag_context.similar_cases,
            recommendations=decision.recommendations
        )
    
    def _aggregate_score(self, rule_results: RuleResults, ml_score: float) -> float:
        """
        综合评分
        规则评分权重: 40%
        模型评分权重: 60%
        """
        rule_score = 0.0
        if rule_results.matched_rules:
            total_weight = sum(r.risk_weight for r in rule_results.matched_rules)
            weighted_sum = sum(r.risk_weight * r.risk_score for r in rule_results.matched_rules)
            rule_score = min(100, weighted_sum / max(total_weight, 1) * 1.5)
        
        final_score = 0.4 * rule_score + 0.6 * (ml_score * 100)
        return min(100, max(0, final_score))
```

### 5.3 知识库挂载配置

```yaml
# AI智能体配置
agent:
  name: "bank-anti-fraud-agent"
  description: "银行反欺诈与反恐融资风险识别智能体"
  
  # 知识库挂载
  knowledge_base:
    enabled: true
    
    # 本地文件挂载
    local_files:
      - path: "./knowledge/rules/"
        name: "风控规则库"
        description: "历史风控规则与作案模式"
      - path: "./knowledge/cases/"
        name: "欺诈案例库"
        description: "历史欺诈案例与处置方案"
        
    # 向量知识库
    vector_store:
      enabled: true
      type: "faiss"
      dimension: 256
      collections:
        - name: "fraud_cases"
          source: "./knowledge/cases/"
          
  # 决策配置
  decision:
    thresholds:
      high_risk: 80
      medium_risk: 50
      low_risk: 0
```

### 5.4 智能体决策流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         智能体决策流程详解                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Step 1: 请求接收 → Step 2: 上下文构建 → Step 3: RAG检索                    │
│                                                                             │
│  Step 4: 规则引擎执行                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │   匹配规则:                                                         │   │
│  │   - R001: 分散转入集中转出 (权重: 40)                                │   │
│  │   - R003: 跨境交易高风险 (权重: 20)                                 │   │
│  │   - R005: 新设备首次交易 (权重: 15)                                 │   │
│  │   规则评分: 35分                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Step 5: ML模型推理                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │   模型输出:                                                          │   │
│  │   - 欺诈概率: 0.72                                                   │   │
│  │   - 综合评分: 70分                                                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Step 6: 综合决策                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │   综合评分 = 0.4 × 规则评分(35) + 0.6 × ML评分(70)                   │   │
│  │          = 14 + 42 = 56分                                           │   │
│  │   风险等级: 中风险 (50-79分)                                         │   │
│  │   决策: 人工审核                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. 核心模块设计

### 6.1 机器学习模型模块

| 模型类型 | 模型名称 | 用途 | 输入 | 输出 |
|----------|----------|------|------|------|
| 有监督 | 欺诈分类模型 | 二分类判断 | 交易特征 | 欺诈概率(0-1) |
| 有监督 | 可疑等级模型 | 多分类判断 | 交易+客户特征 | 可疑等级(1-5) |
| 无监督 | 异常检测模型 | 识别新型欺诈 | 行为特征 | 异常分数 |
| 无监督 | 聚类分析模型 | 客户分群 | 客户特征 | 群体标签 |
| 图网络 | 团伙识别模型 | 关联欺诈检测 | 关系图谱 | 团伙标签 |

### 6.2 规则引擎Drools实现

#### 6.2.1 规则文件示例 (fraud-rules.drl)

```drools
package com.bank.risk.rules;

import com.bank.risk.model.Transaction;
import com.bank.risk.model.RuleResult;
import java.time.LocalTime;
import java.time.DayOfWeek;

// ===== 金额类规则 =====

rule "R001-单笔大额交易"
    salience 100
    when
        $tx: Transaction(amount >= 500000.00)
    then
        insert(new RuleResult("R001", "单笔大额交易",
              "单笔交易金额≥50万元", 30, "MEDIUM"));
end

rule "R003-分散转入集中转出"
    salience 95
    when
        $tx: Transaction(transactionType == "TRANSFER")
        $stats: CustomerStats(
            dailyTransferInCount >= 10,
            dailyTransferOutAmount >= 300000.00
        ) from $tx.getCustomerStats()
    then
        insert(new RuleResult("R003", "分散转入集中转出",
              "单日转入≥10笔且转出累计≥30万元", 60, "HIGH"));
end

rule "R101-凌晨高频交易"
    salience 85
    when
        $tx: Transaction(this.isDuringHours(0, 5), amount >= 50000.00)
        $stats: CustomerStats(todayTransactionCount >= 3) from $tx.getCustomerStats()
    then
        insert(new RuleResult("R101", "凌晨高频交易",
              "00:00-05:00交易次数≥3笔且累计≥5万元", 40, "MEDIUM"));
end

rule "R201-跨省急速交易"
    salience 75
    when
        $tx: Transaction()
        $prevTx: Transaction(
            transactionId != $tx.transactionId,
            ipCity != $tx.ipCity,
            transactionTime >= $tx.transactionTime.minusMinutes(30)
        ) from entry-point "recentTransactions"
    then
        insert(new RuleResult("R201", "跨省急速交易",
              "30分钟内跨省交易", 45, "MEDIUM"));
end

rule "R203-高匿名IP交易"
    salience 90
    when
        $tx: Transaction(ipRiskLevel == "VPN" || ipRiskLevel == "PROXY" || ipRiskLevel == "TOR")
    then
        insert(new RuleResult("R203", "高匿名IP交易",
              "IP为VPN/代理/Tor出口节点", 50, "HIGH"));
end

rule "R301-新设备首次交易"
    salience 70
    when
        $tx: Transaction(amount >= 10000.00)
        $device: DeviceFingerprint(isFirstBinding == true) from $tx.getDeviceInfo()
    then
        insert(new RuleResult("R301", "新设备首次交易",
              "设备首次绑定账户且交易≥1万元", 30, "MEDIUM"));
end

rule "R302-模拟器检测"
    salience 95
    when
        $tx: Transaction()
        $device: DeviceFingerprint(isEmulator == true) from $tx.getDeviceInfo()
    then
        insert(new RuleResult("R302", "模拟器检测",
              "设备为模拟器/虚拟机", 60, "HIGH"));
end

rule "T001-制裁名单命中"
    salience 100
    when
        $tx: Transaction()
        exists BlacklistEntry(
            matchType == "EXACT",
            (entityName == $tx.counterpartyName ||
             accountNumber == $tx.counterpartyAccount),
            isActive == true,
            (expiryDate == null || expiryDate >= LocalDate.now())
        )
    then
        insert(new RuleResult("T001", "制裁名单命中",
              "交易对手命中制裁名单", 100, "HIGH"));
        $tx.setActionRequired("BLOCK");
end
```

#### 6.2.2 规则引擎服务实现

```java
// RuleEngineService.java
@Service
@Slf4j
public class RuleEngineService {

    private final KieContainer kieContainer;
    private final RuleMetricsCollector metricsCollector;

    public RuleEngineService(KieContainer kieContainer,
                             RuleMetricsCollector metricsCollector) {
        this.kieContainer = kieContainer;
        this.metricsCollector = metricsCollector;
    }

    public RuleEngineResult evaluate(Transaction transaction,
                                      CustomerStats customerStats,
                                      CustomerProfile customerProfile) {
        long startTime = System.currentTimeMillis();
        KieSession kieSession = kieContainer.newKieSession();
        List<RuleResult> ruleResults = new ArrayList<>();

        try {
            kieSession.setGlobal("ruleResults", ruleResults);
            kieSession.insert(transaction);
            kieSession.insert(customerStats);
            kieSession.insert(customerProfile);
            kieSession.insert(transaction.getDeviceInfo());

            EntryPoint recentTxEP = kieSession.getEntryPoint("recentTransactions");
            EntryPoint recentInflowEP = kieSession.getEntryPoint("recentInflow");

            int ruleFiredCount = kieSession.fireAllRules();
            long elapsed = System.currentTimeMillis() - startTime;
            metricsCollector.recordRuleExecution(elapsed, ruleFiredCount);

            return aggregateRuleResults(transaction, ruleResults);
        } finally {
            kieSession.dispose();
        }
    }

    private RuleEngineResult aggregateRuleResults(Transaction tx,
                                                   List<RuleResult> results) {
        if (results.isEmpty()) {
            return RuleEngineResult.empty(tx.getTransactionId());
        }
        double totalWeight = results.stream().mapToDouble(RuleResult::getRiskWeight).sum();
        double weightedSum = results.stream()
                .mapToDouble(r -> r.getRiskWeight() * r.getRiskScore()).sum();
        double ruleScore = Math.min(100, weightedSum / Math.max(totalWeight, 1) * 1.5);

        return RuleEngineResult.builder()
                .transactionId(tx.getTransactionId())
                .ruleScore(Math.round(ruleScore * 100.0) / 100.0)
                .matchedRules(results)
                .highestRiskLevel(results.stream()
                        .map(RuleResult::getRiskLevel)
                        .max(Comparator.naturalOrder()).orElse("LOW"))
                .ruleCount(results.size())
                .build();
    }
}
```

### 6.3 Flink实时计算作业设计

#### 6.3.1 Flink作业主程序

```java
// FraudDetectionJob.java
@Slf4j
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProps.setProperty("group.id", "fraud-detection-group");

        // 1. 从Kafka读取交易数据
        DataStream<Transaction> transactionStream = env
            .addSource(new FlinkKafkaConsumer<>(
                "transaction-raw",
                new JSONDeserializationSchema<>(Transaction.class),
                kafkaProps
            ))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((tx, t) -> tx.getTransactionTime().getTime())
            );

        // 2. 数据清洗与特征提取
        DataStream<EnrichedTransaction> enrichedStream = transactionStream
            .map(new DataCleanFunction())
            .keyBy(tx -> tx.getAccountIdHash())
            .process(new FeatureExtractionProcess())
            .name("feature-extraction");

        // 3. 多维度风险评估
        DataStream<RiskAssessment> riskStream = enrichedStream
            .map(new RichMapFunction<EnrichedTransaction, RiskAssessment>() {
                private transient RuleEngineService ruleEngine;
                private transient FraudDetectionModel mlModel;
                private transient KnowledgeBaseClient kbClient;

                @Override
                public void open(Configuration parameters) {
                    ruleEngine = new RuleEngineService();
                    mlModel = new FraudDetectionModel();
                    kbClient = new KnowledgeBaseClient();
                }

                @Override
                public RiskAssessment map(EnrichedTransaction tx) throws Exception {
                    CompletableFuture<RuleResult> ruleFuture =
                        CompletableFuture.supplyAsync(() -> ruleEngine.evaluate(tx));
                    CompletableFuture<MLResult> mlFuture =
                        CompletableFuture.supplyAsync(() -> mlModel.predict(tx));
                    CompletableFuture<KBSearchResult> kbFuture =
                        CompletableFuture.supplyAsync(() -> kbClient.searchSimilar(tx));
                    CompletableFuture.allOf(ruleFuture, mlFuture, kbFuture).join();
                    return RiskAssessmentAggregator.aggregate(
                        tx, ruleFuture.get(), mlFuture.get(), kbFuture.get());
                }
            })
            .name("risk-assessment");

        // 4. 预警生成
        DataStream<AlertEvent> alertStream = riskStream
            .filter(a -> a.getRiskScore() >= 50)
            .map(new AlertGenerator())
            .name("alert-generation");

        // 5. 输出
        enrichedStream.addSink(new FlinkKafkaProducer<>(
            "transaction-assessed", new JSONSerializationSchema<>(), kafkaProps));
        alertStream.addSink(new FlinkKafkaProducer<>(
            "alert-created", new JSONSerializationSchema<>(), kafkaProps));
        alertStream.addSink(new JdbcSink<>(AlertRepository::save,
            new JdbcExecutionOptions.Builder().withBatchSize(100).build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://mysql:3306/risk_db")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("risk_user")
                .withPassword("risk_pass_2026")
                .build()
        ));

        env.execute("FraudDetection-Flink-Job");
    }
}
```

#### 6.3.2 实时特征提取ProcessFunction

```java
// FeatureExtractionProcess.java
public class FeatureExtractionProcess
        extends KeyedProcessFunction<String, Transaction, EnrichedTransaction> {

    private transient ListState<Transaction> recentTransactions;
    private transient ValueState<CustomerBehaviorBaseline> behaviorBaseline;
    private transient MapState<String, Integer> deviceCountState;
    private transient MapState<String, Integer> cityCountState;

    @Override
    public void open(Configuration parameters) {
        recentTransactions = getRuntimeContext()
            .getListState(new ListStateDescriptor<>("recent-tx", Transaction.class));
        behaviorBaseline = getRuntimeContext()
            .getState(new ValueStateDescriptor<>("behavior-baseline", CustomerBehaviorBaseline.class));
        deviceCountState = getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("device-count", String.class, Integer.class));
        cityCountState = getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("city-count", String.class, Integer.class));
    }

    @Override
    public void processElement(Transaction tx, Context ctx,
                                Collector<EnrichedTransaction> out) throws Exception {
        long eventTime = tx.getTransactionTime().getTime();
        recentTransactions.add(tx);
        ctx.timerService().registerEventTimeTimer(eventTime + 86400000L); // 24h清除

        // 更新设备/城市计数
        deviceCountState.put(tx.getDeviceId(),
            deviceCountState.contains(tx.getDeviceId()) ? deviceCountState.get(tx.getDeviceId()) + 1 : 1);
        cityCountState.put(tx.getIpCity(),
            cityCountState.contains(tx.getIpCity()) ? cityCountState.get(tx.getIpCity()) + 1 : 1);

        // 计算1小时窗口特征
        List<Transaction> windowTxs = new ArrayList<>();
        for (Transaction t : recentTransactions.get()) {
            if (eventTime - t.getTransactionTime().getTime() <= 3600000) {
                windowTxs.add(t);
            }
        }

        EnrichedTransaction enriched = new EnrichedTransaction(tx);
        enriched.setAmount1h(windowTxs.stream().mapToDouble(Transaction::getAmount).sum());
        enriched.setCount1h(windowTxs.size());
        enriched.setDeviceCount30d(getMapSize(deviceCountState));
        enriched.setCityCount30d(getMapSize(cityCountState));
        enriched.setIsNightTime(tx.getTransactionTime().getHour() >= 0
            && tx.getTransactionTime().getHour() < 6);

        // 行为偏离度
        CustomerBehaviorBaseline baseline = behaviorBaseline.value();
        if (baseline != null) {
            enriched.setAmountDeviation(
                Math.abs(tx.getAmount() - baseline.getAvgAmount()) / Math.max(baseline.getStdAmount(), 1));
        }
        out.collect(enriched);
    }
}
```

#### 6.3.3 综合评分聚合器

```java
// RiskAssessmentAggregator.java
public class RiskAssessmentAggregator {

    private static final double RULE_WEIGHT = 0.35;
    private static final double ML_WEIGHT = 0.45;
    private static final double KNOWLEDGE_WEIGHT = 0.20;

    public static RiskAssessment aggregate(
            EnrichedTransaction tx, RuleResult ruleResult,
            MLResult mlResult, KBSearchResult kbResult) {

        double ruleScore = ruleResult.getScore();
        double mlScore = mlResult.getProbability() * 100;
        double kbScore = kbResult.getRelevanceScore();
        double finalScore = RULE_WEIGHT * ruleScore
                          + ML_WEIGHT * mlScore
                          + KNOWLEDGE_WEIGHT * kbScore;

        String riskLevel = finalScore >= 80 ? "HIGH" : (finalScore >= 50 ? "MEDIUM" : "LOW");
        String decision = ruleResult.hasSanctionMatch() ? "BLOCK"
            : (finalScore >= 80 && mlResult.getProbability() > 0.85) ? "BLOCK"
            : finalScore >= 50 ? "REVIEW" : "PASS";

        return RiskAssessment.builder()
                .transactionId(tx.getTransactionId())
                .riskScore(Math.round(finalScore * 100.0) / 100.0)
                .riskLevel(riskLevel)
                .decision(decision)
                .confidence(calculateConfidence(ruleResult, mlResult, kbResult))
                .ruleScore(ruleScore)
                .mlScore(mlScore)
                .kbScore(kbScore)
                .matchedRules(ruleResult.getMatchedRules())
                .similarCases(kbResult.getSimilarCases())
                .build();
    }
}
```

---

## 7. 数据架构设计

### 7.1 数据库表结构设计

#### 7.1.1 交易表 (transaction_record)

```sql
CREATE TABLE `transaction_record` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `transaction_id` VARCHAR(64) NOT NULL COMMENT '交易流水号',
  `account_id` VARCHAR(32) NOT NULL COMMENT '发起方账号(脱敏)',
  `account_id_hash` VARCHAR(64) NOT NULL COMMENT '账号SHA256哈希',
  `counterparty_account` VARCHAR(32) DEFAULT NULL COMMENT '对手账号(脱敏)',
  `counterparty_hash` VARCHAR(64) DEFAULT NULL COMMENT '对手账号哈希',
  `transaction_type` VARCHAR(16) NOT NULL COMMENT '交易类型: TRANSFER/PAYMENT/WITHDRAW',
  `channel_code` VARCHAR(16) NOT NULL COMMENT '渠道: EBANK/MBANK/COUNTER/ATM/POS',
  `amount` DECIMAL(18,2) NOT NULL COMMENT '交易金额(元)',
  `currency` VARCHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种ISO代码',
  `transaction_time` DATETIME NOT NULL COMMENT '交易时间',
  `ip_address` VARCHAR(45) DEFAULT NULL COMMENT 'IP地址',
  `ip_country` VARCHAR(64) DEFAULT NULL COMMENT 'IP归属国家',
  `ip_city` VARCHAR(64) DEFAULT NULL COMMENT 'IP归属城市',
  `device_id` VARCHAR(64) DEFAULT NULL COMMENT '设备指纹ID',
  `device_type` VARCHAR(32) DEFAULT NULL COMMENT '设备类型: MOBILE/PC/PAD',
  `is_emulator` TINYINT(1) DEFAULT '0' COMMENT '是否模拟器',
  `gps_lat` DECIMAL(10,7) DEFAULT NULL COMMENT 'GPS纬度',
  `gps_lng` DECIMAL(10,7) DEFAULT NULL COMMENT 'GPS经度',
  `merchant_id` VARCHAR(32) DEFAULT NULL COMMENT '商户ID',
  `remark` VARCHAR(256) DEFAULT NULL COMMENT '交易附言',
  `risk_score` DECIMAL(5,2) DEFAULT NULL COMMENT '风险评分',
  `risk_level` VARCHAR(8) DEFAULT NULL COMMENT '风险等级',
  `processing_status` VARCHAR(16) DEFAULT 'PENDING' COMMENT '处理状态',
  `data_source` VARCHAR(16) NOT NULL COMMENT '数据来源',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_transaction_id` (`transaction_id`),
  KEY `idx_account_id_hash` (`account_id_hash`),
  KEY `idx_transaction_time` (`transaction_time`),
  KEY `idx_channel_code` (`channel_code`),
  KEY `idx_risk_level` (`risk_level`),
  KEY `idx_device_id` (`device_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='交易记录表 - 按天分区';
```

#### 7.1.2 客户表 (customer_profile)

```sql
CREATE TABLE `customer_profile` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `customer_id` VARCHAR(32) NOT NULL COMMENT '客户编号',
  `customer_name_hash` VARCHAR(64) DEFAULT NULL COMMENT '姓名SHA256(脱敏)',
  `customer_type` VARCHAR(8) NOT NULL COMMENT 'PERSONAL/CORPORATE',
  `id_type` VARCHAR(8) DEFAULT NULL COMMENT '证件类型: ID/PASSPORT',
  `id_number_hash` VARCHAR(64) DEFAULT NULL COMMENT '证件号SHA256',
  `phone_hash` VARCHAR(64) DEFAULT NULL COMMENT '手机号SHA256',
  `occupation` VARCHAR(64) DEFAULT NULL COMMENT '职业',
  `industry_code` VARCHAR(8) DEFAULT NULL COMMENT '行业代码',
  `registered_province` VARCHAR(32) DEFAULT NULL COMMENT '注册省份',
  `registered_city` VARCHAR(32) DEFAULT NULL COMMENT '注册城市',
  `risk_level` VARCHAR(8) NOT NULL DEFAULT 'LOW' COMMENT '风险等级',
  `risk_score` DECIMAL(5,2) DEFAULT '0.00' COMMENT '基础风险评分',
  `customer_status` VARCHAR(8) NOT NULL DEFAULT 'ACTIVE' COMMENT '状态',
  `open_date` DATE NOT NULL COMMENT '开户日期',
  `last_active_time` DATETIME DEFAULT NULL COMMENT '最后活跃时间',
  `total_transaction_count` INT DEFAULT '0' COMMENT '累计交易笔数',
  `total_transaction_amount` DECIMAL(18,2) DEFAULT '0.00' COMMENT '累计交易金额',
  `avg_daily_transaction_count` DECIMAL(10,2) DEFAULT '0.00' COMMENT '日均交易笔数',
  `avg_daily_amount` DECIMAL(18,2) DEFAULT '0.00' COMMENT '日均交易金额',
  `device_count_30d` INT DEFAULT '0' COMMENT '30天使用设备数',
  `city_count_30d` INT DEFAULT '0' COMMENT '30天涉及城市数',
  `is_high_risk_occupation` TINYINT(1) DEFAULT '0' COMMENT '是否高危职业',
  `is_politically_exposed` TINYINT(1) DEFAULT '0' COMMENT '是否政治人物',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_customer_id` (`customer_id`),
  KEY `idx_risk_level` (`risk_level`),
  KEY `idx_open_date` (`open_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='客户风险画像表';
```

#### 7.1.3 预警表 (alert_record)

```sql
CREATE TABLE `alert_record` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `alert_id` VARCHAR(64) NOT NULL COMMENT '预警编号',
  `transaction_id` VARCHAR(64) NOT NULL COMMENT '关联交易编号',
  `customer_id` VARCHAR(32) NOT NULL COMMENT '关联客户编号',
  `alert_type` VARCHAR(16) NOT NULL COMMENT '预警类型: FRAUD/AML/CTF/SANCTION',
  `risk_score` DECIMAL(5,2) NOT NULL COMMENT '风险评分',
  `risk_level` VARCHAR(8) NOT NULL COMMENT 'LOW/MEDIUM/HIGH',
  `matched_rules_json` JSON DEFAULT NULL COMMENT '命中规则明细',
  `ml_score` DECIMAL(5,4) DEFAULT NULL COMMENT '模型评分',
  `agent_decision` VARCHAR(16) DEFAULT NULL COMMENT '智能体建议: PASS/REVIEW/BLOCK',
  `alert_status` VARCHAR(16) NOT NULL DEFAULT 'NEW' COMMENT '预警状态',
  `assignee` VARCHAR(32) DEFAULT NULL COMMENT '处理人ID',
  `escalation_level` TINYINT DEFAULT '0' COMMENT '升级等级: 0/1/2/3',
  `process_result` VARCHAR(256) DEFAULT NULL COMMENT '处理结论',
  `processing_deadline` DATETIME NOT NULL COMMENT '处理截止时间',
  `processed_at` DATETIME DEFAULT NULL COMMENT '处理时间',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_alert_id` (`alert_id`),
  KEY `idx_transaction_id` (`transaction_id`),
  KEY `idx_customer_id` (`customer_id`),
  KEY `idx_alert_status` (`alert_status`),
  KEY `idx_risk_level` (`risk_level`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_deadline` (`processing_deadline`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='预警记录表';
```

#### 7.1.4 黑名单表 (blacklist)

```sql
CREATE TABLE `blacklist` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `list_type` VARCHAR(16) NOT NULL COMMENT '名单类型: SANCTION/INTERNAL/HIGH_RISK',
  `match_type` VARCHAR(8) NOT NULL COMMENT '匹配方式: EXACT/FUZZY',
  `entity_type` VARCHAR(16) NOT NULL COMMENT '实体类型: PERSON/ORGANIZATION/COUNTRY',
  `entity_name` VARCHAR(256) NOT NULL COMMENT '实体名称',
  `entity_name_pinyin` VARCHAR(256) DEFAULT NULL COMMENT '拼音(模糊匹配用)',
  `id_number` VARCHAR(64) DEFAULT NULL COMMENT '证件号',
  `account_number` VARCHAR(32) DEFAULT NULL COMMENT '账号',
  `country_code` VARCHAR(8) DEFAULT NULL COMMENT '国家代码',
  `risk_level` VARCHAR(8) NOT NULL DEFAULT 'HIGH' COMMENT '名单风险等级',
  `source` VARCHAR(64) DEFAULT NULL COMMENT '来源: UN/EU/PBC/INTERNAL',
  `reason` VARCHAR(512) DEFAULT NULL COMMENT '列入原因',
  `effective_date` DATE NOT NULL COMMENT '生效日期',
  `expiry_date` DATE DEFAULT NULL COMMENT '失效日期(NULL=永久)',
  `is_active` TINYINT(1) NOT NULL DEFAULT '1' COMMENT '是否生效',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_list_type` (`list_type`),
  KEY `idx_entity_name` (`entity_name`(64)),
  KEY `idx_active_expiry` (`is_active`, `expiry_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='黑名单表';
```

#### 7.1.5 案例表 (fraud_case)

```sql
CREATE TABLE `fraud_case` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `case_id` VARCHAR(32) NOT NULL COMMENT '案例编号',
  `case_title` VARCHAR(256) NOT NULL COMMENT '案例标题',
  `case_type` VARCHAR(16) NOT NULL COMMENT '案例类型: FRAUD/AML/CTF',
  `case_level` VARCHAR(8) NOT NULL COMMENT '案例等级: HIGH/MEDIUM/LOW',
  `case_description` TEXT NOT NULL COMMENT '案例描述',
  `modus_operandi` TEXT DEFAULT NULL COMMENT '作案手法详细描述',
  `fraud_pattern` VARCHAR(64) DEFAULT NULL COMMENT '欺诈模式标签',
  `detection_rules` JSON DEFAULT NULL COMMENT '检测规则清单',
  `key_features` JSON DEFAULT NULL COMMENT '关键特征向量',
  `involved_amount` DECIMAL(18,2) DEFAULT NULL COMMENT '涉案金额',
  `victim_count` INT DEFAULT NULL COMMENT '受害人数',
  `resolution` TEXT DEFAULT NULL COMMENT '处置方案',
  `resolution_effect` VARCHAR(512) DEFAULT NULL COMMENT '处置效果',
  `review_notes` TEXT DEFAULT NULL COMMENT '复盘记录',
  `is_template` TINYINT(1) DEFAULT '0' COMMENT '是否模板案例',
  `created_by` VARCHAR(32) DEFAULT NULL COMMENT '创建人',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_case_id` (`case_id`),
  KEY `idx_case_type` (`case_type`),
  KEY `idx_case_level` (`case_level`),
  KEY `idx_fraud_pattern` (`fraud_pattern`),
  FULLTEXT KEY `ft_search` (`case_title`, `case_description`, `modus_operandi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='欺诈案例库表';
```

#### 7.1.6 嫌疑人报告表 (suspicious_report)

```sql
CREATE TABLE `suspicious_report` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `report_id` VARCHAR(64) NOT NULL COMMENT '报告编号',
  `case_id` VARCHAR(32) NOT NULL COMMENT '关联案例编号',
  `report_type` VARCHAR(16) NOT NULL COMMENT '报告类型: STR/CTR',
  `report_status` VARCHAR(16) NOT NULL DEFAULT 'DRAFT' COMMENT '状态: DRAFT/REVIEW/SUBMITTED',
  `report_content` JSON NOT NULL COMMENT '报告内容(结构化)',
  `report_file_url` VARCHAR(512) DEFAULT NULL COMMENT '附件存储路径',
  `submitter` VARCHAR(32) DEFAULT NULL COMMENT '提交人',
  `submit_time` DATETIME DEFAULT NULL COMMENT '提交时间',
  `regulator_response` VARCHAR(512) DEFAULT NULL COMMENT '监管回复',
  `is_supplemented` TINYINT(1) DEFAULT '0' COMMENT '是否已补充',
  `supplement_note` TEXT DEFAULT NULL COMMENT '补充说明',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_report_id` (`report_id`),
  KEY `idx_case_id` (`case_id`),
  KEY `idx_report_status` (`report_status`),
  KEY `idx_submit_time` (`submit_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='可疑交易报告表';
```

### 7.2 数据存储设计

| 存储类型 | 使用场景 | 技术选型 | 存储策略 |
|----------|----------|----------|----------|
| 关系型存储 | 核心业务数据 | MySQL 8.0/PolarDB | 主从架构 |
| 交易明细 | 高频查询 | ClickHouse | 分区表，按月分区 |
| 特征存储 | 在线特征服务 | Redis + Feature Store | 全量特征 |
| 特征仓库 | 离线特征计算 | Hive + Spark | 分层存储 |
| 图存储 | 关系图谱 | Neo4j/JanusGraph | 关系查询优化 |
| 向量存储 | 知识向量化 | FAISS/Milvus | 相似度检索 |
| 搜索引擎 | 日志、分析 | Elasticsearch | 按日期索引 |
| 消息队列 | 异步处理 | Kafka | 多Partition |

---

## 8. 接口设计

### 8.1 核心接口定义

```yaml
# POST /api/v1/agent/assess - 智能体评估
Request:
  body:
    transaction_id: string
    account_id: string
    amount: number
    currency: string
    transaction_type: string
    channel: string

Response:
  {
    "transaction_id": "string",
    "risk_score": 56,
    "risk_level": "MEDIUM",
    "decision": "REVIEW",
    "confidence": 0.85,
    "matched_rules": [...],
    "similar_cases": [...],
    "recommendations": [...],
    "processing_time_ms": 250
  }

# POST /api/v1/knowledge/search - 知识检索
Request:
  body:
    query: string
    category: string
    top_k: int
```

### 8.2 完整API接口定义 (OpenAPI 3.0)

```yaml
openapi: "3.0.3"
info:
  title: 反欺诈与反恐融资风险识别API
  version: "2.0.0"
  description: 银行风控智能体系统接口规范

servers:
  - url: https://api.bank.com/risk/v2
    description: 生产环境
  - url: https://uat-api.bank.com/risk/v2
    description: UAT环境

paths:
  /agent/assess:
    post:
      summary: 智能体交易风险评估
      operationId: assessTransaction
      tags: [智能体]
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [transaction_id, account_id, amount, transaction_type, channel]
              properties:
                transaction_id:
                  type: string
                  pattern: '^\w{16,64}$'
                  example: "TX2026050412345678"
                account_id:
                  type: string
                  example: "622202******7890"
                amount:
                  type: number
                  format: double
                  minimum: 0.01
                  example: 50000.00
                currency:
                  type: string
                  default: "CNY"
                  example: "CNY"
                transaction_type:
                  type: string
                  enum: [TRANSFER, PAYMENT, WITHDRAW, DEPOSIT, EXCHANGE]
                channel:
                  type: string
                  enum: [EBANK, MBANK, COUNTER, ATM, POS, THIRD_PARTY]
                counterparty_account:
                  type: string
                  example: "622203******6543"
                ip_address:
                  type: string
                  format: ipv4
                  example: "114.114.114.114"
                device_id:
                  type: string
                  example: "DEV-FP-ABC123"
                device_type:
                  type: string
                  enum: [MOBILE, PC, PAD]
                device_os:
                  type: string
                  example: "Android 14"
                gps_lat:
                  type: number
                  format: float
                gps_lng:
                  type: number
                  format: float
                merchant_id:
                  type: string
                remark:
                  type: string
                  maxLength: 256
      responses:
        '200':
          description: 评估成功
          content:
            application/json:
              schema:
                type: object
                properties:
                  code:
                    type: integer
                    example: 0
                  message:
                    type: string
                    example: "success"
                  data:
                    $ref: '#/components/schemas/AssessmentResult'
        '400':
          description: 请求参数错误
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '429':
          description: 请求频率超限
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '503':
          description: 服务暂不可用
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /agent/query:
    post:
      summary: 智能体知识问答
      operationId: queryKnowledge
      tags: [智能体]
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [question]
              properties:
                question:
                  type: string
                  maxLength: 2000
                  example: "请分析当前交易的风险特征，并推荐相似历史案例"
                transaction_context:
                  type: string
                  description: "可选，附加上下文信息"
      responses:
        '200':
          description: 查询成功
          content:
            application/json:
              schema:
                type: object
                properties:
                  code:
                    type: integer
                  data:
                    type: object
                    properties:
                      answer:
                        type: string
                      related_cases:
                        type: array
                        items:
                          $ref: '#/components/schemas/CaseSummary'
                      related_rules:
                        type: array
                        items:
                          $ref: '#/components/schemas/RuleSummary'

  /knowledge/search:
    post:
      summary: 知识库全文检索
      operationId: searchKnowledge
      tags: [知识库]
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [query]
              properties:
                query:
                  type: string
                  example: "跨境洗钱作案手法"
                category:
                  type: string
                  enum: [rules, cases, blacklist, all]
                  default: "all"
                top_k:
                  type: integer
                  default: 10
                  maximum: 50
                vector_search:
                  type: boolean
                  default: true
                  description: "是否使用向量检索"
      responses:
        '200':
          description: 检索成功

  /transaction/assess:
    post:
      summary: 交易风险评分(不含智能体)
      operationId: assessTransactionBasic
      tags: [交易监控]
      requestBody:
        $ref: '#/components/requestBodies/TransactionRequest'
      responses:
        '200':
          description: 评分成功
          content:
            application/json:
              schema:
                type: object
                properties:
                  risk_score:
                    type: number
                    example: 56.5
                  risk_level:
                    type: string
                    enum: [LOW, MEDIUM, HIGH]
                  top_factors:
                    type: array
                    items:
                      type: object
                      properties:
                        factor_name:
                          type: string
                        contribution:
                          type: number

  /alert/list:
    get:
      summary: 获取预警列表
      operationId: listAlerts
      tags: [预警管理]
      parameters:
        - name: status
          in: query
          schema:
            type: string
            enum: [NEW, PENDING, PROCESSING, RESOLVED, CLOSED]
        - name: risk_level
          in: query
          schema:
            type: string
            enum: [LOW, MEDIUM, HIGH]
        - name: page
          in: query
          schema:
            type: integer
            default: 1
        - name: size
          in: query
          schema:
            type: integer
            default: 20
            maximum: 100
      responses:
        '200':
          description: 查询成功

  /alert/{alert_id}/process:
    post:
      summary: 处理预警
      operationId: processAlert
      tags: [预警管理]
      parameters:
        - name: alert_id
          in: path
          required: true
          schema:
            type: string
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required: [action]
              properties:
                action:
                  type: string
                  enum: [ASSIGN, START_PROCESS, RESOLVE, CLOSE, ESCALATE]
                assignee:
                  type: string
                comment:
                  type: string
                resolution:
                  type: string
      responses:
        '200':
          description: 操作成功

components:
  schemas:
    AssessmentResult:
      type: object
      properties:
        transaction_id:
          type: string
        risk_score:
          type: number
          example: 56.5
        risk_level:
          type: string
          enum: [LOW, MEDIUM, HIGH]
        decision:
          type: string
          enum: [PASS, REVIEW, BLOCK]
        confidence:
          type: number
          format: float
          example: 0.85
        matched_rules:
          type: array
          items:
            $ref: '#/components/schemas/MatchedRule'
        similar_cases:
          type: array
          items:
            $ref: '#/components/schemas/CaseSummary'
        risk_factors:
          type: array
          items:
            type: string
        processing_time_ms:
          type: integer
          example: 250
    MatchedRule:
      type: object
      properties:
        rule_id:
          type: string
          example: "R001"
        rule_name:
          type: string
        risk_weight:
          type: integer
        risk_score:
          type: number
    CaseSummary:
      type: object
      properties:
        case_id:
          type: string
        case_title:
          type: string
        similarity:
          type: number
    RuleSummary:
      type: object
      properties:
        rule_id:
          type: string
        rule_name:
          type: string
        description:
          type: string
    ErrorResponse:
      type: object
      properties:
        code:
          type: integer
        message:
          type: string
        request_id:
          type: string

  requestBodies:
    TransactionRequest:
      content:
        application/json:
          schema:
            type: object
            required: [transaction_id, account_id, amount]
            properties:
              transaction_id:
                type: string
              account_id:
                type: string
              amount:
                type: number
              currency:
                type: string
              transaction_type:
                type: string
              channel:
                type: string
```

### 8.3 Kafka Topic设计

| Topic名称 | 用途 |
|-----------|------|
| transaction-raw | 原始交易数据 |
| transaction-assessed | 已评估交易 |
| alert-created | 新建预警事件 |
| knowledge-updated | 知识库更新事件 |

---

## 9. 安全架构设计

### 9.1 安全架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              安全架构                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  网络安全层: 防火墙 | WAF | DDoS防护 | VPN                                  │
│  应用安全层: 身份认证(MFA) | 访问控制(RBAC) | 安全审计 | 漏洞扫描              │
│  数据安全层: 加密存储(AES256) | 脱敏处理 | 数据备份 | 数据水印                │
│  知识库安全层: 访问控制 | 脱敏展示 | 审计日志 | 权限隔离                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. 部署架构设计

### 10.1 部署架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              生产环境部署架构                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                        │
│  │ Web集群     │  │ API集群     │  │ Kafka集群   │                        │
│  │ (3节点)     │  │ (5节点)     │  │ (3节点)     │                        │
│  └─────────────┘  └─────────────┘  └─────────────┘                        │
│                                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                        │
│  │ MySQL集群   │  │ Redis集群   │  │ ES集群      │                        │
│  │ (1主2从)   │  │ (3节点)    │  │ (3节点)    │                        │
│  └─────────────┘  └─────────────┘  └─────────────┘                        │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Kubernetes集群 (K8S)                        │   │
│  │  Flink计算节点 | Spark计算节点 | 微服务Pod | ML推理服务 | 智能体服务    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         向量库集群                                   │   │
│  │  FAISS单机 | Milvus集群 | ES向量插件                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 10.3 Kubernetes部署配置

#### 10.3.1 智能体微服务Deployment

```yaml
# ai-agent-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ai-agent-service
  namespace: risk-system
  labels:
    app: ai-agent
    tier: core
spec:
  replicas: 2
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: ai-agent
  template:
    metadata:
      labels:
        app: ai-agent
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/actuator/prometheus"
    spec:
      containers:
      - name: ai-agent
        image: registry.bank.com/risk/ai-agent:2.0.0
        imagePullPolicy: Always
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 8081
          name: grpc
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "prod"
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-cluster.kafka:9092"
        - name: MILVUS_HOST
          value: "milvus-service.milvus"
        - name: MILVUS_PORT
          value: "19530"
        - name: MYSQL_HOST
          value: "mysql-master.mysql"
        - name: MYSQL_DB
          value: "risk_db"
        - name: REDIS_HOST
          value: "redis-cluster.redis"
        - name: MODEL_SERVICE_URL
          value: "http://ml-serving-service.ml:8501"
        - name: JAVA_OPTS
          value: "-Xms4g -Xmx8g -XX:+UseG1GC"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
        livenessProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /app/config
        - name: knowledge-local
          mountPath: /app/knowledge
      volumes:
      - name: config
        configMap:
          name: ai-agent-config
      - name: knowledge-local
        persistentVolumeClaim:
          claimName: knowledge-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: ai-agent-service
  namespace: risk-system
spec:
  selector:
    app: ai-agent
  ports:
  - name: http
    port: 8080
    targetPort: 8080
  - name: grpc
    port: 8081
    targetPort: 8081
  type: ClusterIP
```

#### 10.3.2 Flink作业部署

```yaml
# flink-job-deployment.yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: fraud-detection-job
  namespace: risk-system
spec:
  image: registry.bank.com/risk/flink-fraud-job:2.0.0
  flinkVersion: v1_16
  serviceAccount: flink
  podTemplate:
    spec:
      containers:
      - name: flink-main-container
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-cluster.kafka:9092"
        - name: MYSQL_JDBC_URL
          value: "jdbc:mysql://mysql-master.mysql:3306/risk_db"
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    parallelism.default: "4"
    jobmanager.memory.process.size: "4096m"
    taskmanager.memory.process.size: "8192m"
    state.backend: "rocksdb"
    state.checkpoints.dir: "hdfs://hdfs-namenode:9000/flink-checkpoints"
    state.savepoints.dir: "hdfs://hdfs-namenode:9000/flink-savepoints"
    restart-strategy: "fixed-delay"
    restart-strategy.fixed-delay.attempts: "5"
    restart-strategy.fixed-delay.delay: "30s"
```

#### 10.3.3 ML模型推理服务部署

```yaml
# ml-serving-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ml-serving
  namespace: risk-system
spec:
  replicas: 2
  selector:
    matchLabels:
      app: ml-serving
  template:
    metadata:
      labels:
        app: ml-serving
    spec:
      containers:
      - name: tf-serving
        image: tensorflow/serving:2.12.0-gpu
        ports:
        - containerPort: 8501
          name: http
        - containerPort: 8500
          name: grpc
        env:
        - name: MODEL_NAME
          value: "fraud_detection"
        - name: NVIDIA_VISIBLE_DEVICES
          value: "all"
        volumeMounts:
        - name: model-storage
          mountPath: /models/fraud_detection
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
            nvidia.com/gpu: 1
          limits:
            memory: "8Gi"
            cpu: "4"
            nvidia.com/gpu: 1
        livenessProbe:
          httpGet:
            path: /v1/models/fraud_detection
            port: 8501
          initialDelaySeconds: 30
      volumes:
      - name: model-storage
        persistentVolumeClaim:
          claimName: ml-models-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: ml-serving-service
  namespace: risk-system
spec:
  selector:
    app: ml-serving
  ports:
  - name: http
    port: 8501
    targetPort: 8501
  - name: grpc
    port: 8500
    targetPort: 8500
  type: ClusterIP
```

#### 10.3.4 Ingress路由配置

```yaml
# risk-ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: risk-api-ingress
  namespace: risk-system
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/proxy-body-size: "10m"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "30"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  tls:
  - hosts:
    - api.bank.com
    secretName: bank-api-tls
  rules:
  - host: api.bank.com
    http:
      paths:
      - path: /risk/v2/agent
        pathType: Prefix
        backend:
          service:
            name: ai-agent-service
            port:
              number: 8080
      - path: /risk/v2/transaction
        pathType: Prefix
        backend:
          service:
            name: risk-assessment-service
            port:
              number: 8080
      - path: /risk/v2/alert
        pathType: Prefix
        backend:
          service:
            name: alert-management-service
            port:
              number: 8080
      - path: /risk/v2/knowledge
        pathType: Prefix
        backend:
          service:
            name: knowledge-base-service
            port:
              number: 8080
```

| 环境 | 用途 | 配置 |
|------|------|------|
| 开发环境 | 开发调试 | 2节点K8S，8C16G×2 |
| 测试环境 | 功能/集成测试 | 3节点K8S，8C16G×3 |
| UAT环境 | 用户验收测试 | 同生产配置50% |
| 生产环境 | 正式运营 | 最小5节点K8S，高可用 |
| 灾备环境 | 灾难恢复 | 同生产配置 |

---

## 11. 高可用与容灾设计

### 11.1 故障切换机制

| 故障场景 | 检测方式 | 切换策略 | RTO目标 |
|----------|----------|----------|---------|
| 服务实例故障 | 健康检查 | 自动重启/重新调度 | <30秒 |
| 可用区故障 | 区域探测 | 自动切换到其他可用区 | <5分钟 |
| 数据库故障 | 主从心跳 | 自动主从切换 | <2分钟 |
| 向量库故障 | 健康检查 | 切换到备库 | <1分钟 |

### 11.2 数据备份策略

| 数据类型 | 备份频率 | 保留期限 |
|----------|----------|----------|
| 核心业务数据 | 实时增量+每日全量 | 7年 |
| 交易数据 | 每小时增量+每日全量 | 10年 |
| 知识库数据 | 变更时备份 | 永久 |
| 向量索引 | 日备份 | 30天 |

---

## 12. 监控运维设计

### 12.1 监控体系

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              全栈监控体系                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  基础设施监控: CPU/内存 | 磁盘 | 网络 | 容器状态 | K8S集群                   │
│  应用监控(APM): 请求量 | 响应时间 | 错误率 | 链路追踪 | 日志分析               │
│  业务监控: 预警数量 | 处理时效 | 模型效果 | 知识检索 | 智能体决策准确率         │
│  智能体专项监控: RAG检索延迟 | 向量检索延迟 | 模型推理延迟 | 决策分布           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 12.1.1 Prometheus监控规则配置

```yaml
# prometheus-rules.yaml
groups:
- name: risk-system-alerts
  interval: 30s
  rules:
  # 服务可用性告警
  - alert: ServiceDown
    expr: up{job=~"ai-agent|risk-assessment|flink-job"} == 0
    for: 30s
    labels:
      severity: critical
    annotations:
      summary: "{{ $labels.job }} 服务不可用"
      description: "{{ $labels.instance }} 已离线超过30秒"

  # 交易处理延迟告警
  - alert: HighTransactionLatency
    expr: histogram_quantile(0.99, rate(http_request_duration_seconds_bucket{handler="assessTransaction"}[5m])) > 0.5
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "交易评估P99延迟超过500ms"
      description: "当前P99延迟: {{ $value }}s"

  # 预警堆积告警
  - alert: AlertBacklog
    expr: sum(alert_status{status="NEW"}) > 100
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "未处理预警堆积"
      description: "当前未处理预警数: {{ $value }}"

  # 高拒绝率告警
  - alert: HighBlockRate
    expr: rate(transaction_decision_total{decision="BLOCK"}[5m]) / rate(transaction_decision_total[5m]) > 0.15
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "交易阻断率超过15%"
      description: "当前阻断率: {{ $value | humanizePercentage }}"

  # 知识库检索延迟告警
  - alert: KnowledgeSearchSlow
    expr: histogram_quantile(0.99, rate(knowledge_search_duration_seconds_bucket[5m])) > 2.0
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "知识库检索P99延迟超过2秒"

  # ML模型漂移检测
  - alert: ModelDriftDetected
    expr: abs(model_accuracy - model_accuracy_avg_7d) > 0.05
    for: 10m
    labels:
      severity: critical
    annotations:
      summary: "模型准确率漂移超过5%"
      description: "当前准确率: {{ $value }}"

  # Kafka消费者延迟
  - alert: KafkaConsumerLag
    expr: kafka_consumer_lag{group="fraud-detection-group"} > 10000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Kafka消费延迟超过10000条"
      description: "当前延迟: {{ $value }}条"

  # 规则引擎错误率
  - alert: RuleEngineErrors
    expr: rate(rule_engine_errors_total[5m]) > 0.01
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "规则引擎错误率超过1%"
```

#### 12.1.2 Grafana核心监控面板设计

```json
{
  "dashboard": {
    "title": "风控系统核心监控",
    "panels": [
      {
        "title": "交易处理总览",
        "type": "stat",
        "targets": [
          {"expr": "rate(transaction_total[5m])", "legendFormat": "TPS"},
          {"expr": "sum(transaction_decision_total)", "legendFormat": "总交易"}
        ]
      },
      {
        "title": "风险等级分布",
        "type": "piechart",
        "targets": [
          {"expr": "sum(transaction_risk_level{level=\"HIGH\"})", "legendFormat": "高风险"},
          {"expr": "sum(transaction_risk_level{level=\"MEDIUM\"})", "legendFormat": "中风险"},
          {"expr": "sum(transaction_risk_level{level=\"LOW\"})", "legendFormat": "低风险"}
        ]
      },
      {
        "title": "P99响应延迟",
        "type": "graph",
        "targets": [
          {"expr": "histogram_quantile(0.99, rate(http_duration_bucket[5m]))", "legendFormat": "P99"}
        ]
      },
      {
        "title": "预警处理时效",
        "type": "graph",
        "targets": [
          {"expr": "avg(alert_processing_time_hours)", "legendFormat": "平均处理时长(h)"}
        ]
      },
      {
        "title": "智能体决策分布",
        "type": "bargauge",
        "targets": [
          {"expr": "sum(agent_decision_total{decision=\"PASS\"})", "legendFormat": "放行"},
          {"expr": "sum(agent_decision_total{decision=\"REVIEW\"})", "legendFormat": "审核"},
          {"expr": "sum(agent_decision_total{decision=\"BLOCK\"})", "legendFormat": "阻断"}
        ]
      },
      {
        "title": "规则命中TOP10",
        "type": "table",
        "targets": [{"expr": "topk(10, rule_hit_count)", "legendFormat": "规则"}]
      }
    ]
  }
}
```

### 12.2 告警策略

| 告警级别 | 触发条件 | 通知方式 | 响应要求 |
|----------|----------|----------|----------|
| P1-紧急 | 系统不可用 | 电话+短信+邮件 | 15分钟内响应 |
| P2-严重 | 性能严重下降 | 短信+邮件 | 30分钟内响应 |
| P3-警告 | 指标异常波动 | 邮件 | 2小时内处理 |
| P4-提示 | 容量预警等 | 企业微信 | 工作时间处理 |

---

## 附录

### 附录A：技术栈汇总

| 类别 | 技术 | 版本 | 许可证 |
|------|------|------|--------|
| 后端框架 | Spring Cloud Alibaba | 2021.0.5.0 | Apache 2.0 |
| 实时计算 | Apache Flink | 1.16+ | Apache 2.0 |
| 离线计算 | Apache Spark | 3.3+ | Apache 2.0 |
| 消息队列 | Apache Kafka | 3.4.0 | Apache 2.0 |
| 数据库 | MySQL | 8.0 | GPL |
| OLAP | ClickHouse | 22.x | Apache 2.0 |
| 搜索引擎 | Elasticsearch | 7.17 | Elastic |
| 向量库 | FAISS/Milvus | 1.7/2.x | Apache 2.0 |
| 规则引擎 | Drools | 7.70.0 | Apache 2.0 |
| 机器学习 | TensorFlow/PyTorch | 2.x | Apache 2.0 |
| AI智能体 | AI智能体框架 | 最新版 | 商业授权 |

### 附录B：部署清单

| 序号 | 组件名称 | 实例数 | CPU | 内存 | 磁盘 |
|------|----------|--------|-----|------|------|
| 1 | K8S Master | 3 | 8C | 16G | 100G |
| 2 | K8S Worker | 10 | 16C | 32G | 200G |
| 3 | Flink | 6 | 8C | 16G | 100G |
| 4 | Spark | 4 | 8C | 16G | 100G |
| 5 | MySQL | 3 | 16C | 64G | 500G |
| 6 | Redis | 3 | 8C | 16G | 50G |
| 7 | ClickHouse | 3 | 16C | 32G | 1T |
| 8 | Kafka | 3 | 8C | 16G | 1T |
| 9 | Elasticsearch | 3 | 16C | 32G | 1T |
| 10 | FAISS/Milvus | 2 | 8C | 16G | 100G |
| 11 | 微服务 | N | 4C | 8G | 50G |
| 12 | ML Serving | 4 | 8C | 16G | 100G |
| 13 | 智能体服务 | 2 | 4C | 8G | 50G |

### 附录C：ML模型训练Pipeline

#### C.1 欺诈检测模型训练代码

```python
# fraud_detection_model.py
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import roc_auc_score, precision_recall_curve
import xgboost as xgb
import joblib
import mlflow
import mlflow.xgboost
from datetime import datetime


class FraudDetectionPipeline:
    """欺诈检测模型训练pipeline"""

    def __init__(self, config: dict = None):
        self.config = config or {
            'features': [
                'amount', 'amount_1h_sum', 'count_1h',
                'device_count_30d', 'city_count_30d',
                'is_night', 'is_cross_province', 'is_new_device',
                'amount_deviation', 'hour_of_day', 'day_of_week',
                'account_age_days', 'transfer_out_amount_today',
                'avg_interval_1h'
            ],
            'categorical_features': ['channel_code', 'transaction_type', 'currency'],
            'test_size': 0.2,
            'random_state': 42,
            'model_params': {
                'n_estimators': 500, 'max_depth': 8,
                'learning_rate': 0.05, 'subsample': 0.8,
                'colsample_bytree': 0.7, 'scale_pos_weight': 30,
                'eval_metric': 'auc', 'early_stopping_rounds': 50
            }
        }
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.model = None

    def load_and_preprocess(self, data_path: str) -> pd.DataFrame:
        """加载和预处理数据"""
        df = pd.read_parquet(data_path) if data_path.endswith('.parquet') \
            else pd.read_csv(data_path)

        df['hour_of_day'] = pd.to_datetime(df['transaction_time']).dt.hour
        df['day_of_week'] = pd.to_datetime(df['transaction_time']).dt.dayofweek
        df['is_night'] = ((df['hour_of_day'] >= 0) & (df['hour_of_day'] < 6)).astype(int)
        df['account_age_days'] = (
            datetime.now() - pd.to_datetime(df['open_date'])
        ).dt.days

        df = df.fillna({
            'device_count_30d': 0, 'city_count_30d': 0,
            'amount_deviation': 0, 'avg_interval_1h': 999999
        })

        for col in ['amount', 'amount_1h_sum']:
            q99 = df[col].quantile(0.99)
            df[col] = df[col].clip(upper=q99)

        return df

    def prepare_features(self, df: pd.DataFrame, is_train: bool = True) -> np.ndarray:
        """准备模型输入特征"""
        feature_df = df[self.config['features']].copy()
        feature_df['amount_per_device'] = df['amount'] / (df['device_count_30d'] + 1)
        feature_df['is_high_risk_time'] = ((df['hour_of_day'] >= 23) | (df['hour_of_day'] < 6)).astype(int)

        for cat_col in self.config['categorical_features']:
            if cat_col in df.columns:
                if is_train:
                    le = LabelEncoder()
                    feature_df[cat_col] = le.fit_transform(df[cat_col].astype(str))
                    self.label_encoders[cat_col] = le
                else:
                    feature_df[cat_col] = df[cat_col].map(
                        lambda x: self.label_encoders[cat_col].transform([str(x)])[0]
                        if str(x) in self.label_encoders[cat_col].classes_ else -1)

        numeric_cols = feature_df.select_dtypes(include=[np.number]).columns.tolist()
        if is_train:
            feature_df[numeric_cols] = self.scaler.fit_transform(feature_df[numeric_cols])
        else:
            feature_df[numeric_cols] = self.scaler.transform(feature_df[numeric_cols])

        return feature_df.values

    def train(self, data_path: str):
        """训练模型"""
        mlflow.set_experiment("fraud_detection")
        with mlflow.start_run(run_name=f"train_{datetime.now().strftime('%Y%m%d_%H%M')}"):
            df = self.load_and_preprocess(data_path)
            X = self.prepare_features(df, is_train=True)
            y = df['is_fraud'].values

            X_train, X_val, y_train, y_val = train_test_split(
                X, y, test_size=self.config['test_size'],
                random_state=self.config['random_state'], stratify=y)

            dtrain = xgb.DMatrix(X_train, label=y_train)
            dval = xgb.DMatrix(X_val, label=y_val)

            self.model = xgb.train(
                self.config['model_params'], dtrain,
                evals=[(dtrain, 'train'), (dval, 'val')], verbose_eval=50)

            y_pred = self.model.predict(dval)
            auc = roc_auc_score(y_val, y_pred)
            precision, recall, thresholds = precision_recall_curve(y_val, y_pred)

            mlflow.log_metrics({'auc': auc})
            mlflow.log_params(self.config['model_params'])
            mlflow.xgboost.log_model(self.model, "model")

            model_path = f"./models/fraud_detection_{datetime.now().strftime('%Y%m%d')}.model"
            self.model.save_model(model_path)
            joblib.dump(self.scaler, model_path.replace('.model', '_scaler.pkl'))
            joblib.dump(self.label_encoders, model_path.replace('.model', '_encoders.pkl'))

            print(f"Model saved to {model_path}, AUC: {auc:.4f}")
            return model_path

    def predict(self, transaction_features: dict, model_path: str) -> dict:
        """在线预测"""
        if self.model is None:
            self.model = xgb.Booster()
            self.model.load_model(model_path)
            self.scaler = joblib.load(model_path.replace('.model', '_scaler.pkl'))
            self.label_encoders = joblib.load(model_path.replace('.model', '_encoders.pkl'))

        df = pd.DataFrame([transaction_features])
        X = self.prepare_features(df, is_train=False)
        dmatrix = xgb.DMatrix(X)
        probability = float(self.model.predict(dmatrix)[0])
        return {'probability': probability, 'prediction': 1 if probability >= 0.5 else 0}
```

#### C.2 模型训练CI/CD流水线

```yaml
# .gitlab-ci.yml
stages:
  - test
  - train
  - evaluate
  - deploy

model-training:
  stage: train
  image: python:3.9-slim
  script:
    - pip install -r requirements-ml.txt
    - python train_pipeline.py --data-path ./data/training/train_20260501.parquet
  artifacts:
    paths:
      - models/*.model
      - models/*.pkl
    expire_in: 7 days

model-evaluation:
  stage: evaluate
  script:
    - python evaluate_model.py --model-path ./models/fraud_detection_*.model --test-data ./data/test/test_dataset.parquet

model-deploy:
  stage: deploy
  script:
    - python package_model.py --model-path ./models/fraud_detection_*.model --output-dir ./model_package
    - docker build -t ${DOCKER_REGISTRY}/risk/fraud-model:${CI_COMMIT_SHORT_SHA} -f Dockerfile.model .
    - docker push ${DOCKER_REGISTRY}/risk/fraud-model:${CI_COMMIT_SHORT_SHA}
    - kubectl set image deployment/ml-serving ml-serving=${DOCKER_REGISTRY}/risk/fraud-model:${CI_COMMIT_SHORT_SHA} -n risk-system
  environment:
    name: production
  only:
    - tags
```

---

**文档结束**

---

*本文档版权归某银行所有，未经授权不得外传*
