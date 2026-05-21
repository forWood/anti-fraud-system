# 反欺诈与反恐融资风险识别智能体系统
# 详细设计说明书 (DDS)

---

**文档版本：** V1.0  
**编写日期：** 2026年5月15日  
**文档状态：** 正式版  
**编制人：** 银行科技部  
**密级：** 内部使用

---

## 目录

1. [概述](#1-概述)
2. [系统架构设计](#2-系统架构设计)
3. [模块详细设计](#3-模块详细设计)
4. [数据库详细设计](#4-数据库详细设计)
5. [接口详细设计](#5-接口详细设计)
6. [AI智能体决策引擎设计](#6-ai智能体决策引擎设计)
7. [安全架构设计](#7-安全架构设计)
8. [性能优化设计](#8-性能优化设计)
9. [异常处理设计](#9-异常处理设计)
10. [附录](#10-附录)

---

## 1. 概述

### 1.1 文档目的

本文档是反欺诈与反恐融资风险识别智能体系统的详细设计说明书，基于需求规格说明书(SRS v2.0)和系统设计说明书(SDS v2.0)编写，旨在为软件实现提供详细的、可执行的设计规范。

**目标读者：**
- 后端开发工程师（Java/Python）
- 大数据开发工程师（Flink/Spark）
- 系统架构师
- 测试工程师
- 运维工程师

### 1.2 设计原则

| 原则 | 说明 |
|------|------|
| **高内聚低耦合** | 各模块独立部署，通过Kafka/gRPC异步通信 |
| **数据一致性** | Flink Exactly-Once语义 + Checkpoint机制 |
| **服务高可用** | K8s HPA自动伸缩 + Pod反亲和部署 |
| **安全合规** | 全链路TLS + API Key认证 + 敏感数据加密脱敏 |
| **可观测性** | Prometheus指标 + 结构化日志 + 分布式追踪 |
| **可扩展性** | 规则热加载 + 模型版本管理 + 插件化架构 |

### 1.3 技术选型对照

| 层级 | 技术选型 | 版本 | 选型理由 |
|------|----------|------|----------|
| 微服务框架 | Spring Boot + Spring Cloud | 2.7.18 / 2021.0.9 | 企业级成熟度、生态丰富 |
| 服务通信 | gRPC + REST | 1.58 / HTTP | gRPC高性能内部调用，REST对外 |
| 规则引擎 | Drools | 7.73.0 | 成熟规则引擎，DRL语法灵活 |
| 实时计算 | Apache Flink | 1.16.3 | Exactly-Once、低延迟、CEP |
| 离线计算 | Apache Spark | 3.3.4 | 批处理性能优异、SQL支持好 |
| 向量引擎 | FAISS | 1.7.4 | 轻量高效、支持内积检索 |
| 消息队列 | Apache Kafka | 3.4 | 高吞吐、持久化、多消费者 |
| ML推理 | TensorFlow Serving | 2.x | 标准化模型服务 |
| Python框架 | Flask + gRPC | - | 轻量、易部署 |
| 容器编排 | Kubernetes | 1.27+ | 自动化部署、HPA |

---

## 2. 系统架构设计

### 2.1 总体架构图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                       反欺诈智能体系统总体架构                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                      数据采集层 (Data Ingestion)                      │    │
│  │  Kafka Producer → transaction-raw → Kafka Cluster (3节点)            │    │
│  │  Debezium CDC  → customer-cdc    → MySQL Binlog捕获                  │    │
│  └──────────────────────────────┬─────────────────────────────────────┘    │
│                                 ▼                                            │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                      Flink 实时计算层 (Stream Processing)              │    │
│  │  ┌──────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐      │    │
│  │  │ 数据清洗  │→  │ 特征提取   │→  │ 规则初筛  │→  │ 结果输出  │      │    │
│  │  │ 去重/脱敏 │   │ 1h/24h窗口│   │ 实时特征   │   │ Kafka/Redis│      │    │
│  │  └──────────┘   └───────────┘   └───────────┘   └───────────┘      │    │
│  └──────────────────────────────┬─────────────────────────────────────┘    │
│                                 ▼                                            │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                       AI智能体决策层 (Agent Engine)                    │    │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐       │    │
│  │  │Drools规则  │  │TF预测模型  │  │FAISS知识库 │  │ 决策融合  │       │    │
│  │  │ 30+规则   │  │ 32维特征  │  │ Top-5案例 │  │ 0.4R+0.6M│       │    │
│  │  └───────────┘  └───────────┘  └───────────┘  └───────────┘       │    │
│  │                         ↓ 综合决策 ↓                               │    │
│  │              PASS(放行) / REVIEW(审核) / BLOCK(阻断)               │    │
│  └──────────────────────────────┬─────────────────────────────────────┘    │
│                                 ▼                                            │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                        微服务层 (Microservices)                       │    │
│  │  API Gateway → 交易监控 → 预警管理 → 案例管理 → 报告生成            │    │
│  │  (Nginx)       (TM)      (AM)       (CM)       (RG)               │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 数据流设计

```
[核心系统] → Kafka(transaction-raw) → Flink(清洗+特征)
    ├── Kafka(transaction-clean) → AI Agent(Drools+TF+FAISS)
    │       ├── Kafka(transaction-assessed) → 交易监控服务
    │       ├── Kafka(alert-created) → 预警管理服务
    │       └── MySQL(决策日志) → ClickHouse(OLAP分析)
    │
    ├── Redis(实时特征) ↑查询
    └── ClickHouse(离线特征) → Spark(批处理聚合)
```

### 2.3 部署架构图

```
┌────────────── Kubernetes Cluster ─────────────────────────────────┐
│                                                                    │
│  ┌─ namespace: anti-fraud ──────────────────────────────────┐    │
│  │                                                            │    │
│  │  ┌─────────────────────────────────────────────────────┐ │    │
│  │  │              Ingress (Nginx/Traefik)                  │ │    │
│  │  │           TLS终止 + 路由 + 限流                       │ │    │
│  │  └───────────────────┬───────────────────────────────┘ │    │
│  │                      │                                    │    │
│  │  ┌──────────────────┴──────────────────────────────┐   │    │
│  │  │         Spring Cloud Gateway (API Gateway)        │   │    │
│  │  │         认证/鉴权/限流/路由/熔断                   │   │    │
│  │  └───────────────────┬──────────────────────────────┘  │    │
│  │                      │                                    │    │
│  │     ┌────────────────┼────────────────────┐             │    │
│  │     ▼                ▼                     ▼             │    │
│  │  [TM x3]      [AM x3]  [CM x2]  [RG x2]              │    │
│  │  Transaction  Alert     Case     Report               │    │
│  │  Monitor      Mgmt      Mgmt     Generator            │    │
│  │     │            │         │         │                 │    │
│  │     └────────────┼─────────┴─────────┘                 │    │
│  │                  │                                      │    │
│  │                  ▼                                      │    │
│  │     ┌─────────────────────────────────┐               │    │
│  │     │   AI Agent Service (gRPC x3)     │               │    │
│  │     │   Drools + TF Client + Decision  │               │    │
│  │     │   ┌──────────────────────┐       │               │    │
│  │     │   │  Knowledge Base x2   │       │               │    │
│  │     │   │  Python/FAISS/gRPC   │       │               │    │
│  │     │   └──────────────────────┘       │               │    │
│  │     └─────────────────────────────────┘               │    │
│  │                                                        │    │
│  │  ┌── 中间件层 ──────────────────────────────────┐    │    │
│  │  │  [Kafka]　[MySQL]　[Redis]　[ClickHouse]       │    │    │
│  │  │  [Milvus] [Nacos]  [Prometheus] [Grafana]     │    │    │
│  │  └───────────────────────────────────────────────┘    │    │
│  └────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. 模块详细设计

### 3.1 公共模块 (anti-fraud-common)

**职责：** 提供全系统共享的数据模型、工具类、常量定义。

**核心类设计：**

#### 3.1.1 Transaction.java — 交易记录模型

```java
// 字段规范遵循SRS 6.2.1节，共33个字段
// 关键字段：
- transactionId: String        // 交易流水号(全局唯一)
- accountIdHash: String        // 账号SHA-256哈希(脱敏)
- amount: BigDecimal           // 交易金额(元, 保留2位小数)
- transactionTime: LocalDateTime // 交易时间(ISO8601)
- ipRiskLevel: String          // IP风险等级(NORMAL/VPN/PROXY/TOR)
- isEmulator: Boolean          // 是否模拟器
- isCrossBorder: Boolean       // 是否跨境

// 辅助方法：
- isNightTime(): 判断夜间交易(00:00-06:00)
- isWeekend(): 判断周末交易
```

#### 3.1.2 RiskAssessment.java — 风险评估结果

```java
// 决策输出模型
- riskScore: BigDecimal        // 综合风险评分(0-100)
- riskLevel: String            // 风险等级(LOW/MEDIUM/HIGH)
- decision: String             // 决策建议(PASS/REVIEW/BLOCK)
- matchedRules: List<RuleResult> // 命中规则列表
- similarCases: List<CaseSummary> // 相似案例(知识库检索)
- topFeatureContributions: List<FeatureContribution> // ML模型TOP特征

// 内嵌枚举：
RiskLevel { LOW(0-49), MEDIUM(50-79), HIGH(80-100) }
Decision { PASS(放行), REVIEW(人工审核), BLOCK(阻断) }
```

#### 3.1.3 工具类设计

| 类名 | 核心方法 | 用途 |
|------|---------|------|
| `DataSecurityUtil` | sha256(), maskPhone(), maskIdCard(), maskBankCard() | 数据脱敏与加密 |
| `JsonUtil` | toJson(), fromJson() | JSON序列化(Jackson) |
| `RiskConstants` | 50+常量定义 | 阈值/Topic/Redis Key前缀 |

### 3.2 数据采集模块 (anti-fraud-data-collection)

#### 3.2.1 TransactionKafkaProducer — 交易模拟生成器

**设计说明：** 模拟生产环境中的核心银行系统，向Kafka topic发送交易流水。

**核心参数：**
```
- bootstrapServers: Kafka连接地址(默认 localhost:9092)
- topic: 目标topic(默认 transaction-raw)
- ratePerSec: 每秒发送速率(默认100, 最大5000)
- durationSeconds: 持续时长(默认60)
```

**金额分布策略（模拟真实场景）：**
```
70% 小额交易: ￥100 - ￥10,000     (日常消费)
20% 中等交易: ￥10,000 - ￥100,000 (商业支付)
 8% 大额交易: ￥100,000 - ￥500,000 (企业转账)
 2% 超大额:    ￥500,000 - ￥1,000,000 (风险关注)
```

**风险场景模拟（10%概率）：**
- 模拟器设备(isEmulator=true) — 30%
- ROOT/越狱设备(isRooted=true) — 20%
- 夜间交易(00:00-06:00) — 40%
- 高风险IP(VPN/Proxy/TOR) — 20%
- 新设备绑定 — 50%
- 跨境交易 — 30%

**运行方式：**
```bash
java -jar anti-fraud-data-collection.jar \
  -Dkafka.servers=kafka:9092 \
  -Dkafka.topic=transaction-raw \
  -Dproducer.rate=1000 \
  -Dproducer.duration=3600
```

#### 3.2.2 Debezium MySQL CDC

**配置路径：** `src/main/resources/debezium-cdc-customer.json`

**CDC流程：**
```
MySQL Binlog → Debezium Connector → Kafka(customer-cdc topic) → Flink消费者
```

**捕获的变更类型：**
- INSERT: 新客户注册 → 触发初始风险评估
- UPDATE: 客户信息变更 → 更新客户画像
- DELETE: 客户销户 → 移除监控范围

### 3.3 Flink实时计算模块 (anti-fraud-flink)

#### 3.3.1 作业拓扑(FraudDetectionFlinkJob)

```
Source: Kafka(transaction-raw)
  │
  ├─ filter: 金额验证(0.01 ≤ amount ≤ 10,000,000)
  ├─ map: DataCleanFunction (去重/脱敏/格式化)
  │   └─ Sink: Kafka(transaction-clean)
  │
  ├─ keyBy(accountIdHash)
  ├─ process: FeatureExtractionProcess
  │   └─ Sink: Kafka(transaction-assessed) // 评估结果
  │   └─ Sink: Redis(实时特征)             // 在线服务
  │   └─ Sink: ClickHouse(离线特征)        // OLAP分析
```

**并行度配置：**
```yaml
flink.parallelism: 4      # 默认4并行度
taskmanager.slots: 4      # 每TM 4个slot
```

**Checkpoint配置：**
```java
间隔: 60秒
模式: EXACTLY_ONCE (精准一次语义)
超时: 120秒
最大并发: 1
状态后端: RocksDB
```

#### 3.3.2 DataCleanFunction — 数据清洗

**处理步骤：**
```
1. 格式标准化: 金额2位小数 → 币种默认CNY → 渠���编码大写
2. 敏感脱敏:   账号SHA-256 → GPS精度到0.01° → 对手账号哈希
3. IP风险评估: 已知高风险IP段(185.220.*, 23.129.*) → HIGH_RISK
4. 异常标记:   极端大额≥1000万 → 记录WARN日志
```

**性能指标：**
- 单算子吞吐: ≥10,000条/秒
- 内存占用: ≤512MB (RocksDB状态)

#### 3.3.3 FeatureExtractionProcess — 特征提取

**设计原理：** 基于Flink KeyedProcessFunction，按accountIdHash分区，使用State存储历史数据。

**State设计（全部基于RocksDB）：**
```
状态名称                  数据类型          用途                    过期策略
──────────────────────────────────────────────────────────────────────
recentTransactionState    ListState         最近24h交易列表          24h定时器清理
deviceState               MapState          30天设备指纹集合         30天TTL
cityState                 MapState          30天城市集合            30天TTL
counterpartyState         MapState          30天交易对手集合         30天TTL
rollingWindowState        ValueState        滚动窗口统计            实时更新
behaviorBaselineState     ValueState        行为基线                日更新
```

**特征计算逻辑：**
```java
// 1小时窗口特征
amount1h = Σ(最近1小时内所有交易金额)
count1h  = 最近1小时内交易笔数

// 24小时窗口特征
amount24h = Σ(最近24小时内所有交易金额)
count24h  = 最近24小时内交易笔数

// 30天多样性特征
deviceCount30d = deviceState中key数量
cityCount30d   = cityState中key数量

// 设备指纹
isFirstDevice = !deviceState.contains(deviceId)

// 金额偏离度
amountDeviation = |当前金额 - 历史均值| / 历史均值
```

#### 3.3.4 RedisFeatureSink — 在线特征存储

**Redis数据结构：**
```
Key:   risk:feature:{accountIdHash}
Type:  String (JSON)
TTL:   86400秒 (24小时)
```

**存储内容：**
```json
{
  "amount1h": 50000.00,
  "count1h": 3,
  "amount24h": 150000.00,
  "count24h": 15,
  "deviceCount30d": 3,
  "cityCount30d": 2,
  "counterpartyCount30d": 8,
  "isNightTime": false,
  "isCrossBorder": false,
  "isFirstDevice": false,
  "ipRiskLevel": "NORMAL",
  "amountDeviation": 0.5,
  "timestamp": 1715688000000
}
```

#### 3.3.5 ClickHouseSink — 离线分析存储

**写入策略：**
- 批量写入: 100条/批次
- 重试机制: 失败日志记录 + 死信队列
- 表引擎: MergeTree, 按月分区

**ClickHouse表结构：**
```sql
CREATE TABLE risk_db.transaction_features (
    transaction_id String,
    account_id_hash String,
    amount Decimal(18,2),
    amount_1h Decimal(18,2),
    count_1h Int32,
    amount_24h Decimal(18,2),
    count_24h Int32,
    device_count_30d Int32,
    city_count_30d Int32,
    is_night_time UInt8,
    is_cross_border UInt8,
    is_first_device UInt8,
    ip_risk_level String,
    amount_deviation Float64,
    create_time DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(create_time)
ORDER BY (account_id_hash, create_time);
```

### 3.4 Spark离线特征计算 (anti-fraud-spark)

**设计说明：** 每日T+1批处理，从ClickHouse读取30天交易数据，计算客户级聚合特征，回写Redis和ClickHouse。

**计算的特征：**
```
1. 金额特征: 30天总额/平均值/最大值/最小值/标准差
2. 频率特征: 30天总笔数/日均笔数
3. 时间特征: 夜间交易次数/周末交易次数
4. 多样性: 渠道数/商户数/交易类型分布
5. 偏离度: 最近7天 vs 前21天均值的偏离比率
```

**运行调度：**
```bash
# 每日凌晨2:00执行
spark-submit --class com.bank.risk.spark.OfflineFeatureJob \
  --master k8s://https://k8s-api:6443 \
  anti-fraud-spark.jar \
  --redis-host redis.anti-fraud.svc \
  --clickhouse-url jdbc:clickhouse://clickhouse:8123/risk_db
```

### 3.5 知识库模块 (anti-fraud-knowledge-base)

#### 3.5.1 系统架构

```
┌───────────────────────────────────────────────────────────────┐
│                    知识库服务 (Python)                          │
│                                                               │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────┐ │
│  │ MySQL Reader │→  │ Vectorizer  │→  │ FAISS Index Manager│ │
│  │ 读取案例数据  │   │SentenceTF   │   │ IndexFlatIP 维护    │ │
│  └─────────────┘   └─────────────┘   └──────────┬──────────┘ │
│                                                  │            │
│  ┌───────────────────────────────────────────────┘            │
│  │                                                            │
│  ▼                                                            │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────┐ │
│  │ gRPC Server │←  │ FraudCaseRAG│←  │ KnowledgeSearch API │ │
│  │ 9091端口    │   │ 检索增强    │   │ /api/v1/knowledge/   │ │
│  └─────────────┘   └─────────────┘   └─────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
```

#### 3.5.2 向量化引擎 (KnowledgeVectorizer)

**支持的模型：**

| 模型Key | HuggingFace | 维度 | 适用场景 |
|---------|-------------|------|----------|
| text2vec-base | shibing624/text2vec-base-chinese | 768 | 通用中文(推荐) |
| bge-large | BAAI/bge-large-zh-v1.5 | 1024 | 精度优先 |
| bge-small | BAAI/bge-small-zh-v1.5 | 512 | 速度优先 |

**案例文本构建策略：**
```python
def build_case_text(case):
    return f"""
    案例标题: {case.title}
    案例类型: {case.type}
    欺诈模式: {case.pattern}
    作案手法: {case.modus_operandi}
    案例描述: {case.description}
    处置方案: {case.resolution}
    """
```

#### 3.5.3 FAISS索引管理 (FAISSIndexManager)

**索引类型：**
- IndexFlatIP (内积检索) — 配合L2归一化向量实现余弦相似度

**持久化策略：**
```
存储路径: ./data/faiss_index/
├── faiss_index.index    # FAISS二进制索引文件
└── faiss_index.metadata.json  # 案例元数据JSON
```

**重建触发条件：**
1. MySQL案例表新增/更新超过50条
2. 向量模型版本变更
3. 定时全量重建 (每周日凌晨2:00)

#### 3.5.4 RAG检索增强 (FraudCaseRAG)

**检索流程：**
```
交易特征 → 构建查询文本 → SentenceTransformer向量化
    → FAISS Top-K检索 → 相似案例 → 构建RAG上下文
```

**查询文本构造模板：**
```
交易金额: 800000.00
交易类型: TRANSFER
渠道: EBANK
1小时交易次数: 5
24小时交易次数: 15
是否新设备: true
是否跨境: true
IP风险: HIGH_RISK
30天设备数: 3
金额偏离度: 2.5
```

**RAG上下文输出格式（用于智能体Prompt）：**
```
## 相似历史案例 (RAG检索结果)

### 案例1: 境外信用卡盗刷 (相似度: 87.5%)
- 类型: FRAUD
- 模式: 境外盗刷
- 手法: 暗网获取信用卡信息;异地消费;夜间交易
- 处置: 立即冻结账户;联系持卡人确认;上报反欺诈中心
- 效果: 成功拦截损失80万元

### 案例2: 电信诈骗快速转出 (相似度: 72.3%)
- 类型: FRAUD
- 模式: 电信诈骗
- 手法: 新设备登录;修改绑定手机;快速多笔转出
- 处置: 临时冻结;人工外呼;公安联动
```

---

## 4. 数据库详细设计

### 4.1 数据库概述

- **数据库名：** risk_db
- **字符集：** utf8mb4
- **排序规则：** utf8mb4_unicode_ci
- **存储引擎：** InnoDB (全部表)

### 4.2 表关系图

```
┌─────────────────┐
│ customer_profile │  客户画像
│ customer_id (PK) │
└────────┬────────┘
         │ 1:N
         ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ transaction_record│────→│   alert_record    │────→│investigation_case│
│ transaction_id(PK)│     │ alert_id (PK)     │     │ case_no (PK)   │
└────────┬────────┘     └──────────────────┘     └─────────────────┘
         │                         │                        │
         ▼                         ▼                        ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│decision_audit_log│     │  fraud_case       │     │   black_list    │
│ transaction_id   │     │  case_id (PK)     │     │ id (PK)        │
└─────────────────┘     └──────────────────┘     └─────────────────┘

         ┌──────────────────┐
         │  rule_definition │  规则定义
         │  rule_id (PK)    │
         └──────────────────┘
```

### 4.3 核心表详细设计

#### 4.3.1 rule_definition — 规则定义表

| 字段名 | 类型 | 长度 | 说明 |
|--------|------|------|------|
| id | BIGINT | - | 自增主键 |
| rule_id | VARCHAR | 32 | 规则编号(R001/T001)，UNIQUE |
| rule_name | VARCHAR | 128 | 规则名称 |
| rule_category | VARCHAR | 32 | 分类: AMOUNT/TIME/REGION/DEVICE/BEHAVIOR/CTF |
| rule_expression | TEXT | - | 规则表达式(DRL/JSON) |
| risk_weight | INT | - | 风险权重0-100 |
| risk_level | VARCHAR | 8 | 风险等级: LOW/MEDIUM/HIGH |
| description | VARCHAR | 512 | 规则描述 |
| conditions_json | JSON | - | 触发条件JSON |
| enabled | TINYINT | 1 | 是否启用(1/0) |
| version | INT | - | 版本号 |

**索引：**
```sql
INDEX idx_rule_enabled (enabled)
INDEX idx_rule_category (rule_category)
```

#### 4.3.2 black_list — 黑名单表

| 字段名 | 类型 | 说明 |
|--------|------|------|
| list_type | VARCHAR(32) | 类型: SANCTION/INTERNAL/HIGH_RISK |
| entity_type | VARCHAR(32) | 实体: ACCOUNT/NAME/ID_CARD/PHONE/IP |
| entity_value | VARCHAR(256) | 实体值(已脱敏) |
| entity_hash | VARCHAR(64) | SHA-256哈希(用于匹配) |
| source | VARCHAR(64) | 名单来源 |
| effective_time | DATETIME | 生效时间 |
| expire_time | DATETIME | 过期时间 |

**索引：**
```sql
INDEX idx_entity_hash (entity_hash)   -- 主要查询索引
INDEX idx_list_type (list_type)
INDEX idx_enabled_expire (enabled, expire_time)
```

#### 4.3.3 transaction_record — 交易记录表

| 字段 | 类型 | 说明 |
|------|------|------|
| transaction_id | VARCHAR(64) | UNIQUE索引 |
| account_id_hash | VARCHAR(64) | 账号哈希 |
| amount | DECIMAL(18,2) | 交易金额 |
| transaction_time | DATETIME | 交易时间 |
| ip_risk_level | VARCHAR(16) | IP风险等级 |
| risk_score | DECIMAL(5,2) | 评估风险评分 |
| risk_level | VARCHAR(8) | 风险等级 |
| decision | VARCHAR(16) | 处置决策(PASS/REVIEW/BLOCK) |

**分区策略：** 按月分区（PARTITION BY RANGE (TO_DAYS(transaction_time))）

#### 4.3.4 fraud_case — 欺诈案例表

| 字段 | 类型 | 说明 |
|------|------|------|
| case_id | VARCHAR(64) | UNIQUE索引 |
| case_title | VARCHAR(256) | 案例标题 |
| case_description | TEXT | 完整描述 |
| modus_operandi | TEXT | 作案手法 |
| fraud_pattern | VARCHAR(128) | 欺诈模式标签 |
| key_features | JSON | 关键特征 |
| resolution | TEXT | 处置方案 |

**全文索引：**
```sql
FULLTEXT idx_fulltext (case_title, case_description, modus_operandi)
```

#### 4.3.5 alert_record — 预警记录表

| 字段 | 类型 | 说明 |
|------|------|------|
| alert_id | VARCHAR(64) | UNIQUE索引 |
| transaction_id | VARCHAR(64) | 关联交易 |
| risk_score | DECIMAL(5,2) | 风险评分 |
| alert_status | VARCHAR(16) | 状态: NEW/PENDING/PROCESSING/RESOLVED/CLOSED |
| escalation_level | INT | 升级等级0-3 |
| processing_deadline | DATETIME | 处理截止时间 |

#### 4.3.6 decision_audit_log — 决策审计日志

| 字段 | 类型 | 说明 |
|------|------|------|
| request_id | VARCHAR(64) | 请求ID |
| rule_score | DECIMAL(5,2) | 规则评分 |
| ml_score | DECIMAL(5,2) | 模型评分 |
| kb_score | DECIMAL(5,2) | 知识库评分 |
| matched_rules | JSON | 命中规则明细 |
| similar_cases | JSON | 相似案例 |
| processing_time_ms | INT | 处理耗时(ms) |

**用途：** 完整审计追溯 + 模型效果分析 + 规则准确性统计

### 4.4 索引策略

| 查询场景 | 索引 | 类型 |
|----------|------|------|
| 按账号查询交易 | idx_account_hash | BTREE |
| 按时间范围查询 | idx_tx_time | BTREE |
| 按风险等级筛选 | idx_risk_level | BTREE |
| 案例全文搜索 | idx_fulltext | FULLTEXT |
| 黑名单匹配 | idx_entity_hash | BTREE |
| 预警状态筛选 | idx_alert_status | BTREE |

---

## 5. 接口详细设计

### 5.1 gRPC接口规范

#### 5.1.1 Proto定义

**文件位置：** `anti-fraud-agent/src/main/proto/anti_fraud_agent.proto`

**服务定义：**
```protobuf
service AntiFraudAgentService {
    rpc AssessTransaction (AssessRequest) returns (AssessResponse);
    rpc BatchAssess (BatchAssessRequest) returns (BatchAssessResponse);
    rpc SearchKnowledge (KnowledgeSearchRequest) returns (KnowledgeSearchResponse);
    rpc HealthCheck (HealthCheckRequest) returns (HealthCheckResponse);
}
```

#### 5.1.2 AssessTransaction — 交易风险评估

**请求 (AssessRequest)：**
```protobuf
message AssessRequest {
    string transaction_id = 1;       // 交易流水号
    double amount = 4;               // 交易金额
    string transaction_type = 6;     // TRANSFER/PAYMENT/WITHDRAW/DEPOSIT
    string channel_code = 7;         // EBANK/MBANK/COUNTER/ATM/POS
    bool is_emulator = 23;           // 是否模拟器
    bool is_first_device = 25;       // 是否新设备
    string ip_risk_level = 33;       // NORMAL/VPN/PROXY/TOR
    double amount_1h = 50;           // 1h交易总额
    int32 count_1h = 51;             // 1h交易次数
    int32 count_24h = 53;            // 24h交易次数
    bool is_cross_border = 58;       // 是否跨境
    double amount_deviation = 59;    // 金额偏离度
}
```

**响应 (AssessResponse)：**
```protobuf
message AssessResponse {
    double risk_score = 2;           // 风险评分 0-100
    string risk_level = 3;           // LOW/MEDIUM/HIGH
    string decision = 4;             // PASS/REVIEW/BLOCK
    double confidence = 5;           // 置信度 0-1
    double rule_score = 10;          // 规则评分
    double ml_score = 11;            // 模型评分
    double kb_score = 12;            // 知识库评分
    repeated MatchedRule matched_rules = 20;      // 命中规则
    repeated SimilarCase similar_cases = 30;      // 相似案例
    repeated string risk_factors = 40;            // 风险因子
    repeated FeatureContribution top_features = 45; // ML主要特征
    int64 processing_time_ms = 60;   // 处理耗时
}
```

#### 5.1.3 错误码定义

| gRPC Status | Code | 说明 |
|-------------|------|------|
| OK | 0 | 评估成功 |
| INVALID_ARGUMENT | 3 | 参数校验失败 |
| INTERNAL | 13 | 内部错误(规则引擎/ML预测失败) |
| UNAVAILABLE | 14 | 依赖服务不可用 |
| DEADLINE_EXCEEDED | 4 | 评估超时(≥500ms) |

### 5.2 REST接口规范

#### 5.2.1 交易监控服务

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/monitor/assess` | 单笔交易评估 |
| POST | `/api/v1/monitor/batch-assess` | 批量评估 |
| GET | `/api/v1/monitor/health` | 健康检查 |

**请求示例：**
```json
POST /api/v1/monitor/assess
Content-Type: application/json
X-API-Key: sk-xxx

{
    "transaction_id": "TX20260515000001",
    "account_id_hash": "sha256_hash...",
    "amount": 800000.00,
    "transaction_type": "TRANSFER",
    "channel_code": "EBANK",
    "device_id": "DEV-ABC123",
    "ip_address": "185.220.101.50",
    "is_emulator": true,
    "is_cross_border": true,
    "amount_1h": 500000.00,
    "count_1h": 5,
    "count_24h": 15,
    "device_count_30d": 3,
    "city_count_30d": 5,
    "amount_deviation": 2.5
}
```

**响应示例：**
```json
{
    "transaction_id": "TX20260515000001",
    "risk_score": 85.5,
    "risk_level": "HIGH",
    "decision": "BLOCK",
    "confidence": 0.92,
    "rule_score": 75.0,
    "ml_score": 92.0,
    "kb_score": 80.0,
    "risk_factors": [
        "[R203] 高匿名IP交易",
        "[R302] 模拟器检测",
        "跨境交易",
        "ML模型判定高风险"
    ],
    "processing_time_ms": 120
}
```

#### 5.2.2 预警管理服务

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/alerts` | 创建预警 |
| GET | `/api/v1/alerts` | 查询预警列表 |
| PUT | `/api/v1/alerts/{alertId}/process` | 处理预警 |
| PUT | `/api/v1/alerts/{alertId}/escalate` | 升级预警 |
| POST | `/api/v1/alerts/{alertId}/notify` | 发送通知 |

---

## 6. AI智能体决策引擎设计

### 6.1 决策流程

```
┌────────────────────────────────────────────────────────────────┐
│                  AI智能体决策流程                                │
│                                                                │
│  交易特征输入                                                    │
│      │                                                         │
│      ├─→ [名单检查] → 命中制裁名单(T001) → 立即阻断 ⚡          │
│      │                                                         │
│      ├─→ [规则引擎] → Drools执行30+规则 → 规则评分(40%权重)     │
│      │         ├─ 金额规则 (R001-R008)                         │
│      │         ├─ 时间规则 (R101-R106)                          │
│      │         ├─ 地域规则 (R201-R206)                          │
│      │         ├─ 设备规则 (R301-R306)                          │
│      │         └─ 反恐规则 (T001-T005)                          │
│      │                                                         │
│      ├─→ [ML模型]  → TF Serving推理 → 欺诈概率(60%权重)         │
│      │         输入: 32维特征向量                                │
│      │         输出: fraud_probability (0-1)                    │
│      │         解释: SHAP Top-5                                 │
│      │                                                         │
│      ├─→ [知识库]  → FAISS检索 → Top-5相似案例                                                  │
│                                                        │
│      └─→ [决策融合] finalScore = 0.4×ruleScore + 0.6×mlScore
│              ├─ ≥80: HIGH → BLOCK(阻断)
│              ├─ 50-79: MEDIUM → REVIEW(人工审核)
│              └─ <50: LOW → PASS(放行)
└────────────────────────────────────────────────────────────────┘
```

### 6.2 决策融合算法

```
输入:
  ruleScore  ∈ [0, 100]   // Drools规则引擎输出
  mlScore    ∈ [0, 1]     // ML模型输出(归一化为[0,100])
  matchedRules: List      // 命中规则列表

输出:
  finalScore ∈ [0, 100]
  riskLevel  ∈ {LOW, MEDIUM, HIGH}
  decision   ∈ {PASS, REVIEW, BLOCK}
  confidence ∈ [0, 1]

算法:
  1. normalizedMlScore = mlScore × 100
  2. finalScore = 0.4 × ruleScore + 0.6 × normalizedMlScore
  3. riskLevel = 
       HIGH   if finalScore ≥ 80
       MEDIUM if 50 ≤ finalScore < 80
       LOW    if finalScore < 50
  4. decision =
       BLOCK  if T001命中 OR (finalScore ≥ 80 AND normalizedMlScore ≥ 85)
       REVIEW if finalScore ≥ 50
       PASS   otherwise
  5. confidence = min(1.0, ruleConfidence×0.3 + mlScore×0.7)
       where ruleConfidence = min(1.0, matchedRules.count / 10)
```

### 6.3 Drools规则引擎

#### 6.3.1 规则组织结构

```
fraud-rules.drl
├── 一、交易金额类规则 (R001-R008) - salience 70-100
│   ├── R001: 单笔大额≥50万
│   ├── R002: 频繁大额≥100万
│   ├── R003: 分散转入集中转出
│   ├── R005: 快进快出
│   └── R007: 小额试探后大额
├── 二、时间行为类规则 (R101-R106) - salience 75-85
│   ├── R101: 凌晨高频交易
│   └── R106: 交易脉冲(10分钟5笔)
├── 三、地域IP类规则 (R201-R206) - salience 70-95
│   ├── R202: FATF高风险国家
│   ├── R203: VPN/代理/Tor
│   └── R206: 高风险地区
├── 四、设备类规则 (R301-R306) - salience 75-95
│   ├── R301: 新设备首笔
│   ├── R302: 模拟器检测
│   └── R305: ROOT/越狱
└── 五、反恐融资专项 (T001-T005) - salience 80-100
    ├── T001: 制裁名单命中 → 强制阻断
    └── T005: 战乱国家交易
```

#### 6.3.2 动态刷新机制

```java
@Scheduled(fixedDelay = 300000) // 每5分钟
public void refreshRulesIfNeeded() {
    // 1. 扫描 rules/ 目录下 .drl 文件
    // 2. 比较文件最后修改时间
    // 3. 有变更 → 重新编译KieBase
    // 4. 热替换 KieContainer (原子操作)
}
```

### 6.4 ML模型推理

#### 6.4.1 特征向量编码 (32维)

```
[0]  log10(amount) / 8            [标准化金额]
[1]  count_1h / 100               [标准化1h频次]
[2]  count_24h / 1000             [标准化24h频次]
[3]  device_count_30d / 50        [标准化设备数]
[4]  city_count_30d / 20          [标准化城市数]
[5]  counterparty_count_30d / 100 [标准化对手数]
[6]  is_night_time (0/1)          [夜间标记]
[7]  is_cross_border (0/1)        [跨境标记]
[8]  is_first_device (0/1)        [新设备]
[9]  ip_risk_level (编码)         [IP风险 0.0-0.9]
[10] amount_deviation / 10        [偏离度]
[11] is_emulator (0/1)            [模拟器]
[12] is_rooted (0/1)              [ROOT]
[13-18] channel one-hot × 6       [渠道编码]
[19-23] tx_type one-hot × 5       [交易类型]
[24-27] amount_bins × 4           [金额分段]
[28] is_weekend (0/1)             [周末]
[29] is_holiday (0/1)             [节假日]
[30] amount_1h / amount           [比例]
[31] count_24h / device_count     [密度]
```

#### 6.4.2 TF Serving调用

```
POST http://tf-serving:8501/v1/models/fraud_detection:predict
{
    "signature_name": "serving_default",
    "instances": [[0.5, 0.03, 0.015, ...]] // 32维
}

Response:
{
    "predictions": [
        [0.15, 0.85]  // [normal_prob, fraud_prob]
    ]
}
```

#### 6.4.3 降级策略

```
当TF Serving不可用时:
  fraud_probability = 0.5  (中性降级)
  model_version = "fallback"
  决策: 仅依赖规则引擎评分
```

---

## 7. 安全架构设计

### 7.1 数据传输安全

```
┌──────────┐  HTTPS/TLS 1.2  ┌──────────┐
│ 外部系统  │←────────────────→│ API Gateway│
└──────────┘                  └─────┬────┘
                                    │ mTLS
                             ┌──────┴──────┐
                             │  gRPC内部调用 │
                             └──────────────┘
```

### 7.2 认证鉴权

```
请求链路: Client → Gateway → Microservice → Agent

三层认证:
1. Gateway层: API Key + IP白名单 + 限流
2. gRPC层: mTLS双向认证
3. 服务层: JWT Token (用户系统)
```

### 7.3 数据脱敏规范

| 数据类型 | 脱敏方式 | 示例 |
|----------|----------|------|
| 手机号 | 中间4位替换 | 138****5678 |
| 身份证 | 中间8位替换 | 110101********1234 |
| 银行卡号 | 中间4位替换 | 6222****7890 |
| 账号 | SHA-256哈希 | a3f5b1c... |
| GPS | 精度到0.01° | 39.90, 116.41 |

### 7.4 审计日志

所有敏感操作记录到 `decision_audit_log` 表：
- 交易ID、请求ID
- 规则评分、模型评分、知识库评分
- 命中规则明细、相似案例
- 处理耗时
- 保留≥7年

---

## 8. 性能优化设计

### 8.1 计算层优化

| 优化点 | 方案 | 效果 |
|--------|------|------|
| Flink状态 | RocksDB后端, 增量Checkpoint | 内存≤512MB |
| 数据压缩 | Kafka Snappy压缩 | 带宽降低60% |
| 批量写入 | ClickHouse 100条/批 | 吞吐提升10倍 |
| gRPC连接池 | 连接复用, Keep-Alive | 减少建连开销 |
| 规则编译缓存 | KieContainer单例 | 规则执行≤100ms |

### 8.2 缓存策略

| 缓存层 | 数据类型 | TTL | 用途 |
|--------|----------|-----|------|
| Redis | 实时特征 | 24h | 在线服务查询 |
| Redis | 黑名单 | 1h | 名单快速匹配 |
| Redis | 规则元数据 | 5min | 规则管理端查询 |
| JVM本地缓存 | 规则KieBase | 永久 | 避免重复编译 |

### 8.3 HPA自动伸缩

```yaml
autoScaling:
  minReplicas: 3
  maxReplicas: 10
  targetCPU: 70%
  targetMemory: 80%
```

---

## 9. 异常处理设计

### 9.1 故障降级策略

| 组件故障 | 影响范围 | 降级策略 |
|----------|----------|----------|
| Kafka不可用 | 数据采集中断 | 本地内存队列缓冲, 重启后回放 |
| ML服务不可用 | 无模型评分 | 仅依赖规则引擎, fraudProb=0.5 |
| 知识库不可用 | 无相似案例 | 跳过RAG检索, kbScore=0 |
| Redis不可用 | 特征缓存失效 | 从ClickHouse查询历史特征 |
| MySQL不可用 | 无法持久化 | Kafka死信队列存储, 恢复后回放 |

### 9.2 重试策略

```java
// gRPC重试: 最多3次
// Kafka消费: 无限重试 + 死信队列
// MySQL写入: 3次重试 + 降级写入ClickHouse
```

---

## 10. 附录

### 10.1 完整文件清单

| 序号 | 文件路径 | 说明 |
|------|----------|------|
| 1 | anti-fraud-common/pom.xml | 公共模块POM |
| 2 | anti-fraud-common/.../Transaction.java | 交易记录模型 |
| 3 | anti-fraud-common/.../EnrichedTransaction.java | 增强交易模型 |
| 4 | anti-fraud-common/.../RiskAssessment.java | 风险评估模型 |
| 5 | anti-fraud-common/.../RuleResult.java | 规则执行结果 |
| 6 | anti-fraud-common/.../AlertEvent.java | 预警事件模型 |
| 7 | anti-fraud-common/.../RiskConstants.java | 常量定义 |
| 8 | anti-fraud-common/.../DataSecurityUtil.java | 安全工具类 |
| 9 | anti-fraud-common/.../JsonUtil.java | JSON工具类 |
| 10 | anti-fraud-data-collection/pom.xml | 采集模块POM |
| 11 | .../TransactionKafkaProducer.java | Kafka生产者 |
| 12 | .../debezium-cdc-customer.json | CDC配置 |
| 13 | anti-fraud-flink/pom.xml | Flink模块POM |
| 14 | .../FraudDetectionFlinkJob.java | Flink主作业 |
| 15 | .../TransactionKafkaSource.java | Kafka数据源 |
| 16 | .../TransactionDeserializationSchema.java | 反序列化器 |
| 17 | .../DataCleanFunction.java | 数据清洗函数 |
| 18 | .../FeatureExtractionProcess.java | 特征提取 |
| 19 | .../RedisFeatureSink.java | Redis Sink |
| 20 | .../ClickHouseSink.java | ClickHouse Sink |
| 21 | .../EnrichedKafkaSink.java | Kafka Sink |
| 22 | anti-fraud-spark/.../OfflineFeatureJob.scala | Spark主作业 |
| 23 | anti-fraud-spark/.../OfflineFeatureComputer.scala | 离线特征计算 |
| 24 | anti-fraud-knowledge-base/requirements.txt | Python依赖 |
| 25 | .../knowledge_base_service.py | 知识库服务 |
| 26 | anti-fraud-agent/pom.xml | Agent POM |
| 27 | .../AntiFraudAgentApplication.java | Spring Boot启动 |
| 28 | .../AntiFraudAgentService.java | gRPC服务 |
| 29 | .../DroolsRuleEngine.java | Drools引擎 |
| 30 | .../RuleEngineResult.java | 规则结果模型 |
| 31 | .../RuleMetricsCollector.java | 指标收集器 |
| 32 | .../MLModelClient.java | ML客户端 |
| 33 | .../DecisionFusionEngine.java | 决策融合 |
| 34 | .../KnowledgeBaseClient.java | 知识库客户端 |
| 35 | .../anti_fraud_agent.proto | Proto定义 |
| 36 | .../fraud-rules.drl | 30+条业务规则 |
| 37 | .../application.yml | 应用配置 |
| 38 | .../TransactionMonitorApplication.java | TM启动 |
| 39 | .../TransactionMonitorController.java | TM REST接口 |
| 40 | .../TransactionAssessService.java | TM gRPC调用服务 |
| 41 | .../AlertManagementApplication.java | AM启动 |
| 42 | .../AlertManagementController.java | AM REST接口 |
| 43 | .../AlertManagementService.java | AM业务逻辑 |
| 44 | anti-fraud-sql/init-schema.sql | 数据库初始化 |
| 45 | anti-fraud-deployment/docker-compose.yml | 本地部署 |
| 46 | .../Dockerfile (agent) | Agent镜像 |
| 47 | .../Dockerfile (knowledge) | KB镜像 |
| 48 | .../Chart.yaml | Helm Chart |
| 49 | .../values.yaml | Helm配置 |
| 50 | .../agent-deployment.yaml | K8s Deployment |
| 51 | .../prometheus.yml | 监控配置 |
| 52 | .../anti-fraud-alerts.yml | 告警规则 |
| 53 | .../anti-fraud-dashboard.json | Grafana面板 |
| 54 | .../AntiFraudAgentServiceTest.java | 单元测试 |
| 55 | .../TransactionE2EIntegrationTest.java | 集成测试 |
| 56 | .../anti-fraud-jmeter.jmx | 压测脚本 |
| 57 | .../transactions.csv | 压测数据 |

### 10.2 关键配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| RULE_WEIGHT | 0.4 | 规则评分权重 |
| ML_WEIGHT | 0.6 | 模型评分权重 |
| HIGH_RISK_THRESHOLD | 80 | 高风险阈值 |
| MEDIUM_RISK_THRESHOLD | 50 | 中风险阈值 |
| FLINK_PARALLELISM | 4 | Flink并行度 |
| REDIS_FEATURE_TTL | 86400 | 特征缓存TTL(秒) |

---

**文档结束**
