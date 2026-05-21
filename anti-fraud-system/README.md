# 反欺诈与反恐融资风险识别智能体系统

> **Anti-Fraud & CFT Risk Identification Agent System**  
> 大数据驱动的金融风控智能体系统 v2.0.0

---

## 项目概述

本系统是银行风险管理体系的核心组成部分，采用**大数据+知识库+AI智能体**三层架构，实现从"规则驱动"到"数据驱动+知识驱动"的智能化升级。

## 技术架构

```
大数据采集层 → 大数据处理层 → 风控知识库层 → AI智能体层 → 服务层
   Kafka        Flink/Spark     MySQL/FAISS   LangChain    Spring Cloud
   CDC          Redis           ES/Milvus     Drools       gRPC/REST
```

## 项目结构

```
anti-fraud-system/
├── pom.xml                          # Maven父POM
├── anti-fraud-common/               # 公共模块(数据模型/工具类/常量)
├── anti-fraud-data-collection/      # 数据采集(Kafka生产者/Debezium CDC)
├── anti-fraud-flink/                # Flink实时计算(数据清洗/特征提取)
├── anti-fraud-spark/                # Spark离线特征计算
├── anti-fraud-knowledge-base/       # 知识库(Python/FAISS/RAG)
├── anti-fraud-agent/                # AI智能体(Spring Boot/Drools/ML)
├── anti-fraud-microservices/        # 微服务模块
│   ├── transaction-monitor/         #   交易监控服务
│   ├── alert-management/            #   预警管理服务
│   ├── case-management/             #   案例管理服务
│   ├── report-generation/           #   报告生成服务
│   └── api-gateway/                 #   API网关
├── anti-fraud-sql/                  # 数据库初始化脚本
├── anti-fraud-deployment/           # 部署配置
│   ├── docker-compose.yml           #   本地开发环境
│   └── kubernetes/                  #   K8s生产部署
└── anti-fraud-testing/              # 测试代码
    ├── unit/                        #   单元测试(JUnit+Mockito)
    ├── integration/                 #   集成测试(TestContainers)
    └── performance/jmeter/          #   性能压测(JMeter)
```

## 模块清单 (50个文件)

### 1. 数据采集与接入
| 文件 | 说明 |
|------|------|
| `TransactionKafkaProducer.java` | Kafka生产者，模拟交易流水数据 |
| `debezium-cdc-customer.json` | Debezium MySQL CDC配置 |

### 2. 特征计算
| 文件 | 说明 |
|------|------|
| `FraudDetectionFlinkJob.java` | Flink实时计算主程序 |
| `DataCleanFunction.java` | 数据清洗(去重/脱敏/标准化) |
| `FeatureExtractionProcess.java` | 实时特征提取(1h/24h窗口) |
| `RedisFeatureSink.java` | Redis在线特征存储 |
| `ClickHouseSink.java` | ClickHouse离线分析存储 |
| `OfflineFeatureJob.scala` | Spark离线特征(30天维度) |

### 3. 知识库与向量化
| 文件 | 说明 |
|------|------|
| `knowledge_base_service.py` | Python知识库服务(FAISS/RAG/gRPC) |
| `init-schema.sql` | MySQL 8张核心表(规则/黑名单/案例/预警等) |

### 4. AI智能体决策引擎
| 文件 | 说明 |
|------|------|
| `AntiFraudAgentService.java` | gRPC智能体服务(核⼼评估接口) |
| `DroolsRuleEngine.java` | Drools规则引擎(动态刷新) |
| `fraud-rules.drl` | 30+条风控规则(6大类) |
| `MLModelClient.java` | TensorFlow Serving模型推理 |
| `DecisionFusionEngine.java` | 决策融合(40%规则+60%模型) |
| `anti_fraud_agent.proto` | gRPC Proto定义 |

### 5. 微服务模块
| 文件 | 说明 |
|------|------|
| `TransactionMonitorController.java` | 交易监控REST接口 |
| `TransactionAssessService.java` | gRPC调用智能体+异步预警 |
| `AlertManagementController.java` | 预警管理(创建/处理/升级/通知) |
| `AlertManagementService.java` | 预警业务逻辑(邮件通知) |

### 6. 部署与运维
| 文件 | 说明 |
|------|------|
| `docker-compose.yml` | 13个服务(Kafka/MySQL/Redis/ClickHouse/Milvus/Flink/Agent等) |
| `agent-deployment.yaml` | K8s Deployment+HPA+Service |
| `values.yaml` | Helm可配置参数 |
| `prometheus.yml` | Prometheus抓取配置 |
| `anti-fraud-alerts.yml` | 5条告警规则 |
| `anti-fraud-dashboard.json` | Grafana监控大盘(8个面板) |
| `Dockerfile` | Agent/KB容器镜像 |

### 7. 测试与验证
| 文件 | 说明 |
|------|------|
| `AntiFraudAgentServiceTest.java` | 9个单元测试(规则/融合/边界/公式) |
| `TransactionE2EIntegrationTest.java` | 5个集成测试(TestContainers) |
| `anti-fraud-jmeter.jmx` | JMeter压测(5000TPS) |

## 快速开始

### 本地开发
```bash
cd anti-fraud-system/anti-fraud-deployment
docker-compose up -d

# 启动交易模拟
mvn exec:java -pl anti-fraud-data-collection \
  -Dexec.mainClass="com.bank.risk.collection.producer.TransactionKafkaProducer" \
  -Dproducer.rate=100

# 提交Flink作业
flink run -c com.bank.risk.flink.FraudDetectionFlinkJob anti-fraud-flink.jar
```

### 构建
```bash
mvn clean package -DskipTests
```

### 运行测试
```bash
mvn test -pl anti-fraud-testing
mvn verify -Pintegration-test
```

### K8s部署
```bash
helm install anti-fraud ./anti-fraud-deployment/kubernetes/helm-chart \
  --namespace risk-management --create-namespace
```

## 核心指标

| 指标 | 目标 |
|------|------|
| 交易处理能力 | ≥5000 TPS |
| P99决策延迟 | ≤500ms |
| 知识库检索 | ≤2s |
| 系统可用性 | ≥99.9% |
| 高风险识别准确率 | ≥95% |
