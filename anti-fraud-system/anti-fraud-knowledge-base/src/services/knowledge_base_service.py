"""
知识库向量化服务 - Python 3.9+
=================================
将风控案例文本向量化，基于 FAISS 构建向量索引，
通过 gRPC 对外提供相似案例检索服务。

功能:
1. 使用 SentenceTransformer 将案例文本向量化
2. FAISS 索引构建与持久化
3. gRPC 检索服务: 输入交易特征文本，返回 Top-K 相似案例
4. LangChain RAG 检索链

依赖:
    pip install sentence-transformers faiss-cpu grpcio grpcio-tools langchain langchain-community
"""
import json
import logging
import os
from concurrent import futures
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

import grpc
# 假设已从 proto 生成 (实际项目中使用 grpcio-tools 编译)
# import knowledge_base_pb2
# import knowledge_base_pb2_grpc

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)


# ============================================================================
# 数据模型
# ============================================================================

@dataclass
class FraudCase:
    """欺诈案例模型"""
    case_id: str
    case_title: str
    case_type: str                 # FRAUD / AML / CTF
    case_level: str                # HIGH / MEDIUM / LOW
    case_description: str          # 完整描述
    modus_operandi: str            # 作案手法
    fraud_pattern: str             # 欺诈模式标签
    key_features: Dict             # 关键特征
    involved_amount: float
    resolution: str                # 处置方案
    resolution_effect: str         # 处置效果
    vector: Optional[np.ndarray] = None  # 文本向量 (768/256维)
    similarity: float = 0.0        # 检索相似度


@dataclass
class KnowledgeSearchResult:
    """知识检索结果"""
    query: str
    results: List[FraudCase]
    search_time_ms: float
    total_index_size: int


# ============================================================================
# 向量化引擎
# ============================================================================

class KnowledgeVectorizer:
    """
    知识库文本向量化引擎
    
    使用 SentenceTransformer 将中文案例文本转换为向量，
    支持多种预训练模型。
    """
    
    # 可选模型及其特点
    AVAILABLE_MODELS = {
        "text2vec-base": {
            "name": "shibing624/text2vec-base-chinese",
            "dimension": 768,
            "description": "通用中文文本向量 (推荐)"
        },
        "bge-large": {
            "name": "BAAI/bge-large-zh-v1.5",
            "dimension": 1024,
            "description": "BGE大模型 (精度更高)"
        },
        "bge-small": {
            "name": "BAAI/bge-small-zh-v1.5",
            "dimension": 512,
            "description": "BGE轻量版 (速度快)"
        }
    }
    
    def __init__(self, model_key: str = "text2vec-base", device: str = "cpu"):
        if model_key not in self.AVAILABLE_MODELS:
            raise ValueError(f"Unknown model: {model_key}. Available: {list(self.AVAILABLE_MODELS.keys())}")
        
        model_config = self.AVAILABLE_MODELS[model_key]
        self.model_name = model_config["name"]
        self.vector_dimension = model_config["dimension"]
        
        logger.info(f"Loading embedding model: {self.model_name} (dim={self.vector_dimension})")
        self.model = SentenceTransformer(self.model_name, device=device)
        logger.info("Embedding model loaded successfully")
    
    def encode(self, texts: List[str], batch_size: int = 32) -> np.ndarray:
        """
        将文本列表编码为向量
        
        Args:
            texts: 待编码的文本列表
            batch_size: 批处理大小
        
        Returns:
            向量矩阵 (N, dimension)
        """
        vectors = self.model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=len(texts) > 100,
            normalize_embeddings=True  # L2归一化，适配内积检索
        )
        return vectors
    
    def encode_single(self, text: str) -> np.ndarray:
        """编码单个文本"""
        return self.encode([text])[0]
    
    def build_case_text(self, case: FraudCase) -> str:
        """
        构建案例的索引文本
        将案例的多个字段拼接为一个可检索的文本
        """
        parts = [
            f"案例标题: {case.case_title}",
            f"案例类型: {case.case_type}",
            f"欺诈模式: {case.fraud_pattern or '未知'}",
            f"作案手法: {case.modus_operandi or '暂无'}",
            f"案例描述: {case.case_description}",
            f"处置方案: {case.resolution or '暂无'}",
        ]
        return "\n".join(parts)
    
    def build_transaction_text(self, features: Dict) -> str:
        """
        构建交易特征的检索文本
        用于查询时与案例进行匹配
        """
        parts = [
            f"交易金额: {features.get('amount', 'N/A')}",
            f"交易类型: {features.get('transaction_type', 'N/A')}",
            f"渠道: {features.get('channel', 'N/A')}",
            f"时间: {features.get('transaction_time', 'N/A')}",
            f"1小时交易次数: {features.get('count_1h', 0)}",
            f"24小时交易次数: {features.get('count_24h', 0)}",
            f"是否新设备: {features.get('is_first_device', False)}",
            f"是否跨境: {features.get('is_cross_border', False)}",
            f"IP风险: {features.get('ip_risk_level', 'NORMAL')}",
            f"30天设备数: {features.get('device_count_30d', 0)}",
            f"30天城市数: {features.get('city_count_30d', 0)}",
            f"金额偏离度: {features.get('amount_deviation', 0)}",
        ]
        return "\n".join(parts)


# ============================================================================
# FAISS 向量索引管理
# ============================================================================

class FAISSIndexManager:
    """
    FAISS 向量索引管理器
    
    功能:
    - 构建和维护内积索引 (IndexFlatIP) 用于余弦相似度检索
    - 索引持久化 (保存/加载)
    - Top-K 相似检索
    """
    
    def __init__(self, dimension: int, index_path: str = "./data/faiss_index"):
        """
        Args:
            dimension: 向量维度
            index_path: 索引文件存储路径
        """
        self.dimension = dimension
        self.index_path = index_path
        self.index: Optional[faiss.IndexFlatIP] = None
        self.case_metadata: List[Dict] = []  # 案例元数据
        
        # 初始化或加载索引
        os.makedirs(os.path.dirname(index_path), exist_ok=True)
        if os.path.exists(f"{index_path}.index"):
            self.load()
        else:
            self.index = faiss.IndexFlatIP(dimension)  # 内积检索 = 余弦相似度 (归一化后)
            logger.info(f"Created new FAISS index: dimension={dimension}")
    
    def add(self, vectors: np.ndarray, metadata_list: List[Dict]):
        """
        添加向量到索引
        
        Args:
            vectors: 向量矩阵 (N, dimension), dtype=float32
            metadata_list: 元数据列表 (与向量一一对应)
        """
        if vectors.shape[1] != self.dimension:
            raise ValueError(f"Vector dimension mismatch: {vectors.shape[1]} vs {self.dimension}")
        
        vectors = vectors.astype(np.float32)
        start_idx = self.index.ntotal
        self.index.add(vectors)
        
        for i, meta in enumerate(metadata_list):
            meta["_faiss_idx"] = start_idx + i
            self.case_metadata.append(meta)
        
        logger.info(f"Added {len(vectors)} vectors to index. Total: {self.index.ntotal}")
    
    def search(self, query_vector: np.ndarray, top_k: int = 5) -> List[Dict]:
        """
        检索 Top-K 相似向量
        
        Args:
            query_vector: 查询向量 (dimension,)
            top_k: 返回数量
        
        Returns:
            结果列表 [{"metadata": {...}, "similarity": float}, ...]
        """
        if self.index.ntotal == 0:
            logger.warning("FAISS index is empty")
            return []
        
        query_vector = query_vector.astype(np.float32).reshape(1, -1)
        k = min(top_k, self.index.ntotal)
        distances, indices = self.index.search(query_vector, k)
        
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx >= 0 and idx < len(self.case_metadata):
                results.append({
                    **self.case_metadata[idx],
                    "similarity": float(dist)
                })
        
        return results
    
    def save(self):
        """保存索引和元数据到磁盘"""
        faiss.write_index(self.index, f"{self.index_path}.index")
        with open(f"{self.index_path}.metadata.json", "w", encoding="utf-8") as f:
            json.dump(self.case_metadata, f, ensure_ascii=False, indent=2)
        logger.info(f"Index saved: {self.index.ntotal} vectors")
    
    def load(self):
        """从磁盘加载索引和元数据"""
        self.index = faiss.read_index(f"{self.index_path}.index")
        metadata_path = f"{self.index_path}.metadata.json"
        if os.path.exists(metadata_path):
            with open(metadata_path, "r", encoding="utf-8") as f:
                self.case_metadata = json.load(f)
        logger.info(f"Index loaded: {self.index.ntotal} vectors")
    
    def rebuild(self, vectors: np.ndarray, metadata_list: List[Dict]):
        """
        重建整个索引 (清空后重新构建)
        
        用于定期重建/模型更新等场景
        """
        self.index = faiss.IndexFlatIP(self.dimension)
        self.case_metadata = []
        self.add(vectors, metadata_list)
        logger.info(f"Index rebuilt with {self.index.ntotal} vectors")
    
    def get_total(self) -> int:
        """获取索引中的向量总数"""
        return self.index.ntotal if self.index else 0


# ============================================================================
# RAG 检索增强
# ============================================================================

class FraudCaseRAG:
    """
    欺诈案例 RAG 检索增强
    
    将交易特征作为查询，检索相似历史案例，生成增强上下文
    可用于 LangChain 的检索链
    """
    
    def __init__(self, 
                 vectorizer: KnowledgeVectorizer,
                 faiss_manager: FAISSIndexManager):
        self.vectorizer = vectorizer
        self.faiss_manager = faiss_manager
    
    def retrieve(self, transaction_features: Dict, top_k: int = 5) -> KnowledgeSearchResult:
        """
        根据交易特征检索相似案例
        
        Args:
            transaction_features: 交易特征字典
            top_k: 返回Top-K个最相似案例
        
        Returns:
            KnowledgeSearchResult
        """
        import time
        start_time = time.time()
        
        # 1. 构建查询文本
        query_text = self.vectorizer.build_transaction_text(transaction_features)
        
        # 2. 向量化查询
        query_vector = self.vectorizer.encode_single(query_text)
        
        # 3. FAISS检索
        raw_results = self.faiss_manager.search(query_vector, top_k)
        
        # 4. 构建返回结果
        cases = []
        for r in raw_results:
            case = FraudCase(
                case_id=r.get("case_id", ""),
                case_title=r.get("case_title", ""),
                case_type=r.get("case_type", ""),
                case_level=r.get("case_level", ""),
                case_description=r.get("case_description", ""),
                modus_operandi=r.get("modus_operandi", ""),
                fraud_pattern=r.get("fraud_pattern", ""),
                key_features=r.get("key_features", {}),
                involved_amount=r.get("involved_amount", 0.0),
                resolution=r.get("resolution", ""),
                resolution_effect=r.get("resolution_effect", ""),
                similarity=r.get("similarity", 0.0)
            )
            cases.append(case)
        
        search_time_ms = (time.time() - start_time) * 1000
        
        return KnowledgeSearchResult(
            query=query_text[:200],
            results=cases,
            search_time_ms=search_time_ms,
            total_index_size=self.faiss_manager.get_total()
        )
    
    def build_rag_context(self, transaction_features: Dict, top_k: int = 3) -> str:
        """
        构建 RAG 上下文文本
        
        将检索到的相似案例组织为 prompt 可用的上下文
        """
        result = self.retrieve(transaction_features, top_k)
        
        parts = ["## 相似历史案例 (RAG检索结果)\n"]
        for i, case in enumerate(result.results, 1):
            parts.append(f"### 案例{i}: {case.case_title} (相似度: {case.similarity:.2%})")
            parts.append(f"- **类型**: {case.case_type}")
            parts.append(f"- **模式**: {case.fraud_pattern}")
            parts.append(f"- **手法**: {case.modus_operandi}")
            parts.append(f"- **处置**: {case.resolution}")
            parts.append(f"- **效果**: {case.resolution_effect or '暂无'}")
            parts.append("")
        
        return "\n".join(parts)


# ============================================================================
# gRPC 服务
# ============================================================================

class KnowledgeBaseService:
    """
    知识库管理服务
    
    统一管理向量化、FAISS索引和RAG检索
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.vectorizer = KnowledgeVectorizer(
            model_key=config.get("model_key", "text2vec-base"),
            device=config.get("device", "cpu")
        )
        self.faiss_manager = FAISSIndexManager(
            dimension=self.vectorizer.vector_dimension,
            index_path=config.get("index_path", "./data/faiss_index")
        )
        self.rag = FraudCaseRAG(self.vectorizer, self.faiss_manager)
    
    def load_cases_from_db(self, db_config: Dict) -> int:
        """
        从MySQL数据库加载案例并构建索引
        
        Args:
            db_config: 数据库连接配置
        
        Returns:
            加载的案例数量
        """
        import pymysql
        
        logger.info("Loading cases from MySQL database...")
        conn = pymysql.connect(
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 3306),
            user=db_config.get("user", "risk_user"),
            password=db_config.get("password", ""),
            database=db_config.get("database", "risk_db"),
            charset="utf8mb4"
        )
        
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("""
                    SELECT case_id, case_title, case_type, case_level,
                           case_description, modus_operandi, fraud_pattern,
                           key_features, involved_amount, resolution,
                           resolution_effect
                    FROM fraud_case
                    ORDER BY created_at DESC
                """)
                rows = cursor.fetchall()
            
            if not rows:
                logger.warning("No cases found in database")
                return 0
            
            # 构建案例文本
            texts = []
            metadata_list = []
            for row in rows:
                case = FraudCase(
                    case_id=row["case_id"],
                    case_title=row["case_title"],
                    case_type=row["case_type"],
                    case_level=row["case_level"],
                    case_description=row["case_description"],
                    modus_operandi=row["modus_operandi"] or "",
                    fraud_pattern=row["fraud_pattern"] or "",
                    key_features=json.loads(row["key_features"]) if row.get("key_features") else {},
                    involved_amount=float(row["involved_amount"] or 0),
                    resolution=row["resolution"] or "",
                    resolution_effect=row["resolution_effect"] or ""
                )
                texts.append(self.vectorizer.build_case_text(case))
                metadata_list.append({
                    "case_id": case.case_id,
                    "case_title": case.case_title,
                    "case_type": case.case_type,
                    "case_level": case.case_level,
                    "case_description": case.case_description[:500],
                    "modus_operandi": case.modus_operandi,
                    "fraud_pattern": case.fraud_pattern,
                    "key_features": case.key_features,
                    "involved_amount": case.involved_amount,
                    "resolution": case.resolution,
                    "resolution_effect": case.resolution_effect
                })
            
            # 向量化
            logger.info(f"Vectorizing {len(texts)} case texts...")
            vectors = self.vectorizer.encode(texts)
            
            # 构建FAISS索引
            self.faiss_manager.rebuild(vectors, metadata_list)
            self.faiss_manager.save()
            
            logger.info(f"Successfully indexed {len(rows)} cases")
            return len(rows)
        finally:
            conn.close()
    
    def search_similar(self, features: Dict, top_k: int = 5) -> KnowledgeSearchResult:
        """检索相似案例"""
        return self.rag.retrieve(features, top_k)
    
    def get_rag_context(self, features: Dict, top_k: int = 3) -> str:
        """获取RAG上下文"""
        return self.rag.build_rag_context(features, top_k)


# ============================================================================
# 初始化与启动
# ============================================================================

def initialize_knowledge_base(config_path: str = "config/knowledge_base.yaml") -> KnowledgeBaseService:
    """
    初始化知识库服务
    
    Args:
        config_path: 配置文件路径
    """
    import yaml
    
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    else:
        config = {
            "model_key": "text2vec-base",
            "device": "cpu",
            "index_path": "./data/faiss_index",
            "database": {
                "host": "localhost",
                "port": 3306,
                "user": "risk_user",
                "password": "risk_pass_2026",
                "database": "risk_db"
            }
        }
    
    service = KnowledgeBaseService(config)
    
    # 从数据库加载案例索引
    if config.get("database"):
        count = service.load_cases_from_db(config["database"])
        logger.info(f"Knowledge base initialized with {count} cases")
    
    return service


if __name__ == "__main__":
    # 初始化服务
    kb_service = initialize_knowledge_base()
    
    # 示例查询
    test_features = {
        "amount": 800000.00,
        "transaction_type": "TRANSFER",
        "channel": "EBANK",
        "count_1h": 5,
        "count_24h": 15,
        "is_first_device": True,
        "is_cross_border": True,
        "ip_risk_level": "HIGH_RISK",
        "device_count_30d": 3,
        "city_count_30d": 5,
        "amount_deviation": 2.5
    }
    
    result = kb_service.search_similar(test_features, top_k=5)
    print(f"\n查询: {result.query[:100]}...")
    print(f"检索时间: {result.search_time_ms:.2f}ms")
    print(f"索引大小: {result.total_index_size}")
    print(f"\n相似案例:")
    for i, case in enumerate(result.results, 1):
        print(f"  {i}. {case.case_title} (相似度: {case.similarity:.2%})")
        print(f"     模式: {case.fraud_pattern}")
        print(f"     处置: {case.resolution[:100]}")
