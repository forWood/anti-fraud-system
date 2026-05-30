# ============================================================================
# 知识库服务 Dockerfile (Python)
# ============================================================================
FROM python:3.9-slim

LABEL maintainer="银行科技部"
LABEL description="风控知识库向量化服务"

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY src/ src/

# 创建运行时目录（代码内置默认配置，无需外部文件）
RUN mkdir -p /app/config /app/data/faiss_index

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8088/health')" || exit 1

EXPOSE 8088 9091

# 启动
CMD ["python", "src/services/knowledge_base_service.py"]
