# syntax=docker/dockerfile:1.4
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 创建日志目录
RUN mkdir -p /app/logs && chmod 777 /app/logs

# 安装依赖
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# 拷贝源码
COPY . .

# 暴露服务端口（与 app/main.py 默认一致）
EXPOSE 1212

# 运行应用
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "1212"]
