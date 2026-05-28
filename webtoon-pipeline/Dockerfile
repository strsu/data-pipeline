# build stage
FROM python:3.12-slim AS builder

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential python3-dev gcc ca-certificates libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /project

COPY pyproject.toml uv.lock /project/

# 1단계: CPU-only torch 먼저 설치 (PyPI 기본 인덱스는 Linux에서 CUDA 버전 반환)
RUN pip install --no-cache-dir torch torchvision \
    --index-url https://download.pytorch.org/whl/cpu

# 2단계: uv로 나머지 패키지 설치 (torch는 이미 설치됨, system Python에 직접 설치)
RUN uv pip install --system --no-cache -r pyproject.toml

# runtime stage
FROM python:3.12-slim

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# libgomp1: PaddlePaddle OpenMP / libgl1: OpenCV
RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

RUN find /usr/local/lib/python3.12/site-packages -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true \
    && find /usr/local/lib/python3.12/site-packages -type f -name "*.pyc" -delete 2>/dev/null || true

WORKDIR /project

COPY src    /project/src
COPY models /project/models

ENV PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=True

RUN python -c "from paddleocr import PaddleOCR; PaddleOCR(use_doc_orientation_classify=False, use_doc_unwarping=False, use_textline_orientation=False, engine='paddle', lang='korean', enable_mkldnn=False)"

CMD ["python", "-m", "faust", "-A", "src.worker", "worker", "-l", "info"]
