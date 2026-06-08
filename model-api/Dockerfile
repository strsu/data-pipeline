# build stage
FROM python:3.12-slim AS builder

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential python3-dev gcc ca-certificates libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /project

COPY pyproject.toml /project/

# CPU-only torch 먼저 설치 (PyPI 기본 인덱스는 Linux에서 CUDA 버전 반환)
RUN pip install --no-cache-dir torch torchvision \
    --index-url https://download.pytorch.org/whl/cpu

RUN uv pip install --system --no-cache -r pyproject.toml

# CLIP 모델 빌드 시점에 캐시 (기동 시 다운로드 없음)
ENV HF_HOME=/project/hf_cache
RUN python -c "\
from transformers import CLIPModel, CLIPProcessor; \
CLIPModel.from_pretrained('openai/clip-vit-large-patch14'); \
CLIPProcessor.from_pretrained('openai/clip-vit-large-patch14'); \
print('CLIP model cached.')"

# runtime stage
FROM python:3.12-slim

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python \
    HF_HOME=/project/hf_cache \
    PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK=True

# libgomp1: PaddlePaddle OpenMP / libgl1: OpenCV
RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /project/hf_cache /project/hf_cache

RUN find /usr/local/lib/python3.12/site-packages -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true \
    && find /usr/local/lib/python3.12/site-packages -type f -name "*.pyc" -delete 2>/dev/null || true

WORKDIR /project

COPY src    /project/src
COPY models /project/models

# PaddleOCR 모델 빌드 시점에 캐시
RUN python -c "from paddleocr import PaddleOCR; PaddleOCR(use_doc_orientation_classify=False, use_doc_unwarping=False, use_textline_orientation=False, engine='paddle', lang='korean', enable_mkldnn=False)"

CMD ["gunicorn", "src.main:app", \
     "--worker-class=uvicorn.workers.UvicornWorker", \
     "--workers=2", \
     "--max-requests=50", \
     "--max-requests-jitter=10", \
     "--timeout=120", \
     "--bind=0.0.0.0:8000"]
