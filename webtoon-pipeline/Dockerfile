# build stage
FROM python:3.12-slim AS builder

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential python3-dev gcc ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -U pip setuptools wheel

WORKDIR /project

COPY pyproject.toml /project/pyproject.toml

# 1단계: CPU-only torch 먼저 설치 (PyPI 기본 인덱스는 Linux에서 CUDA 버전 반환)
# uv pip compile/install은 재해결 시 CUDA로 덮어씌우므로 pip 직접 사용
RUN pip install --no-cache-dir torch torchvision \
    --index-url https://download.pytorch.org/whl/cpu

# 2단계: 나머지 패키지 설치 — torch가 이미 설치돼 있으므로 pip이 재설치하지 않음
RUN pip install --no-cache-dir \
    faust-streaming==0.10.22 \
    psycopg2-binary==2.9.11 \
    boto3==1.38.21 \
    requests==2.32.5 \
    pillow==11.2.1 \
    numpy==2.2.6 \
    onnxruntime==1.21.1 \
    paddleocr==2.10.0 \
    ultralytics==8.3.151

# runtime stage
FROM python:3.12-slim

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on

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

CMD ["python", "-m", "faust", "-A", "src.worker", "worker", "-l", "info"]
