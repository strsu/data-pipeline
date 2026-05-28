# build stage
FROM python:3.12-slim AS builder

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential python3-dev gcc ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir uv

WORKDIR /project

COPY pyproject.toml /project/pyproject.toml

RUN uv pip compile pyproject.toml > requirements.txt && \
    uv pip install --system --no-cache -r requirements.txt && \
    rm -f requirements.txt

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
