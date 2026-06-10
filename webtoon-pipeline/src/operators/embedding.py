"""model-api HTTP 클라이언트 — CLIP 임베딩."""
from __future__ import annotations

import time
import logging

import httpx

from src.config.settings import MODEL_API_URL

logger = logging.getLogger(__name__)

EMBEDDING_MODEL_NAME = "clip"  # Chroma 컬렉션명 및 face_embedding.embedding_model 저장값

_RETRYABLE = (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError)
_MAX_RETRIES = 3
_RETRY_BACKOFF = (2, 4, 8)  # seconds

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(60.0))
    return _client


def extract_embedding(image_bytes: bytes) -> list[float]:
    """이미지 바이트 → CLIP 임베딩 (768-dim, L2 정규화된 float list). 일시적 오류는 최대 3회 재시도."""
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            response = _get_client().post(
                f"{MODEL_API_URL}/embed",
                files={"file": ("image.jpg", image_bytes, "image/jpeg")},
            )
            response.raise_for_status()
            return response.json()["embedding"]
        except _RETRYABLE as e:
            last_exc = e
            wait = _RETRY_BACKOFF[attempt]
            logger.warning("[embedding] attempt=%d/%d %s: %s — retry in %ds", attempt + 1, _MAX_RETRIES, type(e).__name__, e, wait)
            time.sleep(wait)
        except httpx.HTTPStatusError:
            raise
    raise last_exc
