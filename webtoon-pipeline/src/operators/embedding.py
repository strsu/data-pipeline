"""model-api HTTP 클라이언트 — CLIP 임베딩 / CCIP feature·metric."""
from __future__ import annotations

import time
import logging

import httpx

from src.config.settings import EMBED_CLIP_API_URL, EMBED_CCIP_API_URL

logger = logging.getLogger(__name__)

# 기존 코드 호환용 기본 모델명 상수 (해석 함수 미사용 경로 폴백).
EMBEDDING_MODEL_NAME = "clip"  # Chroma 컬렉션명 및 face_embedding.embedding_model 저장값

# model-api는 OOM 방지를 위해 200요청마다 워커를 리사이클한다. 그 재시작 윈도우에서
# 처리 중이던 요청은 connection reset(ReadError) 또는 Server disconnected
# (RemoteProtocolError)로, 다운 중 새 요청은 ConnectError로 끊긴다. 모두 일시적이라
# HTTP 한 호출만 가볍게 재시도해 에피소드 액티비티 전체 재실행을 피한다.
_RETRYABLE = (
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.RemoteProtocolError,
)
_MAX_RETRIES = 5
_RETRY_BACKOFF = (60, 60, 60, 60, 60)  # seconds — 200요청마다 워커 리사이클되는 재시작 윈도우를 견딘다

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(60.0))
    return _client


def _post_with_retry(url: str, **kwargs):
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            response = _get_client().post(url, **kwargs)
            response.raise_for_status()
            if attempt > 0:
                logger.info("[embedding] %s 재시도 성공 (attempt=%d/%d)",
                            url, attempt + 1, _MAX_RETRIES)
            return response
        except _RETRYABLE as e:
            last_exc = e
            wait = _RETRY_BACKOFF[attempt]
            logger.warning("[embedding] %s 일시 오류 attempt=%d/%d %s: %s — %d초 후 재시도",
                           url, attempt + 1, _MAX_RETRIES, type(e).__name__, e, wait)
            time.sleep(wait)
        except httpx.HTTPStatusError as e:
            logger.error("[embedding] %s HTTP 상태 오류 status=%s — 재시도 안 함",
                         url, e.response.status_code)
            raise
    logger.error("[embedding] %s 재시도 %d회 모두 실패 — 마지막 오류 %s: %s",
                 url, _MAX_RETRIES, type(last_exc).__name__, last_exc)
    raise last_exc


def extract_embedding(image_bytes: bytes) -> list[float]:
    """이미지 바이트 → CLIP 임베딩 (768-dim, L2 정규화). 일시적 오류 최대 3회 재시도."""
    response = _post_with_retry(
        f"{EMBED_CLIP_API_URL}/embed",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
    )
    return response.json()["embedding"]


def extract_ccip_feature(image_bytes: bytes) -> list[float]:
    """이미지 바이트 → CCIP feature (float list). 판정은 ccip_compare로."""
    response = _post_with_retry(
        f"{EMBED_CCIP_API_URL}/embed-ccip",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
    )
    return response.json()["feature"]


def ccip_compare(query: list[float], anchors: list[list[float]]) -> dict:
    """query feature vs anchor features들의 CCIP metric 차이.

    반환: {"diffs": [...], "min_diff": float|None, "argmin": int|None}
    """
    if not anchors:
        return {"diffs": [], "min_diff": None, "argmin": None}
    response = _post_with_retry(
        f"{EMBED_CCIP_API_URL}/ccip-compare",
        json={"query": query, "anchors": anchors},
    )
    return response.json()


def embed_for(metric_type: str, image_bytes: bytes) -> list[float]:
    """metric_type에 따른 feature 추출 (cosine=CLIP, ccip=CCIP)."""
    if metric_type == "ccip":
        return extract_ccip_feature(image_bytes)
    return extract_embedding(image_bytes)
