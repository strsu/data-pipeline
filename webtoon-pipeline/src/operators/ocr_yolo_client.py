"""model-api HTTP 클라이언트 — OCR/YOLO (GPU 우선 + 기존 API 폴백)."""
from __future__ import annotations

import logging
import time
from io import BytesIO

import httpx
from PIL import Image

from src.config.settings import (
    OCR_API_URL,
    OCR_API_PRIORITY_URL,
    OCR_YOLO_API_URL,
    YOLO_API_URL,
    YOLO_API_PRIORITY_URL,
)

logger = logging.getLogger(__name__)

# 호출 타임아웃(초). 대부분 초 단위 응답이지만 콜드스타트/큰 세그먼트 대비 60초.
_HTTP_TIMEOUT = 60.0

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(_HTTP_TIMEOUT))
    return _client


def _post_image(base_url: str, path: str, image_bytes: bytes, params: dict) -> dict:
    response = _get_client().post(
        f"{base_url.rstrip('/')}{path}",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
        params=params,
    )
    response.raise_for_status()
    return response.json()


def _post_with_fallback(
    path: str, priority: str, fallback: str, image_bytes: bytes, params: dict, key: str
) -> list[dict]:
    """priority(GPU) 호출 → 실패/무응답 시 fallback(원래 CPU 서버)로 재시도."""
    # 1) GPU(priority) 우선 호출
    t0 = time.perf_counter()
    try:
        result = _post_image(priority, path, image_bytes, params)[key]
        logger.info(
            "[ocr_yolo] %s GPU(priority=%s) 성공 — %.2fs, %d건",
            path, priority, time.perf_counter() - t0, len(result),
        )
        return result
    except Exception as e:
        # fallback이 없거나 priority와 동일하면 폴백 불가 — 그대로 전파
        if not fallback or fallback == priority:
            logger.error(
                "[ocr_yolo] %s GPU(priority=%s) 실패(폴백 없음) — %.2fs: %s",
                path, priority, time.perf_counter() - t0, type(e).__name__,
            )
            raise
        logger.warning(
            "[ocr_yolo] %s GPU(priority=%s) 실패 — %.2fs: %s — fallback(%s) 시도",
            path, priority, time.perf_counter() - t0, type(e).__name__, fallback,
        )

    # 2) CPU(fallback) 재시도
    t1 = time.perf_counter()
    result = _post_image(fallback, path, image_bytes, params)[key]
    logger.info(
        "[ocr_yolo] %s fallback(%s) 성공 — %.2fs, %d건",
        path, fallback, time.perf_counter() - t1, len(result),
    )
    return result


def run_ocr(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> list[dict]:
    """OCR — GPU(priority) 우선, 실패 시 원래 OCR_API_URL 폴백. ocr_blocks 반환."""
    params = {"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut}
    return _post_with_fallback("/ocr", OCR_API_PRIORITY_URL, OCR_API_URL, image_bytes, params, "ocr")


def run_yolo(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> list[dict]:
    """YOLO — GPU(priority) 우선, 실패 시 원래 YOLO_API_URL 폴백. faces 반환."""
    params = {"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut}
    return _post_with_fallback("/yolo", YOLO_API_PRIORITY_URL, YOLO_API_URL, image_bytes, params, "faces")


def run_ocr_yolo(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> tuple[list[dict], list[dict]]:
    """model-api /ocr-yolo 결합 호출 → (ocr_blocks, faces). 레거시 호환용."""
    w, h = Image.open(BytesIO(image_bytes)).size
    print(f"[ocr_yolo_client] {source}/{title_id} ep={episode_no} cut={cut} — 전송 {w}x{h} ({len(image_bytes):,} bytes)")
    response = _get_client().post(
        f"{OCR_YOLO_API_URL}/ocr-yolo",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
        params={"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut},
    )
    response.raise_for_status()
    data = response.json()
    return data["ocr"], data["faces"]
