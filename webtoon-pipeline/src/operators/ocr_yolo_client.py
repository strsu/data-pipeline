"""model-api HTTP 클라이언트 — OCR/YOLO (GPU 서버 단일 호출)."""
from __future__ import annotations

import logging
import time
from io import BytesIO

import httpx
from PIL import Image

from src.config.settings import (
    OCR_API_URL,
    OCR_YOLO_API_URL,
    YOLO_API_URL,
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


def run_ocr(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> list[dict]:
    """OCR — GPU 서버(OCR_API_URL) 호출. ocr_blocks 반환."""
    params = {"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut}
    t0 = time.perf_counter()
    result = _post_image(OCR_API_URL, "/ocr", image_bytes, params)["ocr"]
    logger.info("[ocr_yolo] /ocr 성공 — %.2fs, %d건", time.perf_counter() - t0, len(result))
    return result


def run_yolo(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> list[dict]:
    """YOLO — GPU 서버(YOLO_API_URL) 호출. faces 반환."""
    params = {"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut}
    t0 = time.perf_counter()
    result = _post_image(YOLO_API_URL, "/yolo", image_bytes, params)["faces"]
    logger.info("[ocr_yolo] /yolo 성공 — %.2fs, %d건", time.perf_counter() - t0, len(result))
    return result


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
