"""model-api HTTP 클라이언트 — OCR/YOLO."""
from __future__ import annotations

import httpx

from src.config.settings import MODEL_API_URL

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(60.0))
    return _client


def run_ocr_yolo(image_bytes: bytes) -> tuple[list[dict], list[dict]]:
    """model-api /ocr-yolo 호출 → (ocr_blocks, faces) 반환."""
    response = _get_client().post(
        f"{MODEL_API_URL}/ocr-yolo",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
    )
    response.raise_for_status()
    data = response.json()
    return data["ocr"], data["faces"]
