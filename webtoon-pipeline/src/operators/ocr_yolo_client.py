"""model-api HTTP 클라이언트 — OCR/YOLO."""
from __future__ import annotations

from io import BytesIO

import httpx
from PIL import Image

from src.config.settings import OCR_YOLO_API_URL

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(60.0))
    return _client


def run_ocr_yolo(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> tuple[list[dict], list[dict]]:
    """model-api /ocr-yolo 호출 → (ocr_blocks, faces) 반환."""
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
