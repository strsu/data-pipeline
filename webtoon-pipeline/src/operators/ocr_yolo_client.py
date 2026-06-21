"""model-api HTTP 클라이언트 — OCR/YOLO (분리 엔드포인트 + 레거시 결합)."""
from __future__ import annotations

from io import BytesIO

import httpx
from PIL import Image

from src.config.settings import OCR_API_URL, YOLO_API_URL, OCR_YOLO_API_URL

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(60.0))
    return _client


def run_ocr(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> list[dict]:
    """model-api /ocr 호출 → ocr_blocks 반환 (OCR 전용 서비스)."""
    response = _get_client().post(
        f"{OCR_API_URL}/ocr",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
        params={"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut},
    )
    response.raise_for_status()
    return response.json()["ocr"]


def run_yolo(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> list[dict]:
    """model-api /yolo 호출 → faces 반환 (YOLO 전용 서비스)."""
    response = _get_client().post(
        f"{YOLO_API_URL}/yolo",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
        params={"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut},
    )
    response.raise_for_status()
    return response.json()["faces"]


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
