"""model-api HTTP 클라이언트 — CLIP 임베딩."""
from __future__ import annotations

import httpx

from src.config.settings import MODEL_API_URL

EMBEDDING_MODEL_NAME = "clip"  # Chroma 컬렉션명 및 face_embedding.embedding_model 저장값

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(30.0))
    return _client


def extract_embedding(image_bytes: bytes) -> list[float]:
    """이미지 바이트 → CLIP 임베딩 (768-dim, L2 정규화된 float list)."""
    response = _get_client().post(
        f"{MODEL_API_URL}/embed",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
    )
    response.raise_for_status()
    return response.json()["embedding"]
