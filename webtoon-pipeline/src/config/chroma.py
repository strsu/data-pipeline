"""Chroma HTTP 클라이언트 — 웹툰별 독립 컬렉션 (§5.1, §5.6)."""
from __future__ import annotations

import chromadb
from chromadb.config import Settings as ChromaSettings

from src.config import settings

_client: chromadb.HttpClient | None = None


def get_chroma_client() -> chromadb.HttpClient:
    global _client
    if _client is None:
        chroma_settings = ChromaSettings(
            chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
            chroma_client_auth_credentials=settings.CHROMA_AUTH_TOKEN,
        )
        _client = chromadb.HttpClient(
            host=settings.CHROMA_HOST,
            port=settings.CHROMA_PORT,
            settings=chroma_settings,
        )
    return _client


def get_face_collection(source: str, title_id: str) -> chromadb.Collection:
    """웹툰별 얼굴 임베딩 컬렉션 반환 (없으면 생성)."""
    return get_chroma_client().get_or_create_collection(
        name=f"character_faces_{source}_{title_id}",
        metadata={"hnsw:space": "cosine"},
    )
