"""웹툰별 임베딩 모델/threshold 해석 (§B2).

규칙: WebtoonEmbeddingSetting(is_enabled) > EmbeddingModel(is_default).
threshold: setting.threshold ?? model.default_threshold.
파이프라인은 raw SQL로 읽고 웹툰 단위 처리 동안 캐시한다.
"""
from __future__ import annotations

import threading
from typing import Optional

from src.config.db import db_cursor

# 마이그레이션 전/시드 누락 대비 폴백 (기존 하드코딩 값과 동일)
_FALLBACK = {"name": "clip", "metric_type": "cosine", "threshold": 0.25}

_cache: dict[int, dict] = {}
_lock = threading.Lock()


def resolve_embedding_model(webtoon_id: int) -> dict:
    """반환: {"name": str, "metric_type": "cosine"|"ccip", "threshold": float}."""
    cached = _cache.get(webtoon_id)
    if cached is not None:
        return cached

    result: Optional[dict] = None
    try:
        with db_cursor() as cur:
            cur.execute(
                """
                SELECT em.name, em.metric_type, COALESCE(wes.threshold, em.default_threshold)
                FROM config_webtoon_embedding_setting wes
                JOIN config_embedding_model em ON wes.embedding_model_id = em.id
                WHERE wes.webtoon_id = %s
                  AND wes.is_enabled = true
                  AND wes.deleted_at IS NULL
                  AND em.is_active = true
                ORDER BY wes.id DESC
                LIMIT 1
                """,
                (webtoon_id,),
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    SELECT name, metric_type, default_threshold
                    FROM config_embedding_model
                    WHERE is_default = true AND is_active = true AND deleted_at IS NULL
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                )
                row = cur.fetchone()
            if row is not None:
                result = {"name": row[0], "metric_type": row[1], "threshold": float(row[2])}
    except Exception as e:
        print(f"[model_resolver] resolve 실패 webtoon_id={webtoon_id}, 폴백 사용: {e}")

    if result is None:
        result = dict(_FALLBACK)

    with _lock:
        _cache[webtoon_id] = result
    return result


def clear_cache(webtoon_id: Optional[int] = None) -> None:
    with _lock:
        if webtoon_id is None:
            _cache.clear()
        else:
            _cache.pop(webtoon_id, None)
