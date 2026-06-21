"""웹툰별 LLM 모델 해석 (Step3) — 임베딩 model_resolver와 동일 패턴.

규칙: WebtoonLLMSetting(is_enabled) > LLMModel(is_default).
provider/model_id/endpoint 등은 전부 DB에서 읽는다(코드에 'glm' 하드코딩 없음 — LLM 추상, default=GLM).
"""
from __future__ import annotations

import json
import threading
from typing import Optional

from src.config.db import db_cursor

# 시드 누락 대비 폴백 (마이그레이션 0010 시드와 동일: 비전 모델 default)
_FALLBACK = {
    "id": None,
    "name": "glm-5v-turbo",
    "provider": "zai",
    "model_id": "glm-5v-turbo",
    "params": {"endpoint": "https://api.z.ai/api/paas/v4/chat/completions", "temperature": 0.2},
    "supports_vision": True,
}

_cache: dict[int, dict] = {}
_lock = threading.Lock()


def _parse_params(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def resolve_llm_model(webtoon_id: int) -> dict:
    """반환: {"id", "name", "provider", "model_id", "params"(dict), "supports_vision"}."""
    cached = _cache.get(webtoon_id)
    if cached is not None:
        return cached

    result: Optional[dict] = None
    try:
        with db_cursor() as cur:
            cur.execute(
                """
                SELECT lm.id, lm.name, lm.provider, lm.model_id, lm.params, lm.supports_vision
                FROM webtoon_llm_setting wls
                JOIN llm_model lm ON wls.llm_model_id = lm.id
                WHERE wls.webtoon_id = %s
                  AND wls.is_enabled = true
                  AND wls.deleted_at IS NULL
                  AND lm.is_active = true
                ORDER BY wls.id DESC
                LIMIT 1
                """,
                (webtoon_id,),
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    SELECT id, name, provider, model_id, params, supports_vision
                    FROM llm_model
                    WHERE is_default = true AND is_active = true AND deleted_at IS NULL
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                )
                row = cur.fetchone()
            if row is not None:
                result = {
                    "id": row[0],
                    "name": row[1],
                    "provider": row[2],
                    "model_id": row[3],
                    "params": _parse_params(row[4]),
                    "supports_vision": bool(row[5]),
                }
    except Exception as e:
        print(f"[llm_resolver] resolve 실패 webtoon_id={webtoon_id}, 폴백 사용: {e}")

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
