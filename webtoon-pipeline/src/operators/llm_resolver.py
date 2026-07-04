"""웹툰별 LLM 모델 해석 (Step3) — 임베딩 model_resolver와 동일 패턴.

규칙: WebtoonLLMSetting(is_enabled) > LLMModel(is_default).
provider/model_id/endpoint 등은 전부 DB에서 읽는다(코드에 'glm' 하드코딩 없음 — LLM 추상, default=GLM).
"""
from __future__ import annotations

import json
import threading
from typing import Optional

from src.config.db import db_cursor

# 시드 누락 대비 폴백 (현재 기본값과 동일: vllm 경유 비전 모델). endpoint 생략 → VLLM_API_HOST.
_FALLBACK = {
    "id": None,
    "name": "glm-4.6v",
    "provider": "vllm",
    "model_id": "glm-4.6v",
    "params": {"temperature": 0.2},
    "supports_vision": True,
}

_cache: dict[int, dict] = {}
_lock = threading.Lock()

# 폴백 모델 이름 패턴(name ILIKE) — llm_model에 이 이름을 포함하는 활성 행을 등록해두면
# 비전 콜이 기본 모델로 재시도까지 모두 실패했을 때 3차 시도로 쓰인다(Req 7.4 확장).
_FALLBACK_MODEL_NAME_PATTERN = "%qwen%"
_fallback_cache: Optional[dict] = None
_fallback_lock = threading.Lock()


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


def resolve_fallback_llm_model() -> Optional[dict]:
    """비전/텍스트 콜 폴백 모델 조회(Req 7.4 3차 시도) — 웹툰별 설정과 무관한 전역 폴백.

    `llm_model`에 이름이 `_FALLBACK_MODEL_NAME_PATTERN`(예: qwen)에 매칭하는 활성 행이
    등록돼 있으면 그 모델을 반환한다. 미등록이면 None(호출부는 폴백 없이 기존대로 스킵).
    """
    global _fallback_cache
    with _fallback_lock:
        if _fallback_cache is not None:
            return _fallback_cache.get("model")

    result: Optional[dict] = None
    try:
        with db_cursor() as cur:
            cur.execute(
                """
                SELECT id, name, provider, model_id, params, supports_vision
                FROM llm_model
                WHERE is_active = true AND deleted_at IS NULL AND name ILIKE %s
                ORDER BY id ASC
                LIMIT 1
                """,
                (_FALLBACK_MODEL_NAME_PATTERN,),
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
        print(f"[llm_resolver] fallback resolve 실패, 폴백 생략: {e}")

    with _fallback_lock:
        _fallback_cache = {"model": result}
    return result


def clear_cache(webtoon_id: Optional[int] = None) -> None:
    global _fallback_cache
    with _lock:
        if webtoon_id is None:
            _cache.clear()
        else:
            _cache.pop(webtoon_id, None)
    with _fallback_lock:
        _fallback_cache = None
