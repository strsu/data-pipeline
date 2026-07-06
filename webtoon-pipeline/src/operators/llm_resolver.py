"""스테이지 role별 LLM 모델 해석 (Step3) — 임베딩 model_resolver와 동일 패턴.

역할(role) 2값:
  - "vision": 이미지 필요한 Stage V(컷 비전 추출). supports_vision=True 모델.
  - "text"  : 이미지 없는 Stage R(정체·화자)/N(서사). supports_vision=False 모델(예: glm-5.2).

규칙(전역 전용 — 웹툰별 override는 폐기, B1/§c): role의 기본 모델 =
    config_llm_model WHERE is_default AND is_active AND supports_vision = (role=='vision').
modality당 활성 기본은 DB 제약(uniq_active_default_llm_per_modality)으로 1개만 존재.
role default가 없으면(시드 전/롤백) any is_default로 강등 → 최악이라도 오늘 동작(전역 default) 유지.
provider/model_id/params 등은 전부 DB에서 읽는다(코드에 모델명 하드코딩 없음 — Req 7.1).

`fallback`(런타임 모델 전환)은 B1.5 과제 — 이 resolver는 아직 읽지 않는다.
"""
from __future__ import annotations

import json
import threading
from typing import Optional

from src.config.db import db_cursor

VISION = "vision"
TEXT = "text"

# 시드/DB 완전 부재 시의 최후 안전망(빈 DB 방어). role=text여도 비전 모델로 강등되지만
# 최종값은 동결/결정론으로 보존되므로 크래시보다 낫다. 정상 운영에선 도달하지 않는다.
_FALLBACK = {
    "id": None,
    "name": "glm-4.6v",
    "provider": "vllm",
    "model_id": "glm-4.6v",
    "params": {"temperature": 0.2},
    "supports_vision": True,
}

_cache: dict[tuple[int, str], dict] = {}
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


def _row_to_ctx(row) -> dict:
    return {
        "id": row[0],
        "name": row[1],
        "provider": row[2],
        "model_id": row[3],
        "params": _parse_params(row[4]),
        "supports_vision": bool(row[5]),
    }


def _resolve_fallback(cur, fallback_id) -> Optional[dict]:
    """fallback self-FK → 폴백 모델 ctx(1홉, 자기 폴백은 미포함 — 순환 방지). B1.5 런타임 전환용.

    비활성/삭제 폴백은 무시(None). ctx["fallback"]로 실려 call_llm_json이 primary 소진 시 사용.
    """
    if not fallback_id:
        return None
    cur.execute(
        """
        SELECT id, name, provider, model_id, params, supports_vision
        FROM config_llm_model
        WHERE id = %s AND is_active = true AND deleted_at IS NULL
        """,
        (fallback_id,),
    )
    frow = cur.fetchone()
    return _row_to_ctx(frow) if frow else None


def resolve_llm_model(webtoon_id: int, role: str = VISION) -> dict:
    """스테이지 role("vision"|"text")의 LLM 모델을 해석한다.

    반환: {"id", "name", "provider", "model_id", "params"(dict), "supports_vision"}.
    webtoon_id는 로깅/캐시 키 용도(전역 전용이라 조회엔 미사용 — 향후 per-webtoon 복원 대비 유지).
    """
    if role not in (VISION, TEXT):
        role = VISION
    key = (webtoon_id, role)
    cached = _cache.get(key)
    if cached is not None:
        return cached

    want_vision = role == VISION
    result: Optional[dict] = None
    try:
        with db_cursor() as cur:
            # 1) role 기본 모델(도출): is_default·is_active·supports_vision 일치.
            cur.execute(
                """
                SELECT id, name, provider, model_id, params, supports_vision, fallback_id
                FROM config_llm_model
                WHERE is_default = true AND is_active = true
                  AND supports_vision = %s AND deleted_at IS NULL
                ORDER BY id ASC
                LIMIT 1
                """,
                (want_vision,),
            )
            row = cur.fetchone()
            if row is None:
                # 2) 강등: modality 무관 아무 활성 기본(시드 전/롤백 경로 — 오늘 동작 유지).
                cur.execute(
                    """
                    SELECT id, name, provider, model_id, params, supports_vision, fallback_id
                    FROM config_llm_model
                    WHERE is_default = true AND is_active = true AND deleted_at IS NULL
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                )
                row = cur.fetchone()
                if row is not None:
                    print(f"[llm_resolver] role={role} 전용 기본 없음 — 전역 default로 강등: {row[1]}")
            if row is not None:
                result = _row_to_ctx(row)
                result["fallback"] = _resolve_fallback(cur, row[6])  # 런타임 폴백(B1.5)
    except Exception as e:
        print(f"[llm_resolver] resolve 실패 webtoon_id={webtoon_id} role={role}, 폴백 사용: {e}")

    if result is None:
        result = dict(_FALLBACK)

    with _lock:
        _cache[key] = result
    return result


def clear_cache(webtoon_id: Optional[int] = None) -> None:
    with _lock:
        if webtoon_id is None:
            _cache.clear()
        else:
            for k in [k for k in _cache if k[0] == webtoon_id]:
                _cache.pop(k, None)
