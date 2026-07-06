"""metric_type 분기 매칭 (§B3) — cosine(Chroma 코사인) / ccip(metric 비교).

반환 형식: {"meta": <chroma metadata dict>, "score": float} 또는 None.
meta에는 appearance_id / character_name 등이 들어있다(확정/기존 캐릭터 doc).

ccip는 Chroma의 ANN 인덱스로 비교할 수 없는 전용 metric(§ccip_difference)이라
anchor 전체를 브루트포스로 비교해야 한다. anchor 목록은 에피소드 1회만
`load_ccip_anchors`로 적재해 호출측(step2)이 캐시로 들고 있다가 매 얼굴마다
재사용한다(매 얼굴마다 collection.get() 전체 재조회하지 않음).
"""
from __future__ import annotations

import os
from typing import Optional, TypedDict

from src.operators.embedding import ccip_compare

# ── 과병합(magnet) 방지 파라미터 (env로 튜닝, 코드 하드코딩 없음) ──────────────────
# 문제: load_ccip_anchors가 Chroma의 모든 저장 얼굴을 앵커로 싣던 탓에, 얼굴이 많은 인물은
# 앵커가 무제한 누적(예: 화산귀환 초삼'C 207개)돼 acceptance 영역이 넓어지고 아무 비슷한 얼굴이나
# 빨아들이는 magnet이 됐다(과병합 blob). 대책:
#   1) 앵커 캡: 인물(appearance)당 매칭에 쓰는 앵커를 conf 상위 K개로 제한.
#   2) 마진 룰: 최근접 인물이 2등(다른 인물)보다 margin 이상 더 가까울 때만 확정, 애매하면 보류(신규).
_MAX_ANCHORS_PER_APPEARANCE = int(os.getenv("CCIP_MAX_ANCHORS_PER_APPEARANCE", "12") or "12")
_MATCH_MARGIN = float(os.getenv("CCIP_MATCH_MARGIN", "0.03") or "0.03")


class CcipAnchor(TypedDict):
    embedding: list[float]
    meta: dict


def load_ccip_anchors(
    collection,
    excluded_appearance_ids: Optional[list[int]] = None,
    max_per_appearance: int = _MAX_ANCHORS_PER_APPEARANCE,
) -> list[CcipAnchor]:
    """ccip 매칭용 anchor 캐시를 1회 적재(에피소드 처리 시작 시 한 번).

    인물(appearance_id)당 앵커를 conf 상위 `max_per_appearance`개로 제한한다(magnet 방지).
    max_per_appearance<=0이면 무제한(옛 동작).
    """
    got = collection.get(include=["embeddings", "metadatas"])
    embeddings = got.get("embeddings")
    embeddings = [] if embeddings is None else list(embeddings)
    metadatas = got.get("metadatas")
    metadatas = [] if metadatas is None else list(metadatas)

    excluded = set(excluded_appearance_ids or [])
    by_app: dict = {}
    for emb, meta in zip(embeddings, metadatas):
        if not meta or "appearance_id" not in meta or meta["appearance_id"] in excluded:
            continue
        by_app.setdefault(meta["appearance_id"], []).append(
            {"embedding": list(emb), "meta": meta}
        )

    anchors: list[CcipAnchor] = []
    for lst in by_app.values():
        if max_per_appearance and max_per_appearance > 0 and len(lst) > max_per_appearance:
            # conf(검출 신뢰도) 상위 K개 — 깨끗한 크롭 위주로 대표 앵커 구성.
            lst = sorted(lst, key=lambda a: a["meta"].get("conf", 0.0) or 0.0, reverse=True)
            lst = lst[:max_per_appearance]
        anchors.extend(lst)
    return anchors


def find_match(
    collection,
    feature: list[float],
    metric_type: str,
    threshold: float,
    excluded_appearance_ids: Optional[list[int]] = None,
    ccip_anchors: Optional[list[CcipAnchor]] = None,
) -> Optional[dict]:
    if metric_type == "ccip":
        return _find_match_ccip(feature, threshold, ccip_anchors or [])
    return _find_match_cosine(collection, feature, threshold, excluded_appearance_ids)


def _find_match_cosine(
    collection, feature: list[float], threshold: float, excluded_appearance_ids: Optional[list[int]] = None
) -> Optional[dict]:
    try:
        if collection.count() == 0:
            return None
    except Exception:
        pass
    where = {"appearance_id": {"$nin": excluded_appearance_ids}} if excluded_appearance_ids else None
    qr = collection.query(
        query_embeddings=[feature],
        n_results=1,
        where=where,
        include=["metadatas", "distances"],
    )
    if not qr["ids"][0]:
        return None
    distance = qr["distances"][0][0]
    meta = qr["metadatas"][0][0]
    if distance is not None and distance <= threshold and meta and "appearance_id" in meta:
        return {"meta": meta, "score": distance}
    return None


def _find_match_ccip(
    feature: list[float], threshold: float, anchors: list[CcipAnchor],
    margin: float = _MATCH_MARGIN,
) -> Optional[dict]:
    """캐시된 anchor 목록(§load_ccip_anchors) 대상 CCIP metric 비교 + 마진 룰.

    인물(appearance)별 최소 diff를 구해, 최근접 인물이 (a) threshold 이내이고 (b) 2등(다른 인물)보다
    margin 이상 더 가까울 때만 그 인물로 확정한다. 두 인물이 margin 이내로 붙어 애매하면(시각적으로
    비슷한 인물 경계) None → 신규 클러스터로 보류(잘못된 병합보다 안전). margin<=0이면 순수 1-NN.
    """
    if not anchors:
        return None

    res = ccip_compare(feature, [a["embedding"] for a in anchors])
    diffs = res.get("diffs") or []
    if not diffs:
        return None

    # appearance_id별 최소 diff와 그 대표 앵커 인덱스.
    best_by_app: dict = {}
    for i, d in enumerate(diffs):
        if d is None:
            continue
        aid = anchors[i]["meta"].get("appearance_id")
        if aid is None:
            continue
        cur = best_by_app.get(aid)
        if cur is None or d < cur[0]:
            best_by_app[aid] = (d, i)
    if not best_by_app:
        return None

    ranked = sorted(best_by_app.values(), key=lambda x: x[0])  # (diff, idx) 오름차순
    best_diff, best_i = ranked[0]
    if best_diff > threshold:
        return None
    # 마진: 2등(다른 인물)이 margin 이내면 모호 → 확정하지 않음(신규/보류).
    if margin > 0 and len(ranked) >= 2 and (ranked[1][0] - best_diff) < margin:
        return None
    return {"meta": anchors[best_i]["meta"], "score": float(best_diff)}
