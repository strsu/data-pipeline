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

# ── 과병합(magnet)·과분할(파편화) 방지 파라미터 (env로 튜닝, 코드 하드코딩 없음) ──────
# v1(앵커캡+무조건 마진)의 마진 룰은 "2등=다른 인물"을 전제해, 같은 인물이 중복 클러스터로
# 쪼개진 순간 1·2등이 모두 그 인물이라 영구 기각→파편 무한생산(자기강화)했다 — 화산귀환 실측
# 876얼굴→773클러스터. 873얼굴 72조합 오프라인 시뮬(prd §18.5) 결과로 v2 채택(2026-07-07):
#   1) 앵커 캡: 인물(appearance)당 매칭 앵커를 conf 상위 K개로 제한(비교 비용·magnet 완화).
#   2) 통계량: 인물별 min이 아니라 **가까운 TOPK개 평균**(기본 3, 1이면 옛 min 동작) —
#      앵커 1개의 우연한 근접(min의 요행, magnet의 근본)을 걸러낸다.
#   3) 마진 룰(면제형): 2등도 threshold 이내면 "중복 클러스터 경합"이므로 마진 없이 1등 배정
#      (+호출측에 병합 후보 신호). 2등이 threshold 밖일 때만 마진 미달 시 보류(경계 얼굴 보호).
# ⚠️ threshold 의미가 통계량에 결속: TOPK 평균 기준 권장값 0.12 (min 기준 0.16과 다름).
_MAX_ANCHORS_PER_APPEARANCE = int(os.getenv("CCIP_MAX_ANCHORS_PER_APPEARANCE", "12") or "12")
_MATCH_MARGIN = float(os.getenv("CCIP_MATCH_MARGIN", "0.03") or "0.03")
_MATCH_TOPK = int(os.getenv("CCIP_MATCH_TOPK", "3") or "3")


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
    margin: float = _MATCH_MARGIN, topk: int = _MATCH_TOPK,
) -> Optional[dict]:
    """캐시된 anchor 목록(§load_ccip_anchors) 대상 CCIP metric 비교 — v2(topk 평균 + 면제 마진).

    인물(appearance)별로 가까운 topk개 diff의 **평균**을 통계량으로 쓴다(topk<=1이면 min).
    판정(시뮬 근거는 prd §18.5):
      - 1등 stat > threshold → None(신규).
      - 2등 stat ≤ threshold → 중복 클러스터 경합 신호 — 마진 없이 1등 배정.
        2등과의 차이 < margin이면 반환값에 `ambiguous_with`(2등 대표 meta)를 실어
        호출측(step2)이 병합 후보 suggestion을 발행하게 한다.
      - 2등 stat > threshold → (1등-2등 차이) < margin이면 경계 모호 → None(보류, 옛 마진 유지).
    """
    if not anchors:
        return None

    res = ccip_compare(feature, [a["embedding"] for a in anchors])
    diffs = res.get("diffs") or []
    if not diffs:
        return None

    # appearance_id별 diff 목록 + 대표(최소 diff) 앵커 인덱스.
    by_app: dict = {}  # aid -> [diffs...], rep: aid -> (min_diff, idx)
    rep_by_app: dict = {}
    for i, d in enumerate(diffs):
        if d is None:
            continue
        aid = anchors[i]["meta"].get("appearance_id")
        if aid is None:
            continue
        by_app.setdefault(aid, []).append(d)
        cur = rep_by_app.get(aid)
        if cur is None or d < cur[0]:
            rep_by_app[aid] = (d, i)
    if not by_app:
        return None

    k = max(1, topk)
    ranked = sorted(
        (sum(sorted(ds)[:k]) / min(k, len(ds)), aid) for aid, ds in by_app.items()
    )  # (stat, appearance_id) 오름차순
    best_stat, best_aid = ranked[0]
    if best_stat > threshold:
        return None

    match = {"meta": anchors[rep_by_app[best_aid][1]]["meta"], "score": float(best_stat)}
    if len(ranked) >= 2:
        second_stat, second_aid = ranked[1]
        if second_stat <= threshold:
            # 중복 클러스터 경합 — 배정하되, 근소 차이면 병합 후보 신호를 동봉.
            if margin > 0 and (second_stat - best_stat) < margin:
                match["ambiguous_with"] = anchors[rep_by_app[second_aid][1]]["meta"]
        elif margin > 0 and (second_stat - best_stat) < margin:
            return None  # 경계 모호(2등은 threshold 밖 = 타인/미지 후보) — 보류
    return match
