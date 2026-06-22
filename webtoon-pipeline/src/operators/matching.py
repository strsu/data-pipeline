"""metric_type 분기 매칭 (§B3) — cosine(Chroma 코사인) / ccip(metric 비교).

반환 형식: {"meta": <chroma metadata dict>, "score": float} 또는 None.
meta에는 appearance_id / character_name 등이 들어있다(확정/기존 캐릭터 doc).

ccip는 Chroma의 ANN 인덱스로 비교할 수 없는 전용 metric(§ccip_difference)이라
anchor 전체를 브루트포스로 비교해야 한다. anchor 목록은 에피소드 1회만
`load_ccip_anchors`로 적재해 호출측(step2)이 캐시로 들고 있다가 매 얼굴마다
재사용한다(매 얼굴마다 collection.get() 전체 재조회하지 않음).
"""
from __future__ import annotations

from typing import Optional, TypedDict

from src.operators.embedding import ccip_compare


class CcipAnchor(TypedDict):
    embedding: list[float]
    meta: dict


def load_ccip_anchors(collection, excluded_appearance_ids: Optional[list[int]] = None) -> list[CcipAnchor]:
    """ccip 매칭용 anchor 캐시를 1회 적재. 에피소드 처리 시작 시 한 번만 호출."""
    got = collection.get(include=["embeddings", "metadatas"])
    embeddings = got.get("embeddings")
    embeddings = [] if embeddings is None else list(embeddings)
    metadatas = got.get("metadatas")
    metadatas = [] if metadatas is None else list(metadatas)

    excluded = set(excluded_appearance_ids or [])
    anchors: list[CcipAnchor] = []
    for emb, meta in zip(embeddings, metadatas):
        if meta and "appearance_id" in meta and meta["appearance_id"] not in excluded:
            anchors.append({"embedding": list(emb), "meta": meta})
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


def _find_match_ccip(feature: list[float], threshold: float, anchors: list[CcipAnchor]) -> Optional[dict]:
    """캐시된 anchor 목록(§load_ccip_anchors) 대상 CCIP metric 비교."""
    if not anchors:
        return None

    res = ccip_compare(feature, [a["embedding"] for a in anchors])
    min_diff = res.get("min_diff")
    argmin = res.get("argmin")
    if min_diff is not None and argmin is not None and min_diff <= threshold:
        return {"meta": anchors[argmin]["meta"], "score": float(min_diff)}
    return None
