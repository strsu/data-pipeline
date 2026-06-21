"""metric_type 분기 매칭 (§B3) — cosine(Chroma 코사인) / ccip(metric 비교).

반환 형식: {"meta": <chroma metadata dict>, "score": float} 또는 None.
meta에는 appearance_id / character_name 등이 들어있다(확정/기존 캐릭터 doc).
"""
from __future__ import annotations

from typing import Optional

from src.operators.embedding import ccip_compare


def find_match(
    collection,
    feature: list[float],
    metric_type: str,
    threshold: float,
    excluded_appearance_ids: Optional[list[int]] = None,
) -> Optional[dict]:
    if metric_type == "ccip":
        return _find_match_ccip(collection, feature, threshold, excluded_appearance_ids)
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
    collection, feature: list[float], threshold: float, excluded_appearance_ids: Optional[list[int]] = None
) -> Optional[dict]:
    """컬렉션의 appearance_id 보유 doc feature를 앵커로 모아 CCIP metric 비교."""
    got = collection.get(include=["embeddings", "metadatas"])
    embeddings = got.get("embeddings")
    embeddings = [] if embeddings is None else list(embeddings)
    metadatas = got.get("metadatas")
    metadatas = [] if metadatas is None else list(metadatas)

    excluded = set(excluded_appearance_ids or [])
    anchors: list[list[float]] = []
    anchor_metas: list[dict] = []
    for emb, meta in zip(embeddings, metadatas):
        if meta and "appearance_id" in meta and meta["appearance_id"] not in excluded:
            anchors.append(list(emb))
            anchor_metas.append(meta)

    if not anchors:
        return None

    res = ccip_compare(feature, anchors)
    min_diff = res.get("min_diff")
    argmin = res.get("argmin")
    if min_diff is not None and argmin is not None and min_diff <= threshold:
        return {"meta": anchor_metas[argmin], "score": float(min_diff)}
    return None
