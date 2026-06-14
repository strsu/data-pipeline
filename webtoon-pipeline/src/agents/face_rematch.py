"""미매칭 얼굴 재매칭 Agent — appearance_id IS NULL인 얼굴을 Chroma로 재매칭 (face.rematch)."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import faust

from src.config import settings
from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import fetch_face_crop
from src.operators.embedding import extract_embedding, EMBEDDING_MODEL_NAME
from src.worker import app

logger = logging.getLogger(__name__)

MATCH_THRESHOLD = settings.MATCH_THRESHOLD


class FaceRematchMsg(faust.Record):
    face_record_id: int
    source: str
    title_id: str
    webtoon_id: int


face_rematch_topic = app.topic("face.rematch", value_type=FaceRematchMsg)


def _load_unmatched_face(face_record_id: int) -> Optional[dict]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fr.face_idx,
                   fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fr.conf,
                   wc.cut_number, we.no AS episode_no, we.id AS episode_id
            FROM face_record fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            JOIN webtoon_episode we ON wc.episode_id = we.id
            WHERE fr.id = %s
              AND fr.is_confirmed = false
              AND fr.appearance_id IS NULL
              AND fr.deleted_at IS NULL
            """,
            (face_record_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0], "face_idx": row[1],
            "bbox": [row[2], row[3], row[4], row[5]],
            "conf": row[6],
            "cut_number": row[7], "episode_no": row[8], "episode_id": row[9],
        }


def _assign_appearance(face_id: int, appearance_id: int) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            "UPDATE face_record SET appearance_id = %s, updated_at = %s WHERE id = %s",
            (appearance_id, now, face_id),
        )


def _rematch_face(msg: FaceRematchMsg) -> None:
    face = _load_unmatched_face(msg.face_record_id)
    if face is None:
        logger.warning("[face_rematch] face_id=%s not eligible (already matched, confirmed, or deleted), skip", msg.face_record_id)
        return

    crop_bytes = fetch_face_crop(face["id"], msg.source, msg.title_id)
    if crop_bytes is None:
        logger.warning("[face_rematch] crop not found face_id=%s, skip", face["id"])
        return

    embedding = extract_embedding(crop_bytes)
    collection = get_face_collection(msg.source, msg.title_id, EMBEDDING_MODEL_NAME)

    if collection.count() == 0:
        logger.warning("[face_rematch] face_id=%s Chroma collection empty, skip", face["id"])
        return

    query_result = collection.query(
        query_embeddings=[embedding],
        n_results=1,
        include=["metadatas", "distances"],
    )

    has_match = bool(query_result["ids"][0])
    if not has_match:
        logger.info("[face_rematch] face_id=%s no candidates in collection, skip", face["id"])
        return

    best_distance: float = query_result["distances"][0][0]
    best_meta: dict = query_result["metadatas"][0][0]

    if best_distance > MATCH_THRESHOLD or "appearance_id" not in best_meta:
        logger.info(
            "[face_rematch] face_id=%s no match (best_distance=%.4f threshold=%.4f), skip",
            face["id"], best_distance, MATCH_THRESHOLD,
        )
        return

    appearance_id: int = best_meta["appearance_id"]
    character_name: str = best_meta.get("character_name") or best_meta.get("character_id", "unknown")

    _assign_appearance(face["id"], appearance_id)

    doc_id = f"{msg.webtoon_id}_{face['episode_no']}_{face['cut_number']}_F{face['face_idx']}"
    b = face["bbox"]
    collection.upsert(
        ids=[doc_id],
        embeddings=[embedding],
        metadatas=[{
            "webtoon_id": msg.webtoon_id,
            "episode": face["episode_no"],
            "cut": face["cut_number"],
            "face_idx": face["face_idx"],
            "character_id": character_name,
            "appearance_id": appearance_id,
            "appearance_label": best_meta.get("appearance_label", "기본"),
            "character_name": character_name,
            "is_confirmed": False,
            "bbox_x1": b[0], "bbox_y1": b[1], "bbox_x2": b[2], "bbox_y2": b[3],
            "conf": face["conf"] or 0.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }],
    )

    logger.info(
        "[face_rematch] matched face_id=%s -> character=%s appearance_id=%s distance=%.4f ep=%s cut=%s",
        face["id"], character_name, appearance_id, best_distance, face["episode_no"], face["cut_number"],
    )


@app.agent(face_rematch_topic, concurrency=4)
async def face_rematch_agent(stream):
    loop = asyncio.get_running_loop()
    async for msg in stream:
        try:
            await loop.run_in_executor(None, _rematch_face, msg)
        except Exception as e:
            logger.error("[face_rematch] face_id=%s error: %s: %s", msg.face_record_id, type(e).__name__, e)
