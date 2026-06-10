"""Step 2b Agent: 개별 얼굴 Chroma 동기화 — 수동 재배정 즉시 반영 (face.chroma.sync)."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import faust

from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import fetch_face_crop
from src.operators.embedding import extract_embedding, EMBEDDING_MODEL_NAME
from src.worker import app

logger = logging.getLogger(__name__)


class FaceChromaSyncMsg(faust.Record):
    face_record_id: int
    source: str
    title_id: str
    webtoon_id: int


face_chroma_sync_topic = app.topic("face.chroma.sync", value_type=FaceChromaSyncMsg)


def _load_face_for_sync(face_record_id: int) -> Optional[dict]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fr.face_idx,
                   fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fr.conf, fr.appearance_id,
                   wc.cut_number, we.no AS episode_no,
                   ca.label AS appearance_label,
                   c.name AS character_name,
                   fe.chroma_doc_id
            FROM face_record fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            JOIN webtoon_episode we ON wc.episode_id = we.id
            JOIN character_appearance ca ON fr.appearance_id = ca.id
            JOIN character c ON ca.character_id = c.id
            LEFT JOIN face_embedding fe ON fe.face_record_id = fr.id AND fe.embedding_model = %s
            WHERE fr.id = %s AND fr.deleted_at IS NULL
            """,
            (EMBEDDING_MODEL_NAME, face_record_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0], "face_idx": row[1],
            "bbox": [row[2], row[3], row[4], row[5]],
            "conf": row[6], "appearance_id": row[7],
            "cut_number": row[8], "episode_no": row[9],
            "appearance_label": row[10], "character_name": row[11],
            "chroma_doc_id": row[12],
        }


def _sync_face(msg: FaceChromaSyncMsg) -> None:
    face = _load_face_for_sync(msg.face_record_id)
    if face is None:
        logger.warning("[face_chroma_sync] face_id=%s not found or no appearance, skip", msg.face_record_id)
        return

    crop_bytes = fetch_face_crop(face["id"], msg.source, msg.title_id)
    if crop_bytes is None:
        logger.warning("[face_chroma_sync] crop not found face_id=%s, skip", face["id"])
        return

    embedding = extract_embedding(crop_bytes)

    doc_id = face["chroma_doc_id"] or (
        f"{msg.webtoon_id}_{face['episode_no']}_{face['cut_number']}_F{face['face_idx']}"
    )

    collection = get_face_collection(msg.source, msg.title_id, EMBEDDING_MODEL_NAME)
    b = face["bbox"]
    collection.upsert(
        ids=[doc_id],
        embeddings=[embedding],
        metadatas=[{
            "webtoon_id": msg.webtoon_id,
            "episode": face["episode_no"],
            "cut": face["cut_number"],
            "face_idx": face["face_idx"],
            "character_id": face["character_name"],
            "appearance_id": face["appearance_id"],
            "appearance_label": face["appearance_label"],
            "character_name": face["character_name"],
            "is_confirmed": True,
            "bbox_x1": b[0], "bbox_y1": b[1], "bbox_x2": b[2], "bbox_y2": b[3],
            "conf": face["conf"] or 0.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }],
    )
    logger.info("[face_chroma_sync] synced face_id=%s -> %s doc_id=%s", face["id"], face["character_name"], doc_id)


# concurrency=4: I/O 바운드(S3 + Chroma)이고 순서 무관하므로 병렬 처리
@app.agent(face_chroma_sync_topic, concurrency=4)
async def face_chroma_sync_agent(stream):
    loop = asyncio.get_running_loop()
    async for msg in stream:
        try:
            await loop.run_in_executor(None, _sync_face, msg)
        except Exception as e:
            logger.error("[face_chroma_sync] face_id=%s error: %s: %s", msg.face_record_id, type(e).__name__, e)
