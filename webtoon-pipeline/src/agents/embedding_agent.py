"""Stage B Agent: CLIP 임베딩 추출 → Chroma upsert + face_embedding DB 저장.

episode.phase1a.complete 또는 episode.reembed.start 메시지를 받아 에피소드 내
모든 FaceRecord에 대해 CLIP 임베딩을 추출하고 모델별 Chroma 컬렉션에 upsert한다.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import faust

from src.agents.ocr_yolo import EpisodePhase1aComplete, episode_phase1a_complete
from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import fetch_face_crop
from src.operators.embedding import extract_embedding, EMBEDDING_MODEL_NAME
from src.worker import app


# ── Kafka 메시지 스키마 ───────────────────────────────────────────────────────

class EpisodePhase1bComplete(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int


class WebtoonReembedStart(faust.Record):
    source: str
    title_id: str
    webtoon_id: int


# ── Kafka 토픽 ────────────────────────────────────────────────────────────────

episode_phase1b_complete = app.topic("episode.phase1b.complete", value_type=EpisodePhase1bComplete)
webtoon_reembed_start = app.topic("webtoon.reembed.start", value_type=WebtoonReembedStart)


# ── DB 헬퍼 ───────────────────────────────────────────────────────────────────

def _load_face_records(webtoon_episode_id: int) -> list[dict]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fr.face_idx,
                   fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fr.conf, wc.id AS cut_id, wc.cut_number
            FROM face_record fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            WHERE wc.episode_id = %s AND fr.deleted_at IS NULL
            ORDER BY wc.cut_number, fr.face_idx
            """,
            (webtoon_episode_id,),
        )
        return [
            {
                "id": row[0], "face_idx": row[1],
                "bbox": [row[2], row[3], row[4], row[5]],
                "conf": row[6], "cut_id": row[7], "cut_number": row[8],
            }
            for row in cur.fetchall()
        ]


def _get_webtoon_info(webtoon_episode_id: int) -> dict:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT w.id, w.source, w.title_id
            FROM webtoon_episode we
            JOIN webtoon w ON we.webtoon_id = w.id
            WHERE we.id = %s
            """,
            (webtoon_episode_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"webtoon_episode_id={webtoon_episode_id} not found")
        return {"webtoon_id": row[0], "source": row[1], "title_id": row[2]}


def _upsert_face_embedding(face_id: int, model: str, doc_id: str, score: Optional[float]) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO face_embedding
                (face_record_id, embedding_model, chroma_doc_id, match_score, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (face_record_id, embedding_model)
            DO UPDATE SET chroma_doc_id = EXCLUDED.chroma_doc_id,
                          match_score   = EXCLUDED.match_score,
                          updated_at    = EXCLUDED.updated_at
            """,
            (face_id, model, doc_id, score, now, now),
        )


def _get_first_episode(webtoon_id: int) -> Optional[dict]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.id, we.no, w.source, w.title_id
            FROM webtoon_episode we
            JOIN webtoon w ON we.webtoon_id = w.id
            WHERE we.webtoon_id = %s
            ORDER BY we.no ASC
            LIMIT 1
            """,
            (webtoon_id,),
        )
        row = cur.fetchone()
        return {"id": row[0], "no": row[1], "source": row[2], "title_id": row[3]} if row else None


# ── 핵심 처리 ────────────────────────────────────────────────────────────────

def _embed_episode(
    webtoon_episode_id: int,
    episode_no: int,
    source: str,
    title_id: str,
    webtoon_id: int,
) -> None:
    """에피소드 내 모든 FaceRecord에 CLIP 임베딩 추출 후 Chroma upsert."""
    collection = get_face_collection(source, title_id, EMBEDDING_MODEL_NAME)
    face_records = _load_face_records(webtoon_episode_id)

    for face in face_records:
        crop_bytes = fetch_face_crop(face["id"], source, title_id)
        if crop_bytes is None:
            print(f"[embedding_agent] crop not found face_id={face['id']}, skip")
            continue

        embedding = extract_embedding(crop_bytes)
        doc_id = f"{webtoon_id}_{episode_no}_{face['cut_number']}_F{face['face_idx']}"

        b = face["bbox"]
        collection.upsert(
            ids=[doc_id],
            embeddings=[embedding],
            metadatas=[{
                "webtoon_id": webtoon_id,
                "episode": episode_no,
                "cut": face["cut_number"],
                "face_idx": face["face_idx"],
                "embedding_model": EMBEDDING_MODEL_NAME,
                "bbox_x1": b[0], "bbox_y1": b[1], "bbox_x2": b[2], "bbox_y2": b[3],
                "conf": face["conf"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }],
        )
        _upsert_face_embedding(face["id"], EMBEDDING_MODEL_NAME, doc_id, None)


# ── Faust Agents ──────────────────────────────────────────────────────────────

@app.agent(episode_phase1a_complete, concurrency=1)
async def embedding_agent(stream):
    loop = asyncio.get_running_loop()
    async for msg in stream:
        try:
            webtoon = await loop.run_in_executor(
                None, _get_webtoon_info, msg.webtoon_episode_id
            )
            await loop.run_in_executor(
                None, _embed_episode,
                msg.webtoon_episode_id, msg.episode_no,
                webtoon["source"], webtoon["title_id"], webtoon["webtoon_id"],
            )
        except Exception as e:
            print(f"[embedding_agent] {msg.source}/{msg.title_id} ep={msg.episode_no} error: {e}")
            import traceback; traceback.print_exc()
            continue

        await episode_phase1b_complete.send(
            key=f"{msg.source}_{msg.title_id}",
            value=EpisodePhase1bComplete(
                source=msg.source,
                title_id=msg.title_id,
                episode_no=msg.episode_no,
                webtoon_episode_id=msg.webtoon_episode_id,
            ),
        )


@app.agent(webtoon_reembed_start, concurrency=1)
async def reembed_agent(stream):
    """webtoon.reembed.start → 첫 에피소드부터 임베딩+매칭 체인 재시작."""
    loop = asyncio.get_running_loop()
    async for msg in stream:
        try:
            first_ep = await loop.run_in_executor(
                None, _get_first_episode, msg.webtoon_id
            )
            if not first_ep:
                print(f"[reembed_agent] no episodes for webtoon_id={msg.webtoon_id}")
                continue

            await episode_phase1a_complete.send(
                key=f"{msg.source}_{msg.title_id}",
                value=EpisodePhase1aComplete(
                    source=msg.source,
                    title_id=msg.title_id,
                    episode_no=first_ep["no"],
                    webtoon_episode_id=first_ep["id"],
                    total_cuts=0,
                ),
            )
            print(f"[reembed_agent] triggered reembed from ep={first_ep['no']} webtoon_id={msg.webtoon_id}")
        except Exception as e:
            print(f"[reembed_agent] webtoon_id={msg.webtoon_id} error: {e}")
