"""Step 2 Agent: Chroma 기반 인물 식별 — 웹툰별 에피소드 순차 처리 (§Step 2, §18.3).

파티션 키 {source}_{title_id} → 같은 웹툰은 항상 같은 worker → 에피소드 순서 자동 보장.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import faust
from psycopg2.extras import Json

from src.agents.local_extract import EpisodePhase1Complete, episode_phase1_complete
from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import fetch_face_crop
from src.config import settings
from src.operators.embedding import extract_embedding
from src.worker import app

MATCH_THRESHOLD = settings.MATCH_THRESHOLD


# ── Kafka 토픽 ───────────────────────────────────────────────────────────────

class EpisodePhase3Start(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int


cut_phase3_start = app.topic("cut.phase3.start", value_type=EpisodePhase3Start)


# ── DB 헬퍼 ──────────────────────────────────────────────────────────────────

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


def _get_or_create_pipeline_state(webtoon_id: int) -> dict:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO webtoon_pipeline_state
                (webtoon_id, phase1_status, phase2_status, phase2_processed_count,
                 phase3_enabled, created_at, updated_at)
            VALUES (%s, 'idle', 'idle', 0, false, %s, %s)
            ON CONFLICT (webtoon_id) DO NOTHING
            """,
            (webtoon_id, now, now),
        )
        cur.execute(
            """
            SELECT wps.id, wps.phase2_status,
                   wps.phase2_last_completed_episode_id,
                   wps.phase2_processable_max_episode,
                   wps.phase2_processed_count,
                   wps.phase3_enabled,
                   last_ep.no AS last_completed_no
            FROM webtoon_pipeline_state wps
            LEFT JOIN webtoon_episode last_ep
                   ON wps.phase2_last_completed_episode_id = last_ep.id
            WHERE wps.webtoon_id = %s
            """,
            (webtoon_id,),
        )
        row = cur.fetchone()
        return {
            "id": row[0],
            "phase2_status": row[1],
            "phase2_last_completed_episode_id": row[2],
            "phase2_processable_max_episode": row[3],
            "phase2_processed_count": row[4],
            "phase3_enabled": row[5],
            "last_completed_no": row[6],
        }


def _set_phase2_idle(webtoon_id: int) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            "UPDATE webtoon_pipeline_state SET phase2_status = 'idle', updated_at = %s WHERE webtoon_id = %s",
            (now, webtoon_id),
        )


def _load_face_records(webtoon_episode_id: int) -> list[dict]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fr.face_idx,
                   fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fr.conf, wc.id AS cut_id, wc.cut_number
            FROM face_record fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            WHERE wc.episode_id = %s
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


def _allocate_character(webtoon_id: int, webtoon_episode_id: int, cut_number: int) -> dict:
    """신규 Character + CharacterAppearance 생성 (NEW_CHAR_{N:03d}, 웹툰 글로벌 스코프, §12.2)."""
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        # 현재 웹툰의 최대 NEW_CHAR 번호 계산
        cur.execute(
            """
            SELECT COALESCE(MAX(
                CASE WHEN name ~ '^NEW_CHAR_[0-9]+$'
                THEN CAST(SUBSTRING(name FROM 9) AS INTEGER)
                ELSE 0 END
            ), 0) + 1
            FROM character
            WHERE webtoon_id = %s
            """,
            (webtoon_id,),
        )
        char_name = f"NEW_CHAR_{cur.fetchone()[0]:03d}"

        cur.execute(
            """
            INSERT INTO character
                (webtoon_id, name, aliases, extra,
                 first_seen_episode_id, first_seen_cut,
                 is_confirmed, is_name_auto_assigned, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, false, false, %s, %s)
            RETURNING id
            """,
            (webtoon_id, char_name, Json([]), Json({}),
             webtoon_episode_id, cut_number, now, now),
        )
        char_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO character_appearance
                (character_id, label, is_canonical,
                 first_seen_episode_id, first_seen_cut, created_at, updated_at)
            VALUES (%s, '기본', true, %s, %s, %s, %s)
            RETURNING id
            """,
            (char_id, webtoon_episode_id, cut_number, now, now),
        )
        appearance_id = cur.fetchone()[0]

        return {"char_id": char_id, "char_name": char_name, "appearance_id": appearance_id}


def _update_face_record(
    face_id: int, appearance_id: int, chroma_doc_id: str, match_score: Optional[float]
) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE face_record
            SET appearance_id = %s, chroma_doc_id = %s, match_score = %s, updated_at = %s
            WHERE id = %s
            """,
            (appearance_id, chroma_doc_id, match_score, now, face_id),
        )


def _complete_episode_state(webtoon_id: int, webtoon_episode_id: int) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE webtoon_pipeline_state
            SET phase2_status = 'running',
                phase2_last_completed_episode_id = %s,
                phase2_processed_count = phase2_processed_count + 1,
                updated_at = %s
            WHERE webtoon_id = %s
            """,
            (webtoon_episode_id, now, webtoon_id),
        )


def _get_next_ready_episode(webtoon_id: int, current_no: int) -> Optional[dict]:
    """Phase1이 완료된 다음 에피소드 반환. Step1이 아직 처리 전이면 None."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.id, we.no
            FROM webtoon_episode we
            WHERE we.webtoon_id = %s AND we.no = %s
              AND EXISTS (
                SELECT 1 FROM webtoon_cut wc
                WHERE wc.episode_id = we.id AND wc.processed_at IS NOT NULL
                LIMIT 1
              )
            """,
            (webtoon_id, current_no + 1),
        )
        row = cur.fetchone()
        return {"id": row[0], "no": row[1]} if row else None


# ── 핵심 처리 (동기 — run_in_executor에서 실행) ────────────────────────────────

@dataclass
class _Phase2Result:
    should_trigger_next: bool = False
    next_msg: Optional[EpisodePhase1Complete] = None
    next_key: str = ""
    should_start_phase3: bool = False
    phase3_msg: Optional[EpisodePhase3Start] = None
    phase3_key: str = ""


def _process_episode(msg: EpisodePhase1Complete) -> _Phase2Result:
    result = _Phase2Result()

    webtoon = _get_webtoon_info(msg.webtoon_episode_id)
    webtoon_id: int = webtoon["webtoon_id"]
    source: str = webtoon["source"]
    title_id: str = webtoon["title_id"]
    kafka_key = f"{source}_{title_id}"

    state = _get_or_create_pipeline_state(webtoon_id)

    # ── 멱등성 가드 (§18.3) ───────────────────────────────────────────────────
    last_no = state["last_completed_no"]
    if last_no is not None and last_no >= msg.episode_no:
        return result  # 이미 처리 완료 — 조용히 skip

    # ── processable_max_episode 체크 (§20) ───────────────────────────────────
    max_ep = state["phase2_processable_max_episode"]
    if max_ep is not None and msg.episode_no > max_ep:
        _set_phase2_idle(webtoon_id)
        return result  # idle 전환, 이벤트 미발행 → 자연 대기

    # ── Chroma 컬렉션 로드 ────────────────────────────────────────────────────
    collection = get_face_collection(source, title_id)

    # ── 에피소드 내 얼굴 식별 (컷 순서대로) ──────────────────────────────────
    face_records = _load_face_records(msg.webtoon_episode_id)
    for face in face_records:
        crop_bytes = fetch_face_crop(face["id"], source, title_id)
        if crop_bytes is None:
            print(f"[face_identify] crop not found face_id={face['id']}, skip")
            continue

        embedding = extract_embedding(crop_bytes)
        doc_id = f"{webtoon_id}_{msg.episode_no}_{face['cut_number']}_F{face['face_idx']}"

        # Chroma 코사인 유사도 검색 (낮을수록 유사)
        query_result = collection.query(
            query_embeddings=[embedding],
            n_results=1,
            include=["metadatas", "distances"],
        )
        has_match = bool(query_result["ids"][0])
        best_distance = query_result["distances"][0][0] if has_match else None
        best_meta = query_result["metadatas"][0][0] if has_match else None

        if has_match and best_distance <= MATCH_THRESHOLD:
            # 기존 캐릭터 매칭
            appearance_id: int = best_meta["appearance_id"]
            char_name: str = best_meta.get("character_name") or best_meta["character_id"]
            match_score: Optional[float] = best_distance
        else:
            # 신규 캐릭터 발급 (§5.5-1, §12.2)
            allocated = _allocate_character(webtoon_id, msg.webtoon_episode_id, face["cut_number"])
            appearance_id = allocated["appearance_id"]
            char_name = allocated["char_name"]
            match_score = None

        # Chroma upsert — 재처리 멱등성 보장 (§5.5-2, §12.3)
        b = face["bbox"]
        collection.upsert(
            ids=[doc_id],
            embeddings=[embedding],
            metadatas=[{
                "webtoon_id": webtoon_id,
                "episode": msg.episode_no,
                "cut": face["cut_number"],
                "face_idx": face["face_idx"],
                "character_id": char_name,
                "appearance_id": appearance_id,
                "appearance_label": "기본",
                "character_name": char_name,
                "is_confirmed": False,
                "bbox_x1": b[0], "bbox_y1": b[1], "bbox_x2": b[2], "bbox_y2": b[3],
                "conf": face["conf"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }],
        )

        _update_face_record(face["id"], appearance_id, doc_id, match_score)

    # ── 에피소드 완료 상태 갱신 ──────────────────────────────────────────────
    _complete_episode_state(webtoon_id, msg.webtoon_episode_id)

    # ── 다음 에피소드 자기 트리거 (§18.3) ────────────────────────────────────
    # Step1이 이미 next_ep.phase1.complete를 발행했을 경우 멱등성 가드로 skip
    next_ep = _get_next_ready_episode(webtoon_id, msg.episode_no)
    if next_ep:
        result.should_trigger_next = True
        result.next_key = kafka_key
        result.next_msg = EpisodePhase1Complete(
            source=source,
            title_id=title_id,
            episode_no=next_ep["no"],
            webtoon_episode_id=next_ep["id"],
            total_cuts=0,
        )

    # ── Phase 3 트리거 (활성 웹툰만, §12.10) ─────────────────────────────────
    if state["phase3_enabled"]:
        result.should_start_phase3 = True
        result.phase3_key = kafka_key
        result.phase3_msg = EpisodePhase3Start(
            source=source,
            title_id=title_id,
            episode_no=msg.episode_no,
            webtoon_episode_id=msg.webtoon_episode_id,
        )

    return result


# ── Faust Agent ───────────────────────────────────────────────────────────────

@app.agent(episode_phase1_complete, concurrency=1)
async def face_identify_agent(stream):
    loop = asyncio.get_running_loop()
    async for msg in stream:
        try:
            result: _Phase2Result = await loop.run_in_executor(None, _process_episode, msg)
        except Exception as e:
            print(f"[face_identify] {msg.source}/{msg.title_id} ep={msg.episode_no} error: {e}")
            import traceback
            traceback.print_exc()
            continue

        if result.should_trigger_next:
            await episode_phase1_complete.send(
                key=result.next_key,
                value=result.next_msg,
            )

        if result.should_start_phase3:
            await cut_phase3_start.send(
                key=result.phase3_key,
                value=result.phase3_msg,
            )
