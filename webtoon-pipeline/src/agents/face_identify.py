"""Step 2 Agent: Chroma 기반 인물 식별 — 웹툰별 에피소드 순차 처리 (§Step 2, §18.3).

파티션 키 {source}_{title_id} → 같은 웹툰은 항상 같은 worker → 에피소드 순서 자동 보장.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import faust

logger = logging.getLogger(__name__)
from psycopg2.extras import Json

from src.agents.ocr_yolo import EpisodePhase1aComplete, episode_phase1a_complete
from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import fetch_face_crop
from src.operators.embedding import embed_for
from src.operators.matching import find_match
from src.operators.model_resolver import resolve_embedding_model
from src.worker import app


# ── Kafka 토픽 ────────────────────────────────────────────────────────────────────────────

class EpisodePhase3Start(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int


cut_phase3_start = app.topic("cut.phase3.start", value_type=EpisodePhase3Start)


# ── DB 헬퍼 ────────────────────────────────────────────────────────────────────────────

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


def _set_phase2_completed(webtoon_id: int) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            "UPDATE webtoon_pipeline_state SET phase2_status = 'completed', updated_at = %s WHERE webtoon_id = %s",
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
        cur.execute(
            """
            SELECT COALESCE(MAX(
                CASE WHEN name ~ '^NEW_CHAR_[0-9]+$'
                THEN CAST(SUBSTRING(name FROM 10) AS INTEGER)
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


def _update_face_record(face_id: int, appearance_id: int) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            "UPDATE face_record SET appearance_id = %s, updated_at = %s WHERE id = %s",
            (appearance_id, now, face_id),
        )


def _upsert_face_embedding(face_id: int, model: str, doc_id: str, match_score: Optional[float]) -> None:
    """face_embedding 행 upsert.

    A2 이전엔 embedding_agent가 INSERT하고 face_identify는 score만 UPDATE했으나,
    embedding_agent 제거 후 face_identify가 직접 행을 생성·갱신한다.
    """
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
            (face_id, model, doc_id, match_score, now, now),
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
    """Phase1이 '완전 완료'된 다음 에피소드 반환. 아직이면 None.

    기준을 webtoon_cut.processed_at(컷 일부만 처리돼도 충족)에서
    episode_pipeline_progress(phase=1, status='completed')로 변경(§A1/A2):
    순차 phase1과 phase2 자체 체이닝이 맞물릴 때, phase1이 부분 완료된 에피소드를
    phase2가 선행 처리해 일부 얼굴이 누락되고 멱등 가드로 재처리도 막히는 레이스를 방지한다.
    (phase1 완전 완료 ep는 ocr_yolo가 phase1a.complete로도 트리거하므로 중복 트리거는 가드로 무해.)
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.id, we.no
            FROM webtoon_episode we
            WHERE we.webtoon_id = %s AND we.no = %s
              AND we.deleted_at IS NULL
              AND EXISTS (
                SELECT 1 FROM episode_pipeline_progress p
                WHERE p.episode_id = we.id
                  AND p.phase = 1
                  AND p.status = 'completed'
              )
            """,
            (webtoon_id, current_no + 1),
        )
        row = cur.fetchone()
        return {"id": row[0], "no": row[1]} if row else None


def _seed_confirmed_faces(
    webtoon_id: int, source: str, title_id: str, collection, model_name: str, metric_type: str
) -> int:
    """수동 확정된 얼굴을 Chroma에 시딩 — 수동 등록/재배정 캐릭터가 매칭 기준점으로 사용되도록 보장.

    is_confirmed=True인 face_record를 조회해 Chroma에 upsert한다.
    재배정 후 appearance가 바뀌 경우도 올바른 캐릭터 메타데이터로 덮어쓴다.
    리턴값: upsert된 문서 수 (컴렉션 크기 추적에 활용)
    """
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
            WHERE c.webtoon_id = %s
              AND fr.is_confirmed = true
              AND fr.appearance_id IS NOT NULL
              AND fr.deleted_at IS NULL
            """,
            (model_name, webtoon_id),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    for row in rows:
        (face_id, face_idx, x1, y1, x2, y2, conf,
         appearance_id, cut_number, episode_no,
         appearance_label, character_name, chroma_doc_id) = row

        doc_id = chroma_doc_id or f"{webtoon_id}_{episode_no}_{cut_number}_F{face_idx}"

        crop_bytes = fetch_face_crop(face_id, source, title_id)
        if crop_bytes is None:
            continue

        embedding = embed_for(metric_type, crop_bytes)
        collection.upsert(
            ids=[doc_id],
            embeddings=[embedding],
            metadatas=[{
                "webtoon_id": webtoon_id,
                "episode": episode_no,
                "cut": cut_number,
                "face_idx": face_idx,
                "character_id": character_name,
                "appearance_id": appearance_id,
                "appearance_label": appearance_label,
                "character_name": character_name,
                "is_confirmed": True,
                "bbox_x1": x1, "bbox_y1": y1, "bbox_x2": x2, "bbox_y2": y2,
                "conf": conf or 0.0,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }],
        )

    return len(rows)


# ── 핵심 처리 (동기 — run_in_executor에서 실행) ────────────────────────────────────────────

@dataclass
class _Phase2Result:
    should_trigger_next: bool = False
    next_msg: Optional[EpisodePhase1aComplete] = None
    next_key: str = ""
    should_start_phase3: bool = False
    phase3_msg: Optional[EpisodePhase3Start] = None
    phase3_key: str = ""


def _update_character_first_seen(appearance_id: int, episode_id: int, episode_no: int, cut_number: int) -> None:
    """기존 캐릭터 매칭 시 first_seen이 더 이른 경우에만 업데이트."""
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE character
            SET first_seen_episode_id = %s, first_seen_cut = %s, updated_at = %s
            WHERE id = (
                SELECT c.id FROM character c
                JOIN character_appearance ca ON ca.character_id = c.id
                WHERE ca.id = %s
            )
            AND (
                first_seen_episode_id IS NULL
                OR (SELECT no FROM webtoon_episode WHERE id = first_seen_episode_id) > %s
                OR (
                    (SELECT no FROM webtoon_episode WHERE id = first_seen_episode_id) = %s
                    AND first_seen_cut > %s
                )
            )
            """,
            (episode_id, cut_number, now, appearance_id, episode_no, episode_no, cut_number),
        )


def _process_episode(msg: EpisodePhase1aComplete) -> _Phase2Result:
    result = _Phase2Result()

    webtoon = _get_webtoon_info(msg.webtoon_episode_id)
    webtoon_id: int = webtoon["webtoon_id"]
    source: str = webtoon["source"]
    title_id: str = webtoon["title_id"]
    kafka_key = f"{source}_{title_id}"

    state = _get_or_create_pipeline_state(webtoon_id)

    # ── 멱등성 가드 (§18.3) ───────────────────────────────────────────────────────────────────
    last_no = state["last_completed_no"]
    if last_no is not None and last_no >= msg.episode_no:
        return result  # 이미 처리 완료 — 조용히 skip

    # ── processable_max_episode 체크 (§20) ────────────────────────────────────────────────────
    max_ep = state["phase2_processable_max_episode"]
    if max_ep is not None and msg.episode_no > max_ep:
        _set_phase2_idle(webtoon_id)
        return result  # idle 전환, 이벤트 미발행 → 자연 대기

    # ── 모델/threshold 해석 (§B2) ────────────────────────────────────────────────────────
    ctx = resolve_embedding_model(webtoon_id)
    model_name = ctx["name"]
    metric_type = ctx["metric_type"]
    threshold = ctx["threshold"]

    # ── Chroma 콜렉션 로드 (모델별) ────────────────────────────────────────────────────────
    collection = get_face_collection(source, title_id, model_name)

    # ── 수동 확정 얼굴 시딩 ──────────────────────────────────────────────────────────
    _seed_confirmed_faces(webtoon_id, source, title_id, collection, model_name, metric_type)

    # ── 에피소드 내 얼굴 식별 (컷 순서대로) ──────────────────────────────────────────
    face_records = _load_face_records(msg.webtoon_episode_id)
    for face in face_records:
        crop_bytes = fetch_face_crop(face["id"], source, title_id)
        if crop_bytes is None:
            logger.warning("[face_identify] crop not found face_id=%s, skip", face["id"])
            continue

        feature = embed_for(metric_type, crop_bytes)
        doc_id = f"{webtoon_id}_{msg.episode_no}_{face['cut_number']}_F{face['face_idx']}"

        matched = find_match(collection, feature, metric_type, threshold)
        if matched is not None:
            # 기존/확정 캐릭터 매칭
            best_meta = matched["meta"]
            appearance_id: int = best_meta["appearance_id"]
            char_name: str = best_meta.get("character_name") or best_meta["character_id"]
            match_score: Optional[float] = matched["score"]
            _update_character_first_seen(appearance_id, msg.webtoon_episode_id, msg.episode_no, face["cut_number"])
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
            embeddings=[feature],
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

        _update_face_record(face["id"], appearance_id)
        _upsert_face_embedding(face["id"], model_name, doc_id, match_score)

    # ── 에피소드 완료 상태 갱신 ────────────────────────────────────────────────────────
    _complete_episode_state(webtoon_id, msg.webtoon_episode_id)

    # ── 다음 에피소드 자기 트리거 (§18.3) ────────────────────────────────────────────────
    next_ep = _get_next_ready_episode(webtoon_id, msg.episode_no)
    if next_ep:
        if max_ep is not None and next_ep["no"] > max_ep:
            _set_phase2_idle(webtoon_id)
        else:
            result.should_trigger_next = True
            result.next_key = kafka_key
            result.next_msg = EpisodePhase1aComplete(
                source=source,
                title_id=title_id,
                episode_no=next_ep["no"],
                webtoon_episode_id=next_ep["id"],
                total_cuts=0,
            )
    else:
        _set_phase2_completed(webtoon_id)

    # ── Phase 3 트리거 (활성 웹툰만, §12.10) ─────────────────────────────────────────────
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


# ── Faust Agent ─────────────────────────────────────────────────────────────────────────────

@app.agent(episode_phase1a_complete, concurrency=1)
async def face_identify_agent(stream):
    loop = asyncio.get_running_loop()
    async for msg in stream:
        try:
            result: _Phase2Result = await loop.run_in_executor(None, _process_episode, msg)
        except Exception as e:
            logger.error("[face_identify] %s/%s ep=%s error: %s: %s", msg.source, msg.title_id, msg.episode_no, type(e).__name__, e)
            continue

        if result.should_trigger_next:
            await episode_phase1a_complete.send(
                key=result.next_key,
                value=result.next_msg,
            )

        if result.should_start_phase3:
            await cut_phase3_start.send(
                key=result.phase3_key,
                value=result.phase3_msg,
            )
