"""Step 2 코어 — 얼굴 식별(임베딩 + 매칭), 에피소드 단위 (faust-free).

Temporal 액티비티가 `identify_episode_faces(webtoon_episode_id)`를 호출한다.
임베딩+매칭 1패스(이중 임베딩 없음). 모델/threshold는 웹툰별 해석(model_resolver).
에피소드 순차 보장(ep1 확정이 ep2 매칭에 반영)은 Temporal 워크플로가 담당하므로
여기서는 "다음 에피소드 트리거" 결정 로직을 포함하지 않는다.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2.extras import Json

from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import fetch_face_crop
from src.operators.embedding import embed_for
from src.operators.matching import find_match
from src.operators.model_resolver import resolve_embedding_model

logger = logging.getLogger(__name__)


# ── DB 헬퍼 ───────────────────────────────────────────────────────────────────

def get_webtoon_info(webtoon_episode_id: int) -> dict:
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
    """신규 Character + CharacterAppearance 생성 (NEW_CHAR_{N:03d}, 웹툰 글로벌)."""
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(
                CASE WHEN name ~ '^NEW_CHAR_[0-9]+$'
                THEN CAST(SUBSTRING(name FROM 10) AS INTEGER) ELSE 0 END
            ), 0) + 1
            FROM character WHERE webtoon_id = %s
            """,
            (webtoon_id,),
        )
        char_name = f"NEW_CHAR_{cur.fetchone()[0]:03d}"
        cur.execute(
            """
            INSERT INTO character
                (webtoon_id, name, aliases, extra, first_seen_episode_id, first_seen_cut,
                 is_confirmed, is_name_auto_assigned, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, false, false, %s, %s)
            RETURNING id
            """,
            (webtoon_id, char_name, Json([]), Json({}), webtoon_episode_id, cut_number, now, now),
        )
        char_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO character_appearance
                (character_id, label, is_canonical, first_seen_episode_id, first_seen_cut, created_at, updated_at)
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


def _update_character_first_seen(appearance_id: int, episode_id: int, episode_no: int, cut_number: int) -> None:
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
                OR ((SELECT no FROM webtoon_episode WHERE id = first_seen_episode_id) = %s
                    AND first_seen_cut > %s)
            )
            """,
            (episode_id, cut_number, now, appearance_id, episode_no, episode_no, cut_number),
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


def _get_excluded_appearance_ids(webtoon_id: int) -> list[int]:
    """매칭 후보에서 제외할 캐릭터(죽은 단역 등)의 appearance_id 목록."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT ca.id
            FROM character_appearance ca
            JOIN character c ON ca.character_id = c.id
            WHERE c.webtoon_id = %s AND c.is_match_excluded = true
            """,
            (webtoon_id,),
        )
        return [row[0] for row in cur.fetchall()]


def _seed_confirmed_faces(webtoon_id: int, source: str, title_id: str, collection, model_name: str, metric_type: str) -> int:
    """수동 확정 얼굴을 Chroma에 시딩 — 매칭 기준점 보장."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fr.face_idx, fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fr.conf, fr.appearance_id, wc.cut_number, we.no AS episode_no,
                   ca.label AS appearance_label, c.name AS character_name, fe.chroma_doc_id
            FROM face_record fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            JOIN webtoon_episode we ON wc.episode_id = we.id
            JOIN character_appearance ca ON fr.appearance_id = ca.id
            JOIN character c ON ca.character_id = c.id
            LEFT JOIN face_embedding fe ON fe.face_record_id = fr.id AND fe.embedding_model = %s
            WHERE c.webtoon_id = %s AND fr.is_confirmed = true
              AND fr.appearance_id IS NOT NULL AND fr.deleted_at IS NULL
              AND c.is_match_excluded = false
            """,
            (model_name, webtoon_id),
        )
        rows = cur.fetchall()

    for row in rows:
        (face_id, face_idx, x1, y1, x2, y2, conf, appearance_id, cut_number,
         episode_no, appearance_label, character_name, chroma_doc_id) = row
        doc_id = chroma_doc_id or f"{webtoon_id}_{episode_no}_{cut_number}_F{face_idx}"
        crop_bytes = fetch_face_crop(face_id, source, title_id)
        if crop_bytes is None:
            continue
        embedding = embed_for(metric_type, crop_bytes)
        collection.upsert(
            ids=[doc_id], embeddings=[embedding],
            metadatas=[{
                "webtoon_id": webtoon_id, "episode": episode_no, "cut": cut_number,
                "face_idx": face_idx, "character_id": character_name, "appearance_id": appearance_id,
                "appearance_label": appearance_label, "character_name": character_name,
                "is_confirmed": True, "bbox_x1": x1, "bbox_y1": y1, "bbox_x2": x2, "bbox_y2": y2,
                "conf": conf or 0.0, "created_at": datetime.now(timezone.utc).isoformat(),
            }],
        )
    return len(rows)


# ── 에피소드 단위 식별 진입점 (Temporal 액티비티가 호출) ──────────────────────

def identify_episode_faces(webtoon_episode_id: int, episode_no: int) -> dict:
    """에피소드의 모든 얼굴을 임베딩+매칭 1패스로 식별. 반환: 처리 요약."""
    webtoon = get_webtoon_info(webtoon_episode_id)
    webtoon_id = webtoon["webtoon_id"]
    source = webtoon["source"]
    title_id = webtoon["title_id"]

    ctx = resolve_embedding_model(webtoon_id)
    model_name = ctx["name"]
    metric_type = ctx["metric_type"]
    threshold = ctx["threshold"]

    collection = get_face_collection(source, title_id, model_name)
    _seed_confirmed_faces(webtoon_id, source, title_id, collection, model_name, metric_type)
    excluded_appearance_ids = _get_excluded_appearance_ids(webtoon_id)

    matched_n = 0
    new_n = 0
    faces = _load_face_records(webtoon_episode_id)
    for face in faces:
        crop_bytes = fetch_face_crop(face["id"], source, title_id)
        if crop_bytes is None:
            logger.warning("[step2] crop not found face_id=%s, skip", face["id"])
            continue

        feature = embed_for(metric_type, crop_bytes)
        doc_id = f"{webtoon_id}_{episode_no}_{face['cut_number']}_F{face['face_idx']}"

        match = find_match(collection, feature, metric_type, threshold, excluded_appearance_ids)
        if match is not None:
            meta = match["meta"]
            appearance_id = meta["appearance_id"]
            char_name = meta.get("character_name") or meta["character_id"]
            match_score = match["score"]
            _update_character_first_seen(appearance_id, webtoon_episode_id, episode_no, face["cut_number"])
            matched_n += 1
        else:
            allocated = _allocate_character(webtoon_id, webtoon_episode_id, face["cut_number"])
            appearance_id = allocated["appearance_id"]
            char_name = allocated["char_name"]
            match_score = None
            new_n += 1

        b = face["bbox"]
        collection.upsert(
            ids=[doc_id], embeddings=[feature],
            metadatas=[{
                "webtoon_id": webtoon_id, "episode": episode_no, "cut": face["cut_number"],
                "face_idx": face["face_idx"], "character_id": char_name, "appearance_id": appearance_id,
                "appearance_label": "기본", "character_name": char_name, "is_confirmed": False,
                "bbox_x1": b[0], "bbox_y1": b[1], "bbox_x2": b[2], "bbox_y2": b[3],
                "conf": face["conf"], "created_at": datetime.now(timezone.utc).isoformat(),
            }],
        )
        _update_face_record(face["id"], appearance_id)
        _upsert_face_embedding(face["id"], model_name, doc_id, match_score)

    _complete_episode_state(webtoon_id, webtoon_episode_id)
    return {"faces": len(faces), "matched": matched_n, "new_chars": new_n}
