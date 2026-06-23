"""Step 2 코어 — 얼굴 식별(임베딩 + 매칭), 에피소드 단위 (faust-free).

Temporal 액티비티가 `identify_episode_faces(webtoon_episode_id)`를 호출한다.
임베딩+매칭 1패스(이중 임베딩 없음). 모델/threshold는 웹툰별 해석(model_resolver).
에피소드 순차 보장(ep1 확정이 ep2 매칭에 반영)은 Temporal 워크플로가 담당하므로
여기서는 "다음 에피소드 트리거" 결정 로직을 포함하지 않는다.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Callable, Optional

from psycopg2.extras import Json

from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import fetch_face_crop
from src.operators.embedding import embed_for
from src.operators.matching import find_match, load_ccip_anchors
from src.operators.model_resolver import resolve_embedding_model

logger = logging.getLogger(__name__)

# S3 다운로드 + model-api 임베딩 요청은 얼굴 간 독립적인 I/O라 동시 처리.
# 매칭/캐릭터 할당은 같은 에피소드 내 순서 의존(신규 캐릭터가 다음 얼굴의 매칭 후보가
# 될 수 있음)이라 순차 유지 — 병렬화 대상은 fetch+embed 단계로 한정.
_EMBED_WORKERS = 8


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
              AND fr.is_used = true
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
                 is_confirmed, is_name_auto_assigned, is_match_excluded, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, false, false, false, %s, %s)
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
    """매칭 후보에서 제외할 appearance_id 목록.

    제외 대상: is_match_excluded(죽은 단역 등) + soft-delete된 캐릭터(c.deleted_at)
    + soft-delete된 개별 appearance(ca.deleted_at). 쿼리 시점 제외라 이미 Chroma에
    시딩돼 남아있는 과거 에피소드 doc까지 함께 후보에서 빠진다.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT ca.id
            FROM character_appearance ca
            JOIN character c ON ca.character_id = c.id
            WHERE c.webtoon_id = %s
              AND (
                c.is_match_excluded = true
                OR c.deleted_at IS NOT NULL
                OR ca.deleted_at IS NOT NULL
              )
            """,
            (webtoon_id,),
        )
        return [row[0] for row in cur.fetchall()]


def _seed_confirmed_faces(
    webtoon_id: int, source: str, title_id: str, collection, model_name: str, metric_type: str,
    heartbeat_cb: Optional[Callable[[int], None]] = None, heartbeat_value: int = 0,
) -> int:
    """수동 확정 얼굴을 Chroma에 시딩 — 매칭 기준점 보장.

    웹툰 전체의 확정 얼굴을 순차로 S3 다운로드+임베딩하므로, 확정 얼굴이 누적된
    웹툰에서는 이 단계만으로도 heartbeat_timeout을 넘길 수 있다. 메인 루프와 동일하게
    얼굴 하나가 끝날 때마다 같은 값(heartbeat_value)으로 heartbeat를 보내 타임아웃
    타이머를 갱신한다.
    """
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
              AND fr.is_used = true
              AND c.is_match_excluded = false
              AND c.deleted_at IS NULL
              AND ca.deleted_at IS NULL
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
            if heartbeat_cb:
                heartbeat_cb(heartbeat_value)
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
        if heartbeat_cb:
            heartbeat_cb(heartbeat_value)
    return len(rows)


def _fetch_and_embed(face: dict, source: str, title_id: str, metric_type: str) -> Optional[list[float]]:
    crop_bytes = fetch_face_crop(face["id"], source, title_id)
    if crop_bytes is None:
        return None
    return embed_for(metric_type, crop_bytes)


def _fetch_and_embed_all(
    faces: list[dict], source: str, title_id: str, metric_type: str,
    heartbeat_cb: Optional[Callable[[int], None]] = None, heartbeat_value: int = 0,
) -> dict[int, Optional[list[float]]]:
    """S3 crop 다운로드 + model-api 임베딩을 얼굴 간 동시 처리(독립 I/O).
    매칭/캐릭터 할당은 순서 의존이라 이 단계의 결과를 받아 순차로 처리한다.

    이 단계는 순차 루프 진입 전이라 resume 인덱스(heartbeat_value)는 아직 전진하지
    않지만, 얼굴 수가 많으면 이 단계만으로도 heartbeat_timeout을 넘길 수 있어
    얼굴 하나가 끝날 때마다 같은 값으로 heartbeat를 보내 타임아웃 타이머를 갱신한다.
    """
    if not faces:
        return {}
    total = len(faces)
    workers = min(_EMBED_WORKERS, total)
    logger.info(
        "[step2] 임베딩 시작: %d개 얼굴 (metric=%s, workers=%d, resume_from=%d)",
        total, metric_type, workers, heartbeat_value,
    )
    # 진행 로그 간격: 너무 잦지 않게 약 10% 단위(최소 1개)로 출력.
    log_every = max(1, total // 10)

    results: dict[int, Optional[list[float]]] = {}
    done = 0
    ok = 0
    missing = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_and_embed, f, source, title_id, metric_type): f["id"] for f in faces}
        for future in as_completed(futures):
            face_id = futures[future]
            try:
                feature = future.result()
            except Exception:
                logger.exception("[step2] 임베딩 실패 face_id=%s (%d/%d 처리됨)", face_id, done, total)
                raise
            results[face_id] = feature
            done += 1
            if feature is None:
                missing += 1
            else:
                ok += 1
            if done % log_every == 0 or done == total:
                logger.info(
                    "[step2] 임베딩 진행 %d/%d (성공=%d, crop없음=%d, 진행중=%d)",
                    done, total, ok, missing, total - done,
                )
            if heartbeat_cb:
                heartbeat_cb(heartbeat_value)
    logger.info("[step2] 임베딩 완료: %d/%d (성공=%d, crop없음=%d)", done, total, ok, missing)
    return results


def _summarize_episode_faces(webtoon_episode_id: int) -> tuple[int, int]:
    """face_record/face_embedding 현재 상태로 matched/new_chars 집계 (재시도 후에도 정확).
    match_score IS NULL이면 신규 캐릭터, 값이 있으면 기존 캐릭터 매칭."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE fe.match_score IS NOT NULL) AS matched,
                COUNT(*) FILTER (WHERE fe.match_score IS NULL) AS new_chars
            FROM face_record fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            LEFT JOIN face_embedding fe ON fe.face_record_id = fr.id
            WHERE wc.episode_id = %s AND fr.appearance_id IS NOT NULL
              AND fr.is_used = true
            """,
            (webtoon_episode_id,),
        )
        matched, new_chars = cur.fetchone()
        return matched or 0, new_chars or 0


# ── 에피소드 단위 식별 진입점 (Temporal 액티비티가 호출) ──────────────────────

def identify_episode_faces(
    webtoon_episode_id: int,
    episode_no: int,
    heartbeat_cb: Optional[Callable[[int], None]] = None,
    resume_from: int = 0,
) -> dict:
    """에피소드의 모든 얼굴을 임베딩+매칭 1패스로 식별. 반환: 처리 요약.

    heartbeat_cb/resume_from: 액티비티 타임아웃으로 재시도될 때 처음부터 다시
    처리하지 않도록, 처리 완료한 얼굴 인덱스를 Temporal heartbeat detail로 기록하고
    재시도 시 그 지점부터 재개한다(`face_identify_episode` 액티비티가 연결).
    """
    webtoon = get_webtoon_info(webtoon_episode_id)
    webtoon_id = webtoon["webtoon_id"]
    source = webtoon["source"]
    title_id = webtoon["title_id"]

    ctx = resolve_embedding_model(webtoon_id)
    model_name = ctx["name"]
    metric_type = ctx["metric_type"]
    threshold = ctx["threshold"]

    collection = get_face_collection(source, title_id, model_name)
    _seed_confirmed_faces(
        webtoon_id, source, title_id, collection, model_name, metric_type,
        heartbeat_cb=heartbeat_cb, heartbeat_value=resume_from,
    )
    excluded_appearance_ids = _get_excluded_appearance_ids(webtoon_id)

    # collection.get()/일괄 임베딩이 heartbeat_timeout(2분)보다 오래 걸릴 수 있어,
    # 순차 루프 진입 전인 이 구간에서도 heartbeat로 타임아웃 타이머를 미리 갱신해둔다.
    if heartbeat_cb:
        heartbeat_cb(resume_from)

    # ccip는 anchor 전체를 브루트포스로 비교해야 해서(§matching.py) 얼굴마다
    # collection.get()으로 전체 재조회하지 않도록 에피소드당 1회만 적재해 캐시한다.
    # 새로 추가되는 얼굴(매칭/신규 캐릭터)은 루프 중 캐시에 직접 append해 갱신.
    ccip_anchors = load_ccip_anchors(collection, excluded_appearance_ids) if metric_type == "ccip" else None

    faces = _load_face_records(webtoon_episode_id)
    pending = faces[resume_from:]
    logger.info(
        "[step2] 에피소드 식별 시작 ep_id=%s episode_no=%s: 전체 %d개 얼굴, resume_from=%d → 처리 대상 %d개 (model=%s, metric=%s)",
        webtoon_episode_id, episode_no, len(faces), resume_from, len(pending), model_name, metric_type,
    )
    features_by_face_id = _fetch_and_embed_all(
        pending, source, title_id, metric_type,
        heartbeat_cb=heartbeat_cb, heartbeat_value=resume_from,
    )

    logger.info("[step2] 매칭 시작: %d개 얼굴 순차 처리 (metric=%s)", len(pending), metric_type)
    match_log_every = max(1, len(pending) // 10)
    n_matched = 0
    n_new = 0
    for i, face in enumerate(pending):
        feature = features_by_face_id.get(face["id"])
        if feature is None:
            logger.warning("[step2] crop not found face_id=%s, skip", face["id"])
            if heartbeat_cb:
                heartbeat_cb(resume_from + i + 1)
            continue

        doc_id = f"{webtoon_id}_{episode_no}_{face['cut_number']}_F{face['face_idx']}"

        match = find_match(
            collection, feature, metric_type, threshold, excluded_appearance_ids,
            ccip_anchors=ccip_anchors,
        )
        if match is not None:
            meta = match["meta"]
            appearance_id = meta["appearance_id"]
            char_name = meta.get("character_name") or meta["character_id"]
            match_score = match["score"]
            n_matched += 1
            _update_character_first_seen(appearance_id, webtoon_episode_id, episode_no, face["cut_number"])
        else:
            allocated = _allocate_character(webtoon_id, webtoon_episode_id, face["cut_number"])
            appearance_id = allocated["appearance_id"]
            char_name = allocated["char_name"]
            match_score = None
            n_new += 1

        b = face["bbox"]
        meta_doc = {
            "webtoon_id": webtoon_id, "episode": episode_no, "cut": face["cut_number"],
            "face_idx": face["face_idx"], "character_id": char_name, "appearance_id": appearance_id,
            "appearance_label": "기본", "character_name": char_name, "is_confirmed": False,
            "bbox_x1": b[0], "bbox_y1": b[1], "bbox_x2": b[2], "bbox_y2": b[3],
            "conf": face["conf"], "created_at": datetime.now(timezone.utc).isoformat(),
        }
        collection.upsert(ids=[doc_id], embeddings=[feature], metadatas=[meta_doc])
        if ccip_anchors is not None and match is None:
            # 같은 에피소드 내 다음 얼굴이 방금 만든 신규 캐릭터와 매칭될 수 있어 캐시에 반영.
            ccip_anchors.append({"embedding": feature, "meta": meta_doc})

        _update_face_record(face["id"], appearance_id)
        _upsert_face_embedding(face["id"], model_name, doc_id, match_score)

        done = i + 1
        if done % match_log_every == 0 or done == len(pending):
            logger.info(
                "[step2] 매칭 진행 %d/%d (매칭=%d, 신규=%d, anchor=%s)",
                done, len(pending), n_matched, n_new,
                len(ccip_anchors) if ccip_anchors is not None else "-",
            )

        if heartbeat_cb:
            heartbeat_cb(resume_from + i + 1)

    matched_n, new_n = _summarize_episode_faces(webtoon_episode_id)
    _complete_episode_state(webtoon_id, webtoon_episode_id)
    logger.info(
        "[step2] 에피소드 식별 완료 ep_id=%s episode_no=%s: faces=%d, matched=%d, new_chars=%d",
        webtoon_episode_id, episode_no, len(faces), matched_n, new_n,
    )
    return {"faces": len(faces), "matched": matched_n, "new_chars": new_n}
