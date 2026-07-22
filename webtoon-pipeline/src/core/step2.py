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

from src.config.chroma import chroma_retry, get_face_collection
from src.config.db import db_cursor
from src.config.r2 import fetch_face_crop
from src.operators.embedding import embed_for
from src.operators.matching import find_match, load_ccip_anchors
from src.operators.model_resolver import resolve_embedding_model

logger = logging.getLogger(__name__)

# S3 다운로드 + model-api 임베딩 요청은 얼굴 간 독립적인 I/O라 동시 처리.
# 매칭/캐릭터 할당은 같은 에피소드 내 순서 의존(신규 캐릭터가 다음 얼굴의 매칭 후보가
# 될 수 있음)이라 순차 유지 — 병렬화 대상은 fetch+embed 단계로 한정.
# embed-ccip-api가 gunicorn --workers=2(replicas=1)라 그 이상 동시 요청을 보내면
# 대기열이 쌓여 gunicorn --timeout=120에 걸려 워커가 SIGKILL/재시작된다
# (prd.md §16.2) — 서버 워커 수에 맞춰 2로 제한.
_EMBED_WORKERS = 2


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
    """메인 루프 처리 대상 얼굴 — human 정체 행이 있는 얼굴은 제외한다(human 동결).

    human이 배정/배정해제(appearance NULL 확정)한 얼굴을 메인 루프가 다시 매칭하면
    시딩(is_confirmed=True)·퍼지(doc 삭제)를 같은 런에서 즉시 되덮어 무효화한다
    (2026-07-14 리뷰 — 같은 에피소드 재실행 시 배정해제 doc이 부활하는 마그넷 모드).
    human 확정 얼굴의 Chroma 투영은 시딩/퍼지/T3가 전담하고, step2 정체 행도 human
    동결 원칙상 재계산할 이유가 없다.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fr.face_idx,
                   fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fr.conf, wc.id AS cut_id, wc.cut_number
            FROM analysis_face_detection fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            WHERE wc.episode_id = %s
              AND fr.is_used = true
              AND fr.deleted_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM analysis_face_identity fi
                WHERE fi.detection_id = fr.id AND fi.source = 'human'
                  AND fi.deleted_at IS NULL
              )
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


def _flow_first_enabled(webtoon_id: int) -> bool:
    """흐름-first 게이트(redesign D3·Phase 5) — config_webtoon_pipeline_state.flow_first_enabled.

    step3._flow_first_enabled와 동일 의미(step2 자족성 위해 로컬 복제). ON이면 CCIP 정체 결합 스킵.
    """
    with db_cursor() as cur:
        cur.execute(
            "SELECT flow_first_enabled FROM config_webtoon_pipeline_state WHERE webtoon_id = %s",
            (webtoon_id,),
        )
        row = cur.fetchone()
        return bool(row[0]) if row else False


def _allocate_character(webtoon_id: int, webtoon_episode_id: int, cut_number: int) -> dict:
    """신규 얼굴 클러스터(Character kind=cluster) + CharacterAppearance 생성(v4.0 §17.2).

    클러스터는 기계 산출물이라 이름이 없다(name="") — 구 NEW_CHAR_{N} placeholder 관습 폐기.
    실명 확정/승격(kind=character)은 Stage R apply 또는 human이 수행한다.
    """
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_character
                (webtoon_id, kind, name, aliases, extra, first_seen_episode_id, first_seen_cut,
                 is_confirmed, is_name_auto_assigned, is_match_excluded, created_at, updated_at)
            VALUES (%s, 'cluster', '', %s, %s, %s, %s, false, false, false, %s, %s)
            RETURNING id
            """,
            (webtoon_id, Json([]), Json({}), webtoon_episode_id, cut_number, now, now),
        )
        char_id = cur.fetchone()[0]
        char_name = f"cluster#{char_id}"  # 로그/Chroma 메타 표시용 라벨(DB name은 빈 문자열)
        cur.execute(
            """
            INSERT INTO analysis_character_appearance
                (character_id, label, is_canonical, first_seen_episode_id, first_seen_cut, created_at, updated_at)
            VALUES (%s, '기본', true, %s, %s, %s, %s)
            RETURNING id
            """,
            (char_id, webtoon_episode_id, cut_number, now, now),
        )
        appearance_id = cur.fetchone()[0]
        return {"char_id": char_id, "char_name": char_name, "appearance_id": appearance_id}


def _upsert_face_identity(
    face_id: int, appearance_id: int, score: Optional[float], run_id: Optional[int],
) -> None:
    """step2 정체 행 upsert — human 행은 절대 건드리지 않는다(레이어 분리, v4.0 §17.2)."""
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_face_identity
                (detection_id, source, appearance_id, score, run_id, created_at, updated_at)
            VALUES (%s, 'step2', %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uniq_face_identity_detection_source
            DO UPDATE SET appearance_id = EXCLUDED.appearance_id,
                          score = EXCLUDED.score, run_id = EXCLUDED.run_id,
                          deleted_at = NULL, updated_at = EXCLUDED.updated_at
            """,
            (face_id, appearance_id, score, run_id, now, now),
        )


def _upsert_face_embedding(face_id: int, model: str, doc_id: str, match_score: Optional[float]) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_face_embedding
                (detection_id, embedding_model, chroma_doc_id, match_score, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (detection_id, embedding_model)
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
            UPDATE analysis_character
            SET first_seen_episode_id = %s, first_seen_cut = %s, updated_at = %s
            WHERE id = (
                SELECT c.id FROM analysis_character c
                JOIN analysis_character_appearance ca ON ca.character_id = c.id
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
            FROM analysis_character_appearance ca
            JOIN analysis_character c ON ca.character_id = c.id
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


def _get_valid_appearance_ids(webtoon_id: int) -> set[int]:
    """이 웹툰에 실제로 존재하는(하드 삭제 안 된) character_appearance id 집합.

    `_get_excluded_appearance_ids`는 "존재하는 행 중 매칭에서 뺄 것"만 다루므로, 행 자체가
    하드 삭제돼 아예 없는 경우(예: 관리자 "분석데이터 초기화"가 DB는 지우고 Chroma 컬렉션
    정리에는 실패하는 경우 — 재현 확인됨)는 걸러내지 못한다. Chroma 앵커/매칭 결과의
    appearance_id를 쓰기 전에 이 집합으로 실존 여부를 검증해, 유령 appearance_id가
    face_record.appearance_id FK 위반으로 이어지는 것을 막는다.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT ca.id
            FROM analysis_character_appearance ca
            JOIN analysis_character c ON ca.character_id = c.id
            WHERE c.webtoon_id = %s
            """,
            (webtoon_id,),
        )
        return {row[0] for row in cur.fetchall()}


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
                   fr.conf, fi.appearance_id, wc.cut_number, we.no AS episode_no,
                   ca.label AS appearance_label, c.id AS character_pk,
                   c.name AS character_name, fe.chroma_doc_id
            FROM analysis_face_detection fr
            JOIN analysis_face_identity fi ON fi.detection_id = fr.id
                 AND fi.source = 'human' AND fi.deleted_at IS NULL
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            JOIN webtoon_episode we ON wc.episode_id = we.id
            JOIN analysis_character_appearance ca ON fi.appearance_id = ca.id
            JOIN analysis_character c ON ca.character_id = c.id
            LEFT JOIN analysis_face_embedding fe ON fe.detection_id = fr.id AND fe.embedding_model = %s
            WHERE c.webtoon_id = %s
              AND fi.appearance_id IS NOT NULL AND fr.deleted_at IS NULL
              AND fr.is_used = true
              AND c.is_match_excluded = false
              AND c.deleted_at IS NULL
              AND ca.deleted_at IS NULL
            """,
            (model_name, webtoon_id),
        )
        rows = cur.fetchall()

    hb = (lambda: heartbeat_cb(heartbeat_value)) if heartbeat_cb else None
    for row in rows:
        (face_id, face_idx, x1, y1, x2, y2, conf, appearance_id, cut_number,
         episode_no, appearance_label, character_pk, character_name, chroma_doc_id) = row
        doc_id = chroma_doc_id or f"{webtoon_id}_{episode_no}_{cut_number}_F{face_idx}"
        crop_bytes = fetch_face_crop(face_id, source, title_id)
        if crop_bytes is None:
            if heartbeat_cb:
                heartbeat_cb(heartbeat_value)
            continue
        embedding = embed_for(metric_type, crop_bytes)
        chroma_retry(
            "seed_upsert", collection.upsert, heartbeat=hb,
            ids=[doc_id], embeddings=[embedding],
            metadatas=[{
                "webtoon_id": webtoon_id, "episode": episode_no, "cut": cut_number,
                "face_idx": face_idx,
                # character_id는 표시용 라벨 관례(메인 루프와 동일: 이름 or cluster#id) —
                # 매칭이 실제로 쓰는 키는 appearance_id뿐이다.
                "character_id": character_name or f"cluster#{character_pk}",
                "appearance_id": appearance_id,
                "appearance_label": appearance_label, "character_name": character_name,
                "is_confirmed": True, "bbox_x1": x1, "bbox_y1": y1, "bbox_x2": x2, "bbox_y2": y2,
                "conf": conf or 0.0, "created_at": datetime.now(timezone.utc).isoformat(),
            }],
        )
        if heartbeat_cb:
            heartbeat_cb(heartbeat_value)
    return len(rows)


def _purge_human_negated_docs(webtoon_id: int, model_name: str, collection,
                              heartbeat=None) -> int:
    """human이 부정한 얼굴의 Chroma doc 제거 — 시딩(`_seed_confirmed_faces`)의 반대 방향.

    시딩은 human이 "이 캐릭터"라고 확정한 얼굴만 upsert하므로, human이 부정한 얼굴의
    step2 doc은 옛 캐릭터 밑에 잔존해 다음 에피소드에서 비슷한 얼굴을 그 캐릭터로
    끌어들인다(예: 주연에 섞였던 행인을 배정 해제해도 그 앵커가 주연 밑에 남음).
    대상: ① 배정 해제(human 정체 행의 appearance IS NULL) ② X 제외(is_used=false)
    ③ 탐지 삭제. 다른 캐릭터로의 "이동"은 시딩 upsert가 같은 doc_id를 덮어쓰므로
    여기 대상이 아니다. doc_id가 결정론적이라 Chroma에 없는 id 삭제는 무해(멱등).
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fr.face_idx, wc.cut_number, we.no AS episode_no, fe.chroma_doc_id
            FROM analysis_face_detection fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            JOIN webtoon_episode we ON wc.episode_id = we.id
            LEFT JOIN analysis_face_embedding fe
                   ON fe.detection_id = fr.id AND fe.embedding_model = %s
            WHERE we.webtoon_id = %s
              AND (
                fr.deleted_at IS NOT NULL
                OR fr.is_used = false
                OR EXISTS (
                  SELECT 1 FROM analysis_face_identity fi
                  WHERE fi.detection_id = fr.id AND fi.source = 'human'
                    AND fi.deleted_at IS NULL AND fi.appearance_id IS NULL
                )
              )
            """,
            (model_name, webtoon_id),
        )
        rows = cur.fetchall()

    doc_ids = sorted({
        chroma_doc_id or f"{webtoon_id}_{episode_no}_{cut_number}_F{face_idx}"
        for _face_id, face_idx, cut_number, episode_no, chroma_doc_id in rows
    })
    for i in range(0, len(doc_ids), 500):
        chroma_retry("purge_delete", collection.delete, heartbeat=heartbeat,
                     ids=doc_ids[i:i + 500])
    if doc_ids:
        logger.info(
            "[step2] human 부정 얼굴 Chroma doc 정리: webtoon_id=%s — 대상 id %d개 delete 요청"
            "(배정해제/제외/탐지삭제 — upsert된 적 없는 id 포함 가능, 미존재 삭제는 무해)",
            webtoon_id, len(doc_ids),
        )
    return len(doc_ids)


def _restore_missing_step2_docs(
    webtoon_id: int, source: str, title_id: str, model_name: str, metric_type: str,
    collection, heartbeat=None,
) -> int:
    """활성 step2 얼굴인데 Chroma doc이 없는 것을 재임베딩 upsert — 복원 방향 안전망.

    시딩(human 확정)·퍼지(human 부정)는 doc을 만들거나 지우는 방향만 커버한다. 제외했다
    복원(is_used false→true)한 step2-only 얼굴은 T3(sync_face_doc)가 실패하면 어느 배치
    경로도 doc을 되살리지 않아 앵커 공백이 영구화된다(2026-07-14 리뷰 — "배치가 안전망"
    명제가 삭제 방향만 참이던 비대칭 해소). 평시에는 존재 확인(get ids)만 하고 끝나며,
    누락분만 재임베딩하므로 비용은 누락 수에 비례한다(Chroma만 초기화된 드리프트 복구도
    이 경로로 수렴).
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fr.face_idx, fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fr.conf, fi.appearance_id, wc.cut_number, we.no AS episode_no,
                   ca.label AS appearance_label, c.id AS character_pk,
                   c.name AS character_name, fe.chroma_doc_id
            FROM analysis_face_detection fr
            JOIN analysis_face_identity fi ON fi.detection_id = fr.id
                 AND fi.source = 'step2' AND fi.deleted_at IS NULL
                 AND fi.appearance_id IS NOT NULL
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            JOIN webtoon_episode we ON wc.episode_id = we.id
            JOIN analysis_character_appearance ca ON ca.id = fi.appearance_id
                 AND ca.deleted_at IS NULL
            JOIN analysis_character c ON c.id = ca.character_id AND c.deleted_at IS NULL
            LEFT JOIN analysis_face_embedding fe
                   ON fe.detection_id = fr.id AND fe.embedding_model = %s
            WHERE we.webtoon_id = %s
              AND fr.is_used = true AND fr.deleted_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM analysis_face_identity h
                WHERE h.detection_id = fr.id AND h.source = 'human' AND h.deleted_at IS NULL
              )
            """,
            (model_name, webtoon_id),
        )
        rows = cur.fetchall()
    if not rows:
        return 0

    by_doc = {}
    for row in rows:
        doc_id = row[13] or f"{webtoon_id}_{row[9]}_{row[8]}_F{row[1]}"
        by_doc[doc_id] = row

    existing: set[str] = set()
    all_ids = sorted(by_doc)
    for i in range(0, len(all_ids), 500):
        got = chroma_retry("restore_check_get", collection.get, heartbeat=heartbeat,
                           ids=all_ids[i:i + 500], include=[])
        existing.update(got.get("ids") or [])
    missing = [d for d in all_ids if d not in existing]
    if not missing:
        return 0

    logger.info("[step2] step2 doc 누락 %d개 발견(T3 실패/드리프트) — 재임베딩 복원 시작", len(missing))
    restored = 0
    for doc_id in missing:
        (face_id, face_idx, x1, y1, x2, y2, conf, appearance_id, cut_number,
         episode_no, appearance_label, character_pk, character_name, _doc) = by_doc[doc_id]
        crop_bytes = fetch_face_crop(face_id, source, title_id)
        if heartbeat:
            heartbeat()
        if crop_bytes is None:
            continue
        embedding = embed_for(metric_type, crop_bytes)
        chroma_retry(
            "restore_upsert", collection.upsert, heartbeat=heartbeat,
            ids=[doc_id], embeddings=[embedding],
            metadatas=[{
                "webtoon_id": webtoon_id, "episode": episode_no, "cut": cut_number,
                "face_idx": face_idx,
                "character_id": character_name or f"cluster#{character_pk}",
                "appearance_id": appearance_id,
                "appearance_label": appearance_label, "character_name": character_name,
                "is_confirmed": False, "bbox_x1": x1, "bbox_y1": y1, "bbox_x2": x2, "bbox_y2": y2,
                "conf": conf or 0.0, "created_at": datetime.now(timezone.utc).isoformat(),
            }],
        )
        restored += 1
    logger.info("[step2] step2 doc 복원 완료: %d/%d (crop 없음 %d)",
                restored, len(missing), len(missing) - restored)
    return restored


def sync_face_doc(face_detection_id: int) -> dict:
    """human 교정 1건의 실시간 Chroma 투영(T3, prd §10) — 시딩/퍼지의 단건 즉시판.

    detection의 현재 유효 상태(human > step2)를 읽어 원하는 상태를 그대로 적용한다:
      - 탐지 삭제 / is_used=false / human 배정 해제(appearance NULL) → doc 삭제
      - 유효 정체의 캐릭터가 활성 → crop 재임베딩 후 그 캐릭터로 upsert
        (bbox 재크롭 후 재임베딩도 이 경로가 흡수 — DB 진실의 재투영이므로 멱등)
      - 정체 행이 아예 없음 → doc 삭제(멱등 — doc이 원래 없으면 no-op). 캐릭터 삭제로
        identity가 일괄 제거된 얼굴의 잔존 doc까지 이 경로가 청소한다.
    라벨링과 체인 동시 진행 시 "다음 step2 시딩까지 반영 지연" 레이스(2026-07-14
    바바리안 2881 실측 — 매 에피소드 마그넷 부활)를 없애는 실시간 경로. 배치
    시딩/퍼지/복원은 안전망으로 유지된다.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.face_idx, fr.is_used, fr.deleted_at IS NOT NULL AS det_deleted,
                   fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2, fr.conf,
                   wc.cut_number, we.no AS episode_no,
                   w.id AS webtoon_id, w.source, w.title_id,
                   eff.source AS ident_source, eff.appearance_id,
                   ca.label, (ca.id IS NOT NULL AND ca.deleted_at IS NULL) AS app_alive,
                   c.id AS char_pk, c.name,
                   (c.id IS NOT NULL AND c.deleted_at IS NULL) AS char_alive
            FROM analysis_face_detection fr
            JOIN webtoon_cut wc ON wc.id = fr.cut_id
            JOIN webtoon_episode we ON we.id = wc.episode_id
            JOIN webtoon w ON w.id = we.webtoon_id
            LEFT JOIN LATERAL (
                SELECT fi.source, fi.appearance_id
                FROM analysis_face_identity fi
                WHERE fi.detection_id = fr.id AND fi.deleted_at IS NULL
                ORDER BY (fi.source = 'human') DESC
                LIMIT 1
            ) eff ON true
            LEFT JOIN analysis_character_appearance ca ON ca.id = eff.appearance_id
            LEFT JOIN analysis_character c ON c.id = ca.character_id
            WHERE fr.id = %s
            """,
            (face_detection_id,),
        )
        row = cur.fetchone()
    if not row:
        return {"action": "skip", "reason": "detection 없음"}
    (face_idx, is_used, det_deleted, x1, y1, x2, y2, conf, cut_number, episode_no,
     webtoon_id, source, title_id, ident_source, appearance_id,
     app_label, app_alive, char_pk, char_name, char_alive) = row

    ctx = resolve_embedding_model(webtoon_id)
    model_name, metric_type = ctx["name"], ctx["metric_type"]
    with db_cursor() as cur:
        cur.execute(
            "SELECT chroma_doc_id FROM analysis_face_embedding WHERE detection_id = %s AND embedding_model = %s",
            (face_detection_id, model_name),
        )
        fe = cur.fetchone()
    doc_id = (fe[0] if fe and fe[0] else None) or f"{webtoon_id}_{episode_no}_{cut_number}_F{face_idx}"
    collection = get_face_collection(source, title_id, model_name)

    # 부정(삭제 방향): 탐지 삭제 / 제외 / human 배정해제 / 소멸 캐릭터 / 정체 행 전무.
    # "정체 없음→삭제"는 no-op이던 종전과 달리 캐릭터 삭제(identity 일괄 제거) 잔존 doc과
    # 하드 리셋 잔재까지 청소한다 — doc이 원래 없으면 미존재 id 삭제라 무해(멱등).
    negated = det_deleted or not is_used or appearance_id is None \
        or not (app_alive and char_alive)
    if negated:
        chroma_retry("t3_delete", collection.delete, ids=[doc_id])
        logger.info("[t3-sync] face=%s doc=%s 삭제 (used=%s, ident=%s/%s)",
                    face_detection_id, doc_id, is_used, ident_source, appearance_id)
        return {"action": "delete", "doc_id": doc_id}

    crop_bytes = fetch_face_crop(face_detection_id, source, title_id)
    if crop_bytes is None:
        logger.warning("[t3-sync] face=%s crop 없음 — 투영 생략", face_detection_id)
        return {"action": "skip", "reason": "crop 없음"}
    embedding = embed_for(metric_type, crop_bytes)
    char_label = char_name or f"cluster#{char_pk}"  # 표시용 라벨 관례(메인 루프와 동일)
    chroma_retry(
        "t3_upsert", collection.upsert,
        ids=[doc_id], embeddings=[embedding],
        metadatas=[{
            "webtoon_id": webtoon_id, "episode": episode_no, "cut": cut_number,
            "face_idx": face_idx, "character_id": char_label, "appearance_id": appearance_id,
            "appearance_label": app_label or "기본", "character_name": char_name,
            "is_confirmed": ident_source == "human",
            "bbox_x1": x1, "bbox_y1": y1, "bbox_x2": x2, "bbox_y2": y2,
            "conf": conf or 0.0, "created_at": datetime.now(timezone.utc).isoformat(),
        }],
    )
    logger.info("[t3-sync] face=%s doc=%s → '%s'(app=%s, %s) upsert",
                face_detection_id, doc_id, char_label, appearance_id, ident_source)
    return {"action": "upsert", "doc_id": doc_id, "appearance_id": appearance_id}


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
    """face_identity(step2)/face_embedding 현재 상태로 matched/new_chars 집계 (재시도 후에도 정확).
    match_score IS NULL이면 신규 클러스터, 값이 있으면 기존 캐릭터/클러스터 매칭."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE fe.match_score IS NOT NULL) AS matched,
                COUNT(*) FILTER (WHERE fe.match_score IS NULL) AS new_chars
            FROM analysis_face_detection fr
            JOIN analysis_face_identity fi ON fi.detection_id = fr.id
                 AND fi.source = 'step2' AND fi.deleted_at IS NULL
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            LEFT JOIN analysis_face_embedding fe ON fe.detection_id = fr.id
            WHERE wc.episode_id = %s AND fi.appearance_id IS NOT NULL
              AND fr.is_used = true
            """,
            (webtoon_episode_id,),
        )
        matched, new_chars = cur.fetchone()
        return matched or 0, new_chars or 0


def _record_merge_candidates(webtoon_id: int, candidates: dict) -> int:
    """매칭 중 감지된 중복 클러스터 경합 쌍을 suggestion(type=merge)으로 적재.

    후보 = `_find_match_ccip`의 `ambiguous_with`(1·2등 모두 threshold 이내 + 근소 차) —
    두 appearance가 같은 인물의 중복 클러스터라는 강한 신호(prd §18.5 v2). 사람이 수락하면
    service 병합 경로가 실제로 합친다(§19). episode_id는 넣지 않는다 — apply(step3c)의
    에피소드 스코프 pending 재적재(delete-reinsert)에 쓸려 지워지지 않게(§17.6).
    dedup: 웹툰 내 동일 (unordered) 캐릭터 쌍의 pending이 이미 있으면 재적재하지 않는다.
    """
    if not candidates:
        return 0
    with db_cursor() as cur:
        aids = sorted({a for pair in candidates for a in pair})
        cur.execute(
            """
            SELECT ca.id, ca.character_id FROM analysis_character_appearance ca
            JOIN analysis_character c ON c.id = ca.character_id AND c.deleted_at IS NULL
            WHERE ca.id = ANY(%s) AND ca.deleted_at IS NULL
            """,
            (aids,),
        )
        char_by_app = dict(cur.fetchall())
        cur.execute(
            """
            SELECT character_id, payload FROM analysis_suggestion
            WHERE webtoon_id = %s AND type = 'merge' AND status = 'pending' AND deleted_at IS NULL
            """,
            (webtoon_id,),
        )
        existing = set()
        for cid, payload in cur.fetchall():
            for other in (payload or {}).get("other_character_ids") or []:
                existing.add(frozenset({cid, other}))

        now = datetime.now(timezone.utc)
        inserted = 0
        for pair, evidence in candidates.items():
            char_ids = {char_by_app.get(a) for a in pair}
            char_ids.discard(None)
            if len(char_ids) != 2:
                continue  # 같은 캐릭터의 외형끼리거나 해석 불가 — 병합 제안 무의미
            key = frozenset(char_ids)
            if key in existing:
                continue
            primary, other = sorted(char_ids)
            cur.execute(
                """
                INSERT INTO analysis_suggestion
                    (webtoon_id, type, character_id, payload, status, created_at, updated_at)
                VALUES (%s, 'merge', %s, %s, 'pending', %s, %s)
                """,
                (webtoon_id, primary,
                 Json({"other_character_ids": [other],
                       "evidence": f"step2 매칭 경합 — ep{evidence['episode_no']} 컷{evidence['cut']} "
                                   f"얼굴이 두 클러스터 모두와 threshold 이내(diff {evidence['score']})",
                       "source": "step2"}),
                 now, now),
            )
            existing.add(key)
            inserted += 1
    return inserted


# ── 에피소드 단위 식별 진입점 (Temporal 액티비티가 호출) ──────────────────────

def identify_episode_faces(
    webtoon_episode_id: int,
    episode_no: int,
    heartbeat_cb: Optional[Callable[[int], None]] = None,
    resume_from: int = 0,
    run_id: Optional[int] = None,
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

    # ── 흐름-first 얼굴 강등(redesign §5·Phase 5) ──────────────────────────────────
    # flow_first_enabled면 CCIP 정체 결합을 전면 스킵한다. 정체성 척추가 익명 슬롯+대사 흐름으로
    # 옮겨졌으므로(step3 정리단계), CCIP의 교차회차 매칭이 만들던 자석(죽은 카락·빙의 이한수에 얼굴
    # 흡수)을 원천 차단한다. 얼굴은 step1 탐지(analysis_face_detection)로만 존재하고, 대표 crop은
    # 후속(Phase 5.2)이 슬롯별로 고른다. human 확정 얼굴(source='human')은 그대로 서빙(불가침).
    if _flow_first_enabled(webtoon_id):
        with db_cursor() as cur:
            cur.execute(
                """SELECT count(*) FROM analysis_face_detection d
                   JOIN webtoon_cut c ON c.id = d.cut_id
                   WHERE c.episode_id = %s AND d.deleted_at IS NULL""",
                (webtoon_episode_id,),
            )
            n_faces = cur.fetchone()[0]
        logger.info("[step2] ep_id=%s flow_first — CCIP 정체 결합 스킵(얼굴 강등, Phase 5). 얼굴 %s개는 "
                    "step1 탐지로만 존재(자석 원천 차단).", webtoon_episode_id, n_faces)
        return {"mode": "flow_first_skip", "n_faces": n_faces, "n_matched": 0, "n_new": 0}

    ctx = resolve_embedding_model(webtoon_id)
    model_name = ctx["name"]
    metric_type = ctx["metric_type"]
    threshold = ctx["threshold"]

    # 순차 루프 진입 전 구간용 heartbeat(같은 resume 값 재전송 — 타이머 갱신만).
    hb_pre = (lambda: heartbeat_cb(resume_from)) if heartbeat_cb else None

    collection = get_face_collection(source, title_id, model_name, heartbeat=hb_pre)
    _seed_confirmed_faces(
        webtoon_id, source, title_id, collection, model_name, metric_type,
        heartbeat_cb=heartbeat_cb, heartbeat_value=resume_from,
    )
    # 시딩(확정 upsert)과 짝: human이 부정한 얼굴의 잔존 doc 제거 — anchor 적재 전에
    # 실행해야 이번 에피소드 매칭 후보에서 확실히 빠진다.
    _purge_human_negated_docs(webtoon_id, model_name, collection, heartbeat=hb_pre)
    # 복원 방향 안전망: 활성 step2 얼굴인데 doc이 없는 것(T3 실패한 복원 등) 재투영.
    _restore_missing_step2_docs(
        webtoon_id, source, title_id, model_name, metric_type, collection, heartbeat=hb_pre,
    )
    # 리스트로 고정 — 아래에서 유령 appearance_id를 발견할 때마다 append해 이후 쿼리에서도
    # 제외되게 한다(list는 참조로 넘겨지므로 find_match 재호출 시 갱신된 내용이 그대로 반영됨).
    excluded_appearance_ids = list(_get_excluded_appearance_ids(webtoon_id))
    valid_appearance_ids = _get_valid_appearance_ids(webtoon_id)

    # collection.get()/일괄 임베딩이 heartbeat_timeout(2분)보다 오래 걸릴 수 있어,
    # 순차 루프 진입 전인 이 구간에서도 heartbeat로 타임아웃 타이머를 미리 갱신해둔다.
    if heartbeat_cb:
        heartbeat_cb(resume_from)

    # ccip는 anchor 전체를 브루트포스로 비교해야 해서(§matching.py) 얼굴마다
    # collection.get()으로 전체 재조회하지 않도록 에피소드당 1회만 적재해 캐시한다.
    # 새로 추가되는 얼굴(매칭/신규 캐릭터)은 루프 중 캐시에 직접 append해 갱신.
    ccip_anchors = (load_ccip_anchors(collection, excluded_appearance_ids, heartbeat=hb_pre)
                    if metric_type == "ccip" else None)

    if ccip_anchors is not None:
        stale = [a for a in ccip_anchors if a["meta"]["appearance_id"] not in valid_appearance_ids]
        if stale:
            stale_ids = sorted({a["meta"]["appearance_id"] for a in stale})
            logger.warning(
                "[step2] ep_id=%s — Chroma anchor %d개가 Postgres에 없는 appearance_id를 "
                "가리켜 매칭 후보에서 제외(DB/Chroma 드리프트, 예: 리셋 후 Chroma 정리 누락): %s",
                webtoon_episode_id, len(stale), stale_ids,
            )
            ccip_anchors = [a for a in ccip_anchors if a["meta"]["appearance_id"] in valid_appearance_ids]

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
    # 중복 클러스터 경합(1·2등 모두 in-threshold) 쌍 — 에피소드 끝에 merge 제안으로 적재.
    merge_candidates: dict = {}  # frozenset({aid1, aid2}) → 최초 발견 evidence
    for i, face in enumerate(pending):
        feature = features_by_face_id.get(face["id"])
        if feature is None:
            logger.warning("[step2] crop not found face_id=%s, skip", face["id"])
            if heartbeat_cb:
                heartbeat_cb(resume_from + i + 1)
            continue

        doc_id = f"{webtoon_id}_{episode_no}_{face['cut_number']}_F{face['face_idx']}"
        # 이 얼굴 처리 중의 chroma_retry 대기용 — 직전 완료 인덱스를 재전송(재개 지점 보존).
        hb_loop = (lambda: heartbeat_cb(resume_from + i)) if heartbeat_cb else None

        match = find_match(
            collection, feature, metric_type, threshold, excluded_appearance_ids,
            ccip_anchors=ccip_anchors, heartbeat=hb_loop,
        )
        if match is not None and match["meta"]["appearance_id"] not in valid_appearance_ids:
            # cosine 경로는 Chroma를 직접 쿼리해 사전 필터(위 ccip_anchors 필터링)를
            # 못 거치므로 여기서 최종 방어선으로 재검증한다. 발견한 유령 id는 이후
            # 같은 에피소드 내 재매칭을 막도록 제외 목록에 추가(list라 참조로 반영됨).
            stale_id = match["meta"]["appearance_id"]
            logger.warning(
                "[step2] ep_id=%s cut=%s face_idx=%s — 매칭된 appearance_id=%s가 Postgres에 "
                "없음(DB/Chroma 드리프트) — 신규 캐릭터로 재할당",
                webtoon_episode_id, face["cut_number"], face["face_idx"], stale_id,
            )
            excluded_appearance_ids.append(stale_id)
            if ccip_anchors is not None:
                ccip_anchors = [a for a in ccip_anchors if a["meta"]["appearance_id"] != stale_id]
            match = None

        if match is not None:
            meta = match["meta"]
            appearance_id = meta["appearance_id"]
            char_name = meta.get("character_name") or meta["character_id"]
            match_score = match["score"]
            n_matched += 1
            _update_character_first_seen(appearance_id, webtoon_episode_id, episode_no, face["cut_number"])
            other = match.get("ambiguous_with")
            if other is not None and other.get("appearance_id") not in (None, appearance_id):
                merge_candidates.setdefault(
                    frozenset({appearance_id, other["appearance_id"]}),
                    {"episode_no": episode_no, "cut": face["cut_number"],
                     "score": round(float(match["score"]), 4)},
                )
        else:
            allocated = _allocate_character(webtoon_id, webtoon_episode_id, face["cut_number"])
            appearance_id = allocated["appearance_id"]
            char_name = allocated["char_name"]
            match_score = None
            n_new += 1
            # 방금 만든 appearance_id를 실존 집합에 즉시 반영 — 그래야 같은 에피소드 내
            # 다음 얼굴이 이 캐릭터와 매칭됐을 때 "valid_appearance_ids 스냅샷에 없다"는
            # 이유로 유령(ghost)으로 오판되어 신규 캐릭터로 잘못 쪼개지는 걸 막는다.
            valid_appearance_ids.add(appearance_id)

        b = face["bbox"]
        meta_doc = {
            "webtoon_id": webtoon_id, "episode": episode_no, "cut": face["cut_number"],
            "face_idx": face["face_idx"], "character_id": char_name, "appearance_id": appearance_id,
            "appearance_label": "기본", "character_name": char_name, "is_confirmed": False,
            "bbox_x1": b[0], "bbox_y1": b[1], "bbox_x2": b[2], "bbox_y2": b[3],
            "conf": face["conf"], "created_at": datetime.now(timezone.utc).isoformat(),
        }
        chroma_retry("upsert", collection.upsert, heartbeat=hb_loop,
                     ids=[doc_id], embeddings=[feature], metadatas=[meta_doc])
        if ccip_anchors is not None and match is None:
            # 같은 에피소드 내 다음 얼굴이 방금 만든 신규 캐릭터와 매칭될 수 있어 캐시에 반영.
            ccip_anchors.append({"embedding": feature, "meta": meta_doc})

        _upsert_face_identity(face["id"], appearance_id, match_score, run_id)
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

    n_merge_suggested = _record_merge_candidates(webtoon_id, merge_candidates)

    matched_n, new_n = _summarize_episode_faces(webtoon_episode_id)
    logger.info(
        "[step2] 에피소드 식별 완료 ep_id=%s episode_no=%s: faces=%d, matched=%d, new_chars=%d, "
        "병합후보 제안=%d",
        webtoon_episode_id, episode_no, len(faces), matched_n, new_n, n_merge_suggested,
    )
    return {"faces": len(faces), "matched": matched_n, "new_chars": new_n,
            "merge_suggested": n_merge_suggested}
