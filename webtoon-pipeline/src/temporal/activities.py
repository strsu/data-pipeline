"""Temporal 액티비티 — 코어(faust-free) 로직을 동기 호출하는 얇은 래퍼.

모든 I/O(model-api HTTP / DB / S3 / Chroma)는 core.step1 / core.step2가 담당.
코어/무거운 의존성은 **함수 내부에서 지연 import**한다 — 액티비티 모듈을 import하는 것만으로
chromadb/boto3/psycopg2를 끌어오지 않게 해, 오케스트레이션 단위 테스트(temporalio만)로도 워크플로를
검증할 수 있게 한다.
"""
from __future__ import annotations

from temporalio import activity

from src.temporal.shared import CutRef, EpisodeInput


# ── 메타 조회 ─────────────────────────────────────────────────────────────────

@activity.defn
def get_episode_max_cut(ep: EpisodeInput) -> int:
    """에피소드 총 컷 수(webtoon_episode.image_count)."""
    from src.core import step1
    return step1.get_image_count(ep.webtoon_episode_id)


@activity.defn
def resolve_episode(source: str, title_id: str, episode_no: int) -> EpisodeInput | None:
    """(source, title_id, episode_no) → 처리 대상 EpisodeInput.

    다운로드 완료됐고 아직 phase1 미완료인 에피소드면 반환, 아니면 None.
    """
    from src.config.db import db_cursor

    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.id
            FROM webtoon_episode we
            JOIN webtoon w ON we.webtoon_id = w.id
            WHERE w.source = %s AND w.title_id = %s AND we.no = %s
              AND we.is_downloaded = true AND we.deleted_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM episode_pipeline_progress p
                WHERE p.episode_id = we.id AND p.phase = 1
                  AND p.status IN ('completed', 'error')
              )
            """,
            (source, title_id, episode_no),
        )
        row = cur.fetchone()
    if not row:
        return None
    return EpisodeInput(
        source=source, title_id=title_id, episode_no=episode_no,
        webtoon_episode_id=row[0], start_cut=1, max_cut=0,
    )


@activity.defn
def mark_phase1_complete(ep: EpisodeInput) -> None:
    """에피소드 phase1 완료를 episode_pipeline_progress에 기록."""
    from datetime import datetime, timezone
    from src.config.db import db_cursor

    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO episode_pipeline_progress
                (episode_id, phase, status, completed_at, created_at, updated_at)
            VALUES (%s, 1, 'completed', %s, %s, %s)
            ON CONFLICT (episode_id, phase)
            DO UPDATE SET status = 'completed', completed_at = EXCLUDED.completed_at,
                          updated_at = EXCLUDED.updated_at
            """,
            (ep.webtoon_episode_id, now, now, now),
        )


# ── 에피소드 준비(재처리 정리) ────────────────────────────────────────────────

@activity.defn
def prepare_episode(ep: EpisodeInput) -> None:
    """에피소드 시작 시 기존 OCR/얼굴 데이터 정리(재처리 멱등)."""
    from src.core import step1
    step1.prepare_episode_ocr(ep.webtoon_episode_id)
    step1.prepare_episode_yolo(ep.webtoon_episode_id, ep.source, ep.title_id)


# ── Step1: OCR / YOLO (분리) ──────────────────────────────────────────────────

@activity.defn
def ocr_cut(cut: CutRef) -> bool:
    """단일 컷 OCR. 반환: 다음 컷 존재 여부."""
    from src.core import step1
    return step1.process_cut_ocr(
        cut.source, cut.title_id, cut.episode_no, cut.webtoon_episode_id, cut.cut_no
    )


@activity.defn
def yolo_cut(cut: CutRef) -> bool:
    """단일 컷 YOLO. 반환: 다음 컷 존재 여부."""
    from src.core import step1
    return step1.process_cut_yolo(
        cut.source, cut.title_id, cut.episode_no, cut.webtoon_episode_id, cut.cut_no
    )


# ── Step2: 얼굴 식별 (에피소드 단위) ──────────────────────────────────────────

@activity.defn
def face_identify_episode(ep: EpisodeInput) -> dict:
    """에피소드 단위 임베딩+매칭 1패스. 반환: {faces, matched, new_chars}."""
    from src.core import step2
    return step2.identify_episode_faces(ep.webtoon_episode_id, ep.episode_no)
