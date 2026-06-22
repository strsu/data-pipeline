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
    """에피소드 단위 임베딩+매칭 1패스. 반환: {faces, matched, new_chars}.

    얼굴 수가 많거나 anchor 집합이 커지면 처리 시간이 활동 타임아웃에 가까워질 수
    있다. heartbeat로 처리 완료한 얼굴 인덱스를 기록해두면, 타임아웃으로 재시도될 때
    처음부터 다시 처리하지 않고 이어서 진행한다.
    """
    from src.core import step2

    info = activity.info()
    resume_from = info.heartbeat_details[0] if info.heartbeat_details else 0

    def _heartbeat(done_count: int) -> None:
        activity.heartbeat(done_count)

    return step2.identify_episode_faces(
        ep.webtoon_episode_id, ep.episode_no,
        heartbeat_cb=_heartbeat, resume_from=resume_from,
    )


@activity.defn
def is_phase1_done(webtoon_episode_id: int) -> bool:
    """Step1(OCR/YOLO)이 이미 완료(episode_pipeline_progress phase=1 completed)됐는지."""
    from src.config.db import db_cursor
    with db_cursor() as cur:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM episode_pipeline_progress "
            "WHERE episode_id = %s AND phase = 1 AND status = 'completed')",
            (webtoon_episode_id,),
        )
        return bool(cur.fetchone()[0])


# ── Step3: LLM 장면/화자 분석 (활성 웹툰만) ───────────────────────────────────

@activity.defn
def is_phase3_enabled(webtoon_episode_id: int) -> bool:
    """해당 웹툰의 phase3_enabled 여부."""
    from src.config.db import db_cursor
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(wps.phase3_enabled, false)
            FROM webtoon_episode we
            JOIN webtoon_pipeline_state wps ON wps.webtoon_id = we.webtoon_id
            WHERE we.id = %s
            """,
            (webtoon_episode_id,),
        )
        row = cur.fetchone()
        return bool(row[0]) if row else False


@activity.defn
def scene_llm_cut(cut: CutRef, prev_context: str) -> str:
    """단일 컷 LLM 분석. 반환: 다음 컷용 prev_context."""
    from src.core import step3
    return step3.analyze_cut_scene(
        cut.source, cut.title_id, cut.episode_no, cut.webtoon_episode_id,
        cut.cut_no, prev_context,
    )


@activity.defn
def prepare_scene(ep: EpisodeInput) -> None:
    """Step3 시작 시 기존 llm 어노테이션/scene_meta 정리(재실행 완전 교체)."""
    from src.core import step3
    step3.prepare_episode_scene(ep.webtoon_episode_id)
