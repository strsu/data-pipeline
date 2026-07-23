"""AnalysisRun 라이프사이클 — v4.0 "run 단위 쓰고 버리기"의 파이프라인 쪽 헬퍼(§17.1).

run은 **결과 정본**이다(Temporal=실행 정본):
  - LLM 산출물(text_annotation llm행/cut_scene_meta/episode_report/episode_beat/
    narrative_thread/character_claim/suggestion/character_profile llm행)은 run FK로 귀속된다.
  - 진행도는 저장하지 않고 도출한다: "step N 됐나" = 해당 kind의 succeeded run 존재,
    "stale인가" = webtoon_cut.human_modified_at > 최신 succeeded resolve run.finished_at.

kind: step1 | step2 | vision | resolve | arc (service `AnalysisRunKind`와 일치).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2.extras import Json

from src.config.db import db_cursor

logger = logging.getLogger(__name__)

KIND_STEP1 = "step1"
KIND_STEP2 = "step2"
KIND_VISION = "vision"
KIND_RESOLVE = "resolve"
KIND_ARC = "arc"
KIND_PROFILE = "profile"  # 캐릭터 프로필 재도출(§20) — episode NULL, stats.character_id/mode


def start_run(
    webtoon_id: int,
    episode_id: Optional[int],
    kind: str,
    *,
    llm_model_id: Optional[int] = None,
    vision_run_id: Optional[int] = None,
    stats: Optional[dict] = None,
) -> int:
    """새 run(status=running)을 만들고 id를 반환한다.

    같은 (episode, kind)의 기존 running 행은 failed(superseded)로 정리한다 — Temporal 재시도가
    새 run을 시작할 때 이전 시도의 고아 행이 running으로 남지 않게(멱등 재시도 안전).
    stats: 시작 시점 마킹(예: {"origin": "regen"} — 정리 패스 심판의 자동수락 제외 판별,
    §22.6 수렴 가드). finish_run이 merge라 종료 시에도 보존된다.
    """
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE analysis_run
            SET status='failed', error='superseded by new run', finished_at=%s, updated_at=%s
            WHERE kind=%s AND status='running'
              AND (episode_id = %s OR (episode_id IS NULL AND %s::int IS NULL AND webtoon_id=%s))
            """,
            (now, now, kind, episode_id, episode_id, webtoon_id),
        )
        cur.execute(
            """
            INSERT INTO analysis_run
                (webtoon_id, episode_id, kind, status, llm_model_id, vision_run_id,
                 started_at, stats, error, created_at, updated_at)
            VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, '', %s, %s)
            RETURNING id
            """,
            (webtoon_id, episode_id, kind, llm_model_id, vision_run_id, now, Json(stats or {}),
             now, now),
        )
        run_id = cur.fetchone()[0]
    logger.info("[runs] start run=%s kind=%s webtoon=%s episode=%s", run_id, kind, webtoon_id, episode_id)
    return run_id


def finish_run(
    run_id: int,
    *,
    status: str = "succeeded",
    stats: Optional[dict] = None,
    error: str = "",
) -> None:
    """run을 종료 상태로 전이하고 stats/error를 기록한다.

    stats는 기존 값에 **merge**(`||`)한다 — start_run이 시작 시점에 심은 마킹
    (예: origin='regen')을 종료 통계가 덮어 지우지 않게. 기존 호출은 전부 시작 stats가
    빈 dict였으므로 merge == replace(동작 불변).
    """
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE analysis_run
            SET status=%s, stats=COALESCE(stats, '{}'::jsonb) || %s, error=%s, finished_at=%s, updated_at=%s
            WHERE id=%s
            """,
            (status, Json(stats or {}), error or "", now, now, run_id),
        )
    logger.info("[runs] finish run=%s status=%s", run_id, status)


def fail_run_if_running(run_id: int, error: str) -> bool:
    """run이 아직 running일 때만 failed로 전이한다. 전이했으면 True.

    `finish_run`과 달리 종료 상태(succeeded/failed)를 덮지 않는다 — step3c 최종 실패
    (액티비티 재시도 소진) 시 워크플로가 running 좀비를 정리하는 용도. apply가 커밋·
    succeeded 전이까지 끝낸 뒤 결과 보고만 유실된 경계 케이스에서 succeeded를 되돌리지
    않기 위한 가드다.
    """
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE analysis_run
            SET status='failed', error=%s, finished_at=%s, updated_at=%s
            WHERE id=%s AND status='running'
            """,
            (error or "", now, now, run_id),
        )
        changed = bool(cur.rowcount)
    if changed:
        logger.info("[runs] fail-if-running run=%s → failed: %s", run_id, error)
    return changed


def record_completed_run(webtoon_id: int, episode_id: int, kind: str, stats: Optional[dict] = None) -> int:
    """시작/종료를 한 번에 기록하는 원샷 완료 run(step1/step2 완료 마킹용).

    step1/2는 산출물이 run FK로 귀속되지 않으므로(불변 탐지/매칭 레이어) 완료 원장 행만 남긴다
    — 구 episode_pipeline_progress(phase, completed)의 대체.
    """
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_run
                (webtoon_id, episode_id, kind, status, started_at, finished_at,
                 stats, error, created_at, updated_at)
            VALUES (%s, %s, %s, 'succeeded', %s, %s, %s, '', %s, %s)
            RETURNING id
            """,
            (webtoon_id, episode_id, kind, now, now, Json(stats or {}), now, now),
        )
        return cur.fetchone()[0]


def latest_succeeded_run_id(episode_id: int, kind: str) -> Optional[int]:
    """에피소드의 최신 succeeded run id(서빙/입력 선택 기준). 없으면 None."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT id FROM analysis_run
            WHERE episode_id=%s AND kind=%s AND status='succeeded' AND deleted_at IS NULL
            ORDER BY finished_at DESC NULLS LAST, id DESC
            LIMIT 1
            """,
            (episode_id, kind),
        )
        row = cur.fetchone()
        return row[0] if row else None


def episode_needs_reresolve(episode_id: int) -> bool:
    """human 수정이 최신 succeeded resolve run 이후에 있었는지(=stale 도출, §17.1).

    resolve run이 아예 없으면 '최초 해소 필요'이지 재해소 대상이 아니므로 False를 반환한다
    (최초 해소는 정규 step3 경로가 처리).
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT ar.finished_at
            FROM analysis_run ar
            WHERE ar.episode_id=%s AND ar.kind='resolve' AND ar.status='succeeded'
              AND ar.deleted_at IS NULL
            ORDER BY ar.finished_at DESC NULLS LAST, ar.id DESC
            LIMIT 1
            """,
            (episode_id,),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return False
        cur.execute(
            """
            SELECT EXISTS(
                SELECT 1 FROM webtoon_cut
                WHERE episode_id=%s AND human_modified_at IS NOT NULL AND human_modified_at > %s
            )
            """,
            (episode_id, row[0]),
        )
        return bool(cur.fetchone()[0])


