"""Temporal 액티비티 — 코어(faust-free) 로직을 동기 호출하는 얇은 래퍼.

모든 I/O(model-api HTTP / DB / S3 / Chroma)는 core.step1 / core.step2 / core.step3가 담당.
코어/무거운 의존성은 **함수 내부에서 지연 import**한다 — 액티비티 모듈을 import하는 것만으로
chromadb/boto3/psycopg2를 끌어오지 않게 해, 오케스트레이션 단위 테스트(temporalio만)로도 워크플로를
검증할 수 있게 한다.

액티비티는 두 부류로 나뉜다:
- 오케스트레이터 큐(ORCH_QUEUE)에서 도는 가벼운 메타/판정 액티비티
  (resolve_episode_for_chain / next_chain_episode / mark_phase_complete / is_phase3_enabled).
- step별 전용 큐(STEP1/2/3_QUEUE, 동시성 1)에서 도는 무거운 작업 액티비티
  (prepare_episode / step1_episode / face_identify_episode / step3_episode).
어느 큐에서 실행할지는 호출하는 워크플로(EpisodeChainWorkflow)가 task_queue로 지정한다.
"""
from __future__ import annotations

from temporalio import activity

from src.temporal.shared import STEP_PHASE, ChainInput, EpisodeInput


# ── 체인 메타/판정 (오케스트레이터 큐) ────────────────────────────────────────

@activity.defn
def resolve_episode_for_chain(source: str, title_id: str, episode_no: int) -> EpisodeInput | None:
    """(source, title_id, episode_no) → EpisodeInput. 다운로드 완료 + 미삭제 회차만.

    체인이 "이번 회차"를 실행하기 위해 webtoon_episode_id를 해석한다. 회차가 없거나
    아직 다운로드되지 않았으면 None — 워크플로는 이 회차 작업을 건너뛰고 다음으로 넘어간다.
    phase 완료 여부로는 필터링하지 않는다(어떤 step을 돌릴지는 체인의 steps가 결정).
    """
    from src.config.db import db_cursor

    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.id, we.is_downloaded
            FROM webtoon_episode we
            JOIN webtoon w ON we.webtoon_id = w.id
            WHERE w.source = %s AND w.title_id = %s AND we.no = %s
              AND we.deleted_at IS NULL
            """,
            (source, title_id, episode_no),
        )
        row = cur.fetchone()
    if not row or not row[1]:
        return None
    return EpisodeInput(
        source=source, title_id=title_id, episode_no=episode_no,
        webtoon_episode_id=row[0], start_cut=1, max_cut=0,
    )


@activity.defn
def next_chain_episode(inp: ChainInput) -> int | None:
    """모든 step 조합 공통 "다음 ep로 이어갈지" 판정. 다음 회차 번호 또는 None.

    - 범위(bounded) 모드(max_ep > 0): cur_ep + 1 이 max_ep 이하면 그 번호, 아니면 None.
      (admin 범위 실행 — 비어 있는 회차는 워크플로가 작업 없이 통과한다.)
    - 자동(unbounded) 모드(max_ep == 0): 진입 step(steps[0])이 아직 종료되지 않은,
      cur_ep 보다 큰 다음 다운로드 회차를 찾는다. 없으면 None(체인 종료).
      진입 step의 종료 여부는 episode_pipeline_progress(phase, status in completed/error)로 판정.
    """
    if inp.max_ep and inp.max_ep > 0:
        nxt = inp.cur_ep + 1
        return nxt if nxt <= inp.max_ep else None

    from src.config.db import db_cursor

    entry_step = inp.steps[0] if inp.steps else "step1"
    phase = STEP_PHASE.get(entry_step, 1)
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.no
            FROM webtoon_episode we
            JOIN webtoon w ON we.webtoon_id = w.id
            WHERE w.source = %s AND w.title_id = %s AND we.no > %s
              AND we.is_downloaded = true AND we.deleted_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM episode_pipeline_progress p
                WHERE p.episode_id = we.id AND p.phase = %s
                  AND p.status IN ('completed', 'error')
              )
            ORDER BY we.no
            LIMIT 1
            """,
            (inp.source, inp.title_id, inp.cur_ep, phase),
        )
        row = cur.fetchone()
    return row[0] if row else None


@activity.defn
def mark_phase_complete(ep: EpisodeInput, phase: int) -> None:
    """에피소드의 특정 phase 완료를 episode_pipeline_progress에 멱등 기록.

    (episode, phase) 1행 — 자동 모드 다음-ep 판정(`next_chain_episode`)이 이 행으로
    진입 step 종료 여부를 본다. 컷/얼굴 데이터 정리는 step별 prepare가 따로 수행한다.
    """
    from datetime import datetime, timezone
    from src.config.db import db_cursor

    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO episode_pipeline_progress
                (episode_id, phase, status, completed_at, created_at, updated_at)
            VALUES (%s, %s, 'completed', %s, %s, %s)
            ON CONFLICT (episode_id, phase)
            DO UPDATE SET status = 'completed', completed_at = EXCLUDED.completed_at,
                          updated_at = EXCLUDED.updated_at
            """,
            (ep.webtoon_episode_id, phase, now, now, now),
        )


@activity.defn
def is_phase3_enabled(webtoon_episode_id: int) -> bool:
    """해당 웹툰의 phase3_enabled 여부 (step3 자동 게이트)."""
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


# ── step 작업 (step별 전용 큐, 동시성 1) ──────────────────────────────────────

@activity.defn
def prepare_episode(ep: EpisodeInput) -> None:
    """Step1 시작 시 기존 OCR/얼굴/세그먼트 데이터 정리(재처리 멱등)."""
    from src.core import step1
    step1.prepare_episode_ocr(ep.webtoon_episode_id)
    step1.prepare_episode_yolo(ep.webtoon_episode_id, ep.source, ep.title_id)
    step1.prepare_episode_segments(ep.webtoon_episode_id)


@activity.defn
def step1_episode(ep: EpisodeInput) -> dict:
    """에피소드 전체를 스트립으로 결합 후 콘텐츠 세그먼트 단위로 OCR+YOLO를 단일 패스로 처리.

    단일 다운로드/분할로 세그먼트마다 OCR과 YOLO를 함께 실행한다. 세그먼트 완료마다
    heartbeat_cb(처리한 세그먼트 수)를 호출해 Temporal 하트비트로 전달한다.
    반환: {"segments": n, "texts": t, "faces": f}.
    """
    from src.core import step1

    def _hb(done: int) -> None:
        activity.heartbeat(done)

    return step1.process_episode_step1(
        ep.source, ep.title_id, ep.episode_no, ep.webtoon_episode_id, heartbeat_cb=_hb
    )


@activity.defn
def face_identify_episode(ep: EpisodeInput) -> dict:
    """에피소드 단위 임베딩+매칭 1패스. 반환: {faces, matched, new_chars}.

    얼굴 수가 많거나 anchor 집합이 커지면 처리 시간이 활동 타임아웃에 가까워질 수 있다.
    heartbeat로 처리 완료한 얼굴 인덱스를 기록해두면, 타임아웃 재시도 시 처음부터 다시
    처리하지 않고 이어서 진행한다.
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
def step3_episode(ep: EpisodeInput) -> dict:
    """에피소드의 모든 컷을 순차 LLM 분석(Step3). 반환: {"cuts_analyzed": n}.

    기존 컷별 분석을 에피소드 단위 1개 액티비티로 흡수한다(컷마다 continue-as-new 불필요).
    prev_context 연속성은 코어 analyze_episode_scenes가 컷을 순차로 돌며 유지하고, 컷마다
    heartbeat를 보내 긴 LLM 처리에서도 타임아웃 타이머를 갱신한다. 기존 'llm' 어노테이션
    정리는 코어가 내부에서 수행한다(재실행 완전 교체).
    """
    from src.core import step3

    def _hb(done: int) -> None:
        activity.heartbeat(done)

    return step3.analyze_episode_scenes(ep.webtoon_episode_id, heartbeat_cb=_hb)


# ── step3 2-pass (step3a 추출 → step3b 해소 → step3c 커밋) ─────────────────────
#
# 에피소드 단위 2-pass 재구성(Req 9.1). LLM 스테이지는 2개로 한정한다:
#   step3a_extract — 비전(Pass-1, 컷당 1콜),  step3b_resolve — 텍스트(Pass-2a, 윈도우).
#   step3c_apply   — LLM 없음(Pass-2b, 결정론 커밋).
#
# 단계 간 데이터 전달(Req 9.3): step3a의 `ExtractResult`(Pass-1 레코드 + belief)와 step3b의
# `ResolveResult`를 activity 반환값/입력으로 흘린다. 이 레포 Temporal 워커는 기본 데이터 컨버터를
# 쓰며 별도 커스텀 컨버터가 없다(shared.EpisodeInput 데이터클래스를 그대로 주고받는 것이 그 증거).
# 다만 activities 모듈은 **무거운 코어 의존성을 지연 import** 하는 계약을 지켜야 하므로(모듈 import만으로
# chromadb/boto3/psycopg2를 끌어오지 않음), 코어 데이터클래스(ExtractResult/ResolveResult/Pass1Record)를
# 모듈 최상단에서 import해 시그니처 타입으로 쓰지 않는다. 대신 **경계에서 dict로 직렬화**한다
# (반환은 dataclasses.asdict, 입력은 함수 내부 지연 import로 재구성). 시그니처 타입은 내장 `dict`라
# 컨버터가 그대로 통과시키며, 코어 dict/list 필드는 모두 JSON 직렬화 가능하다.


@activity.defn
def step3a_extract(ep: EpisodeInput) -> dict:
    """Pass-1 추출(step3a) — 에피소드 컷을 비전 1콜씩 순회. 반환: `ExtractResult` 직렬화 dict.

    `step3.extract_episode`가 컷마다 `heartbeat_cb`(누적 처리 컷 수)를 호출하므로, 컷 단위로
    Temporal 하트비트를 보내 긴 비전 루프에서도 타임아웃 타이머를 갱신한다(Req 9.4). 결과
    `ExtractResult`(Pass-1 레코드 + belief state + usage 집계)는 `asdict`로 직렬화해 step3b 입력으로
    넘긴다(Req 9.3). per-call LLMUsage 적재는 코어가 내부에서 수행한다(Req 6.7).
    """
    from dataclasses import asdict
    from src.core import step3

    def _hb(done: int) -> None:
        activity.heartbeat(done)

    result = step3.extract_episode(ep.webtoon_episode_id, heartbeat_cb=_hb)
    return asdict(result)


@activity.defn
def step3b_resolve(ep: EpisodeInput, extract: dict) -> dict:
    """Pass-2a 전역 해소(step3b) — 누적 서사 prior 조립 후 에피소드 텍스트 해소. 반환: `ResolveResult` dict.

    `extract`는 step3a_extract가 반환한 `ExtractResult` 직렬화 dict다. 누적 서사 컨텍스트(prior)는
    `narrative_context.load_prior(webtoon_id, ep.episode_no)`로 조립한다(이전 화까지의 확정 로스터/
    미해결 떡밥/running 요약 — Req 4.1, 11.3). webtoon_id는 `EpisodeInput`에 없으므로 코어 헬퍼
    `step3._get_webtoon_id(webtoon_episode_id)`로 해석한다(현재 코어에 공개 경로가 없어 private 헬퍼를
    사용 — 문서화). 해소 진입점은 컨텍스트 적응형 윈도우(`resolve_episode_windowed`)이며 토큰 예산에
    따라 단일콜/다중윈도우를 자동 선택한다(Req 8). 윈도우 경로는 콜백 기반 하트비트가 없으므로 긴
    텍스트 콜 전에 최소 1회 하트비트를 보낸다(Req 9.4).
    """
    from dataclasses import asdict
    from src.core import step3
    from src.core.step3 import ExtractResult, Pass1Record
    from src.operators.narrative_context import load_prior

    # 긴 텍스트 해소 콜 전 하트비트(윈도우 경로는 콜백 없음 — 최소 1회 갱신).
    activity.heartbeat("resolve:start")

    webtoon_id = step3._get_webtoon_id(ep.webtoon_episode_id)
    prior = load_prior(webtoon_id, ep.episode_no)

    # ExtractResult 재구성(records는 Pass1Record 데이터클래스로 복원 — windowed 경로가 속성 접근).
    records = [Pass1Record(**r) for r in extract.get("records", [])]
    extract_obj = ExtractResult(
        webtoon_episode_id=extract.get("webtoon_episode_id", ep.webtoon_episode_id),
        records=records,
        belief=extract.get("belief", {}),
        cuts_total=extract.get("cuts_total", 0),
        cuts_analyzed=extract.get("cuts_analyzed", 0),
        cuts_skipped=extract.get("cuts_skipped", 0),
        usage_total=extract.get("usage_total", {}),
    )

    result = step3.resolve_episode_windowed(extract_obj, prior, webtoon_id=webtoon_id)
    return asdict(result)


@activity.defn
def step3c_apply(ep: EpisodeInput, resolution: dict) -> dict:
    """Pass-2b 결정론 커밋(step3c) — **LLM 없음**. `ResolveResult`를 에피소드 전체 DB에 투영. 반환: episode_meta.

    `resolution`은 step3b_resolve가 반환한 `ResolveResult` 직렬화 dict다. 함수 내부에서 `ResolveResult`로
    복원해 `step3.apply_resolution`에 넘긴다(소급 전파·멱등·동결 보장 — Req 5). 결정론 단계라 빠르지만,
    안전하게 시작 시 하트비트를 1회 보낸다. apply_resolution은 커밋 후 `narrative_context.fold`로 누적
    서사 상태를 갱신하고 fold 입력으로 쓴 episode_meta dict를 반환한다(Req 11.4).
    """
    from src.core import step3
    from src.core.step3 import ResolveResult

    activity.heartbeat("apply:start")

    result = ResolveResult(**resolution)
    return step3.apply_resolution(ep.webtoon_episode_id, result)
