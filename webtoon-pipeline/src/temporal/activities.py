"""Temporal 액티비티 — 코어(faust-free) 로직을 동기 호출하는 얇은 래퍼.

모든 I/O(model-api HTTP / DB / S3 / Chroma)는 core.step1 / core.step2 / core.step3가 담당.
코어/무거운 의존성은 **함수 내부에서 지연 import**한다 — 액티비티 모듈을 import하는 것만으로
chromadb/boto3/psycopg2를 끌어오지 않게 해, 오케스트레이션 단위 테스트(temporalio만)로도 워크플로를
검증할 수 있게 한다.

액티비티는 두 부류로 나뉜다:
- 오케스트레이터 큐(ORCH_QUEUE)에서 도는 가벼운 메타/판정 액티비티
  (resolve_episode_for_chain / next_chain_episode / mark_phase_complete / is_phase3_enabled).
- step별 전용 큐(STEP1/2/3_QUEUE, 동시성 1)에서 도는 무거운 작업 액티비티
  (prepare_episode / step1_episode / face_identify_episode /
  step3a_extract / step3b_resolve / step3c_apply).
어느 큐에서 실행할지는 호출하는 워크플로(EpisodeChainWorkflow)가 task_queue로 지정한다.
"""
from __future__ import annotations

import logging

from temporalio import activity

from src.temporal.shared import STEP_RUN_KIND, ChainInput, EpisodeInput

logger = logging.getLogger(__name__)


def _run_with_heartbeat(fn, *, args=(), kwargs=None, detail: str = "working",
                        interval: float = 30.0):
    """긴 동기 작업(fn)을 서브스레드에서 실행하고 액티비티 본 스레드가 interval초마다 heartbeat.

    LLM 콜 하나가 수 분~십수 분 걸려도(step3b의 roster/resolve/narrate: naver/769209 ep4 resolve가
    ~14분) heartbeat_timeout을 넘기지 않게 한다. `activity.heartbeat()`는 Temporal 컨텍스트가 있는
    액티비티 본 스레드에서만 호출 가능하므로, 실제 작업을 서브스레드로 보내고 본 스레드는 대기하며
    주기적으로 heartbeat만 친다. fn의 반환값/예외는 그대로 전파한다.
    """
    import concurrent.futures

    kwargs = kwargs or {}
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(fn, *args, **kwargs)
        while True:
            try:
                return fut.result(timeout=interval)
            except concurrent.futures.TimeoutError:
                activity.heartbeat(detail)  # 아직 진행 중 — 살아있음 신호
    finally:
        # 취소/예외로 빠져나갈 때 이미 도는 서브스레드 완료를 기다려 블로킹하지 않는다.
        ex.shutdown(wait=False)


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
      진입 step의 종료 여부는 analysis_run(kind, status in succeeded/failed)로 판정한다(v4.0 §17.1).
    """
    if inp.max_ep and inp.max_ep > 0:
        nxt = inp.cur_ep + 1
        return nxt if nxt <= inp.max_ep else None

    from src.config.db import db_cursor

    entry_step = inp.steps[0] if inp.steps else "step1"
    kind = STEP_RUN_KIND.get(entry_step, "step1")
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.no
            FROM webtoon_episode we
            JOIN webtoon w ON we.webtoon_id = w.id
            WHERE w.source = %s AND w.title_id = %s AND we.no > %s
              AND we.is_downloaded = true AND we.deleted_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM analysis_run ar
                WHERE ar.episode_id = we.id AND ar.kind = %s
                  AND ar.status IN ('succeeded', 'failed')
              )
            ORDER BY we.no
            LIMIT 1
            """,
            (inp.source, inp.title_id, inp.cur_ep, kind),
        )
        row = cur.fetchone()
    return row[0] if row else None


@activity.defn
def mark_phase_complete(ep: EpisodeInput, phase: int) -> None:
    """step1/step2 완료를 analysis_run 원장에 기록한다(v4.0 §17.1 — 구 episode_pipeline_progress 대체).

    자동 모드 다음-ep 판정(`next_chain_episode`)이 이 run 존재로 진입 step 종료 여부를 본다.
    step3는 step3b/c가 자체 resolve run을 관리하므로 여기로 오지 않는다(workflows 참조).
    """
    from src.config.db import db_cursor
    from src.core import runs

    kind = {1: runs.KIND_STEP1, 2: runs.KIND_STEP2}.get(phase)
    if kind is None:
        logger.warning("[mark_phase_complete] phase=%s는 run 매핑 없음(무시)", phase)
        return
    with db_cursor() as cur:
        cur.execute(
            "SELECT webtoon_id FROM webtoon_episode WHERE id=%s", (ep.webtoon_episode_id,),
        )
        webtoon_id = cur.fetchone()[0]
    runs.record_completed_run(webtoon_id, ep.webtoon_episode_id, kind)


@activity.defn
def is_phase3_enabled(webtoon_episode_id: int) -> bool:
    """해당 웹툰의 phase3_enabled 여부 (step3 자동 게이트)."""
    from src.config.db import db_cursor
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(wps.phase3_enabled, false)
            FROM webtoon_episode we
            JOIN config_webtoon_pipeline_state wps ON wps.webtoon_id = we.webtoon_id
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
    네트워크 순단 등으로 도중 실패해 Temporal이 재시도할 때는 마지막 하트비트 detail
    (resume_from)을 읽어, 이미 커밋된 세그먼트를 다시 처리하지 않고 이어서 진행한다
    (`face_identify_episode`와 동일한 resume 패턴).
    반환: {"segments": n, "texts": t, "faces": f}.
    """
    from src.core import step1

    info = activity.info()
    resume_from = info.heartbeat_details[0] if info.heartbeat_details else 0
    logger.info(
        "[step1_episode] %s/%s ep=%s webtoon_episode_id=%s — attempt=%d resume_from=%d",
        ep.source, ep.title_id, ep.episode_no, ep.webtoon_episode_id,
        info.attempt, resume_from,
    )

    def _hb(done: int) -> None:
        activity.heartbeat(done)

    return step1.process_episode_step1(
        ep.source, ep.title_id, ep.episode_no, ep.webtoon_episode_id,
        heartbeat_cb=_hb, resume_from=resume_from,
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
    from src.core import runs, step3
    from src.operators.llm_resolver import VISION, resolve_llm_model

    def _hb(done: int) -> None:
        activity.heartbeat(done)

    webtoon_id = step3._get_webtoon_id(ep.webtoon_episode_id)
    ctx = resolve_llm_model(webtoon_id, VISION)
    run_id = runs.start_run(webtoon_id, ep.webtoon_episode_id, runs.KIND_VISION,
                            llm_model_id=ctx.get("id"))
    try:
        result = step3.extract_episode(ep.webtoon_episode_id, heartbeat_cb=_hb, run_id=run_id)
    except Exception as e:
        runs.finish_run(run_id, status="failed", error=str(e))
        raise
    runs.finish_run(run_id, stats={
        "cuts_total": result.cuts_total, "cuts_analyzed": result.cuts_analyzed,
        "cuts_skipped": result.cuts_skipped, "usage": result.usage_total,
    })
    out = asdict(result)
    out["run_id"] = run_id
    return out


@activity.defn
def step3b_resolve(ep: EpisodeInput, extract: dict) -> dict:
    """Pass-2a 전역 해소(step3b) — 누적 서사 prior 조립 후 에피소드 텍스트 해소. 반환: `ResolveResult` dict.

    `extract`는 step3a_extract가 반환한 `ExtractResult` 직렬화 dict다. 누적 서사 컨텍스트(prior)는
    `narrative_context.load_prior(webtoon_id, ep.episode_no)`로 조립한다(이전 화까지의 확정 로스터/
    미해결 떡밥/running 요약 — Req 4.1, 11.3). webtoon_id는 `EpisodeInput`에 없으므로 코어 헬퍼
    `step3._get_webtoon_id(webtoon_episode_id)`로 해석한다(현재 코어에 공개 경로가 없어 private 헬퍼를
    사용 — 문서화). 해소 진입점은 컨텍스트 적응형 윈도우(`resolve_episode_windowed`)이며 토큰 예산에
    따라 단일콜/다중윈도우를 자동 선택한다(Req 8). roster/resolve/narrate 각 텍스트 콜이 수 분~
    십수 분 걸리므로 `_run_with_heartbeat`로 실제 해소를 서브스레드에서 돌리고 본 스레드가 주기적
    하트비트를 보낸다(긴 콜 중 heartbeat_timeout 초과 → 재시도 루프 방지 — Req 9.4).
    """
    from dataclasses import asdict
    from src.core import step3
    from src.core.step3 import ExtractResult, Pass1Record
    from src.operators.narrative_context import load_prior

    from src.core import runs

    activity.heartbeat("resolve:start")  # 준비 단계(load_prior 등) 동안의 초기 갱신

    webtoon_id = step3._get_webtoon_id(ep.webtoon_episode_id)
    prior = load_prior(webtoon_id, ep.episode_no)

    # ExtractResult 재구성(records는 Pass1Record 데이터클래스로 복원 — windowed 경로가 속성 접근).
    vision_run_id = extract.get("run_id")
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

    # resolve run 시작(R+N+apply가 공유; apply 성공 시 step3c가 succeeded 전이).
    # Stage R/N은 텍스트 전용 → text role 모델(예: glm-5.2)로 해석.
    from src.operators.llm_resolver import TEXT, resolve_llm_model
    ctx = resolve_llm_model(webtoon_id, TEXT)
    run_id = runs.start_run(webtoon_id, ep.webtoon_episode_id, runs.KIND_RESOLVE,
                            llm_model_id=ctx.get("id"), vision_run_id=vision_run_id)
    try:
        result = _run_with_heartbeat(
            step3.resolve_and_narrate,
            args=(extract_obj, prior),
            kwargs=dict(webtoon_id=webtoon_id, ctx=ctx, run_id=run_id),
            detail="resolve:working",
        )
    except Exception as e:
        runs.finish_run(run_id, status="failed", error=str(e))
        raise
    out = asdict(result)
    out["run_id"] = run_id
    return out


@activity.defn
def step3c_apply(ep: EpisodeInput, resolution: dict) -> dict:
    """Pass-2b 결정론 커밋(step3c) — **LLM 없음**. `ResolveResult`를 에피소드 전체 DB에 투영. 반환: episode_meta.

    `resolution`은 step3b_resolve가 반환한 `ResolveResult` 직렬화 dict(+run_id)다. 함수 내부에서
    `ResolveResult`로 복원해 `step3.apply_resolution`에 넘긴다(소급 전파·멱등·동결 보장 — Req 5).
    커밋 성공 시 resolve run을 succeeded로 전이한다 — 이 순간이 "에피소드 step3 완료"의 정본이다
    (§17.1: 진행도 도출, stale 도출 기준 시각).
    """
    from src.core import runs, step3
    from src.core.step3 import ResolveResult

    activity.heartbeat("apply:start")

    run_id = resolution.pop("run_id", None)
    result = ResolveResult(**resolution)
    # 결정론 커밋이지만 대용량 회차(수백 speaker/annotation)에서 DB 쓰기가 길어질 수 있어
    # 서브스레드 + 주기 하트비트로 감싼다(step3c heartbeat_timeout 초과 방지).
    meta = _run_with_heartbeat(
        step3.apply_resolution,
        args=(ep.webtoon_episode_id, result),
        kwargs=dict(run_id=run_id),
        detail="apply:working",
    )
    if run_id is not None:
        if result.error:
            runs.finish_run(run_id, status="failed", error=str(result.error))
        else:
            runs.finish_run(run_id, stats=meta.get("stats") or {})
    return meta
