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
import threading
from contextlib import contextmanager

from temporalio import activity

from src.temporal.shared import STEP_RUN_KIND, ChainInput, ConsolidateInput, EpisodeInput, RegenInput

logger = logging.getLogger(__name__)


# ── 웹툰 단위 직렬화 락 (STEP3_QUEUE 동시성 2 전제) ──────────────────────────
#
# STEP3_QUEUE는 동시성 2로 **서로 다른 웹툰**의 step3류 작업을 병렬 처리한다. 같은 웹툰을
# 겹쳐 건드리는 두 작업(정규 체인 step3 ↔ regen reresolve ↔ 정리 패스 심판)은 suggestion
# delete-reinsert 경합, 동명 승격 TOCTOU, run supersede 상호 덮어쓰기를 일으키므로,
# 무거운 step3류 액티비티는 진입 시 webtoon_id별 락을 잡아 같은 웹툰을 직렬화한다.
# 워커가 replicas=1(단일 프로세스)이라 프로세스 내 threading.Lock으로 충분하다 —
# replicas를 늘리려면 이 락을 pg advisory lock으로 교체해야 한다.
#
# 대기 정책: 락을 못 잡으면 heartbeat를 보내며 블로킹 대기한다(retryable 에러로 슬롯을
# 반납하면 _REGEN_RETRY(5회)가 긴 점유를 못 넘겨 워크플로가 죽는다). 같은 웹툰 경합 시
# 슬롯 하나가 일시적으로 대기에 묶이는 건 감수 — 종전 동시성 1 수준으로 강등될 뿐이고,
# 경합이 없는 평시에는 웹툰 2개가 병렬로 돈다.

_webtoon_locks: dict[int, threading.Lock] = {}
_webtoon_locks_guard = threading.Lock()


def _get_webtoon_lock(webtoon_id: int) -> threading.Lock:
    with _webtoon_locks_guard:
        lock = _webtoon_locks.get(webtoon_id)
        if lock is None:
            lock = _webtoon_locks[webtoon_id] = threading.Lock()
        return lock


@contextmanager
def _webtoon_serialized(webtoon_id: int, what: str = ""):
    """같은 웹툰의 step3류 작업을 프로세스 내에서 직렬화한다(위 주석 참조).

    대기 중에는 15초마다 heartbeat를 보내 heartbeat_timeout을 넘기지 않는다.
    액티비티가 취소되면 heartbeat가 CancelledError를 던져 대기 스레드도 정리된다.
    """
    lock = _get_webtoon_lock(webtoon_id)
    if not lock.acquire(blocking=False):
        logger.info("[webtoon-lock] webtoon=%s 대기 시작 (%s) — 같은 웹툰 작업 진행 중", webtoon_id, what)
        while not lock.acquire(timeout=15.0):
            activity.heartbeat(f"webtoon-lock:wait:{what}")
        logger.info("[webtoon-lock] webtoon=%s 획득 (%s)", webtoon_id, what)
    try:
        yield
    finally:
        lock.release()


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
    with _webtoon_serialized(webtoon_id, "step3a"):
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
    with _webtoon_serialized(webtoon_id, "step3b"):
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

    예외 시 여기서 run을 failed로 전이하지 않는다(attempt 단위 failed 전이는 재시도 성공 시
    failed↔succeeded 왕복을 만든다) — 재시도 소진(최종 실패) 처리는 워크플로가
    `mark_resolve_run_failed`로 수행한다(running 좀비 방지).
    """
    from src.core import runs, step3
    from src.core.step3 import ResolveResult

    activity.heartbeat("apply:start")

    run_id = resolution.pop("run_id", None)
    result = ResolveResult(**resolution)
    webtoon_id = step3._get_webtoon_id(ep.webtoon_episode_id)
    with _webtoon_serialized(webtoon_id, "step3c"):
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


@activity.defn
def mark_resolve_run_failed(run_id: int | None, error: str) -> None:
    """step3c 최종 실패(액티비티 재시도 소진) 시 워크플로가 호출 — resolve run을 failed로 닫는다.

    running일 때만 전이한다(이미 succeeded/superseded면 no-op) — apply가 커밋·전이까지 끝낸 뒤
    결과 보고만 유실된 재시도 경계 케이스에서 succeeded를 failed로 되돌리지 않기 위함.
    """
    from src.core import runs

    if run_id is None:
        return
    if runs.fail_run_if_running(run_id, error):
        logger.warning("[step3c] run=%s 최종 실패 — failed 전이(running 좀비 방지): %s", run_id, error)


# ── 캐릭터 재분석(재도출, §20) ────────────────────────────────────────────────
#
# RegenerateCharacterWorkflow가 사용. regen_begin(가벼움, ORCH_QUEUE)이 대상 해석 +
# umbrella run(kind=profile)을 만들고, 무거운 작업(regen_reresolve_episode /
# regen_profile)은 STEP3_QUEUE(동시성 2)에서 다른 step3/LLM 작업과 함께 처리된다 —
# 같은 웹툰을 정규 체인이 동시에 건드리는 경우는 _webtoon_serialized 락이 직렬화한다.


@activity.defn
def regen_begin(inp: RegenInput) -> dict | None:
    """재분석 대상 해석 + umbrella run 시작. 캐릭터가 없으면 None(워크플로 no-op 종료).

    반환: {"webtoon_id", "run_id", "episodes": [{"episode_id","episode_no"}...]}.
    episodes는 mode=reresolve일 때만 채운다(등장 에피소드 = 얼굴 등장 ∪ 화자 귀속).
    """
    from src.core import regen
    from src.operators.llm_resolver import TEXT, resolve_llm_model

    info = regen._character_info(inp.character_id)
    if info is None:
        logger.warning("[regen_begin] character=%s 없음/삭제됨 — 재분석 건너뜀", inp.character_id)
        return None
    episodes = regen.character_episode_ids(inp.character_id) if inp.mode == "reresolve" else []
    ctx = resolve_llm_model(info["webtoon_id"], TEXT)
    run_id = regen.begin_profile_run(
        info["webtoon_id"], inp.character_id, inp.mode,
        llm_model_id=ctx.get("id"), episodes_total=len(episodes),
    )
    return {"webtoon_id": info["webtoon_id"], "run_id": run_id, "episodes": episodes}


@activity.defn
def regen_reresolve_episode(webtoon_episode_id: int, webtoon_id: int,
                            run_id: int, episodes_done: int) -> dict:
    """등장 에피소드 1개 재해소 — reresolve_episode(rerun_extract=True)(§20.3 두 모드).

    rerun_extract=True 필수 근거: 텍스트 전용 재해소는 `_load_provisional_blocks`가 옛
    speaker_id를 hint로 재주입해 섞임 화자가 되살아날 수 있다(§20.3) — 비전 재실행이
    provisional을 교정 얼굴 기준으로 새로 산출해야 깨끗하다. 에피소드당 자체 vision/resolve
    run을 만들며(진행도/stale 정본), umbrella run stats.episodes_done도 갱신한다.
    LLM 콜이 회차당 1시간을 넘을 수 있어 서브스레드 + 주기 하트비트로 감싼다.
    """
    from src.core import regen, step3

    with _webtoon_serialized(webtoon_id, "regen:reresolve"):
        # 좀비 차단: umbrella run이 superseded/종료됐으면 무거운 작업 없이 워크플로를 끝낸다.
        # (락 획득 후 확인 — 대기 중 supersede된 run이 무거운 작업을 시작하지 않게.)
        if not regen.run_is_live(run_id):
            logger.info("[regen] run=%s superseded — ep%s 재해소 건너뜀(워크플로 종료 신호)",
                        run_id, webtoon_episode_id)
            return {"superseded": True, "webtoon_episode_id": webtoon_episode_id}

        out = _run_with_heartbeat(
            step3.reresolve_episode,
            args=(webtoon_episode_id,),
            kwargs=dict(rerun_extract=True, webtoon_id=webtoon_id),
            detail="regen:reresolve",
        )
        regen.bump_profile_run_progress(run_id, episodes_done)
    return {"webtoon_episode_id": webtoon_episode_id,
            "resolve_error": out.get("resolve_error"), "run_id": out.get("run_id")}


@activity.defn
def regen_profile(inp: RegenInput, webtoon_id: int, run_id: int) -> dict:
    """프로필 원천 재도출(LLM 1콜, 무캡 replace) + umbrella run 종료(§20.6).

    reresolve 모드에서도 마지막에 호출된다 — 재해소로 화자가 깨끗해진 근거 위에서 프로필을
    다시 뽑는다(union 재봉합은 오염 사실을 제거 못함 — §20.5 실측).
    """
    from src.core import regen, runs

    with _webtoon_serialized(webtoon_id, "regen:profile"):
        if not regen.run_is_live(run_id):
            logger.info("[regen] run=%s superseded — 프로필 재도출 건너뜀", run_id)
            return {"character_id": inp.character_id, "superseded": True}

        result = _run_with_heartbeat(
            regen.regenerate_character_profile,
            args=(inp.character_id,),
            kwargs=dict(absorbed_character_ids=inp.absorbed_character_ids or [], run_id=run_id),
            detail="regen:profile",
        )
        profile = result.get("profile") or {}
        if result.get("error"):
            runs.finish_run(run_id, status="failed", error=str(result["error"]))
        else:
            runs.finish_run(run_id, stats={
                "character_id": inp.character_id, "mode": inp.mode,
                "key_facts": len(profile.get("key_facts") or []),
                "progression": len(profile.get("progression") or []),
                "usage": result.get("usage") or {},
            })
    return {"character_id": inp.character_id, "error": result.get("error")}


# ── 정리 패스(§22.3~22.4): 제안검토 심판 + 실행 위임 ─────────────────────────
# ConsolidateWebtoonWorkflow가 사용. 판정(LLM 심판+가드)은 pipeline(adjudicate.py),
# 실행(제안 수락=§19 병합 시맨틱, 자동 훅 §20)은 service celery로 위임(service_bus).

# service celery task 이름/큐 — service apps/api/toon/tasks.py의 명시 name과 일치해야 함.
_EXECUTE_CONSOLIDATION_TASK = "apps.api.toon.tasks.execute_consolidation"
_EXECUTE_CONSOLIDATION_QUEUE = "middle"


@activity.defn
def consolidation_due_for_episode(webtoon_episode_id: int) -> int | None:
    """체인 훅용 트리거 판정 — due면 webtoon_id, 아니면 None."""
    from src.config.db import db_cursor
    from src.core import runs
    from src.temporal.shared import CONSOLIDATE_EVERY_N_RESOLVES

    with db_cursor() as cur:
        cur.execute("SELECT webtoon_id FROM webtoon_episode WHERE id = %s", (webtoon_episode_id,))
        row = cur.fetchone()
    if not row:
        return None
    webtoon_id = row[0]
    if runs.consolidation_due(webtoon_id, CONSOLIDATE_EVERY_N_RESOLVES):
        return webtoon_id
    return None


@activity.defn
def consolidation_begin(inp: ConsolidateInput) -> dict:
    """umbrella run(kind=consolidate, episode NULL) 시작 — 잔재 running은 start_run이 supersede."""
    from src.core import runs
    from src.operators.llm_resolver import TEXT, resolve_llm_model

    ctx = resolve_llm_model(inp.webtoon_id, TEXT)
    run_id = runs.start_run(inp.webtoon_id, None, runs.KIND_CONSOLIDATE,
                            llm_model_id=ctx.get("id"))
    return {"run_id": run_id}


@activity.defn
def consolidation_adjudicate(webtoon_id: int, run_id: int) -> dict:
    """심판 본체 — 도시에 구성→LLM 판정(순차)→교차대조/가드→권고 영속(adjudicate.py).

    STEP3_QUEUE(동시성 2)에서 정규 step3/LLM 작업과 함께 처리하되, 같은 웹툰과는
    _webtoon_serialized 락으로 직렬화한다(apply의 pending suggestion delete-reinsert와
    겹치면 심판 중이던 sid가 소멸). 도시에 수×콜 시간이 길 수 있어 서브스레드 +
    주기 하트비트로 감싼다(§18.4 패턴).
    """
    from src.core import adjudicate

    with _webtoon_serialized(webtoon_id, "consolidate:judge"):
        return _run_with_heartbeat(
            adjudicate.adjudicate_webtoon,
            args=(webtoon_id,),
            kwargs=dict(run_id=run_id),
            detail="consolidate:judge",
        )


@activity.defn
def consolidation_finish(webtoon_id: int, run_id: int, result: dict) -> dict:
    """실행 위임 + umbrella run 종료.

    수락/기각 목록이 있으면 service celery(execute_consolidation)로 위임하고 run은
    succeeded(stats.execution='enqueued')로 닫는다 — 실행 결과는 service task가
    같은 run stats에 덧붙인다(진행 정본은 여전히 run 1행). 브로커 미설정/전송 실패면
    수동 실행이 가능하도록 stats에 결정 목록을 남기고 failed로 닫는다.
    """
    from src.core import runs
    from src.operators import service_bus

    accepts = result.get("accept_suggestion_ids") or []
    rejects = result.get("reject_suggestion_ids") or []
    stats = {"judge": result.get("stats") or {},
             "accept_suggestion_ids": accepts, "reject_suggestion_ids": rejects}

    if result.get("error"):
        runs.finish_run(run_id, status="failed", error=str(result["error"]), stats=stats)
        return {"run_id": run_id, "enqueued": False, "error": result["error"]}

    if not accepts and not rejects:
        stats["execution"] = "noop"
        runs.finish_run(run_id, stats=stats)
        return {"run_id": run_id, "enqueued": False, "error": None}

    try:
        service_bus.send_service_task(
            _EXECUTE_CONSOLIDATION_TASK,
            args=[webtoon_id, run_id, accepts, rejects],
            queue=_EXECUTE_CONSOLIDATION_QUEUE,
        )
        stats["execution"] = "enqueued"
        runs.finish_run(run_id, stats=stats)
        return {"run_id": run_id, "enqueued": True, "error": None}
    except Exception as e:  # noqa: BLE001 — 전송 실패는 run에 남기고 수동 수습 가능하게
        stats["execution"] = "enqueue_failed"
        runs.finish_run(run_id, status="failed", error=f"celery enqueue 실패: {e}", stats=stats)
        logger.error("[consolidate] w%s run=%s 실행 위임 실패: %s", webtoon_id, run_id, e)
        return {"run_id": run_id, "enqueued": False, "error": str(e)}
