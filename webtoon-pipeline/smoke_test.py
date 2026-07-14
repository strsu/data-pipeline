"""오케스트레이션 런타임 스모크 — 실제 체인 워크플로 + stub 액티비티.

인프라(DB/S3/Chroma/model-api) 없이 Temporal 오케스트레이션(steps 순서·step별 큐 라우팅·
에피소드 자동 체이닝)만 검증한다. 코어 I/O는 stub으로 대체.

    uv run --with temporalio --no-project python smoke_test.py
"""
import asyncio
import contextlib

from temporalio import activity
from temporalio.worker import Worker
from temporalio.testing import WorkflowEnvironment

from src.temporal.shared import (
    ORCH_QUEUE, STEP1_QUEUE, STEP2_QUEUE, STEP3_QUEUE,
    ChainInput, EpisodeInput, RegenBatchInput, RegenInput,
)
from src.temporal.workflows import (
    EpisodeChainWorkflow, RegenerateBatchWorkflow, RegenerateCharacterWorkflow,
)

# 호출 순서 추적
calls: list[str] = []


# ── stub 액티비티 (실제와 동일한 name) ────────────────────────────────────────

@activity.defn(name="resolve_episode_for_chain")
async def resolve_episode_for_chain(source: str, title_id: str, episode_no: int):
    if episode_no >= 3:  # ep1, ep2만 다운로드됨
        return None
    return EpisodeInput(source=source, title_id=title_id, episode_no=episode_no,
                        webtoon_episode_id=episode_no * 1000, start_cut=1, max_cut=0)


@activity.defn(name="next_chain_episode")
async def next_chain_episode(inp: ChainInput):
    # 자동 모드 미러: 다음 다운로드 회차(ep2)까지 이어가고 그 뒤엔 종료.
    return inp.cur_ep + 1 if inp.cur_ep < 2 else None


@activity.defn(name="mark_phase_complete")
async def mark_phase_complete(ep: EpisodeInput, phase: int) -> None:
    calls.append(f"phase{phase}done:ep{ep.episode_no}")


@activity.defn(name="is_phase3_enabled")
async def is_phase3_enabled(webtoon_episode_id: int) -> bool:
    return False  # Step3 비활성 — Step1/2 오케스트레이션만 검증


@activity.defn(name="prepare_episode")
async def prepare_episode(ep: EpisodeInput) -> None:
    calls.append(f"prepare:ep{ep.episode_no}")


@activity.defn(name="step1_episode")
async def step1_episode(ep: EpisodeInput) -> dict:
    calls.append(f"step1:ep{ep.episode_no}")
    return {"segments": 4, "texts": 5, "faces": 3}


@activity.defn(name="face_identify_episode")
async def face_identify_episode(ep: EpisodeInput) -> dict:
    calls.append(f"identify:ep{ep.episode_no}")
    return {"faces": 10, "matched": 7, "new_chars": 3}


@activity.defn(name="step3a_extract")
async def step3a_extract(ep: EpisodeInput) -> dict:
    calls.append(f"scene:ep{ep.episode_no}")
    return {"webtoon_episode_id": ep.webtoon_episode_id, "records": [], "belief": {},
            "cuts_total": 0, "cuts_analyzed": 0, "cuts_skipped": 0, "usage_total": {},
            "run_id": None}


@activity.defn(name="step3b_resolve")
async def step3b_resolve(ep: EpisodeInput, extract: dict) -> dict:
    calls.append(f"resolve:ep{ep.episode_no}")
    return {"webtoon_episode_id": ep.webtoon_episode_id, "characters": [],
            "speaker_resolution": [], "beats": [], "episode": {}, "deceptions": [],
            "threads": [], "profiles": [], "usage": {}, "error": None, "run_id": None}


@activity.defn(name="step3c_apply")
async def step3c_apply(ep: EpisodeInput, resolution: dict) -> dict:
    calls.append(f"apply:ep{ep.episode_no}")
    return {"stats": {}}


# ── 캐릭터 재분석(재도출, §20) stub ───────────────────────────────────────────

@activity.defn(name="regen_begin")
async def regen_begin(inp: RegenInput):
    calls.append(f"regen_begin:{inp.character_id}:{inp.mode}")
    if inp.character_id == 404:
        return None  # 캐릭터 없음 → 워크플로 no-op
    episodes = ([{"episode_id": 11, "episode_no": 1}, {"episode_id": 13, "episode_no": 3}]
                if inp.mode == "reresolve" else [])
    return {"webtoon_id": 23, "run_id": 777, "episodes": episodes}


@activity.defn(name="regen_reresolve_episode")
async def regen_reresolve_episode(episode_id: int, webtoon_id: int, run_id: int, done: int,
                                  rerun_extract: bool = False,
                                  invalidate_character_ids: list[int] | None = None):
    calls.append(f"regen_reresolve:ep{episode_id}:done{done}:rerun{int(rerun_extract)}"
                 f":inv{invalidate_character_ids or []}")
    return {"webtoon_episode_id": episode_id, "resolve_error": None, "run_id": 900 + episode_id}


@activity.defn(name="regen_profile")
async def regen_profile(inp: RegenInput, webtoon_id: int, run_id: int):
    calls.append(f"regen_profile:{inp.character_id}:run{run_id}:absorbed{inp.absorbed_character_ids}")
    return {"character_id": inp.character_id, "error": None}


# ── 웹툰 단위 배치 재분석(§20.9) stub ─────────────────────────────────────────

@activity.defn(name="regen_batch_begin")
async def regen_batch_begin(inp: RegenBatchInput):
    calls.append(f"batch_begin:w{inp.webtoon_id}:items{len(inp.items)}:key{inp.batch_key}")
    if not inp.items:
        return None
    # 등장 에피소드 "합집합"(중복 제거) + 캐릭터별 umbrella run — 실제 액티비티 반환 shape 미러.
    return {
        "webtoon_id": inp.webtoon_id,
        "episodes": [{"episode_id": 11, "episode_no": 1}, {"episode_id": 13, "episode_no": 3}],
        "items": [
            {"character_id": 21, "mode": "reresolve", "absorbed_character_ids": [], "run_id": 801},
            {"character_id": 22, "mode": "profile", "absorbed_character_ids": [5], "run_id": 802},
        ],
        "invalidate_character_ids": [21, 30],
    }


@activity.defn(name="regen_batch_reresolve_episode")
async def regen_batch_reresolve_episode(episode_id: int, webtoon_id: int, run_ids: list[int],
                                        done: int, invalidate_character_ids: list[int] | None = None):
    calls.append(f"batch_reresolve:ep{episode_id}:done{done}:runs{run_ids}"
                 f":inv{invalidate_character_ids or []}")
    return {"webtoon_episode_id": episode_id, "resolve_error": None, "run_id": 950 + episode_id}


async def main() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        workers = [
            Worker(env.client, task_queue=ORCH_QUEUE,
                   workflows=[EpisodeChainWorkflow, RegenerateCharacterWorkflow,
                              RegenerateBatchWorkflow],
                   activities=[resolve_episode_for_chain, next_chain_episode,
                               mark_phase_complete, is_phase3_enabled, regen_begin,
                               regen_batch_begin]),
            Worker(env.client, task_queue=STEP1_QUEUE,
                   activities=[prepare_episode, step1_episode]),
            Worker(env.client, task_queue=STEP2_QUEUE, activities=[face_identify_episode]),
            Worker(env.client, task_queue=STEP3_QUEUE,
                   activities=[step3a_extract, step3b_resolve, step3c_apply,
                               regen_reresolve_episode, regen_batch_reresolve_episode,
                               regen_profile]),
        ]
        async with contextlib.AsyncExitStack() as stack:
            for w in workers:
                await stack.enter_async_context(w)
            await env.client.execute_workflow(
                EpisodeChainWorkflow.run,
                ChainInput(source="naver", title_id="12345",
                           steps=["step1", "step2", "step3"], cur_ep=1, max_ep=0, force=False),
                id="naver_12345_chain",
                task_queue=ORCH_QUEUE,
            )
            # 재분석: profile(경량 1콜) / reresolve(에피소드 순차 후 프로필) / 캐릭터 없음 no-op.
            await env.client.execute_workflow(
                RegenerateCharacterWorkflow.run,
                RegenInput(character_id=1858, mode="profile", absorbed_character_ids=[1862]),
                id="regen-1858-profile", task_queue=ORCH_QUEUE,
            )
            await env.client.execute_workflow(
                RegenerateCharacterWorkflow.run,
                RegenInput(character_id=1883, mode="reresolve"),
                id="regen-1883-reresolve", task_queue=ORCH_QUEUE,
            )
            # 깊은 모드(수동 버튼) — rerun_extract=True + invalidate 전파 검증.
            await env.client.execute_workflow(
                RegenerateCharacterWorkflow.run,
                RegenInput(character_id=1884, mode="reresolve", rerun_extract=True,
                           invalidate_character_ids=[7, 8]),
                id="regen-1884-reresolve-deep", task_queue=ORCH_QUEUE,
            )
            await env.client.execute_workflow(
                RegenerateCharacterWorkflow.run,
                RegenInput(character_id=404, mode="profile"),
                id="regen-404", task_queue=ORCH_QUEUE,
            )
            # 웹툰 단위 배치(§20.9) — 합집합 1회 재해소 + 항목별 프로필 재도출.
            await env.client.execute_workflow(
                RegenerateBatchWorkflow.run,
                RegenBatchInput(webtoon_id=17, items=[
                    {"character_id": 21, "mode": "reresolve", "invalidate_character_ids": [21, 30]},
                    {"character_id": 22, "mode": "profile", "absorbed_character_ids": [5]},
                ], batch_key="r735"),
                id="regen-batch-w17-r735", task_queue=ORCH_QUEUE,
            )

    # ── 불변식 검증 ──────────────────────────────────────────────────────────
    step1 = [c for c in calls if c.startswith("step1:")]
    identify = [c for c in calls if c.startswith("identify:")]
    prepares = [c for c in calls if c.startswith("prepare:")]
    scenes = [c for c in calls if c.startswith("scene:")]

    # 통합 단일 step1은 에피소드당 1회씩(ep1, ep2)
    assert step1 == ["step1:ep1", "step1:ep2"], f"step1 호출 이상: {step1}"
    assert identify == ["identify:ep1", "identify:ep2"], f"identify 순서/횟수 이상: {identify}"
    # prepare는 에피소드당 1회
    assert prepares == ["prepare:ep1", "prepare:ep2"], f"prepare 이상: {prepares}"
    # ep1이 ep2보다 먼저 (에피소드 순차)
    assert calls.index("step1:ep1") < calls.index("step1:ep2"), "에피소드 순차 위반"
    # 각 에피소드에서 Step1/Step2 완료 마킹
    assert "phase1done:ep1" in calls and "phase1done:ep2" in calls
    assert "phase2done:ep1" in calls and "phase2done:ep2" in calls
    # phase3 비활성 → step3 미실행
    assert scenes == [], f"step3는 phase3 비활성에서 실행되면 안 됨: {scenes}"

    # ── 재분석(§20) 불변식 ──────────────────────────────────────────────────
    regen = [c for c in calls if c.startswith("regen")]
    assert regen == [
        "regen_begin:1858:profile",
        "regen_profile:1858:run777:absorbed[1862]",       # profile 모드 = 재도출 1콜만
        "regen_begin:1883:reresolve",
        "regen_reresolve:ep11:done1:rerun0:inv[]",        # 자동 훅 기본 = 텍스트 전용(§20.3 개정)
        "regen_reresolve:ep13:done2:rerun0:inv[]",
        "regen_profile:1883:run777:absorbedNone",         # 마지막에 프로필 재도출
        "regen_begin:1884:reresolve",
        "regen_reresolve:ep11:done1:rerun1:inv[7, 8]",    # 수동 깊은 모드 = 비전 재실행 + 무효화 전파
        "regen_reresolve:ep13:done2:rerun1:inv[7, 8]",
        "regen_profile:1884:run777:absorbedNone",
        "regen_begin:404:profile",                        # 캐릭터 없음 → begin만(no-op)
        "regen_profile:21:run801:absorbed[]",             # 배치(§20.9) 마무리 — 항목별 프로필 재도출
        "regen_profile:22:run802:absorbed[5]",
    ], f"재분석 호출 이상: {regen}"

    # ── 배치 재분석(§20.9) 불변식 ──────────────────────────────────────────
    batch = [c for c in calls if c.startswith("batch")]
    assert batch == [
        "batch_begin:w17:items2:keyr735",
        "batch_reresolve:ep11:done1:runs[801]:inv[21, 30]",  # 합집합 1번씩만 + 무효화 동반
        "batch_reresolve:ep13:done2:runs[801]:inv[21, 30]",
    ], f"배치 재해소 호출 이상: {batch}"
    # 배치 마무리 프로필 재도출 — 항목별 run으로.
    assert "regen_profile:21:run801:absorbed[]" in calls, f"배치 프로필(21) 누락: {calls[-6:]}"
    assert "regen_profile:22:run802:absorbed[5]" in calls, f"배치 프로필(22) 누락: {calls[-6:]}"

    print(f"SMOKE PASSED — step1={step1} identify={identify} prepare={prepares} "
          f"regen={len(regen)} batch={len(batch)}")


if __name__ == "__main__":
    asyncio.run(main())
