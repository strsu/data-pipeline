"""Temporal 워크플로 — 흐름 제어만(결정적). I/O는 전부 activity.

EpisodeChainWorkflow — 정식 단일 오케스트레이터.
  에피소드를 순차로 밟으며(cur_ep), 회차마다 steps에 명시된 step을 순서대로 실행하고,
  끝에서 공통 판정(next_chain_episode)으로 다음 회차로 continue-as-new 한다.

  - steps          : 회차당 실행할 step 목록(["step1"], ["step1","step2"],
                     ["step1","step2","step3"], ["step3"] 등). 정규 경로와 admin 단독/범위
                     실행이 동일 워크플로를 steps만 바꿔 사용한다.
  - 동시성          : 무거운 step 작업은 step별 전용 큐(STEP1/2/3_QUEUE)로 보내고, 그 큐를
                     서빙하는 워커가 동시성 1이라 step별 전역 1개 실행이 보장된다. 체인
                     워크플로 자체와 가벼운 판정 액티비티는 ORCH_QUEUE에서 돈다.
  - 다음 ep 판정    : "이어갈지"는 모든 step 조합에서 next_chain_episode가 공통으로 결정
                     (자동=진입 step 미완료 다음 회차 / 범위=cur_ep..max_ep).
"""
from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.temporal import activities
    from src.temporal.shared import (
        ORCH_QUEUE, STEP1, STEP1_QUEUE, STEP2, STEP2_QUEUE, STEP3, STEP3_QUEUE,
        ChainInput, EpisodeInput, RegenInput,
    )

_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    # model-api 재시작/콜드스타트(Connection refused)를 견디도록 관대하게.
    # 일시 장애로 에피소드 워크플로 전체가 실패하지 않게 한다(durable execution).
    maximum_attempts=100,
)

# 가벼운 판정/메타 액티비티 공통 타임아웃.
_META_TIMEOUT = timedelta(seconds=30)


@workflow.defn
class EpisodeChainWorkflow:
    """정식 에피소드 체인 — steps를 회차마다 순차 실행하고 다음 회차로 이어간다."""

    @workflow.run
    async def run(self, inp: ChainInput) -> None:
        # [1] 이번 회차 해석(다운로드 완료 + 미삭제만). 없으면 작업 건너뛰고 다음으로.
        ep = await workflow.execute_activity(
            activities.resolve_episode_for_chain,
            args=[inp.source, inp.title_id, inp.cur_ep],
            task_queue=ORCH_QUEUE,
            start_to_close_timeout=_META_TIMEOUT,
            retry_policy=_RETRY,
        )

        # [2] steps를 순서대로 실행(각 step은 전용 큐에서 동시성 1로 직렬화).
        if ep is not None:
            for step in inp.steps:
                await self._run_step(step, ep, inp.force)

        # [3] 공통 "다음 ep?" 판정 → 있으면 continue-as-new(같은 workflow_id 유지).
        nxt = await workflow.execute_activity(
            activities.next_chain_episode,
            inp,
            task_queue=ORCH_QUEUE,
            start_to_close_timeout=_META_TIMEOUT,
            retry_policy=_RETRY,
        )
        if nxt is not None:
            workflow.continue_as_new(replace(inp, cur_ep=nxt))

    # ── step 실행 ──────────────────────────────────────────────────────────────

    async def _run_step(self, step: str, ep: EpisodeInput, force: bool) -> None:
        if step == STEP1:
            await self._run_step1(ep)
        elif step == STEP2:
            await self._run_step2(ep)
        elif step == STEP3:
            await self._run_step3(ep, force)
        else:
            workflow.logger.warning("[chain] 알 수 없는 step=%s 무시", step)

    async def _run_step1(self, ep: EpisodeInput) -> None:
        # 재처리 정리 → OCR+YOLO 통합 1패스 → phase1 완료 마킹.
        await workflow.execute_activity(
            activities.prepare_episode, ep,
            task_queue=STEP1_QUEUE,
            start_to_close_timeout=timedelta(minutes=5), retry_policy=_RETRY,
        )
        await workflow.execute_activity(
            activities.step1_episode, ep,
            task_queue=STEP1_QUEUE,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=5), retry_policy=_RETRY,
        )
        await self._mark(ep, 1)

    async def _run_step2(self, ep: EpisodeInput) -> None:
        await workflow.execute_activity(
            activities.face_identify_episode, ep,
            task_queue=STEP2_QUEUE,
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=2), retry_policy=_RETRY,
        )
        await self._mark(ep, 2)

    async def _run_step3(self, ep: EpisodeInput, force: bool) -> None:
        # 자동 모드(force=False)에서는 phase3_enabled 웹툰만 실행. admin force는 무시하고 실행.
        if not force:
            enabled = await workflow.execute_activity(
                activities.is_phase3_enabled, ep.webtoon_episode_id,
                task_queue=ORCH_QUEUE,
                start_to_close_timeout=_META_TIMEOUT, retry_policy=_RETRY,
            )
            if not enabled:
                workflow.logger.info(
                    "[chain] %s/%s ep=%s — phase3 비활성, step3 건너뜀",
                    ep.source, ep.title_id, ep.episode_no,
                )
                return

        # 에피소드 단위 2-pass(Req 9.1): step3a(추출) → step3b(해소) → step3c(커밋).
        # LLM 스테이지는 2개(Pass-1 비전, Pass-2a 텍스트)로 한정하며, 세 액티비티 모두 STEP3_QUEUE에서
        # 돌아 LLM 스테이지 전체에 걸쳐 동시성 1(두 에피소드의 step3가 동시에 돌지 않음)을 보존한다(Req 9.2).
        # 단계 간 데이터는 activity 반환값/입력으로 흘린다(Req 9.3): step3a의 ExtractResult dict를
        # step3b 입력으로, step3b의 ResolveResult dict를 step3c 입력으로 직접 전달한다.
        workflow.logger.info(
            "[chain] %s/%s ep=%s — step3 2-pass 시작(extract→resolve→apply)",
            ep.source, ep.title_id, ep.episode_no,
        )

        # step3a: 컷별 비전 루프(Pass-1). 길어서 관대한 start_to_close + 컷 단위 heartbeat.
        extract = await workflow.execute_activity(
            activities.step3a_extract, ep,
            task_queue=STEP3_QUEUE,
            start_to_close_timeout=timedelta(hours=2),
            heartbeat_timeout=timedelta(minutes=10), retry_policy=_RETRY,
        )

        # step3b: 에피소드 텍스트 전역 해소(Pass-2a). extract 결과 dict를 입력으로 thread.
        resolution = await workflow.execute_activity(
            activities.step3b_resolve,
            args=[ep, extract],
            task_queue=STEP3_QUEUE,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=10), retry_policy=_RETRY,
        )

        # step3c: 결정론 커밋(Pass-2b, LLM 없음). resolution 결과 dict를 입력으로 thread.
        await workflow.execute_activity(
            activities.step3c_apply,
            args=[ep, resolution],
            task_queue=STEP3_QUEUE,
            start_to_close_timeout=timedelta(minutes=15),
            heartbeat_timeout=timedelta(minutes=2), retry_policy=_RETRY,
        )
        # v4.0: step3 완료 마킹은 별도 없음 — step3c가 resolve run을 succeeded로 전이하는 것이
        # 진행도의 정본이다(§17.1). _mark는 step1/2(run 원장 완료 기록)에만 쓴다.

    async def _mark(self, ep: EpisodeInput, phase: int) -> None:
        await workflow.execute_activity(
            activities.mark_phase_complete,
            args=[ep, phase],
            task_queue=ORCH_QUEUE,
            start_to_close_timeout=_META_TIMEOUT, retry_policy=_RETRY,
        )


# 재해소/재도출은 재시도를 관대하게 하지 않는다 — LLM 콜 자체가 llm_client에서 10회
# 재시도 + 폴백 모델 1회전을 이미 소화하므로, 액티비티 수준 재시도는 일시 인프라 장애
# (워커 재시작 등)만 흡수하면 된다.
_REGEN_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=5,
)


@workflow.defn
class RegenerateCharacterWorkflow:
    """캐릭터 재분석(§20) — mode=profile(경량 1콜) / mode=reresolve(등장 에피소드 재해소).

    - profile  : 병합 후. 근거(귀속 대사+장면+흡수분 조각) 전량 주입 LLM 1콜 → 프로필 무캡 replace.
    - reresolve: 얼굴 이동/섞임 풀기 후. 등장 에피소드 전부 reresolve_episode(rerun_extract=True)
                 순차(§20.3 — 얼굴 교정 반영엔 비전 재실행 필수) 후 프로필 재도출로 마무리.
    무거운 액티비티는 STEP3_QUEUE(동시성 1)로 보내 정규 step3/LLM 작업과 직렬화한다.
    진행표시는 umbrella run(kind=profile, stats.character_id/mode/episodes_done)이 정본.
    """

    @workflow.run
    async def run(self, inp: RegenInput) -> None:
        meta = await workflow.execute_activity(
            activities.regen_begin, inp,
            task_queue=ORCH_QUEUE,
            start_to_close_timeout=timedelta(minutes=2), retry_policy=_RETRY,
        )
        if meta is None:
            return  # 캐릭터 없음/삭제 — no-op

        if inp.mode == "reresolve":
            episodes = meta.get("episodes") or []
            for i, ep in enumerate(episodes):
                workflow.logger.info(
                    "[regen] character=%s — ep%s 재해소 (%d/%d)",
                    inp.character_id, ep["episode_no"], i + 1, len(episodes),
                )
                await workflow.execute_activity(
                    activities.regen_reresolve_episode,
                    args=[ep["episode_id"], meta["webtoon_id"], meta["run_id"], i + 1],
                    task_queue=STEP3_QUEUE,
                    # rerun_extract=True 실측 회차당 ~1.5h(§20.5) — 넉넉히.
                    start_to_close_timeout=timedelta(hours=4),
                    heartbeat_timeout=timedelta(minutes=10),
                    retry_policy=_REGEN_RETRY,
                )

        await workflow.execute_activity(
            activities.regen_profile,
            args=[inp, meta["webtoon_id"], meta["run_id"]],
            task_queue=STEP3_QUEUE,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=10),
            retry_policy=_REGEN_RETRY,
        )
