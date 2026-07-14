"""Temporal 워커 진입점.

    python -m src.temporal.worker

한 프로세스에서 큐별 Worker 인스턴스를 동시에 띄운다(asyncio.gather):
- ORCH_QUEUE  : EpisodeChainWorkflow + 가벼운 판정/메타 액티비티.
- STEP1/2_QUEUE : 무거운 step 작업 액티비티. max_concurrent_activities=1로 제한해
                  step별 전역 동시성 1을 보장한다(개인 서버 자원 보호).
- STEP3_QUEUE : 동시성 2(정규 체인이 다른 웹툰의 step3 처리를 오래 막지 않도록).
                같은 웹툰을 겹쳐 건드리는 두 step3류 작업(예: 정규 체인의 apply와
                그 웹툰의 regen reresolve/정리 패스 심판)은 액티비티 진입 시
                webtoon_id별 프로세스 내 락(activities._webtoon_serialized)이
                직렬화한다 — 즉 "같은 웹툰 직렬, 다른 웹툰끼리만 병렬 2".
                replicas=1(단일 프로세스) 전제 — 늘리려면 pg advisory lock으로 교체.

액티비티가 동기 함수(블로킹 I/O)이므로 ThreadPoolExecutor로 실행한다. step1/2 워커는
동시성 1이라 단일 스레드 executor면 충분하고, step3 워커는 동시성 2에 맞춰 스레드 2개를
두며, 오케스트레이터 워커는 가벼운 판정 액티비티 다수를 동시에 처리할 수 있게 약간의
여유를 둔다.
"""
from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from src.temporal import activities
from src.temporal.shared import (
    ORCH_QUEUE,
    STEP1_QUEUE,
    STEP2_QUEUE,
    STEP3_QUEUE,
    TEMPORAL_ADDRESS,
    TEMPORAL_NAMESPACE,
)
from src.temporal.workflows import (
    ConsolidateWebtoonWorkflow,
    EpisodeChainWorkflow,
    RegenerateBatchWorkflow,
    RegenerateCharacterWorkflow,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s KST %(levelname)s %(name)s %(message)s",
)
# 컨테이너 TZ(UTC)와 무관하게 로그 시각을 KST로 고정(+9h, DST 없음).
# DB/litellm 타임스탬프는 전부 명시적 UTC(datetime.now(timezone.utc))라 영향 없다 —
# 로그 대조 시 "로그는 KST, DB는 UTC"만 기억할 것(포맷에 KST를 박아 오독 방지).
# staticmethod: 클래스 속성에 넣은 일반 함수는 self가 바인딩돼 TypeError가 난다.
logging.Formatter.converter = staticmethod(lambda ts: time.gmtime(ts + 9 * 3600))


async def _connect_with_retry() -> Client:
    """Temporal 준비 전/재시작 중에도 죽지 않고 연결될 때까지 백오프 재시도."""
    attempt = 0
    while True:
        try:
            return await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
        except Exception as e:
            attempt += 1
            wait = min(30, 2 ** min(attempt, 5))
            logging.warning(
                "temporal 연결 실패 (addr=%s): %s — %ds 후 재시도 [%d]",
                TEMPORAL_ADDRESS, e, wait, attempt,
            )
            await asyncio.sleep(wait)


async def main() -> None:
    client = await _connect_with_retry()

    # 오케스트레이터: 체인 워크플로 + 가벼운 판정/메타 액티비티(동시 처리 여유 8).
    orch_executor = ThreadPoolExecutor(max_workers=8)
    orch_worker = Worker(
        client,
        task_queue=ORCH_QUEUE,
        workflows=[EpisodeChainWorkflow, RegenerateCharacterWorkflow, RegenerateBatchWorkflow,
                   ConsolidateWebtoonWorkflow],
        activities=[
            activities.resolve_episode_for_chain,
            activities.next_chain_episode,
            activities.mark_phase_complete,
            activities.is_phase3_enabled,
            activities.mark_resolve_run_failed,
            activities.regen_begin,
            activities.regen_batch_begin,
            activities.consolidation_due_for_episode,
            activities.consolidation_begin,
            activities.consolidation_finish,
        ],
        activity_executor=orch_executor,
    )

    # step별 워커: 무거운 작업, 동시성 1(전역 step 직렬화).
    step1_executor = ThreadPoolExecutor(max_workers=1)
    step1_worker = Worker(
        client,
        task_queue=STEP1_QUEUE,
        activities=[activities.prepare_episode, activities.step1_episode],
        activity_executor=step1_executor,
        max_concurrent_activities=1,
    )

    step2_executor = ThreadPoolExecutor(max_workers=1)
    step2_worker = Worker(
        client,
        task_queue=STEP2_QUEUE,
        activities=[activities.face_identify_episode],
        activity_executor=step2_executor,
        max_concurrent_activities=1,
    )

    step3_executor = ThreadPoolExecutor(max_workers=2)
    step3_worker = Worker(
        client,
        task_queue=STEP3_QUEUE,
        activities=[
            activities.step3a_extract,
            activities.step3b_resolve,
            activities.step3c_apply,
            activities.regen_reresolve_episode,
            activities.regen_batch_reresolve_episode,
            activities.regen_profile,
            activities.consolidation_adjudicate,
        ],
        activity_executor=step3_executor,
        max_concurrent_activities=2,
    )

    logging.info(
        "temporal worker 시작 — orch=%s step1=%s step2=%s step3=%s addr=%s",
        ORCH_QUEUE, STEP1_QUEUE, STEP2_QUEUE, STEP3_QUEUE, TEMPORAL_ADDRESS,
    )
    try:
        await asyncio.gather(
            orch_worker.run(),
            step1_worker.run(),
            step2_worker.run(),
            step3_worker.run(),
        )
    finally:
        orch_executor.shutdown(wait=False)
        step1_executor.shutdown(wait=False)
        step2_executor.shutdown(wait=False)
        step3_executor.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(main())
