"""Temporal 워커 진입점.

    python -m src.temporal.worker

액티비티가 동기 함수(블로킹 I/O)이므로 ThreadPoolExecutor로 실행한다.
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from src.temporal import activities
from src.temporal.shared import TASK_QUEUE, TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE
from src.temporal.workflows import (
    EpisodeFaceIdentifyWorkflow,
    EpisodeSceneWorkflow,
    EpisodeWorkflow,
    WebtoonWorkflow,
)

logging.basicConfig(level=logging.INFO)


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

    with ThreadPoolExecutor(max_workers=16) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[WebtoonWorkflow, EpisodeWorkflow, EpisodeFaceIdentifyWorkflow, EpisodeSceneWorkflow],
            activities=[
                activities.get_episode_max_cut,
                activities.resolve_episode,
                activities.mark_phase1_complete,
                activities.prepare_episode,
                activities.ocr_cut,
                activities.yolo_cut,
                activities.face_identify_episode,
                activities.is_phase1_done,
                activities.is_phase3_enabled,
                activities.prepare_scene,
                activities.scene_llm_cut,
            ],
            activity_executor=executor,
        )
        logging.info("temporal worker 시작 — task_queue=%s addr=%s", TASK_QUEUE, TEMPORAL_ADDRESS)
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
