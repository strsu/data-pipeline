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
from src.temporal.workflows import EpisodeWorkflow, WebtoonWorkflow

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)

    with ThreadPoolExecutor(max_workers=16) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[WebtoonWorkflow, EpisodeWorkflow],
            activities=[
                activities.get_episode_max_cut,
                activities.resolve_episode,
                activities.mark_phase1_complete,
                activities.prepare_episode,
                activities.ocr_cut,
                activities.yolo_cut,
                activities.face_identify_episode,
            ],
            activity_executor=executor,
        )
        logging.info("temporal worker 시작 — task_queue=%s addr=%s", TASK_QUEUE, TEMPORAL_ADDRESS)
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
