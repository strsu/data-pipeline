"""웹툰 파이프라인 트리거(멱등 kick).

    python -m src.temporal.starter <source> <title_id> [start_episode_no]

workflow_id = "{source}_{title_id}" 고정 → 같은 웹툰 중복 kick은 무시(멱등).
service(Django)의 config/kafka.py send_phase1_trigger를 이 호출로 대체한다.
"""
from __future__ import annotations

import asyncio
import sys

from temporalio.client import Client

from src.temporal.shared import TASK_QUEUE, TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, WebtoonInput
from src.temporal.workflows import WebtoonWorkflow


async def kick(source: str, title_id: str, start_episode_no: int = 1) -> None:
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    handle = await client.start_workflow(
        WebtoonWorkflow.run,
        WebtoonInput(source=source, title_id=title_id, start_episode_no=start_episode_no),
        id=f"{source}_{title_id}",
        task_queue=TASK_QUEUE,
    )
    print(f"started workflow id={handle.id} run_id={handle.result_run_id}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python -m src.temporal.starter <source> <title_id> [start_episode_no]")
        raise SystemExit(2)
    asyncio.run(kick(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 1))
