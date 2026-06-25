"""웹툰 파이프라인 트리거(멱등 kick).

    python -m src.temporal.starter <source> <title_id> [start_episode_no] [steps] [max_ep]

    steps  : 콤마 구분(기본 "step1,step2,step3"). 예) "step1" / "step1,step2"
    max_ep : 0(기본)이면 자동 모드 — 진입 step 미완료 회차를 끝까지 이어감.
             양수면 start_episode_no..max_ep 범위만 처리(admin 범위 실행과 동일).

workflow_id = "{source}_{title_id}_chain" 고정 → 같은 웹툰 중복 kick은 무시(멱등).
service(Django)의 config/temporal.py 트리거가 동일 워크플로(EpisodeChainWorkflow)를 start한다.
"""
from __future__ import annotations

import asyncio
import sys

from temporalio.client import Client

from src.temporal.shared import (
    ORCH_QUEUE, STEP1, STEP2, STEP3, ChainInput, TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE,
)
from src.temporal.workflows import EpisodeChainWorkflow


async def kick(
    source: str, title_id: str, start_episode_no: int = 1,
    steps: list[str] | None = None, max_ep: int = 0,
) -> None:
    steps = steps or [STEP1, STEP2, STEP3]
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    handle = await client.start_workflow(
        EpisodeChainWorkflow.run,
        ChainInput(
            source=source, title_id=title_id, steps=steps,
            cur_ep=start_episode_no, max_ep=max_ep, force=False,
        ),
        id=f"{source}_{title_id}_chain",
        task_queue=ORCH_QUEUE,
    )
    print(f"started workflow id={handle.id} run_id={handle.result_run_id}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python -m src.temporal.starter <source> <title_id> "
              "[start_episode_no] [steps] [max_ep]")
        raise SystemExit(2)
    _start_ep = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    _steps = sys.argv[4].split(",") if len(sys.argv) > 4 else None
    _max_ep = int(sys.argv[5]) if len(sys.argv) > 5 else 0
    asyncio.run(kick(sys.argv[1], sys.argv[2], _start_ep, _steps, _max_ep))
