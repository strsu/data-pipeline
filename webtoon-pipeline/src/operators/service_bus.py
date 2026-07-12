"""service celery로의 작업 위임(send_task) — §22.3 역할 분리의 전송 계층.

pipeline은 판정(LLM 심판)까지만 하고, 도메인 실행(제안 수락=병합 시맨틱 §19 /
자동 훅 §20)은 service의 celery task를 이름으로 호출해 위임한다. service 코드를
import하지 않고 브로커(redis)로만 결합한다(레포 책임 분리, §20.4 중복 방지).
"""
from __future__ import annotations

import logging

from src.config import settings

logger = logging.getLogger(__name__)

_app = None


def _celery_app():
    global _app
    if _app is None:
        from celery import Celery

        _app = Celery(broker=settings.CELERY_BROKER_URL)
        # service celery.py와 동일한 기본값 계열 — 전송만 하므로 최소 설정.
        _app.conf.broker_transport_options = {"priority_steps": [0]}
    return _app


def send_service_task(name: str, args: list, queue: str = "middle") -> None:
    """service celery task를 이름으로 큐잉. 브로커 미설정 시 RuntimeError.

    task name은 service `@app.task(name=...)`의 명시 이름과 일치해야 한다
    (예: "apps.api.toon.tasks.execute_consolidation"). 큐는 hipri|middle|lopri.
    """
    if not settings.CELERY_BROKER_URL:
        raise RuntimeError("CELERY_BROKER_URL 미설정 — BROKER_URL_/BROKER_PASSWORD env 필요")
    _celery_app().send_task(name, args=args, queue=queue)
    logger.info("[service_bus] send_task %s queue=%s args=%s", name, queue, str(args)[:200])
