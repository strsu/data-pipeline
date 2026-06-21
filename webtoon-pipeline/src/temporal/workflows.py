"""Temporal 워크플로 — 흐름 제어만(결정적). I/O는 전부 activity.

WebtoonWorkflow (id="{source}_{title_id}")   # 웹툰당 1개 → 에피소드 순차/웹툰 간 병렬
  └─ EpisodeWorkflow (child)                  # 에피소드 순차
       └─ 컷 루프: ocr_cut ∥ yolo_cut (분리 병렬)
       └─ (모든 컷 완료 후) face_identify_episode   # 에피소드 단위
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.temporal import activities
    from src.temporal.shared import CUTS_PER_RUN, CutRef, EpisodeInput, EpisodeResult, WebtoonInput

_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    # model-api 재시작/콜드스타트(Connection refused)를 견디도록 관대하게.
    # 일시 장애로 에피소드 워크플로 전체가 실패하지 않게 한다(durable execution).
    maximum_attempts=100,
)


@workflow.defn
class EpisodeWorkflow:
    @workflow.run
    async def run(self, ep: EpisodeInput) -> EpisodeResult:
        # 첫 배치에서만 재처리 정리.
        if ep.start_cut == 1:
            await workflow.execute_activity(
                activities.prepare_episode, ep,
                start_to_close_timeout=timedelta(minutes=5), retry_policy=_RETRY,
            )

        max_cut = ep.max_cut or await workflow.execute_activity(
            activities.get_episode_max_cut, ep,
            start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
        )

        end = min(ep.start_cut + CUTS_PER_RUN - 1, max_cut) if max_cut else ep.start_cut + CUTS_PER_RUN - 1

        cut_no = ep.start_cut
        last_has_next = True
        while cut_no <= end:
            ref = CutRef(
                source=ep.source, title_id=ep.title_id, episode_no=ep.episode_no,
                webtoon_episode_id=ep.webtoon_episode_id, cut_no=cut_no,
            )
            # OCR과 YOLO는 독립 경로 → 병렬. 각자 model-api 서비스/재시도 분리.
            ocr_has_next, _ = await asyncio.gather(
                workflow.execute_activity(
                    activities.ocr_cut, ref,
                    start_to_close_timeout=timedelta(minutes=5), retry_policy=_RETRY,
                ),
                workflow.execute_activity(
                    activities.yolo_cut, ref,
                    start_to_close_timeout=timedelta(minutes=5), retry_policy=_RETRY,
                ),
            )
            last_has_next = ocr_has_next
            # 404로 에피소드 경계 도달(이미지 없음) → 컷 루프 종료.
            if not ocr_has_next:
                break
            cut_no += 1

        # 아직 컷이 남았으면(max_cut 미도달 & 경계 미도달) 다음 배치로 재진입.
        if last_has_next and (not max_cut or end < max_cut):
            workflow.continue_as_new(
                EpisodeInput(
                    source=ep.source, title_id=ep.title_id, episode_no=ep.episode_no,
                    webtoon_episode_id=ep.webtoon_episode_id,
                    start_cut=end + 1, max_cut=max_cut,
                )
            )

        # ── 마지막 배치: phase1 완료 마킹 + 에피소드 단위 얼굴 식별 ──
        await workflow.execute_activity(
            activities.mark_phase1_complete, ep,
            start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
        )
        summary = await workflow.execute_activity(
            activities.face_identify_episode, ep,
            start_to_close_timeout=timedelta(minutes=30), retry_policy=_RETRY,
        )

        return EpisodeResult(
            source=ep.source, title_id=ep.title_id, episode_no=ep.episode_no,
            total_cuts=cut_no, faces=summary.get("faces", 0),
            matched=summary.get("matched", 0), new_chars=summary.get("new_chars", 0),
        )


@workflow.defn
class WebtoonWorkflow:
    @workflow.run
    async def run(self, w: WebtoonInput) -> None:
        ep = await workflow.execute_activity(
            activities.resolve_episode,
            args=[w.source, w.title_id, w.start_episode_no],
            start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
        )
        if ep is None:
            workflow.logger.info("[webtoon] %s/%s — 처리할 에피소드 없음, 종료", w.source, w.title_id)
            return

        await workflow.execute_child_workflow(
            EpisodeWorkflow.run, ep,
            id=f"{w.source}_{w.title_id}_{ep.episode_no}",
        )

        # 다음 에피소드로 재진입(같은 웹툰 워크플로가 순차 진행).
        workflow.continue_as_new(
            WebtoonInput(source=w.source, title_id=w.title_id, start_episode_no=ep.episode_no + 1)
        )
