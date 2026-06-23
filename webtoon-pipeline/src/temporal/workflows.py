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
    from src.temporal.shared import (
        CUTS_PER_RUN, CutRef, EpisodeInput, EpisodeResult, SceneInput, WebtoonInput,
    )

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
        # Step1이 이미 완료된 에피소드면 재추출(OCR/YOLO) 생략하고 Step2만 재실행.
        if await workflow.execute_activity(
            activities.is_phase1_done, ep.webtoon_episode_id,
            start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
        ):
            summary = await workflow.execute_activity(
                activities.face_identify_episode, ep,
                start_to_close_timeout=timedelta(minutes=30), heartbeat_timeout=timedelta(minutes=2),
                retry_policy=_RETRY,
            )
            return EpisodeResult(
                source=ep.source, title_id=ep.title_id, episode_no=ep.episode_no,
                total_cuts=0, faces=summary.get("faces", 0),
                matched=summary.get("matched", 0), new_chars=summary.get("new_chars", 0),
            )

        # 재처리 정리(기존 OCR/얼굴 데이터 제거).
        await workflow.execute_activity(
            activities.prepare_episode, ep,
            start_to_close_timeout=timedelta(minutes=5), retry_policy=_RETRY,
        )

        # 에피소드 단위 Step1 — 전체 컷을 스트립으로 결합 후 콘텐츠 세그먼트별 OCR/YOLO.
        # OCR과 YOLO는 독립 경로 → 병렬. 에피소드 전체라 오래 걸릴 수 있어 timeout 넉넉 +
        # heartbeat(세그먼트마다)로 진행 보고. 컷 배치(continue-as-new)는 더 이상 필요 없다.
        await asyncio.gather(
            workflow.execute_activity(
                activities.ocr_episode, ep,
                start_to_close_timeout=timedelta(hours=1), heartbeat_timeout=timedelta(minutes=2),
                retry_policy=_RETRY,
            ),
            workflow.execute_activity(
                activities.yolo_episode, ep,
                start_to_close_timeout=timedelta(hours=1), heartbeat_timeout=timedelta(minutes=2),
                retry_policy=_RETRY,
            ),
        )

        # phase1 완료 마킹 + 에피소드 단위 얼굴 식별(Step2).
        await workflow.execute_activity(
            activities.mark_phase1_complete, ep,
            start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
        )
        summary = await workflow.execute_activity(
            activities.face_identify_episode, ep,
            start_to_close_timeout=timedelta(minutes=30), heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_RETRY,
        )

        return EpisodeResult(
            source=ep.source, title_id=ep.title_id, episode_no=ep.episode_no,
            total_cuts=0, faces=summary.get("faces", 0),
            matched=summary.get("matched", 0), new_chars=summary.get("new_chars", 0),
        )


@workflow.defn
class EpisodeFaceIdentifyWorkflow:
    """Step2 단독 — 이미 추출된 얼굴로 임베딩+매칭만 재실행(OCR/YOLO 재실행 없음).
    admin에서 에피소드 단위로 트리거."""

    @workflow.run
    async def run(self, ep: EpisodeInput) -> dict:
        return await workflow.execute_activity(
            activities.face_identify_episode, ep,
            start_to_close_timeout=timedelta(minutes=30), heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_RETRY,
        )


@workflow.defn
class EpisodeSceneWorkflow:
    """Step3 — 에피소드의 컷을 순차 LLM 분석(슬라이딩 윈도우). 활성 웹툰만.
    CUTS_PER_RUN 단위로 continue-as-new 하며 prev_context를 이어 전달."""

    @workflow.run
    async def run(self, s: SceneInput) -> None:
        # 첫 배치에서만 기존 Step3 결과 정리(재실행 완전 교체).
        if s.start_cut == 1:
            await workflow.execute_activity(
                activities.prepare_scene,
                EpisodeInput(s.source, s.title_id, s.episode_no, s.webtoon_episode_id),
                start_to_close_timeout=timedelta(minutes=2), retry_policy=_RETRY,
            )

        max_cut = s.max_cut or await workflow.execute_activity(
            activities.get_episode_max_cut,
            EpisodeInput(s.source, s.title_id, s.episode_no, s.webtoon_episode_id),
            start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
        )

        end = min(s.start_cut + CUTS_PER_RUN - 1, max_cut)
        prev = s.prev_context
        cut_no = s.start_cut
        while cut_no <= end:
            ref = CutRef(
                source=s.source, title_id=s.title_id, episode_no=s.episode_no,
                webtoon_episode_id=s.webtoon_episode_id, cut_no=cut_no,
            )
            prev = await workflow.execute_activity(
                activities.scene_llm_cut,
                args=[ref, prev],
                start_to_close_timeout=timedelta(minutes=10), retry_policy=_RETRY,
            )
            cut_no += 1

        if end < max_cut:
            workflow.continue_as_new(
                SceneInput(
                    source=s.source, title_id=s.title_id, episode_no=s.episode_no,
                    webtoon_episode_id=s.webtoon_episode_id,
                    start_cut=end + 1, max_cut=max_cut, prev_context=prev,
                )
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

        # Step3(LLM) — 활성 웹툰만. Step1+2 완료 후 컷 순차 분석.
        if await workflow.execute_activity(
            activities.is_phase3_enabled, ep.webtoon_episode_id,
            start_to_close_timeout=timedelta(seconds=30), retry_policy=_RETRY,
        ):
            await workflow.execute_child_workflow(
                EpisodeSceneWorkflow.run,
                SceneInput(
                    source=ep.source, title_id=ep.title_id, episode_no=ep.episode_no,
                    webtoon_episode_id=ep.webtoon_episode_id,
                ),
                id=f"{w.source}_{w.title_id}_{ep.episode_no}_scene",
            )

        # 다음 에피소드로 재진입(같은 웹툰 워크플로가 순차 진행).
        workflow.continue_as_new(
            WebtoonInput(source=w.source, title_id=w.title_id, start_episode_no=ep.episode_no + 1)
        )
