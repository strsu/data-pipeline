"""오케스트레이션 런타임 스모크 — 실제 워크플로 + stub 액티비티.

인프라(DB/S3/Chroma/model-api) 없이 Temporal 오케스트레이션(순서·OCR/YOLO 병렬·
continue-as-new·에피소드 체이닝)만 검증한다. 코어 I/O는 stub으로 대체.

    uv run --with temporalio --no-project python smoke_test.py
"""
import asyncio

from temporalio import activity
from temporalio.worker import Worker
from temporalio.testing import WorkflowEnvironment

from src.temporal.shared import TASK_QUEUE, CutRef, EpisodeInput, WebtoonInput
from src.temporal.workflows import EpisodeWorkflow, WebtoonWorkflow

# 호출 순서 추적
calls: list[str] = []


# ── stub 액티비티 (실제와 동일한 name) ────────────────────────────────────────

@activity.defn(name="resolve_episode")
async def resolve_episode(source: str, title_id: str, episode_no: int):
    if episode_no >= 3:  # ep1, ep2만 존재
        return None
    return EpisodeInput(source=source, title_id=title_id, episode_no=episode_no,
                        webtoon_episode_id=episode_no * 1000, start_cut=1, max_cut=0)


@activity.defn(name="get_episode_max_cut")
async def get_episode_max_cut(ep: EpisodeInput) -> int:
    return 120  # CUTS_PER_RUN(50) 초과 → continue-as-new 3회 유발


@activity.defn(name="prepare_episode")
async def prepare_episode(ep: EpisodeInput) -> None:
    calls.append(f"prepare:ep{ep.episode_no}:cut{ep.start_cut}")


@activity.defn(name="ocr_cut")
async def ocr_cut(cut: CutRef) -> bool:
    calls.append(f"ocr:ep{cut.episode_no}:cut{cut.cut_no}")
    return True


@activity.defn(name="yolo_cut")
async def yolo_cut(cut: CutRef) -> bool:
    calls.append(f"yolo:ep{cut.episode_no}:cut{cut.cut_no}")
    return True


@activity.defn(name="mark_phase1_complete")
async def mark_phase1_complete(ep: EpisodeInput) -> None:
    calls.append(f"phase1done:ep{ep.episode_no}")


@activity.defn(name="face_identify_episode")
async def face_identify_episode(ep: EpisodeInput) -> dict:
    calls.append(f"identify:ep{ep.episode_no}")
    return {"faces": 10, "matched": 7, "new_chars": 3}


async def main() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[WebtoonWorkflow, EpisodeWorkflow],
            activities=[resolve_episode, get_episode_max_cut, prepare_episode,
                        ocr_cut, yolo_cut, mark_phase1_complete, face_identify_episode],
        ):
            await env.client.execute_workflow(
                WebtoonWorkflow.run,
                WebtoonInput(source="naver", title_id="12345"),
                id="naver_12345",
                task_queue=TASK_QUEUE,
            )

    # ── 불변식 검증 ──────────────────────────────────────────────────────────
    ocr = [c for c in calls if c.startswith("ocr:")]
    yolo = [c for c in calls if c.startswith("yolo:")]
    identify = [c for c in calls if c.startswith("identify:")]
    prepares = [c for c in calls if c.startswith("prepare:")]

    assert len(ocr) == 240, f"ocr 호출 수 기대 240(=2ep*120cut), 실제 {len(ocr)}"
    assert len(yolo) == 240, f"yolo 호출 수 기대 240, 실제 {len(yolo)}"
    assert identify == ["identify:ep1", "identify:ep2"], f"identify 순서/횟수 이상: {identify}"
    # prepare는 에피소드당 1회(start_cut==1)만
    assert prepares == ["prepare:ep1:cut1", "prepare:ep2:cut1"], f"prepare 이상: {prepares}"
    # ep1 컷이 ep2 컷보다 먼저 (에피소드 순차)
    assert calls.index("ocr:ep1:cut120") < calls.index("ocr:ep2:cut1"), "에피소드 순차 위반"
    # 컷 내 ocr/yolo 둘 다 호출
    assert "ocr:ep1:cut50" in calls and "yolo:ep1:cut50" in calls

    print(f"SMOKE PASSED — ocr={len(ocr)} yolo={len(yolo)} identify={identify} prepare={prepares}")


if __name__ == "__main__":
    asyncio.run(main())
