"""Temporal 공용 타입 / 설정."""
from __future__ import annotations

import os
from dataclasses import dataclass

TASK_QUEUE = "webtoon-pipeline"
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")

# EpisodeWorkflow 한 실행이 처리하는 컷 수 — 이 단위로 continue-as-new (history 제한).
CUTS_PER_RUN = int(os.getenv("CUTS_PER_RUN", "50"))


@dataclass
class WebtoonInput:
    """웹툰 단위 워크플로 입력. workflow_id = f'{source}_{title_id}'."""
    source: str
    title_id: str
    start_episode_no: int = 1


@dataclass
class EpisodeInput:
    """에피소드 단위 워크플로 입력. continue-as-new 시 start_cut 갱신."""
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    start_cut: int = 1
    max_cut: int = 0  # 0이면 activity로 조회


@dataclass
class CutRef:
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    cut_no: int


@dataclass
class EpisodeResult:
    source: str
    title_id: str
    episode_no: int
    total_cuts: int
    faces: int
    matched: int
    new_chars: int


@dataclass
class SceneInput:
    """Step3(LLM) 에피소드 처리 입력. continue-as-new 시 start_cut/prev_context 갱신."""
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    start_cut: int = 1
    max_cut: int = 0
    prev_context: str = ""
