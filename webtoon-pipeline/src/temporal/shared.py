"""Temporal 공용 타입 / 설정."""
from __future__ import annotations

import os
from dataclasses import dataclass

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")

# ── Task Queue 구성 (step별 독립 동시성) ──────────────────────────────────────
#
# 체인 오케스트레이터(EpisodeChainWorkflow)는 ORCH_QUEUE에서 돌고, 무거운 step 작업은
# step별 전용 큐로 분기한다. 각 step 큐를 서빙하는 워커를 max_concurrent_activities=1로
# 두면 step1/step2/step3가 각각 전역에서 1개씩만 동시 실행된다(개인 서버 자원 보호).
# 체인이 몇 개 떠 있든(웹툰 여러 개·범위 실행 등) 같은 step 액티비티는 그 큐에서 줄 서므로
# 자동 직렬화된다.
ORCH_QUEUE = os.getenv("ORCH_TASK_QUEUE", "webtoon-orchestrator")
STEP1_QUEUE = os.getenv("STEP1_TASK_QUEUE", "webtoon-step1")
STEP2_QUEUE = os.getenv("STEP2_TASK_QUEUE", "webtoon-step2")
STEP3_QUEUE = os.getenv("STEP3_TASK_QUEUE", "webtoon-step3")

# 하위호환: 기존 코드/문서가 참조하던 단일 큐 이름 → 오케스트레이터 큐로 매핑.
TASK_QUEUE = ORCH_QUEUE

# step 이름 → phase 번호 / 전용 큐.
STEP1, STEP2, STEP3 = "step1", "step2", "step3"
STEP_PHASE = {STEP1: 1, STEP2: 2, STEP3: 3}
# v4.0(§17.1): 진행도는 analysis_run으로 도출 — step→run kind 매핑(step3의 결과 정본은 resolve run).
STEP_RUN_KIND = {STEP1: "step1", STEP2: "step2", STEP3: "resolve"}
STEP_QUEUE = {STEP1: STEP1_QUEUE, STEP2: STEP2_QUEUE, STEP3: STEP3_QUEUE}

# EpisodeSceneWorkflow(레거시)가 처리하던 컷 배치 단위 — step3가 에피소드 단위 액티비티로
# 흡수되며 더 이상 워크플로 분기에는 쓰이지 않지만, 환경변수 호환을 위해 남겨둔다.
CUTS_PER_RUN = int(os.getenv("CUTS_PER_RUN", "50"))


@dataclass
class ChainInput:
    """제네릭 에피소드 체인 입력 — 정식 경로(자동/admin 공통).

    steps   : 에피소드마다 순서대로 실행할 step 목록(["step1"], ["step1","step2"],
              ["step1","step2","step3"], ["step3"] 등). STEP1/STEP2/STEP3 값 사용.
    cur_ep  : 이번 반복에서 처리할 회차 번호.
    max_ep  : 0이면 자동(unbounded) 모드 — 진입 step 미완료인 다음 다운로드 회차를 계속
              찾아 이어간다(다운로드 증분/정규 경로). 0보다 크면 범위(bounded) 모드 —
              cur_ep..max_ep 를 순차로 밟고 끝나면 종료(admin 범위 실행).
    force   : True면 admin 재실행 — step3 phase3_enabled 게이트 등 자동 게이트를 무시하고
              steps에 명시된 step을 그대로 실행한다.

    "다음 ep로 이어갈지"는 모든 step 조합에서 공통으로 `next_chain_episode`가 판정한다.
    """
    source: str
    title_id: str
    steps: list[str]
    cur_ep: int
    max_ep: int = 0
    force: bool = False


@dataclass
class EpisodeInput:
    """에피소드 단위 step 작업 입력 — 체인이 회차마다 step 액티비티에 넘긴다."""
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    start_cut: int = 1
    max_cut: int = 0  # 0이면 activity로 조회


# 정리 패스(§22.3) 트리거 임계 — 마지막 정리 이후 succeeded resolve 개수.
CONSOLIDATE_EVERY_N_RESOLVES = int(os.getenv("CONSOLIDATE_EVERY_N_RESOLVES", "5"))


@dataclass
class ConsolidateInput:
    """웹툰 단위 정리 패스(§22.3~22.4) 입력 — 제안검토 심판 + 실행 위임."""
    webtoon_id: int


@dataclass
class RegenInput:
    """캐릭터 재분석(재도출) 입력 — RegenerateCharacterWorkflow(§20).

    mode:
      - "profile"  : 병합 후 경량 재도출 — 근거 전량 주입 LLM 1콜로 프로필 replace.
      - "reresolve": 얼굴 이동/섞임 풀기 후 — 등장 에피소드 전부를
                     reresolve_episode(rerun_extract=True)로 순차 재해소한 뒤,
                     clean 근거 위에서 프로필 재도출로 마무리.
    absorbed_character_ids: 병합 훅이 넘기는 흡수 캐릭터 id들 — soft-delete된 프로필 조각
    (key_facts 등)을 재도출 근거로 회수한다(§20.4). 수동 트리거면 빈 리스트.
    """
    character_id: int
    mode: str = "profile"
    absorbed_character_ids: list[int] | None = None
