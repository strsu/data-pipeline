"""콘텐츠 구간(여백/단색 제거) 검출.

세로로 이어붙인 스트립에서 가로 전체가 (거의) 단일색인 '여백 행'이 일정 길이 이상
연속되는 구간(밴드)을 구분선으로 보고 통째로 버린다. 남은 콘텐츠 구간만 반환한다
(얼굴/텍스트가 있는 영역). 콘텐츠 블록은 쪼개지 않으며 블록 내부의 짧은 여백은 흡수한다.

에피소드 단위 처리(core.step1.process_episode_*)와 시각화 도구(cut-splitter-viz)가
`_content_intervals` / `MARGIN_PX` 를 공유한다.
"""
from __future__ import annotations

import numpy as np

# 여백(행) 판정 허용오차: 행의 채널별 (max-min) 최대값이 이 값 이하이면 단일색 취급.
# JPEG 노이즈가 있는 흰/검 배경도 여백으로 인정하기 위해 exact(0)가 아닌 약간의 tol 사용.
NEAR_UNIFORM_TOL = 12
# 구분선으로 인정할 최소 연속 여백 행 수. 이보다 짧은 여백은 콘텐츠에 흡수(블록 유지).
MIN_BAND_PX = 10
# 콘텐츠 구간 중 배경색(대표색)에 가까운 픽셀 비율이 이 값 이상이면 '사실상 단색'으로 보고 버린다.
# (배경 위 티끌/노이즈 제거. 글자가 있으면 글자 픽셀이 이 임계를 넘겨 유지된다.)
UNIFORM_BG_RATIO = 0.98
# 콘텐츠 구간 위/아래로 남길 여백(글자/얼굴 가장자리 클리핑 방지). 세그먼트 크롭 시 사용.
MARGIN_PX = 3


def _is_near_uniform_block(block: np.ndarray, tol: int, ratio: float) -> bool:
    """블록(픽셀 N×채널)의 ratio 이상이 대표색(채널 중앙값)에 tol 이내로 가까우면
    '사실상 단색'(배경+티끌)으로 판정."""
    if block.size == 0:
        return True
    med = np.median(block, axis=0)
    dist = np.abs(block.astype(np.int16) - med.astype(np.int16)).max(axis=1)
    return float((dist <= tol).mean()) >= ratio


def _content_intervals(
    arr: np.ndarray, tol: int = NEAR_UNIFORM_TOL, min_band: int = MIN_BAND_PX,
    bg_ratio: float = UNIFORM_BG_RATIO,
) -> list[tuple[int, int]]:
    """콘텐츠 구간 [y0,y1) 목록 반환.

    - 행의 채널별 (max-min) 최대값 <= tol 이면 '여백 행'.
    - 여백 행이 min_band 이상 연속이면 구분 밴드 → 콘텐츠 종료(밴드 통째 제거).
    - 짧은 여백(<min_band)은 콘텐츠에 흡수해 블록을 쪼개지 않는다.
    - 구간의 bg_ratio 이상이 대표색(채널 중앙값)에 가까우면 '사실상 단색'으로 보고 버린다
      (배경 위 티끌/노이즈 제거. 글자가 있으면 글자 픽셀이 임계를 넘겨 유지된다).
    """
    h = arr.shape[0]
    rows = arr.reshape(h, -1, arr.shape[-1])
    row_ptp = (rows.max(axis=1) - rows.min(axis=1)).max(axis=1)
    blank = row_ptp <= tol

    intervals: list[tuple[int, int]] = []
    i = 0
    while i < h:
        if blank[i]:
            i += 1
            continue
        start = i
        j = i
        while j < h:
            if blank[j]:
                k = j
                while k < h and blank[k]:
                    k += 1
                if (k - j) >= min_band or k == h:
                    break          # 구분 밴드 → 콘텐츠 종료
                j = k              # 짧은 여백 → 콘텐츠로 흡수
            else:
                j += 1
        block = arr[start:j].reshape(-1, arr.shape[-1])
        if not _is_near_uniform_block(block, tol, bg_ratio):
            intervals.append((start, j))
        i = j
    return intervals
