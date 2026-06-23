"""컷 병합 및 콘텐츠 구간 기반 분할.

인접한 두 컷을 수직 결합한 뒤, 가로 전체가 (거의) 단일색인 '여백 행'이 일정 길이
이상 연속되는 구간(밴드)을 구분선으로 보고 통째로 버린다. 남은 콘텐츠 구간만
세그먼트로 만든다(얼굴/텍스트가 있는 영역). 콘텐츠 블록은 쪼개지 않으며, 블록 내부의
짧은 여백은 흡수한다. bbox는 항상 원본 컷 좌표 기준으로 변환된다.
"""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

import numpy as np
from PIL import Image

# 여백(행) 판정 허용오차: 행의 채널별 (max-min) 최대값이 이 값 이하이면 단일색 취급.
# JPEG 노이즈가 있는 흰/검 배경도 여백으로 인정하기 위해 exact(0)가 아닌 약간의 tol 사용.
NEAR_UNIFORM_TOL = 12
# 구분선으로 인정할 최소 연속 여백 행 수. 이보다 짧은 여백은 콘텐츠에 흡수(블록 유지).
MIN_BAND_PX = 10
# 콘텐츠 구간 중 배경색(대표색)에 가까운 픽셀 비율이 이 값 이상이면 '사실상 단색'으로 보고 버린다.
# (배경 위 티끌/노이즈 제거. 글자가 있으면 글자 픽셀이 이 임계를 넘겨 유지된다.)
UNIFORM_BG_RATIO = 0.98
# 콘텐츠 구간 위/아래로 남길 여백(글자/얼굴 가장자리 클리핑 방지).
MARGIN_PX = 3


@dataclass
class CutSegment:
    image_bytes: bytes
    y_offset: int    # combined 이미지 내 세그먼트 시작 y (원본 컷 좌표 계산용)
    cut_n_height: int  # cut[N] 높이 (경계 귀속 판단용)


def _to_pil(image_bytes: bytes) -> Image.Image:
    return Image.open(BytesIO(image_bytes)).convert("RGB")


def _to_bytes(img: Image.Image) -> bytes:
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=92)
    return buf.getvalue()


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


def split_cut_pair(
    cut_n_bytes: bytes,
    cut_n1_bytes: bytes | None,
) -> list[CutSegment]:
    """cut[N]과 cut[N+1]을 결합 후 콘텐츠 구간만 세그먼트로 반환.

    여백(거의 단일색) 밴드는 통째로 버리고, 얼굴/텍스트가 있는 콘텐츠 구간만 남긴다.
    콘텐츠 블록은 쪼개지 않으며 위/아래로 MARGIN_PX 여백을 남긴다. 콘텐츠가 전혀 없으면
    각 컷을 원본 그대로 반환한다(안전 폴백).
    """
    img_n = _to_pil(cut_n_bytes)
    h_n = img_n.height
    w_n = img_n.width

    if cut_n1_bytes is None:
        arr = np.asarray(img_n)
        segs = _segments_from_intervals(img_n, arr, h_n, h_n)
        return segs or [CutSegment(cut_n_bytes, 0, h_n)]

    img_n1 = _to_pil(cut_n1_bytes)

    # 너비가 다르면 cut[N] 너비 기준으로 리사이즈
    if img_n1.width != w_n:
        img_n1 = img_n1.resize((w_n, img_n1.height), Image.LANCZOS)

    h_n1 = img_n1.height
    combined = Image.new("RGB", (w_n, h_n + h_n1))
    combined.paste(img_n, (0, 0))
    combined.paste(img_n1, (0, h_n))

    arr = np.asarray(combined)
    segs = _segments_from_intervals(combined, arr, h_n, h_n + h_n1)
    if not segs:
        # 콘텐츠 미검출(전부 여백) → 원본 각 컷 반환(안전 폴백)
        return [
            CutSegment(cut_n_bytes, 0, h_n),
            CutSegment(cut_n1_bytes, h_n, h_n),
        ]
    return segs


def _segments_from_intervals(
    img: Image.Image, arr: np.ndarray, h_n: int, total_h: int
) -> list[CutSegment]:
    """콘텐츠 구간을 잘라 CutSegment 목록 생성(위/아래 MARGIN_PX 여백 포함)."""
    w = img.width
    segments: list[CutSegment] = []
    for y0, y1 in _content_intervals(arr):
        top = max(0, y0 - MARGIN_PX)
        bot = min(total_h, y1 + MARGIN_PX)
        seg_img = img.crop((0, top, w, bot))
        segments.append(CutSegment(_to_bytes(seg_img), top, h_n))
    return segments


def adjust_bbox_to_cut(
    bbox: list[float],
    segment: CutSegment,
) -> tuple[list[float], int]:
    """세그먼트 좌표계 bbox를 원본 컷 좌표계로 변환.

    귀속 컷은 bbox 중심(center) 기준으로 결정한다. 좌표계 변환도 center가 정한
    컷에 맞추므로, 귀속 판정과 변환 좌표계가 항상 일치한다 (cross-boundary
    중복 dedup이 같은 cut 내에서 정상 동작하도록 보장).

    Returns:
        (변환된 bbox [x1,y1,x2,y2], 귀속 cut_offset)
        cut_offset=0: cut[N], cut_offset=1: cut[N+1]
    """
    x1, y1, x2, y2 = bbox
    abs_y1 = segment.y_offset + y1
    abs_y2 = segment.y_offset + y2
    abs_yc = (abs_y1 + abs_y2) / 2

    if abs_yc < segment.cut_n_height:
        # center가 cut[N]에 귀속 (경계 걸쳐도 중심이 위 컷이면 위 컷 기준)
        return [x1, abs_y1, x2, abs_y2], 0
    else:
        # center가 cut[N+1]에 귀속
        return [x1, abs_y1 - segment.cut_n_height, x2, abs_y2 - segment.cut_n_height], 1
