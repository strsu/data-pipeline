"""컷 병합 및 자연 구분선 기반 분할.

인접한 두 컷을 수직 결합한 뒤, 가로 전체 픽셀 색상이 동일한 행이 3px 이상
연속되는 구간을 찾아 그 위치에서 분할한다. 구분선이 없으면 각 컷을 원본 그대로
개별 세그먼트로 반환한다. bbox는 항상 원본 컷 좌표 기준으로 변환된다.
"""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

import numpy as np
from PIL import Image

MIN_BAND_PX = 3   # 구분선으로 인정할 최소 연속 행 수
MARGIN_PX = 3     # 분할 후 남길 여백


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


def _find_band_splits(arr: np.ndarray) -> list[tuple[int, int]]:
    """uniform-color 밴드 (시작 y, 끝 y) 목록 반환."""
    height = arr.shape[0]
    bands: list[tuple[int, int]] = []
    band_start = -1

    for y in range(height):
        row = arr[y]
        is_uniform = bool(np.all(row == row[0]))
        if is_uniform:
            if band_start < 0:
                band_start = y
        else:
            if band_start >= 0 and (y - band_start) >= MIN_BAND_PX:
                bands.append((band_start, y - 1))
            band_start = -1

    if band_start >= 0 and (height - band_start) >= MIN_BAND_PX:
        bands.append((band_start, height - 1))

    return bands


def split_cut_pair(
    cut_n_bytes: bytes,
    cut_n1_bytes: bytes | None,
) -> list[CutSegment]:
    """cut[N]과 cut[N+1]을 결합 후 자연 구분선에서 분할.

    cut_n1_bytes가 None이거나 구분선이 없으면 각 컷을 원본 그대로 반환한다.
    """
    img_n = _to_pil(cut_n_bytes)
    h_n = img_n.height
    w_n = img_n.width

    if cut_n1_bytes is None:
        return [CutSegment(cut_n_bytes, 0, h_n)]

    img_n1 = _to_pil(cut_n1_bytes)

    # 너비가 다르면 cut[N] 너비 기준으로 리사이즈
    if img_n1.width != w_n:
        img_n1 = img_n1.resize((w_n, img_n1.height), Image.LANCZOS)

    h_n1 = img_n1.height
    combined = Image.new("RGB", (w_n, h_n + h_n1))
    combined.paste(img_n, (0, 0))
    combined.paste(img_n1, (0, h_n))

    arr = np.array(combined)
    bands = _find_band_splits(arr)

    if not bands:
        # 구분선 없음 → 각 컷 원본 반환
        return [
            CutSegment(cut_n_bytes, 0, h_n),
            CutSegment(cut_n1_bytes, h_n, h_n),
        ]

    # 분할점: 각 밴드 중앙에서 ±MARGIN_PX 남기고 컷팅
    split_points: list[int] = []
    for band_start, band_end in bands:
        mid = (band_start + band_end) // 2
        cut_top = max(band_start + MARGIN_PX, mid)
        cut_bot = min(band_end - MARGIN_PX + 1, mid + 1)
        split_points.append(max(cut_top, cut_bot))

    # 중복·역순 제거 후 정렬
    split_points = sorted(set(split_points))

    segments: list[CutSegment] = []
    prev_y = 0
    for sp in split_points:
        if sp <= prev_y:
            continue
        seg_img = combined.crop((0, prev_y, w_n, sp))
        segments.append(CutSegment(_to_bytes(seg_img), prev_y, h_n))
        prev_y = sp

    # 마지막 세그먼트
    if prev_y < h_n + h_n1:
        seg_img = combined.crop((0, prev_y, w_n, h_n + h_n1))
        segments.append(CutSegment(_to_bytes(seg_img), prev_y, h_n))

    return segments


def adjust_bbox_to_cut(
    bbox: list[float],
    segment: CutSegment,
) -> tuple[list[float], int]:
    """세그먼트 좌표계 bbox를 원본 컷 좌표계로 변환.

    Returns:
        (변환된 bbox [x1,y1,x2,y2], 귀속 cut_offset)
        cut_offset=0: cut[N], cut_offset=1: cut[N+1]
    """
    x1, y1, x2, y2 = bbox
    abs_y1 = segment.y_offset + y1
    abs_y2 = segment.y_offset + y2

    if abs_y1 < segment.cut_n_height:
        # cut[N]에 귀속 (경계 걸쳐도 위 컷 기준)
        return [x1, abs_y1, x2, abs_y2], 0
    else:
        # cut[N+1]에 귀속
        return [x1, abs_y1 - segment.cut_n_height, x2, abs_y2 - segment.cut_n_height], 1
