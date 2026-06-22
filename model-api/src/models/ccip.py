"""CCIP feature 추출 + metric 비교 (deepghs imgutils, 애니/웹툰 동일인 판별)."""
from __future__ import annotations

from io import BytesIO

import numpy as np
from PIL import Image

from src.config import CCIP_MODEL

_loaded = False


def _ensure_loaded() -> None:
    """CCIP feature/metric 모델 preload (imgutils 내부 onnx 캐시 워밍업)."""
    global _loaded
    if _loaded:
        return
    from imgutils.metrics import ccip_extract_feature
    # 더미 이미지로 1회 호출해 onnx 세션 로딩
    dummy = Image.new("RGB", (384, 384), (127, 127, 127))
    ccip_extract_feature(dummy, model=CCIP_MODEL)
    _loaded = True


def extract_ccip_feature(image_bytes: bytes) -> list[float]:
    """이미지 바이트 → CCIP feature (float list)."""
    from imgutils.metrics import ccip_extract_feature
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    feat = ccip_extract_feature(img, model=CCIP_MODEL)
    return np.asarray(feat, dtype=np.float32).tolist()


def compare_features(query: list[float], anchors: list[list[float]]) -> dict:
    """query feature vs anchor features 들의 CCIP metric 차이 계산.

    반환: {"diffs": [...], "min_diff": float|None, "argmin": int|None}
    낮을수록 동일인. 임계값(예: 0.16) 판정은 호출측(파이프라인)에서 수행.

    ccip_difference(x, y)는 내부적으로 ccip_batch_differences([x, y])[0, 1]과 동일하며,
    호출 1회당 ONNX metric 모델 forward pass 1회가 발생한다. anchor마다 따로 호출하면
    anchor 수만큼 forward pass가 반복되므로(N개 → N회), query+anchor 전체를 한 번에
    배치로 묶어 forward pass 1회로 전체 pairwise 행렬을 구하고 query 행(0번)만 취한다.
    """
    if not anchors:
        return {"diffs": [], "min_diff": None, "argmin": None}

    from imgutils.metrics import ccip_batch_differences
    q = np.asarray(query, dtype=np.float32)
    batch = [q] + [np.asarray(a, dtype=np.float32) for a in anchors]
    diff_matrix = ccip_batch_differences(batch, model=CCIP_MODEL)
    diffs = diff_matrix[0, 1:].astype(float).tolist()
    argmin = int(np.argmin(diffs))
    return {"diffs": diffs, "min_diff": diffs[argmin], "argmin": argmin}
