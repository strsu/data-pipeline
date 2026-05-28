"""PaddleOCR 추출 — pipeline.py parse_ocr_result 로직 그대로 이식."""
from __future__ import annotations

from typing import Any

import cv2
import numpy as np
from paddleocr import PaddleOCR

_ocr: PaddleOCR | None = None


def get_ocr() -> PaddleOCR:
    global _ocr
    if _ocr is None:
        _ocr = PaddleOCR(
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            engine="paddle",
            lang="korean",
            text_detection_model_name="PP-OCRv5_server_det",
            text_recognition_model_name="korean_PP-OCRv5_mobile_rec",
            use_mkldnn=False,  # AMD CPU: OneDNN은 Intel 전용이라 비활성화
        )
    return _ocr


def run_ocr(image_bytes: bytes) -> list[dict[str, Any]]:
    img = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        return []
    return _parse_ocr_result(get_ocr().predict(img))


# ── 파싱 헬퍼 (pipeline.py parse_ocr_result 동일) ───────────────────────────

def _quad_to_bbox(quad: Any) -> list[int] | None:
    if quad is None:
        return None
    pts = np.asarray(quad)
    if pts.ndim != 2 or pts.shape[0] < 4 or pts.shape[1] < 2:
        return None
    try:
        xs = [int(round(float(p[0]))) for p in pts]
        ys = [int(round(float(p[1]))) for p in pts]
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    return [min(xs), min(ys), max(xs), max(ys)]


def _ocr_item_to_dict(item: Any) -> dict[str, Any] | None:
    if isinstance(item, dict):
        return item
    if hasattr(item, "json") and isinstance(item.json, dict):
        return item.json
    if hasattr(item, "to_dict"):
        d = item.to_dict()
        return d if isinstance(d, dict) else None
    return None


def _parse_ocr_result(ocr_result: Any) -> list[dict[str, Any]]:
    if not ocr_result:
        return []
    first = ocr_result[0] if isinstance(ocr_result, list) else ocr_result
    item = _ocr_item_to_dict(first)
    lines: list[dict[str, Any]] = []

    if item is not None:
        for i, text in enumerate(item.get("rec_texts") or []):
            clean = text.strip() if isinstance(text, str) else ""
            if not clean:
                continue
            scores = item.get("rec_scores") or []
            polys = item.get("rec_polys") or item.get("dt_polys") or []
            try:
                score = float(scores[i]) if i < len(scores) else 0.0
            except (TypeError, ValueError):
                score = 0.0
            bbox = _quad_to_bbox(polys[i] if i < len(polys) else None)
            entry: dict[str, Any] = {"text": clean, "score": round(score, 4)}
            if bbox:
                entry["bbox_2d"] = bbox
            lines.append(entry)
        if lines:
            lines.sort(key=lambda x: (x.get("bbox_2d", [0, 0])[1], x.get("bbox_2d", [0, 0])[0]))
            return lines

    if isinstance(first, list):
        for line in first:
            if not (isinstance(line, list) and len(line) >= 2):
                continue
            quad, rec = line[0], line[1]
            if not (isinstance(rec, (list, tuple)) and len(rec) >= 2):
                continue
            clean = rec[0].strip() if isinstance(rec[0], str) else ""
            if not clean:
                continue
            try:
                score = float(rec[1])
            except (TypeError, ValueError):
                score = 0.0
            bbox = _quad_to_bbox(quad)
            entry = {"text": clean, "score": round(score, 4)}
            if bbox:
                entry["bbox_2d"] = bbox
            lines.append(entry)
        lines.sort(key=lambda x: (x.get("bbox_2d", [0, 0])[1], x.get("bbox_2d", [0, 0])[0]))

    return lines
