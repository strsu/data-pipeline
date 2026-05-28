"""YOLO 얼굴 탐지 — pipeline.py FaceOperator._detect 로직 이식."""
from __future__ import annotations

from io import BytesIO

from PIL import Image
from ultralytics import YOLO

from src.config.settings import (
    ASPECT_RATIO_MAX,
    ASPECT_RATIO_MIN,
    FACE_CONF_THRESHOLD,
    FACE_MIN_PX,
    YOLO_MODEL_PATH,
)

_model: YOLO | None = None


def get_model() -> YOLO:
    global _model
    if _model is None:
        _model = YOLO(YOLO_MODEL_PATH)
    return _model


def detect_faces(image_bytes: bytes) -> list[dict]:
    """YOLO로 얼굴 탐지. 반환: [{"bbox": [x1,y1,x2,y2], "conf": float}]"""
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    faces = []
    for r in get_model()(img, conf=FACE_CONF_THRESHOLD, verbose=False):
        for box in r.boxes:
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            w, h = x2 - x1, y2 - y1
            if w < FACE_MIN_PX or h < FACE_MIN_PX:
                continue
            if not (ASPECT_RATIO_MIN <= w / h <= ASPECT_RATIO_MAX):
                continue
            faces.append({"bbox": [float(x1), float(y1), float(x2), float(y2)], "conf": float(box.conf[0])})
    return faces
