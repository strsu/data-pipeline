import os

# ── 모델 서빙 모드 (A3 분리) ───────────────────────────────────────────────
# all        : OCR+YOLO+CLIP 전부 로드 (기존 동작, 기본값)
# ocr-yolo   : PaddleOCR + YOLO (결합, 기존 Faust 호환)
# ocr        : PaddleOCR 만 (로드 분리)
# yolo       : YOLO 만 (로드 분리)
# embed-clip : CLIP 만
# embed-ccip : CCIP feature/metric 만
MODEL_API_MODE = os.getenv("MODEL_API_MODE", "all")


def serves_ocr() -> bool:
    return MODEL_API_MODE in ("all", "ocr-yolo", "ocr")


def serves_yolo() -> bool:
    return MODEL_API_MODE in ("all", "ocr-yolo", "yolo")


def serves_clip() -> bool:
    return MODEL_API_MODE in ("all", "embed-clip")


def serves_ccip() -> bool:
    return MODEL_API_MODE in ("all", "embed-ccip")


# ── 스레드 과구독 방지 (§A3) ───────────────────────────────────────────────
# 워커당 BLAS/OMP/torch 스레드 수. 미설정(0/빈값)이면 라이브러리 기본값 유지.
MODEL_API_THREADS = int(os.getenv("MODEL_API_THREADS", "0") or "0")

# ── YOLO ───────────────────────────────────────────────────────────────────
YOLO_MODEL_PATH = os.getenv("YOLO_MODEL_PATH", "/project/models/anime_face_detection.pt")
FACE_CONF_THRESHOLD = float(os.getenv("FACE_CONF_THRESHOLD", "0.3"))
FACE_MIN_PX = 30
ASPECT_RATIO_MIN = 0.4
ASPECT_RATIO_MAX = 2.5

# ── CCIP ─────────────────────────────────────────────────────────────────────
CCIP_MODEL = os.getenv("CCIP_MODEL", "ccip-caformer-24-randaug-pruned")


def apply_thread_limits() -> None:
    """torch 스레드 수 제한(있을 때만). paddle/OMP는 env(OMP_NUM_THREADS)로 제어."""
    if MODEL_API_THREADS <= 0:
        return
    try:
        import torch
        torch.set_num_threads(MODEL_API_THREADS)
    except Exception:
        pass
