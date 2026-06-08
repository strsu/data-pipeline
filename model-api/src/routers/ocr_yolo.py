import logging
from io import BytesIO

from fastapi import APIRouter, UploadFile
from PIL import Image

from src.models.ocr import run_ocr
from src.models.yolo import detect_faces

log = logging.getLogger(__name__)
router = APIRouter()


@router.post("/ocr-yolo")
async def ocr_yolo(
    file: UploadFile,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
):
    image_bytes = await file.read()
    w, h = Image.open(BytesIO(image_bytes)).size
    log.info("[ocr-yolo] %s/%s ep=%d cut=%d — 수신 %dx%d (%s bytes)",
             source, title_id, episode_no, cut, w, h, f"{len(image_bytes):,}")
    ocr_blocks = run_ocr(image_bytes)
    faces = detect_faces(image_bytes)
    return {"ocr": ocr_blocks, "faces": faces}
