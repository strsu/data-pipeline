import logging
from io import BytesIO

from fastapi import APIRouter, UploadFile
from PIL import Image

from src.models.yolo import detect_faces

log = logging.getLogger(__name__)
router = APIRouter()


@router.post("/yolo")
async def yolo(
    file: UploadFile,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
):
    image_bytes = await file.read()
    w, h = Image.open(BytesIO(image_bytes)).size
    log.info("[yolo] %s/%s ep=%d cut=%d — 수신 %dx%d (%s bytes)",
             source, title_id, episode_no, cut, w, h, f"{len(image_bytes):,}")
    return {"faces": detect_faces(image_bytes)}
