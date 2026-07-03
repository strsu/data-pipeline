import logging
from io import BytesIO

from fastapi import APIRouter, UploadFile
from PIL import Image
from starlette.concurrency import run_in_threadpool

from src.models.ocr import run_ocr

log = logging.getLogger(__name__)
router = APIRouter()


@router.post("/ocr")
async def ocr(
    file: UploadFile,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
):
    image_bytes = await file.read()
    w, h = Image.open(BytesIO(image_bytes)).size
    log.info("[ocr] %s/%s ep=%d cut=%d — 수신 %dx%d (%s bytes)",
             source, title_id, episode_no, cut, w, h, f"{len(image_bytes):,}")
    return {"ocr": await run_in_threadpool(run_ocr, image_bytes)}
