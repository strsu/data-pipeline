from fastapi import APIRouter, UploadFile
from src.models.ocr import run_ocr
from src.models.yolo import detect_faces

router = APIRouter()


@router.post("/ocr-yolo")
async def ocr_yolo(file: UploadFile):
    image_bytes = await file.read()
    ocr_blocks = run_ocr(image_bytes)
    faces = detect_faces(image_bytes)
    return {"ocr": ocr_blocks, "faces": faces}
