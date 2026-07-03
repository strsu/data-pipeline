from fastapi import APIRouter, UploadFile
from starlette.concurrency import run_in_threadpool

from src.models.embedding import extract_embedding

router = APIRouter()


@router.post("/embed")
async def embed(file: UploadFile):
    image_bytes = await file.read()
    embedding = await run_in_threadpool(extract_embedding, image_bytes)
    return {"embedding": embedding}
