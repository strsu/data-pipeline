from fastapi import APIRouter, UploadFile
from src.models.embedding import extract_embedding

router = APIRouter()


@router.post("/embed")
async def embed(file: UploadFile):
    image_bytes = await file.read()
    embedding = extract_embedding(image_bytes)
    return {"embedding": embedding}
