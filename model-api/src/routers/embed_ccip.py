from fastapi import APIRouter, UploadFile
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from src.models.ccip import compare_features, extract_ccip_feature

router = APIRouter()


class CcipCompareRequest(BaseModel):
    query: list[float]
    anchors: list[list[float]]


@router.post("/embed-ccip")
async def embed_ccip(file: UploadFile):
    image_bytes = await file.read()
    feature = await run_in_threadpool(extract_ccip_feature, image_bytes)
    return {"feature": feature}


@router.post("/ccip-compare")
async def ccip_compare(req: CcipCompareRequest):
    """query feature와 anchor feature들의 CCIP metric 차이. 판정 임계값은 호출측."""
    return await run_in_threadpool(compare_features, req.query, req.anchors)
