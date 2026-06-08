from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from src.models.ocr import get_ocr
from src.models.yolo import get_model as get_yolo
from src.models.embedding import get_model as get_clip
from src.routers import ocr_yolo, embed

_ready = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ready
    get_ocr()
    get_yolo()
    get_clip()
    _ready = True
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(ocr_yolo.router)
app.include_router(embed.router)


@app.get("/health")
def health():
    if not _ready:
        return JSONResponse(status_code=503, content={"status": "loading"})
    return {"status": "ok"}
