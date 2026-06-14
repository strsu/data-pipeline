import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src import config

_ready = False

_LOG_FMT = "%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(format=_LOG_FMT, datefmt=_LOG_DATEFMT, level=logging.INFO, force=True)
log = logging.getLogger(__name__)


# uvicorn.access 핸들러에도 동일한 timestamp 포맷 적용
def _patch_access_log() -> None:
    try:
        from uvicorn.logging import AccessFormatter
        logger = logging.getLogger("uvicorn.access")
        fmt = AccessFormatter(
            '%(asctime)s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            datefmt=_LOG_DATEFMT,
        )
        for handler in logger.handlers:
            handler.setFormatter(fmt)
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ready
    _patch_access_log()
    config.apply_thread_limits()
    log.info("[model-api] MODE=%s threads=%s", config.MODEL_API_MODE, config.MODEL_API_THREADS or "default")

    if config.serves_ocr_yolo():
        from src.models.ocr import get_ocr
        from src.models.yolo import get_model as get_yolo
        get_ocr()
        get_yolo()
    if config.serves_clip():
        from src.models.embedding import get_model as get_clip
        get_clip()
    if config.serves_ccip():
        from src.models.ccip import _ensure_loaded
        _ensure_loaded()

    _ready = True
    yield


app = FastAPI(lifespan=lifespan)

if config.serves_ocr_yolo():
    from src.routers import ocr_yolo
    app.include_router(ocr_yolo.router)
if config.serves_clip():
    from src.routers import embed
    app.include_router(embed.router)
if config.serves_ccip():
    from src.routers import embed_ccip
    app.include_router(embed_ccip.router)


@app.get("/health")
def health():
    if not _ready:
        return JSONResponse(status_code=503, content={"status": "loading"})
    return {"status": "ok", "mode": config.MODEL_API_MODE}
