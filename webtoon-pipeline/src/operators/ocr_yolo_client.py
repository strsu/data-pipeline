"""model-api HTTP 클라이언트 — OCR/YOLO (GPU 서버 단일 호출)."""
from __future__ import annotations

import logging
import time
from io import BytesIO

import httpx
from PIL import Image

from src.config.settings import (
    OCR_API_URL,
    OCR_YOLO_API_URL,
    YOLO_API_URL,
)

logger = logging.getLogger(__name__)

# 호출 타임아웃(초). 대부분 초 단위 응답이지만 콜드스타트/큰 세그먼트 대비 60초.
_HTTP_TIMEOUT = 60.0

# 일시 장애(모델 서버 재시작, 홈랩↔Cloudflare 터널 순단 등) 대비 재시도.
# 5xx(model-api 자체 재시작 중 502뿐 아니라 Cloudflare 엣지가 origin에 못 붙었을 때의
# 520~526도 전부 >=500 범위라 함께 걸린다) + 커넥션/타임아웃(httpx.TransportError, 애초에
# 응답을 못 받는 경우)만 재시도한다. 4xx는 우리 쪽 요청 문제라 재시도해도 소용없으므로 즉시 전파.
_MAX_ATTEMPTS = 10
_RETRY_BASE_DELAY = 1.0   # 초
_RETRY_MAX_DELAY = 8.0    # 초 — 최악의 경우(10회) 총 대기 ~55s. 세그먼트당 heartbeat_timeout(5분) 대비 여유.

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(_HTTP_TIMEOUT))
    return _client


def _post_image(base_url: str, path: str, image_bytes: bytes, params: dict) -> dict:
    url = f"{base_url.rstrip('/')}{path}"
    files = {"file": ("image.jpg", image_bytes, "image/jpeg")}
    # params에 source/title_id/episode_no/cut이 들어 있어 로그만 보고도 어느 컷 호출인지 특정 가능.
    ctx = " ".join(f"{k}={v}" for k, v in params.items())

    last_exc: Exception = RuntimeError("unreachable")
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = _get_client().post(url, files=files, params=params)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            if isinstance(e, httpx.HTTPStatusError) and e.response.status_code < 500:
                logger.error(
                    "[ocr_yolo] %s 4xx 실패(재시도 안 함) %s — %s", path, ctx, e,
                )
                raise  # 4xx는 재시도 대상 아님(요청 자체 문제) — 즉시 전파
            last_exc = e
            if attempt == _MAX_ATTEMPTS:
                logger.error(
                    "[ocr_yolo] %s 최종 실패(%d/%d회 모두 실패) %s — %s",
                    path, attempt, _MAX_ATTEMPTS, ctx, e,
                )
                break
            delay = min(_RETRY_BASE_DELAY * (2 ** (attempt - 1)), _RETRY_MAX_DELAY)
            logger.warning(
                "[ocr_yolo] %s 실패(attempt %d/%d) %s — %s — %.1fs 후 재시도",
                path, attempt, _MAX_ATTEMPTS, ctx, e, delay,
            )
            time.sleep(delay)
    raise last_exc


def run_ocr(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> list[dict]:
    """OCR — GPU 서버(OCR_API_URL) 호출. ocr_blocks 반환."""
    params = {"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut}
    t0 = time.perf_counter()
    result = _post_image(OCR_API_URL, "/ocr", image_bytes, params)["ocr"]
    logger.info("[ocr_yolo] /ocr 성공 — %.2fs, %d건", time.perf_counter() - t0, len(result))
    return result


def run_yolo(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> list[dict]:
    """YOLO — GPU 서버(YOLO_API_URL) 호출. faces 반환."""
    params = {"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut}
    t0 = time.perf_counter()
    result = _post_image(YOLO_API_URL, "/yolo", image_bytes, params)["faces"]
    logger.info("[ocr_yolo] /yolo 성공 — %.2fs, %d건", time.perf_counter() - t0, len(result))
    return result


def run_ocr_yolo(
    image_bytes: bytes,
    *,
    source: str = "",
    title_id: str = "",
    episode_no: int = 0,
    cut: int = 0,
) -> tuple[list[dict], list[dict]]:
    """model-api /ocr-yolo 결합 호출 → (ocr_blocks, faces). 레거시 호환용."""
    w, h = Image.open(BytesIO(image_bytes)).size
    print(f"[ocr_yolo_client] {source}/{title_id} ep={episode_no} cut={cut} — 전송 {w}x{h} ({len(image_bytes):,} bytes)")
    response = _get_client().post(
        f"{OCR_YOLO_API_URL}/ocr-yolo",
        files={"file": ("image.jpg", image_bytes, "image/jpeg")},
        params={"source": source, "title_id": title_id, "episode_no": episode_no, "cut": cut},
    )
    response.raise_for_status()
    data = response.json()
    return data["ocr"], data["faces"]
