import os

FAUST_APP_NAME = os.getenv("FAUST_APP_NAME", "webtoon-pipeline")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "").split(",")

# DB (service 레포와 동일한 변수명 사용)
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# S3
S3_ENDPOINT_URL = os.getenv("S3_HOST", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
S3_REGION_NAME = os.getenv("S3_REGION_NAME", "us-east-1")
S3_LOCATION = os.getenv("S3_LOCATION", "media")

# source → S3 media path 매핑 (service 레포 imageBaseForSource 패턴과 동일)
SOURCE_MEDIA_PATH: dict[str, str] = {
    "kakao": "kakao_webtoon",
    "naver": "webtoon",
}

# Model API
MODEL_API_URL = os.getenv("MODEL_API_URL", "http://localhost:8000")


def _normalize_host(value: str) -> str:
    """호스트만 들어와도(예: "gpgpu.prup.xyz") 스킴이 없으면 https 로 보정."""
    v = (value or "").strip().rstrip("/")
    if v and not v.startswith(("http://", "https://")):
        v = "https://" + v
    return v


# OCR/YOLO — GPU 서버 단일 타깃. GPU_SERVER(예: gpgpu.prup.xyz)를 사용한다.
# 클라이언트가 {base}/ocr · {base}/yolo 로 호출하므로 base 는 스킴+호스트까지만 둔다.
_OCR_YOLO_API = _normalize_host(os.getenv("GPU_SERVER", "")) or os.getenv("OCR_YOLO_API_URL", MODEL_API_URL)
OCR_API_URL = os.getenv("OCR_API_URL", _OCR_YOLO_API)
YOLO_API_URL = os.getenv("YOLO_API_URL", _OCR_YOLO_API)
OCR_YOLO_API_URL = os.getenv("OCR_YOLO_API_URL", _OCR_YOLO_API)  # 결합(레거시 /ocr-yolo) 호환
EMBED_CLIP_API_URL = os.getenv("EMBED_CLIP_API_URL", MODEL_API_URL)
EMBED_CCIP_API_URL = os.getenv("EMBED_CCIP_API_URL", MODEL_API_URL)

# HTTP
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 5, 15]

# Chroma
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))
CHROMA_AUTH_TOKEN = os.getenv("CHROMA_AUTH_TOKEN", "")

# Step 2 — face identification
MATCH_THRESHOLD = float(os.getenv("MATCH_THRESHOLD", "0.25"))  # P0 시작값 (cosine distance)
