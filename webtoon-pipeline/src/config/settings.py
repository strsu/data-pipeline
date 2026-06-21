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
# 모델별 서비스 URL (미설정 시 폴백 체인).
OCR_YOLO_API_URL = os.getenv("OCR_YOLO_API_URL", MODEL_API_URL)  # 결합(레거시/하위호환)
# OCR/YOLO 분리: 각자 별도 서비스. 미설정 시 결합 URL로 폴백.
OCR_API_URL = os.getenv("OCR_API_URL", OCR_YOLO_API_URL)
YOLO_API_URL = os.getenv("YOLO_API_URL", OCR_YOLO_API_URL)
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
