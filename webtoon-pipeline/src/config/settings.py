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

# YOLO
YOLO_MODEL_PATH = os.getenv("YOLO_MODEL_PATH", "/project/models/anime_face_detection.pt")
FACE_CONF_THRESHOLD = float(os.getenv("FACE_CONF_THRESHOLD", "0.3"))
FACE_MIN_PX = 30
ASPECT_RATIO_MIN = 0.4
ASPECT_RATIO_MAX = 2.5

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
