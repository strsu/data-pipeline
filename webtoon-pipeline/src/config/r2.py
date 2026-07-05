"""얼굴 crop 전용 Cloudflare R2 (S3 호환 API, boto3)."""
from __future__ import annotations

import boto3
from botocore.exceptions import ClientError

from src.config import settings

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = boto3.client(
            "s3",
            aws_access_key_id=settings.R2_ACCESS_KEY,
            aws_secret_access_key=settings.R2_SECRET_KEY,
            region_name="auto",
            endpoint_url=settings.R2_ENDPOINT_URL,
        )
    return _client


def _face_crop_key(source: str, title_id: str, face_record_id: int) -> str:
    """R2 "face" 버킷은 전용 버킷이라 S3_LOCATION("media") prefix를 붙이지 않는다."""
    media_dir = settings.SOURCE_MEDIA_PATH[source]
    return f"{media_dir}/{title_id}/face_crop/{face_record_id}.jpg"


def upload_face_crop(face_record_id: int, source: str, title_id: str, crop_bytes: bytes) -> str:
    """크롭 이미지를 R2에 업로드하고 key를 반환."""
    key = _face_crop_key(source, title_id, face_record_id)
    from io import BytesIO
    _get_client().put_object(
        Bucket=settings.R2_BUCKET_NAME,
        Key=key,
        Body=BytesIO(crop_bytes),
        ContentType="image/jpeg",
    )
    return key


def delete_face_crop(face_record_id: int, source: str, title_id: str) -> None:
    """R2에서 얼굴 크롭 이미지 삭제. 없어도 무시한다."""
    key = _face_crop_key(source, title_id, face_record_id)
    try:
        _get_client().delete_object(Bucket=settings.R2_BUCKET_NAME, Key=key)
    except ClientError:
        pass


def fetch_face_crop(face_record_id: int, source: str, title_id: str) -> bytes | None:
    """R2에서 얼굴 크롭 이미지 다운로드. 없으면 None 반환."""
    key = _face_crop_key(source, title_id, face_record_id)
    try:
        response = _get_client().get_object(Bucket=settings.R2_BUCKET_NAME, Key=key)
        return response["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
