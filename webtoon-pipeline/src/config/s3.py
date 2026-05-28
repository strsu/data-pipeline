"""S3 이미지 직접 다운로드 (boto3)."""
from __future__ import annotations

import time

import boto3
from botocore.exceptions import ClientError

from src.config import settings

_client = None


def _get_client():
    global _client
    if _client is None:
        kwargs: dict = dict(
            aws_access_key_id=settings.S3_ACCESS_KEY,
            aws_secret_access_key=settings.S3_SECRET_KEY,
            region_name=settings.S3_REGION_NAME,
        )
        if settings.S3_ENDPOINT_URL:
            kwargs["endpoint_url"] = settings.S3_ENDPOINT_URL
        _client = boto3.client("s3", **kwargs)
    return _client


def upload_face_crop(face_record_id: int, source: str, title_id: str, crop_bytes: bytes) -> str:
    """크롭 이미지를 S3에 업로드하고 s3_key를 반환."""
    media_dir = settings.SOURCE_MEDIA_PATH[source]
    key = f"{settings.S3_LOCATION}/{media_dir}/{title_id}/face_crop/{face_record_id}.jpg"
    from io import BytesIO
    _get_client().put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=key,
        Body=BytesIO(crop_bytes),
        ContentType="image/jpeg",
    )
    return key


def fetch_face_crop(face_record_id: int, source: str, title_id: str) -> bytes | None:
    """S3에서 얼굴 크롭 이미지 다운로드. 없으면 None 반환."""
    media_dir = settings.SOURCE_MEDIA_PATH[source]
    key = f"{settings.S3_LOCATION}/{media_dir}/{title_id}/face_crop/{face_record_id}.jpg"
    try:
        response = _get_client().get_object(Bucket=settings.S3_BUCKET_NAME, Key=key)
        return response["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def fetch_cut_image(source: str, title_id: str, episode_no: int, cut: int) -> bytes | None:
    """S3에서 컷 이미지 다운로드.

    Returns None on 404 (episode boundary). Raises on persistent transient errors
    after exhausting RETRY_BACKOFF retries (§12.16).
    """
    media_dir = settings.SOURCE_MEDIA_PATH[source]
    key = f"{settings.S3_LOCATION}/{media_dir}/{title_id}/{episode_no}/{title_id}_{episode_no}_{cut}.jpg"
    delays = [0] + settings.RETRY_BACKOFF
    for attempt, delay in enumerate(delays):
        if delay:
            time.sleep(delay)
        try:
            response = _get_client().get_object(Bucket=settings.S3_BUCKET_NAME, Key=key)
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            if attempt < len(settings.RETRY_BACKOFF):
                continue
            raise
