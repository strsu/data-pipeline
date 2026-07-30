"""얼굴 crop 전용 Cloudflare R2 (S3 호환 API, boto3).

재시도 정책은 `s3.py`의 `s3_retry` 하나를 공유한다(버킷이 달라도 정책이 갈라질 이유가 없다).
`head_object`를 쓰지 않는 이유도 거기 적혀 있다.
"""
from __future__ import annotations

import boto3
from botocore.exceptions import ClientError

from src.config import settings
from src.config.s3 import is_missing, s3_retry

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

    s3_retry(
        "save",
        key,
        lambda: _get_client().put_object(
            Bucket=settings.R2_BUCKET_NAME,
            Key=key,
            # ⚠️ 재시도마다 새 BytesIO — 같은 스트림을 재사용하면 두 번째 시도가 0바이트를 올린다.
            Body=BytesIO(crop_bytes),
            ContentType="image/jpeg",
        ),
    )
    return key


def delete_face_crop(face_record_id: int, source: str, title_id: str) -> None:
    """R2에서 얼굴 크롭 이미지 삭제. 없어도 무시한다.

    "없음"만 무시한다 — 예전의 `except ClientError: pass`는 일시 오류까지 삼켜서, 지워야 할
    crop이 남아도 성공처럼 보였다(step1이 재검출 때 옛 crop을 지우는 경로다).
    """
    key = _face_crop_key(source, title_id, face_record_id)
    try:
        s3_retry("delete", key, lambda: _get_client().delete_object(Bucket=settings.R2_BUCKET_NAME, Key=key))
    except ClientError as e:
        if not is_missing(e):
            raise


def fetch_face_crop(face_record_id: int, source: str, title_id: str) -> bytes | None:
    """R2에서 얼굴 크롭 이미지 다운로드. 없으면 None 반환."""
    key = _face_crop_key(source, title_id, face_record_id)
    try:
        return s3_retry(
            "read", key, lambda: _get_client().get_object(Bucket=settings.R2_BUCKET_NAME, Key=key)["Body"].read()
        )
    except ClientError as e:
        if is_missing(e):
            return None
        raise
