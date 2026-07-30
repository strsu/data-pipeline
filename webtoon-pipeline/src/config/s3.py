"""S3 이미지 직접 다운로드 (boto3).

오브젝트 스토리지 호출은 **전부 `s3_retry`를 거친다**(R2 쪽 `r2.py`도 이 헬퍼를 쓴다).
예전엔 `fetch_cut_image`만 재시도 루프를 갖고 나머지는 한 번 실패하면 그대로 터졌다.

⚠️ **`head_object`를 쓰지 않는다.** 이 버킷들은 Cloudflare(터널/CDN)를 거치는데
   (`s3.prup.xyz`), 그 구간에서 **서명(SigV4)된 HEAD는 비결정적으로 403**이 된다 —
   origin이 보는 메서드가 서명된 메서드와 달라져 SignatureDoesNotMatch가 나고, HEAD 응답엔
   본문이 없어 botocore가 "(403) Forbidden"으로만 보여준다. 크기·존재 확인이 필요하면
   `get_object` / `list_objects_v2`로 한다. 이 파이프라인이 저수준 get/put/delete만 써서
   같은 인프라에서 그 사고를 안 겪었다(service 레포 `apps/common/s3_utils.py` 상단 참고).
"""
from __future__ import annotations

import logging
import time

import boto3
from botocore.exceptions import ClientError

from src.config import settings

logger = logging.getLogger(__name__)

_client = None

# 없는 키/버킷 — 다시 물어봐도 답이 같으므로 재시도하지 않고 즉시 올린다.
# 호출부가 `is_missing`으로 None 처리하거나(회차 경계) 그대로 전파한다.
PERMANENT_CODES = ("NoSuchKey", "NoSuchBucket", "404")


def is_missing(exc: Exception) -> bool:
    """"오브젝트 없음"인가. 회차 경계(cut/episode 끝)를 오류와 구분하는 데 쓴다."""
    return ((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") in ("NoSuchKey", "404")


def s3_retry(op: str, key: str, fn):
    """스토리지 호출을 `RETRY_BACKOFF`(2·5·15초) 백오프로 재시도한다. 총 4회 시도.

    - `PERMANENT_CODES`(없는 키/버킷)는 **재시도하지 않고** 즉시 전파한다.
    - 그 밖의 모든 예외(503, 403, 연결 끊김, 타임아웃…)는 일시 오류로 보고 재시도한다.
      **403도 재시도 대상이다** — Cloudflare 경유 403이 간헐이라 여기서 포기하면
      회복 가능한 실패를 버리게 된다(모듈 상단 참고).
    """
    delays = [0] + list(settings.RETRY_BACKOFF)
    last_error: Exception | None = None
    for attempt, delay in enumerate(delays, 1):
        if delay:
            time.sleep(delay)
        try:
            return fn()
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in PERMANENT_CODES:
                raise
            last_error = e
        except Exception as e:
            last_error = e
        logger.warning("S3 %s 실패 (%s) %d/%d회: %s", op, key, attempt, len(delays), last_error)
    raise last_error


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


def _face_crop_key(source: str, title_id: str, face_record_id: int) -> str:
    media_dir = settings.SOURCE_MEDIA_PATH[source]
    return f"{settings.S3_LOCATION}/{media_dir}/{title_id}/face_crop/{face_record_id}.jpg"


# ⚠️ 아래 얼굴 crop 3개 함수는 **현재 호출부가 없다** — step1/step2는 R2 쪽(`r2.py`)의
#    같은 이름 함수를 쓴다. 지우지 않고 두는 대신 재시도 정책은 R2 쪽과 같이 간다
#    (남겨두고 정책만 다르면, 나중에 되살릴 때 옛 동작을 그대로 물려받는다).
def upload_face_crop(face_record_id: int, source: str, title_id: str, crop_bytes: bytes) -> str:
    """크롭 이미지를 S3에 업로드하고 s3_key를 반환."""
    key = _face_crop_key(source, title_id, face_record_id)
    from io import BytesIO

    s3_retry(
        "save",
        key,
        lambda: _get_client().put_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=key,
            # ⚠️ 재시도마다 새 BytesIO를 만든다 — 같은 스트림을 넘기면 두 번째 시도에서
            #    커서가 끝에 있어 **0바이트 오브젝트**가 올라간다.
            Body=BytesIO(crop_bytes),
            ContentType="image/jpeg",
        ),
    )
    return key


def delete_face_crop(face_record_id: int, source: str, title_id: str) -> None:
    """S3에서 얼굴 크롭 이미지 삭제. 없어도 무시한다.

    예전엔 `except ClientError: pass`라 **일시 오류까지 조용히 삼켰다** — 지워야 할 오브젝트가
    남았는데 성공처럼 보였다. 이제 "없음"만 무시하고, 일시 오류는 재시도 후 전파한다.
    """
    key = _face_crop_key(source, title_id, face_record_id)
    try:
        s3_retry("delete", key, lambda: _get_client().delete_object(Bucket=settings.S3_BUCKET_NAME, Key=key))
    except ClientError as e:
        if not is_missing(e):
            raise


def fetch_face_crop(face_record_id: int, source: str, title_id: str) -> bytes | None:
    """S3에서 얼굴 크롭 이미지 다운로드. 없으면 None 반환."""
    key = _face_crop_key(source, title_id, face_record_id)
    try:
        return s3_retry(
            "read", key, lambda: _get_client().get_object(Bucket=settings.S3_BUCKET_NAME, Key=key)["Body"].read()
        )
    except ClientError as e:
        if is_missing(e):
            return None
        raise


def fetch_cut_image(source: str, title_id: str, episode_no: int, cut: int) -> bytes | None:
    """S3에서 컷 이미지 다운로드.

    Returns None on 404 (episode boundary). Raises on persistent transient errors
    after exhausting RETRY_BACKOFF retries (§12.16).
    """
    media_dir = settings.SOURCE_MEDIA_PATH[source]
    key = f"{settings.S3_LOCATION}/{media_dir}/{title_id}/{episode_no}/{title_id}_{episode_no}_{cut}.jpg"
    try:
        return s3_retry(
            "read", key, lambda: _get_client().get_object(Bucket=settings.S3_BUCKET_NAME, Key=key)["Body"].read()
        )
    except ClientError as e:
        if is_missing(e):
            return None  # 회차 경계 — 오류가 아니다
        raise
