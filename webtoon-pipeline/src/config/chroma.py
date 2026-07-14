"""Chroma HTTP 클라이언트 — 웹툰별 독립 컬렉션 (§5.1, §5.6).

원격 Chroma(인터넷 경유)라 커넥션 스톨이 실제로 발생한다 — 2026-07-14 step2가
upsert에서 무한 행에 걸려 워커 슬롯(max_concurrent_activities=1)을 좀비 점유,
Temporal 재시도조차 배차되지 못했다. chromadb 클라이언트는 내부 httpx 세션을
timeout=None으로 하드코딩하므로 두 겹으로 방어한다:
  1) FastAPI.__init__ 패치로 세션 생성 직후 타임아웃/5xx 훅 설정(스톨 → 유한 시간 내
     예외; 생성자 내부 tenant 검증 콜까지 보호) + 사후 주입을 2차 방어선으로 유지
  2) 호출부는 `chroma_retry`로 일시 장애(타임아웃/커넥션/5xx)만 재시도
     (ocr_yolo_client와 동일 패턴 — 4xx는 요청 문제라 즉시 전파)

주의: chromadb는 non-2xx 응답을 자체 예외로 변환하는데, 바디가 정형 JSON이 아니면
일반 Exception이 돼 재시도 판별이 불가능하다(비정형 5xx — 프록시/게이트웨이의 HTML
바디 502/503/524 등). 그래서 응답 이벤트 훅에서 5xx를 chromadb보다 먼저
httpx.HTTPStatusError로 던져 재시도 대상으로 살린다.
"""
from __future__ import annotations

import logging
import time

import chromadb
import httpx
from chromadb.config import Settings as ChromaSettings
from chromadb.errors import ChromaError

from src.config import settings

logger = logging.getLogger(__name__)

# httpx read timeout은 "바이트 간 무응답 한도"라, 큰 컬렉션 get(anchor 전체 적재)도
# 데이터가 흐르는 한 안 걸린다 — 진짜 스톨(무응답)만 잡는다.
_HTTP_TIMEOUT = httpx.Timeout(30.0, connect=5.0)

# ocr_yolo_client와 동일한 재시도 정책. 최악(전 시도 read timeout)에는 ~7분을 점유하므로
# 액티비티 경로에서는 heartbeat 콜백을 넘겨 대기 중에도 heartbeat_timeout(2분)을 갱신한다.
# 유한 시간 안에 끝나 슬롯을 반납하므로(기존 timeout=None 무한 행과 달리) 최악에도
# Temporal 재시도 attempt가 정상적으로 이어받는다.
_MAX_ATTEMPTS = 10
_RETRY_BASE_DELAY = 1.0   # 초
_RETRY_MAX_DELAY = 8.0    # 초

_client: chromadb.HttpClient | None = None


def _raise_on_server_error(response: httpx.Response) -> None:
    """응답 훅: 5xx를 chromadb의 예외 변환보다 먼저 HTTPStatusError로 던진다(모듈 docstring)."""
    if response.status_code >= 500:
        response.raise_for_status()


def _configure_session(session) -> bool:
    """httpx 세션에 타임아웃 + 5xx 훅 설정(멱등). 세션이 아니면 False."""
    if not isinstance(session, httpx.Client):
        return False
    session.timeout = _HTTP_TIMEOUT
    hooks = list(session.event_hooks.get("response") or [])
    if _raise_on_server_error not in hooks:
        session.event_hooks["response"] = [*hooks, _raise_on_server_error]
    return True


def _patch_fastapi_session_init() -> None:
    """chromadb FastAPI.__init__가 세션(timeout=None 하드코딩)을 만들자마자 설정하도록 패치.

    HttpClient()가 chroma_api_impl을 강제 덮어써 서브클래스 주입이 불가능해 __init__을
    감싼다. 사후 주입과 달리 생성자 내부 tenant/database 검증 콜(프로세스당 1회)까지
    타임아웃이 걸린다 — 사고 모드가 '장수 프로세스의 커넥션 스톨'이라 첫 콜도 예외가 아니다.
    비공개 구조 의존이라 실패 시 경고만 남긴다(사후 주입 + chroma_retry가 방어선).
    """
    try:
        from chromadb.api.fastapi import FastAPI

        if getattr(FastAPI, "_session_config_patched", False):
            return
        orig_init = FastAPI.__init__

        def _init(self, system):
            orig_init(self, system)
            if not _configure_session(getattr(self, "_session", None)):
                logger.warning("[chroma] FastAPI 세션 설정 실패 — chromadb 내부 구조 변경?")

        FastAPI.__init__ = _init
        FastAPI._session_config_patched = True
    except Exception:  # noqa: BLE001 — 패치 실패는 방어선 축소일 뿐, 기동을 막지 않는다
        logger.warning("[chroma] FastAPI.__init__ 패치 실패 — 사후 주입으로만 방어", exc_info=True)


def _is_retryable(e: Exception) -> bool:
    if isinstance(e, httpx.TransportError):  # 타임아웃/커넥션 — 응답 자체를 못 받은 경우
        return True
    if isinstance(e, httpx.HTTPStatusError):  # 5xx — _raise_on_server_error 훅이 승격시킴
        return e.response.status_code >= 500
    if isinstance(e, ChromaError):  # chromadb가 정형 JSON 바디를 해석해 던지는 서버측 에러
        return e.code() >= 500
    return False


def chroma_retry(op: str, fn, *args, heartbeat=None, **kwargs):
    """Chroma 호출 재시도 래퍼 — `chroma_retry("upsert", collection.upsert, ids=..., ...)`.

    heartbeat: 시도 사이(백오프 전후)에 부르는 콜백. Temporal 액티비티 경로에서는 재시도
    누적(최악 ~7분)이 heartbeat_timeout(2분)을 넘겨 attempt가 잘리므로 반드시 넘길 것
    (액티비티의 resume details를 보존하려면 호출측이 자기 값으로 재전송하는 콜백을 준다).
    콜백이 예외(취소)를 던지면 그대로 전파한다.
    """
    last_exc: Exception = RuntimeError("unreachable")
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if not _is_retryable(e):
                raise
            last_exc = e
            if attempt == _MAX_ATTEMPTS:
                logger.error(
                    "[chroma] %s 최종 실패(%d/%d회 모두 실패) — %s", op, attempt, _MAX_ATTEMPTS, e,
                )
                break
            delay = min(_RETRY_BASE_DELAY * (2 ** (attempt - 1)), _RETRY_MAX_DELAY)
            logger.warning(
                "[chroma] %s 실패(attempt %d/%d) — %s — %.1fs 후 재시도",
                op, attempt, _MAX_ATTEMPTS, e, delay,
            )
            if heartbeat is not None:
                heartbeat()
            time.sleep(delay)
            if heartbeat is not None:
                heartbeat()
    raise last_exc


def get_chroma_client() -> chromadb.HttpClient:
    global _client
    if _client is None:
        _patch_fastapi_session_init()  # 생성자 내부 콜부터 타임아웃 적용
        chroma_settings = ChromaSettings(
            chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
            chroma_client_auth_credentials=settings.CHROMA_AUTH_TOKEN,
        )
        _client = chromadb.HttpClient(
            host=settings.CHROMA_HOST,
            port=settings.CHROMA_PORT,
            settings=chroma_settings,
        )
        # 2차 방어선 — __init__ 패치가 구조 변경으로 무력화됐어도 여기서 설정된다.
        if not _configure_session(getattr(getattr(_client, "_server", None), "_session", None)):
            logger.warning(
                "[chroma] httpx 세션 타임아웃 주입 실패 — chromadb 내부 구조 변경? "
                "(timeout=None 유지, chroma_retry만으로 방어)"
            )
    return _client


def get_face_collection(source: str, title_id: str, model: str = "clip", heartbeat=None) -> chromadb.Collection:
    """웹툰·모델별 얼굴 임베딩 컬렉션 반환 (없으면 생성)."""
    return chroma_retry(
        "get_or_create_collection",
        get_chroma_client().get_or_create_collection,
        heartbeat=heartbeat,
        name=f"character_faces_{source}_{title_id}_{model}",
        metadata={"hnsw:space": "cosine"},
    )
