"""LLM 호출 클라이언트 (Step3 멀티모달) — provider 추상화, OpenAI 호환.

- endpoint / model_id / temperature 등은 LLMModel(params)에서 읽는다(코드에 모델명 하드코딩 없음).
- API 키는 환경변수에서: params["api_key_env"] > "{PROVIDER}_API_KEY" > "LLM_API_KEY".
- 요청/응답은 OpenAI Chat Completions(vision) 형식. OpenAI 호환 provider(vllm 등)가 호환.
- 응답 content를 JSON으로 파싱해 반환(코드펜스 제거).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

_logger = logging.getLogger(__name__)


@dataclass
class LLMCallResult:
    """LLM 호출 결과 + 토큰 사용량.

    호출부는 `result`(파싱된 JSON dict)를 그대로 쓰고, `usage`를 `LLMUsage` 테이블에
    적재한다(Req 6.7). `usage`는 provider가 값을 생략하면 0/None으로 채운 안정 shape:
      {"prompt_tokens": int, "completion_tokens": int, "total_tokens": int,
       "finish_reason": str|None}
    """

    result: dict
    usage: dict = field(default_factory=dict)


def _build_usage(raw: Optional[dict], finish: Optional[str]) -> dict:
    """provider usage(OpenAI 호환) → 안정 shape로 정규화. 누락 값은 0/None 기본."""
    raw = raw or {}

    def _int(key: str) -> int:
        val = raw.get(key)
        try:
            return int(val) if val is not None else 0
        except (TypeError, ValueError):
            return 0

    return {
        "prompt_tokens": _int("prompt_tokens"),
        "completion_tokens": _int("completion_tokens"),
        "total_tokens": _int("total_tokens"),
        "finish_reason": finish,
    }

# glm-5v-turbo 등 동시호출 제한 모델 대응 — LLM HTTP 호출을 전역 직렬화.
# webtoon-pipeline replicas=1 전제(프로세스 세마포어). 한도가 늘면 LLM_MAX_CONCURRENCY로 상향.
# 멀티 레플리카로 가면 분산 제한기(또는 Step3 전용 단일 워커/task queue)가 필요.
_LLM_MAX_CONCURRENCY = int(os.getenv("LLM_MAX_CONCURRENCY", "1") or "1")
_LLM_SEMAPHORE = threading.Semaphore(_LLM_MAX_CONCURRENCY)

def _resolve_endpoint(ctx: dict) -> str:
    """호출 endpoint 결정.

    우선순위: params["endpoint"](DB 명시) > provider 기본값.
    provider=vllm 이면 VLLM_API_HOST 에 /v1/chat/completions 를 붙여 구성한다
    (LiteLLM 통합 게이트웨이, 모델 라우팅은 body 의 model_id 로).
    그 외 provider 는 DB params.endpoint 로 명시해야 한다.
    """
    params = ctx.get("params") or {}
    if params.get("endpoint"):
        return params["endpoint"]
    if ctx.get("provider") == "vllm":
        host = (os.getenv("VLLM_API_HOST") or "").strip().rstrip("/")
        if host:
            return f"{host}/v1/chat/completions"
    return ""

# vllm.prup.xyz도 Cloudflare 터널 경유라 502/522/530(터널 재연결/일시 다운) 등이 간헐적으로
# 발생한다(prd.md §16.1). ocr_yolo_client.py와 동일한 패턴(재시도 10회 + 지수 백오프,
# 4xx는 즉시 실패)을 적용 — 여기서 흡수해야 step3.py 쪽 컷 스킵(_PASS1_RETRIES)이 실제
# 일시 장애 구간(수십 초~분 단위)을 버틸 수 있다.
_LLM_MAX_ATTEMPTS = 10
_LLM_RETRY_BASE_DELAY = 1.0
_LLM_RETRY_MAX_DELAY = 8.0

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(180.0))
    return _client


def _resolve_api_key(ctx: dict) -> str:
    provider = (ctx.get("provider") or "").upper()
    params = ctx.get("params") or {}
    candidates = []
    if params.get("api_key_env"):
        candidates.append(params["api_key_env"])
    if provider:
        candidates.append(f"{provider}_API_KEY")
    candidates.append("LLM_API_KEY")
    for env_name in candidates:
        val = os.getenv(env_name)
        if val:
            return val
    raise RuntimeError(f"LLM API key 미설정 — 시도한 env: {candidates}")


def _data_url(image_bytes: bytes) -> str:
    b64 = base64.b64encode(image_bytes).decode("ascii")
    return f"data:image/jpeg;base64,{b64}"


def call_llm_json(
    ctx: dict,
    system_prompt: str,
    user_text: str,
    images: list[bytes],
) -> LLMCallResult:
    """멀티모달 LLM 호출 → `LLMCallResult`(파싱 JSON + 토큰 usage) 반환.

    ctx: resolve_llm_model() 결과. images: 순서대로 전달할 이미지 바이트 목록.
    반환 `.result`는 파싱된 JSON dict, `.usage`는 LLMUsage 적재용
    {prompt_tokens, completion_tokens, total_tokens, finish_reason} (Req 6.7).
    """
    params = ctx.get("params") or {}
    endpoint = _resolve_endpoint(ctx)
    if not endpoint:
        raise RuntimeError(f"LLM endpoint 미설정 (provider={ctx.get('provider')}). LLMModel.params.endpoint 또는 VLLM_API_HOST 지정 필요")

    content: list[dict] = [{"type": "text", "text": user_text}]
    for img in images:
        content.append({"type": "image_url", "image_url": {"url": _data_url(img)}})

    body = {
        "model": ctx["model_id"],
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": content},
        ],
        "temperature": params.get("temperature", 0.2),
        "response_format": {"type": "json_object"},
    }
    if params.get("max_tokens"):
        body["max_tokens"] = params["max_tokens"]

    headers = {"Authorization": f"Bearer {_resolve_api_key(ctx)}"}
    # 스트리밍으로 호출한다 — Cloudflare 터널 idle timeout 회피(토큰이 계속 흘러와 연결 유휴 없음).
    body["stream"] = True
    body["stream_options"] = {"include_usage": True}

    last_exc: Exception = RuntimeError("unreachable")
    for attempt in range(1, _LLM_MAX_ATTEMPTS + 1):
        try:
            # 동시호출 제한(glm-5v-turbo=1) 준수 — 전역 직렬화. 백오프 대기 중엔 슬롯을
            # 비워둬야 다른 컷의 시도가 막히지 않으므로 세마포어는 시도 단위로만 쥔다.
            with _LLM_SEMAPHORE:
                return _stream_llm_once(endpoint, body, headers)
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            if isinstance(e, httpx.HTTPStatusError) and e.response.status_code < 500:
                _logger.error("[llm] %s 4xx 실패(재시도 안 함) — %s", endpoint, e)
                raise
            last_exc = e
            if attempt == _LLM_MAX_ATTEMPTS:
                _logger.error(
                    "[llm] %s 최종 실패(%d/%d회 모두 실패) — %s",
                    endpoint, attempt, _LLM_MAX_ATTEMPTS, e,
                )
                break
            delay = min(_LLM_RETRY_BASE_DELAY * (2 ** (attempt - 1)), _LLM_RETRY_MAX_DELAY)
            _logger.warning(
                "[llm] %s 실패(attempt %d/%d) — %s — %.1fs 후 재시도",
                endpoint, attempt, _LLM_MAX_ATTEMPTS, e, delay,
            )
            time.sleep(delay)
    raise last_exc


def _stream_llm_once(endpoint: str, body: dict, headers: dict) -> LLMCallResult:
    """스트리밍 LLM 콜 1회 시도(재시도 없음 — `call_llm_json`이 감싼다).

    추론형 모델(glm-4.6v 등)은 사고과정이 reasoning_content delta로, 최종 답은 content delta로
    나뉘어 온다. content 와 reasoning 을 **분리 누적**해 content 우선 사용한다
    (delta마다 폴백으로 합치면 reasoning이 섞여 JSON이 깨진다).
    """
    content_chunks: list[str] = []
    reasoning_chunks: list[str] = []
    usage: Optional[dict] = None
    finish: Optional[str] = None
    with _get_client().stream("POST", endpoint, json=body, headers=headers) as resp:
        if resp.status_code >= 400:
            resp.read()
            resp.raise_for_status()
        for line in resp.iter_lines():
            if not line:
                continue
            if line.startswith("data:"):
                line = line[5:].strip()
            if line == "[DONE]":
                break
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("usage"):
                usage = obj["usage"]
            choices = obj.get("choices") or []
            if not choices:
                continue
            if choices[0].get("finish_reason"):
                finish = choices[0]["finish_reason"]
            delta = choices[0].get("delta") or {}
            if isinstance(delta.get("content"), str):
                content_chunks.append(delta["content"])
            for k in ("reasoning", "reasoning_content"):
                if isinstance(delta.get(k), str):
                    reasoning_chunks.append(delta[k])
    if usage:
        _logger.info(
            "[llm] usage: prompt=%s completion=%s total=%s finish=%s",
            usage.get("prompt_tokens"), usage.get("completion_tokens"),
            usage.get("total_tokens"), finish,
        )
    # content 우선, 비어 있으면 reasoning 폴백(intern-vl처럼 답이 reasoning에만 오는 모델 대응).
    text = "".join(content_chunks).strip() or "".join(reasoning_chunks).strip()
    try:
        parsed = _parse_json_content(text)
    except (ValueError, json.JSONDecodeError) as e:
        # finish_reason='length'면 max_tokens 절단이 원인 — 추론형 모델(glm-4.6v 등)은
        # 사고과정(reasoning_content)이 토큰 예산을 먼저 소모해 본문이 잘릴 수 있다.
        # 원인 판별이 바로 되도록 finish_reason/토큰 사용량/응답 길이를 에러 메시지에 싣는다.
        raise ValueError(
            f"LLM 응답 JSON 파싱 실패(finish_reason={finish!r}, "
            f"completion_tokens={(usage or {}).get('completion_tokens')!r}, "
            f"응답 길이={len(text)}자): {e}"
        ) from e
    return LLMCallResult(result=parsed, usage=_build_usage(usage, finish))


def extract_message_text(message: dict) -> str:
    """assistant 메시지에서 본문 텍스트를 모델 차이에 무관하게 뽑는다.

    모델별 응답 위치가 다르다:
      - 대부분(kanana/glm 등): 답이 `content`.
      - 일부(예: intern-vl): `content`가 null이고 답이 `reasoning`에 들어옴.
    규칙: **content 우선, 비어 있으면 reasoning → reasoning_content 폴백.**
    호출부가 항상 이 함수만 쓰면 모델 추가 시 파싱 실수가 없다.

    스트리밍(delta)에서도 동일 키 우선순위로 동작한다(delta dict 전달).
    """
    if not isinstance(message, dict):
        return ""
    for key in ("content", "reasoning", "reasoning_content"):
        val = message.get(key)
        if isinstance(val, str) and val.strip():
            return val
    return ""


# 강건 JSON 파싱용 디코더 — raw_decode로 첫 객체만 떼어내 뒤 잡텍스트("Extra data")를 무시한다.
_DECODER = json.JSONDecoder()


def _parse_json_content(text: str) -> dict:
    """모델 응답에서 첫 JSON 객체만 강건하게 추출(코드펜스/여분 텍스트 방어).

    raw_decode 기반: 코드펜스(```json ... ```)를 벗기고, 첫 '{' 위치부터 한 개의
    JSON 객체만 디코드한다. 객체 뒤에 잡텍스트가 따라와도(JSONDecoder.raw_decode가
    Extra data를 던지지 않으므로) 무시하고 첫 객체만 반환한다.
    """
    s = (text or "").strip()
    if s.startswith("```"):
        s = s.split("```", 2)[1] if "```" in s[3:] else s.strip("`")
        if s.startswith("json"):
            s = s[4:]
        s = s.strip().rstrip("`").strip()
    i = s.find("{")
    if i == -1:
        raise ValueError("no json object")
    obj, _ = _DECODER.raw_decode(s[i:])  # 첫 객체만, 뒤 잡텍스트 무시
    return obj
