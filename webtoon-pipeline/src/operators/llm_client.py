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
from typing import Optional

import httpx

_logger = logging.getLogger(__name__)

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
) -> dict:
    """멀티모달 LLM 호출 → JSON dict 반환.

    ctx: resolve_llm_model() 결과. images: 순서대로 전달할 이미지 바이트 목록.
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
    # 추론형 모델(glm-4.6v 등)은 사고과정이 reasoning_content delta로, 최종 답은 content delta로
    # 나뉘어 온다. content 와 reasoning 을 **분리 누적**해 content 우선 사용한다
    # (delta마다 폴백으로 합치면 reasoning이 섞여 JSON이 깨진다).
    body["stream"] = True
    body["stream_options"] = {"include_usage": True}
    content_chunks: list[str] = []
    reasoning_chunks: list[str] = []
    usage: Optional[dict] = None
    finish: Optional[str] = None
    # 동시호출 제한(glm-5v-turbo=1) 준수 — 전역 직렬화.
    with _LLM_SEMAPHORE:
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
    return _parse_json_content(text)


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


def _parse_json_content(text: str) -> dict:
    """모델 응답에서 JSON 추출(코드펜스/잡텍스트 방어)."""
    s = (text or "").strip()
    if s.startswith("```"):
        s = s.split("```", 2)[1] if "```" in s[3:] else s.strip("`")
        if s.startswith("json"):
            s = s[4:]
        s = s.strip().rstrip("`").strip()
    try:
        return json.loads(s)
    except Exception:
        start, end = s.find("{"), s.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(s[start:end + 1])
        raise
