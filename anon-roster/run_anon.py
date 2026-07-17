"""익명 로스터 하네스 — qwen-base 9B에게 Claude와 '같은 하네스'를 주고 격차를 실측한다.

prd-for-improve.md §4(레퍼런스 구현)를 기계로 옮긴 것. 핵심은 모델 크기가 아니라 **하네스**라는
가설의 검증: 큰 컨텍스트가 아니라 **작은 상태를 계속 이월**하는 구조로 충분한가?

Claude가 한 것과 같은 조건:
- 입력은 **오버레이 없는 원본 컷**(F0/F1 라벨 = 컷 스코프 정체성 오염이므로 금지)
- OCR 텍스트·얼굴 bbox·CCIP 귀속·prior 로스터 **전부 미주입**
- 컷을 **읽기 순서대로 1장씩**, 익명 로스터(A/B/C…)와 최근 대사만 이월
- 텍스트는 **모델이 이미지에서 직접 전사**(OCR 없음 → 원칙 1: 컷을 버리지 않음)
- 이름은 컷 단위로 묻지 않고 **마지막에 회차 전역 1콜**로 붙임(§C6)
- 명명은 **null 허용**(§C5) — 억지 명명 안 하는지가 핵심 지표

실행:
  cd webtoon-pipeline && set -a && source ../prod.env && set +a && \
  PYTHONPATH=. .venv/bin/python ../anon-roster/run_anon.py --webtoon 23 --episodes 10
집계:
  ... ../anon-roster/run_anon.py --webtoon 23 --episodes 10 --report
"""
from __future__ import annotations

import os

os.environ.setdefault("LLM_MAX_CONCURRENCY", "2")

import argparse
import hashlib
import io
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from pathlib import Path

_PIPELINE = Path(__file__).resolve().parents[1] / "webtoon-pipeline"
if str(_PIPELINE) not in sys.path:
    sys.path.insert(0, str(_PIPELINE))

from PIL import Image  # noqa: E402

from src.config.db import db_cursor  # noqa: E402
from src.config.s3 import fetch_cut_image  # noqa: E402
from src.core.step3 import _episode_info, _pass1_ctx  # noqa: E402
from src.operators.llm_client import (  # noqa: E402
    _data_url,
    _get_client,
    _parse_json_content_ex,
    _resolve_api_key,
    _resolve_endpoint,
    call_llm_json,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("anon")

SCHEMA_VERSION = 1
MAX_DIM = 1000          # Claude가 읽은 것과 동일 규격
CTX_TAIL_CUTS = 3       # 이월할 최근 대사 컷 수(= '작은 상태')

# 컷 단위 데드라인. 정상은 ~9s인데, 2026-07-17 ep11 c19에서 **23분 행**이 실측됐다:
# 응답 헤더(200)는 받고 스트림 본문을 읽다가 멈췄고 llm_client의 httpx Timeout(180) 이 안 걸렸다
# (스트리밍 read 타임아웃이 청크마다 리셋되는 것으로 보임). 프로덕션 llm_client는 건드리지 않고
# 여기서 감싼다 — 초과하면 그 컷만 에러로 기록하고 다음으로 넘어간다(러너 전체가 멈추는 것 방지).
CALL_DEADLINE_S = 150

_TYPES = ("speech", "monologue", "narration", "system", "other")

# ── 프롬프트 ─────────────────────────────────────────────────────────────────

CUT_SYSTEM = (
    "당신은 웹툰 컷 분석기입니다. 입력: 컷 이미지 1장 + 지금까지의 인물 로스터 + 직전 대사 흐름. "
    "이미지를 직접 보고 **JSON만** 출력하세요.\n"
    "⚠️ 모든 자연어 출력은 **한국어**로 (transcript만 이미지 원문 그대로).\n"
    "⚠️ 서술형 텍스트 안에서 큰따옴표(\") 금지 — JSON이 깨집니다. 작은따옴표(')나 「」를 쓰세요.\n"
    "\n[1단계: 텍스트 전사]\n"
    "이미지에 보이는 **모든 글자**를 직접 읽어 blocks에 담으세요. OCR 결과는 주어지지 않습니다. "
    "말풍선·나레이션 박스·상태창·효과음·작은 방백까지 전부. 읽기 순서(위→아래)대로.\n"
    "**글자가 하나도 없으면 blocks는 빈 배열** — 지어내지 마세요.\n"
    "\n[2단계: 분류] type은 말풍선 '모양'으로 판단합니다:\n"
    "- 검은 사각 박스(흰 글씨) → narration (주인공 독백/해설)\n"
    "- 검은 가시/삐죽 말풍선 → monologue (속마음)\n"
    "- 흰 둥근 말풍선 → speech\n"
    "- 점선 테두리 말풍선 → speech (속삭임)\n"
    "- 붉은/검붉은 말풍선 → speech (긴장·낮은 목소리)\n"
    "- 각진 폭발형 말풍선 → speech (외침)\n"
    "- 금색/장식 테두리 사각 → system (상태창·설정 타이틀)\n"
    "- 말풍선 없는 스타일 글자 → other (효과음). 단 인물 이름/별명 카드면 other + is_name_card=true\n"
    "\n[3단계: 인물 = 익명 ID]\n"
    "이 컷에 보이는 인물을 로스터와 대조하세요. **이름을 쓰지 마세요. A/B/C 같은 익명 ID만.**\n"
    "- 로스터에 있는 인물이면 그 local_id를 그대로 (is_new=false)\n"
    "- 처음 보는 인물이면 **다음 알파벳**을 새로 발급 (is_new=true) + description 필수\n"
    "- description은 재식별용 시각 서술: 머리색/헤어스타일/눈색/복장/무기/체격/종족 특징\n"
    "- 같은 인물인지 **애매하면 새로 발급하지 말고** local_id=null + note에 애매하다고 적으세요\n"
    "- prominence: main(전경/대사) | minor | extra(군중·배경·1컷 단역)\n"
    "\n[4단계: 화자] speech/monologue 블록에만. 우선순위: "
    "①말풍선 꼬리가 가리키는 인물 ②대사 교대(질문→대답) ③화법(하대/존대/호칭) ④POV(narration은 주인공). "
    "**모르면 speaker=null.** 억지로 채우지 마세요.\n"
    "\n[5단계: 이름 증거] 대사·나레이션·별명 카드에서 **인물의 실제 이름이 글자로 드러난 경우에만** "
    "name_evidence에 적으세요. **이름이 안 나오면 빈 배열** — 추측 금지.\n"
    "⛔ 이름이 **아닌** 것: ①'아저씨'·'형님'·'사형'·'왕초' 같은 **호칭/직함** "
    "②'고블린'·'늑대' 같은 **종족/몬스터** ③**`#A`·`#B` 같은 슬롯 기호**.\n"
    "⛔ **`#A`, `#B`는 인물을 가리키는 임시 슬롯 기호일 뿐 이름이 절대 아닙니다.** 입력 "
    "recent_dialogue에 `by:\"#A\"`가 보여도 그건 '#A라고 불렸다'는 뜻이 **아닙니다**. "
    "name에 `#A`나 `A`를 쓰지 마세요.\n"
    "\n스키마: {\"cut_summary\":\"\",\"people\":[{\"local_id\":\"A\",\"is_new\":false,"
    "\"description\":\"\",\"prominence\":\"main|minor|extra\",\"note\":null}],"
    "\"blocks\":[{\"transcript\":\"\",\"type\":\"speech|monologue|narration|system|other\","
    "\"is_name_card\":false,\"speaker\":null,\"basis\":\"tail|context|speech_style|pov|none\"}],"
    "\"name_evidence\":[{\"local_id\":\"A\",\"name\":\"\",\"evidence\":\"\"}]}"
)

NAME_SYSTEM = (
    "당신은 웹툰 회차의 **인물 명명기**입니다. 입력: 익명 로스터(슬롯 기호 + 시각 서술 + 등장 컷) + "
    "회차 전체에서 수집된 이름 증거 + 대사 전문. **JSON만** 출력. 자연어는 한국어.\n"
    "임무: 각 슬롯에 실제 이름을 붙이거나, **붙일 수 없으면 null**.\n"
    "\n⛔ **`#A`, `#B` 는 인물을 가리키는 임시 슬롯 기호이지 이름이 아닙니다.** 대사의 `by:\"#A\"`는 "
    "'#A가 말했다'는 뜻이지 '#A라고 불렸다'가 **아닙니다**. name에 `#A`/`A`를 쓰면 **틀린 답**입니다.\n"
    "\n⚠️ **가장 중요: 근거 없으면 null입니다.** 이름이 회차 안에서 **글자로 드러나지 않은** 인물은 "
    "반드시 name=null. 추측·창작·'아마 주인공이니까' 금지. **없는 이름을 지어내는 것은 최악의 오류다.**\n"
    "⚠️ **name은 반드시 대사/나레이션/카드에 실제로 등장한 문자열이어야 합니다.** 들어본 적 없는 "
    "이름을 만들지 마세요. 비슷한 발음으로 바꾸지도 마세요(원문 그대로).\n"
    "⛔ 이름이 **아닌** 것: 호칭/직함(아저씨·형님·사형·사제·장문·왕초·언니·누나·선배), "
    "종족/몬스터(고블린·늑대·골렘), 슬롯 기호(#A). 전부 name=null이고 reason에만 적으세요.\n"
    "⚠️ 이름 증거가 있어도 **어느 슬롯을 가리키는지 불확실하면 null**로 두고 reason에 적으세요.\n"
    "\nconfidence(0~1): 호명 문장이 명확하면 높게, 정황뿐이면 **낮게**. **전부 1.0을 주지 마세요** — "
    "확신도가 다르면 값도 달라야 합니다. significance: main(주역)|supporting(조역)|extra(군중·단역).\n"
    "\n스키마: {\"roster\":[{\"local_id\":\"#A\",\"name\":null,\"confidence\":0,"
    "\"significance\":\"main|supporting|extra\",\"reason\":\"\"}]}"
)

# 프롬프트로 안 되는 건 구조로 막는다(§C5 — null 허용이 지시만으로 되는지가 이 실험의 쟁점).
# 아래 가드는 **거부 사유를 기록**하므로, 모델이 얼마나 자주 시도하는지 자체가 지표가 된다.
_HONORIFICS = {
    "아저씨", "아주머니", "형님", "누님", "언니", "누나", "형", "오빠", "선배", "사형", "사제",
    "사부", "스승", "장문", "장문인", "왕초", "교주", "대협", "소협", "공자", "낭자", "어르신",
    "주인님", "대장", "두목", "선생", "선생님", "아가씨", "도련님",
}
_SPECIES = {"고블린", "늑대", "골렘", "좀비", "몬스터", "오크", "슬라임", "드래곤", "요정", "엘프"}


# 행 난 콜을 버리고 진행하려면 별도 스레드가 필요하다(호출 자체는 취소 불가 — 결과만 무시).
_CALL_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="llmcall")


class _Call:
    """call_llm_json 반환값과 같은 모양(result/raw_text/repaired/usage)."""

    def __init__(self, result, raw_text, repaired, usage):
        self.result, self.raw_text, self.repaired, self.usage = result, raw_text, repaired, usage


def _call_no_think(ctx: dict, system: str, user: str, images: list[bytes]) -> _Call:
    """`chat_template_kwargs.enable_thinking=false`를 실어 직접 호출(call_llm_json 우회).

    qwen-vl(27B)은 reasoning 모델이라 thinking ON이면 콜당 수 분이 걸린다 —
    `face-classify-w23-2026-07-16.md` 실측(얼굴당 3분 → 17s). call_llm_json엔 이 제어가 없다.
    """
    endpoint = _resolve_endpoint(ctx)
    headers = {"Authorization": f"Bearer {_resolve_api_key(ctx)}"}
    content: list[dict] = [{"type": "text", "text": user}]
    for im in images:
        content.append({"type": "image_url", "image_url": {"url": _data_url(im)}})
    body = {
        "model": ctx["model_id"],
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": content}],
        "temperature": (ctx.get("params") or {}).get("temperature", 0.0),
        "response_format": {"type": "json_object"},
        "stream": True, "stream_options": {"include_usage": True},
        "chat_template_kwargs": {"enable_thinking": False},
    }
    chunks: list[str] = []
    usage: dict = {}
    finish = None
    with _get_client().stream("POST", endpoint, json=body, headers=headers) as resp:
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
            for ch in (obj.get("choices") or []):
                if isinstance((ch.get("delta") or {}).get("content"), str):
                    chunks.append(ch["delta"]["content"])
                if ch.get("finish_reason"):
                    finish = ch["finish_reason"]
    text = "".join(chunks).strip()
    parsed, repaired = _parse_json_content_ex(text)
    return _Call(parsed if isinstance(parsed, dict) else {}, text, bool(repaired), {
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "total_tokens": usage.get("total_tokens"),
        "finish_reason": finish,
    })


def _caller(no_think: bool):
    return _call_no_think if no_think else call_llm_json


def _phash() -> str:
    return hashlib.sha256((CUT_SYSTEM + NAME_SYSTEM).encode()).hexdigest()[:12]


def _ctx(model: str) -> dict:
    return _pass1_ctx({"provider": "vllm", "model_id": model,
                       "params": {"temperature": 0.0}, "supports_vision": True})


# ── 입력 ─────────────────────────────────────────────────────────────────────

def _episode(webtoon_id: int, no: int) -> tuple[int, dict, list[int]]:
    with db_cursor() as cur:
        cur.execute("SELECT id FROM webtoon_episode WHERE webtoon_id=%s AND no=%s", (webtoon_id, no))
        row = cur.fetchone()
        if not row:
            raise SystemExit(f"w{webtoon_id} ep{no} 없음")
        ep = row[0]
        cur.execute("SELECT cut_number FROM webtoon_cut WHERE episode_id=%s ORDER BY cut_number", (ep,))
        cuts = [r[0] for r in cur.fetchall()]
    return ep, _episode_info(ep), cuts


def _raw_image(info: dict, cut: int, raw_dir: Path) -> bytes | None:
    """오버레이 없는 원본(장변 1000px) — Claude가 읽은 것과 동일 규격. 로컬 캐시."""
    p = raw_dir / f"c{cut:03d}.jpg"
    if p.exists():
        return p.read_bytes()
    b = fetch_cut_image(info["source"], info["title_id"], info["episode_no"], cut)
    if b is None:
        return None
    im = Image.open(io.BytesIO(b)).convert("RGB")
    w, h = im.size
    s = min(1.0, MAX_DIM / max(w, h))
    if s < 1.0:
        im = im.resize((int(w * s), int(h * s)), Image.LANCZOS)
    im.save(p, "JPEG", quality=88)
    return p.read_bytes()


def _slot(lid: str | None) -> str | None:
    """익명 ID를 **이름으로 오인될 수 없는 표기**로 렌더한다.

    v1 버그: 최근 대사를 "B: 잘한다 잘해" 형식으로 이월했더니 명명 콜이 그걸 'B로 호명됨'으로 읽고
    name="B"를 냈다(실측 7/12). **익명 ID가 이름 슬롯으로 샌 것** — 이 아키텍처의 진짜 함정이다.
    """
    return f"#{lid}" if lid else None


def _state_text(roster: dict, tail: list[dict], cut: int) -> str:
    """이월 상태 — 로스터 + 최근 대사. 이게 '작은 상태'의 전부다(큰 컨텍스트 대신)."""
    ros = [{"slot": _slot(k), "description": v["description"]} for k, v in sorted(roster.items())]
    nxt = chr(ord("A") + len(roster)) if len(roster) < 26 else "?"
    recent = []
    for t in tail[-CTX_TAIL_CUTS:]:
        # "B: text"가 아니라 구조화 — 문자열 연결이 곧 오인의 원인이었다.
        lines = [{"by": _slot(b.get("speaker")), "text": b.get("transcript", "")}
                 for b in t["blocks"] if b.get("type") in ("speech", "monologue", "narration")]
        if lines:
            recent.append({"cut": t["cut"], "lines": lines})
    return json.dumps({
        "current_cut": cut,
        "roster_so_far": ros,
        "next_available_id": nxt,
        "note": "roster_so_far/by 의 #X 는 익명 슬롯 기호다 — 인물의 이름이 아니다.",
        "recent_dialogue": recent,
    }, ensure_ascii=False)


def _norm_name(s: str) -> str:
    return "".join(ch for ch in (s or "") if not ch.isspace())


def _guard_names(roster_out: list, transcripts: list[str], slots: set[str],
                 nameable: set[str] | None = None) -> tuple[list, list]:
    """프롬프트로 안 되는 걸 구조로 막는다 → (정제된 roster, 거부 기록).

    가드 5종 (전부 실측된 실패에서 나왔다):
      1) **슬롯 기호** — name이 'A'/'#A'면 거부. v1: 12명 중 7명.
      2) **호칭/직함** — '아저씨'·'사형'. v1 실측.
      3) **종족/몬스터** — '고블린'. v1 실측.
      4) **근거 접지(grounding)** — 회차 전사 어디에도 없는 이름은 창작. v1: '디드루이'·'에로웨이'.
         ⚠️ 한계: 전사 자체가 오독이면(에르웬→에로웨이) 통과한다. 그건 **전사 오류**라는
         다른 실패이므로 여기서 잡지 않는 게 맞다.
      5) **이름 유일성(v3)** — 두 슬롯이 같은 이름을 가질 수 없다. confidence가 높은 쪽을 남기고
         나머지는 거부. **ep11 실측**: `#A=에르웬(0.95)` + `#B=에르웬(1.0)` — A(비요른)가 에르웬을
         *부르는* 걸 보고 A를 에르웬이라 판정했다(화자↔호명대상 혼동).
      6) **SFX 출처(v3)** — `nameable`(=speech/monologue/narration/system 또는 is_name_card인
         블록의 텍스트)에 없으면 거부. **ep11 실측**: `#C=그록` — 고블린 울음소리(other/SFX)를
         이름으로 승격. 4)만으론 못 잡는다(전사에 실재하므로).
         ⚠️ 타이틀/별명 카드는 `other`지만 `is_name_card=true`라 nameable에 포함된다(§C1.1).
    """
    # ⚠️ 반드시 깊은 복사. v2 버그: list()는 얕은 복사라 안쪽 dict가 raw와 같은 객체였고,
    # x["name"]=None 이 **모델 원본(raw)까지 변형**했다 → 저장된 raw가 '가드 적용 후' 상태가 되어
    # (a) 모델이 실제로 뭘 냈는지 감사 불가 (b) --reguard로 가드만 바꿔 재비교 불가.
    roster_out = json.loads(json.dumps(roster_out))
    blob = _norm_name(" ".join(transcripts))
    nameable_blob = _norm_name(" ".join(nameable)) if nameable is not None else None
    bare = {s.lstrip("#").upper() for s in slots}
    out, rejected = [], []

    def _rej(x, n, reason):
        rejected.append({"local_id": x.get("local_id"), "name": n, "reason": reason})
        x["name"] = None
        x["guard"] = reason

    for x in roster_out:
        if not isinstance(x, dict):
            continue
        nm = x.get("name")
        if not isinstance(nm, str) or not nm.strip():
            x["name"] = None
            out.append(x)
            continue
        n = nm.strip()
        reason = None
        if n.lstrip("#").upper() in bare or len(n.lstrip("#")) <= 1:
            reason = "slot_symbol"
        elif n in _HONORIFICS:
            reason = "honorific"
        elif n in _SPECIES:
            reason = "species"
        elif _norm_name(n) not in blob:
            reason = "ungrounded"
        elif nameable_blob is not None and _norm_name(n) not in nameable_blob:
            reason = "sfx_source"  # SFX/효과음에서만 나온 문자열은 이름이 아니다
        if reason:
            _rej(x, n, reason)
        out.append(x)

    # 5) 이름 유일성 — 같은 이름을 쓴 슬롯이 여럿이면 confidence 최고 하나만 남긴다.
    by_name: dict[str, list] = {}
    for x in out:
        if x.get("name"):
            by_name.setdefault(x["name"], []).append(x)
    for nm, xs in by_name.items():
        if len(xs) < 2:
            continue
        xs.sort(key=lambda y: (y.get("confidence") or 0), reverse=True)
        for loser in xs[1:]:
            _rej(loser, nm, "duplicate_name")
    return out, rejected


# ── 정규화 ───────────────────────────────────────────────────────────────────

def _lid(v) -> str | None:
    """모델이 '#A'로 답하든 'A'로 답하든 내부 표준은 **접두어 없는 'A'**로 통일.

    (v2에서 슬롯 표기를 '#A'로 바꿨더니 모델도 '#A'로 답한다 → 그대로 저장하면 _slot()이
    다시 '#'을 붙여 '##A'가 된다. 여기서 한 번 벗긴다.)
    """
    if not isinstance(v, str) or not v.strip():
        return None
    s = v.strip().lstrip("#").strip().upper()
    return s or None


def _sanitize(raw: dict, roster: dict) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    known = set(roster)

    people = []
    for p in (raw.get("people") or []):
        if not isinstance(p, dict):
            continue
        lid = _lid(p.get("local_id"))
        prom = p.get("prominence")
        people.append({
            "local_id": lid,
            "is_new": bool(p.get("is_new")) or (lid is not None and lid not in known),
            "description": p.get("description") if isinstance(p.get("description"), str) else None,
            "prominence": prom if prom in ("main", "minor", "extra") else None,
            "note": p.get("note") if isinstance(p.get("note"), str) else None,
        })

    blocks = []
    for b in (raw.get("blocks") or []):
        if not isinstance(b, dict):
            continue
        t = b.get("transcript")
        if not isinstance(t, str) or not t.strip():
            continue
        bt = b.get("type")
        sp = _lid(b.get("speaker"))
        if bt not in ("speech", "monologue"):
            sp = None          # 화자는 speech/monologue에만(§Req 1.5 정신 유지)
        blocks.append({
            "transcript": t.strip(),
            "type": bt if bt in _TYPES else None,
            "is_name_card": bool(b.get("is_name_card")),
            "speaker": sp,
            "basis": b.get("basis") if b.get("basis") in
                     ("tail", "context", "speech_style", "pov", "none") else "none",
        })

    ev = []
    for e in (raw.get("name_evidence") or []):
        if not isinstance(e, dict):
            continue
        nm = e.get("name")
        if not isinstance(nm, str) or not nm.strip():
            continue
        ev.append({
            "local_id": _lid(e.get("local_id")),
            "name": nm.strip(),
            "evidence": e.get("evidence") if isinstance(e.get("evidence"), str) else None,
        })

    return {
        "cut_summary": raw.get("cut_summary") if isinstance(raw.get("cut_summary"), str) else None,
        "people": people, "blocks": blocks, "name_evidence": ev,
    }


# ── 실행 ─────────────────────────────────────────────────────────────────────

def run_episode(webtoon_id: int, no: int, out_dir: Path, ctx: dict,
                limit: int | None = None, no_think: bool = False) -> None:
    ep, info, cuts = _episode(webtoon_id, no)
    raw_dir = out_dir / "raw" / f"w{webtoon_id}_e{no}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"w{webtoon_id}_e{no}.jsonl"
    if limit:
        cuts = cuts[:limit]

    # resume: 기록된 컷까지 로스터/tail 재구성(상태 이월이 핵심이라 그냥 스킵하면 안 된다)
    roster: dict = {}
    tail: list[dict] = []
    done: set[int] = set()
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            done.add(r["cut_number"])
            for p in r["result"]["people"]:
                if p["local_id"] and p["local_id"] not in roster:
                    roster[p["local_id"]] = {"description": p["description"] or "",
                                             "first_cut": r["cut_number"]}
            tail.append({"cut": r["cut_number"], "blocks": r["result"]["blocks"]})
    todo = [c for c in cuts if c not in done]
    logger.info("w%s ep%s — 컷 %d, 처리 %d (기존 %d, 로스터 %s)",
                webtoon_id, no, len(cuts), len(todo), len(done), sorted(roster))

    with path.open("a") as fh:
        for n, cut in enumerate(todo, 1):
            img = _raw_image(info, cut, raw_dir)
            if img is None:
                logger.warning("컷 %s 이미지 없음 — 스킵", cut)
                continue
            user = _state_text(roster, tail, cut)
            t0 = time.monotonic()
            try:
                # 데드라인 초과 시 그 컷만 버리고 진행(행 방지 — CALL_DEADLINE_S 주석 참고).
                # 스레드는 데몬 풀에 남아 결국 정리된다; 결과만 무시한다.
                fut = _CALL_POOL.submit(_caller(no_think), ctx, CUT_SYSTEM, user, [img])
                call = fut.result(timeout=CALL_DEADLINE_S)
                raw = call.result if isinstance(call.result, dict) else {}
                err, usage, repaired = None, call.usage, call.repaired
            except FuturesTimeout:
                fut.cancel()
                raw, err, usage, repaired = {}, f"deadline_exceeded({CALL_DEADLINE_S}s)", {}, False
                logger.warning("컷 %s — %ss 초과, 스킵", cut, CALL_DEADLINE_S)
            except Exception as e:  # noqa: BLE001
                raw, err, usage, repaired = {}, str(e), {}, False
            res = _sanitize(raw, roster)

            for p in res["people"]:
                if p["local_id"] and p["local_id"] not in roster:
                    roster[p["local_id"]] = {"description": p["description"] or "",
                                             "first_cut": cut}
            tail.append({"cut": cut, "blocks": res["blocks"]})

            fh.write(json.dumps({
                "schema_version": SCHEMA_VERSION, "webtoon_id": webtoon_id,
                "episode_id": ep, "episode_no": no, "cut_number": cut,
                "model": ctx["model_id"], "input_state": user,
                "raw": raw, "result": res, "roster_size_after": len(roster),
                "usage": usage, "repaired": repaired,
                "latency_s": round(time.monotonic() - t0, 2), "error": err,
            }, ensure_ascii=False) + "\n")
            fh.flush()
            if n % 10 == 0 or n == len(todo):
                logger.info("w%s ep%s — %d/%d (로스터 %d)", webtoon_id, no, n, len(todo), len(roster))

    naming(webtoon_id, no, out_dir, ctx, no_think=no_think)


def naming(webtoon_id: int, no: int, out_dir: Path, ctx: dict, no_think: bool = False) -> None:
    """회차 전역 명명 1콜 — 이름 증거가 3~7컷에만 있으므로 컷 단위로는 물어볼 수조차 없다(§C6)."""
    path = out_dir / f"w{webtoon_id}_e{no}.jsonl"
    rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    roster: dict = {}
    ev: list[dict] = []
    dialogue: list[dict] = []
    for r in rows:
        for p in r["result"]["people"]:
            if not p["local_id"]:
                continue
            e = roster.setdefault(p["local_id"], {"local_id": p["local_id"], "description": None,
                                                  "cuts": [], "prominence": []})
            if p["description"] and not e["description"]:
                e["description"] = p["description"]
            e["cuts"].append(r["cut_number"])
            if p["prominence"]:
                e["prominence"].append(p["prominence"])
        for e2 in r["result"]["name_evidence"]:
            ev.append({"slot": _slot(e2.get("local_id")), "name": e2.get("name"),
                       "evidence": e2.get("evidence"), "cut": r["cut_number"]})
        lines = [{"by": _slot(b["speaker"]), "type": b["type"], "text": b["transcript"],
                  **({"is_name_card": True} if b.get("is_name_card") else {})}
                 for b in r["result"]["blocks"]
                 if b["type"] in ("speech", "monologue", "narration") or b.get("is_name_card")]
        if lines:
            dialogue.append({"cut": r["cut_number"], "lines": lines})

    payload = {
        "note": "roster의 slot(#A)과 대사의 by(#A)는 익명 슬롯 기호다 — 인물 이름이 아니다.",
        "roster": [{"slot": _slot(k), "description": v["description"],
                    "n_cuts": len(set(v["cuts"])), "cuts": sorted(set(v["cuts"]))[:40],
                    "prominence": max(set(v["prominence"]), key=v["prominence"].count)
                                  if v["prominence"] else None}
                   for k, v in sorted(roster.items())],
        "name_evidence": ev,
        "dialogue": dialogue,
    }
    t0 = time.monotonic()
    try:
        call = _caller(no_think)(ctx, NAME_SYSTEM, json.dumps(payload, ensure_ascii=False), [])
        raw = call.result if isinstance(call.result, dict) else {}
        err, usage = None, call.usage
    except Exception as e:  # noqa: BLE001
        raw, err, usage = {}, str(e), {}

    # 구조 가드 — 프롬프트 지시가 안 먹힌 만큼이 그대로 지표가 된다.
    transcripts = [b["transcript"] for r in rows for b in r["result"]["blocks"]]
    # 이름이 나올 수 있는 출처만 = 발화/서술/상태창 + 이름 카드. SFX(other)는 제외(§가드 6).
    nameable = [b["transcript"] for r in rows for b in r["result"]["blocks"]
                if b["type"] in ("speech", "monologue", "narration", "system") or b.get("is_name_card")]
    guarded, rejected = _guard_names(list(raw.get("roster") or []),
                                     transcripts, {_slot(k) for k in roster}, nameable=nameable)
    out = {"webtoon_id": webtoon_id, "episode_no": no, "model": ctx["model_id"],
           "input": payload, "raw": raw, "guarded_roster": guarded,
           "guard_rejections": rejected, "usage": usage, "error": err,
           "latency_s": round(time.monotonic() - t0, 2)}
    (out_dir / f"naming_w{webtoon_id}_e{no}.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2))
    named = [x for x in guarded if x.get("name")]
    logger.info("w%s ep%s 명명 — 로스터 %d명 → 명명 %d, null %d | 가드 거부 %d건 %s%s",
                webtoon_id, no, len(payload["roster"]), len(named),
                len(guarded) - len(named), len(rejected),
                [r["reason"] for r in rejected], f" (에러: {err})" if err else "")


def reguard(webtoon_id: int, no: int, out_dir: Path) -> None:
    """**LLM 재호출 없이** 저장된 raw 출력에 가드만 다시 적용한다.

    실험 3의 방법론 결함 해소: `--naming-only`는 명명 콜을 재호출하는데, temperature 0인데도
    vLLM 출력이 달라져(배칭 등) **가드 효과와 재샘플링 효과가 섞였다**(실측: #A `바바리안`→`쏘곤`).
    가드만 바꿔 비교하려면 **동일 raw**에 적용해야 한다.
    """
    np_path = out_dir / f"naming_w{webtoon_id}_e{no}.json"
    if not np_path.exists():
        logger.warning("w%s ep%s — 명명 산출 없음, 스킵", webtoon_id, no)
        return
    out = json.loads(np_path.read_text())
    rows = [json.loads(l) for l in
            (out_dir / f"w{webtoon_id}_e{no}.jsonl").read_text().splitlines() if l.strip()]
    transcripts = [b["transcript"] for r in rows for b in r["result"]["blocks"]]
    nameable = [b["transcript"] for r in rows for b in r["result"]["blocks"]
                if b["type"] in ("speech", "monologue", "narration", "system") or b.get("is_name_card")]
    slots = {x.get("slot") for x in out["input"]["roster"]}
    guarded, rejected = _guard_names(json.loads(json.dumps(out.get("raw", {}).get("roster") or [])),
                                     transcripts, slots, nameable=nameable)
    out["guarded_roster"], out["guard_rejections"] = guarded, rejected
    out["reguarded"] = True
    np_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    named = [x for x in guarded if x.get("name")]
    logger.info("w%s ep%s 재가드(무재호출) — 명명 %d, null %d | 거부 %d건 %s",
                webtoon_id, no, len(named), len(guarded) - len(named), len(rejected),
                [(r["name"], r["reason"]) for r in rejected])


def report(out_dir: Path, webtoon_id: int, nos: list[int]) -> dict:
    out: dict = {}
    for no in nos:
        p = out_dir / f"w{webtoon_id}_e{no}.jsonl"
        if not p.exists():
            continue
        rows = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
        ok = [r for r in rows if not r["error"]]
        blocks = sum(len(r["result"]["blocks"]) for r in ok)
        empty_cuts = [r["cut_number"] for r in ok if not r["result"]["blocks"]]
        roster: dict = {}
        for r in ok:
            for pp in r["result"]["people"]:
                if pp["local_id"]:
                    roster.setdefault(pp["local_id"], []).append(r["cut_number"])
        np_path = out_dir / f"naming_w{webtoon_id}_e{no}.json"
        naming_out = json.loads(np_path.read_text()) if np_path.exists() else {}
        nr = naming_out.get("guarded_roster") or []
        rej = naming_out.get("guard_rejections") or []
        out[f"ep{no}"] = {
            "cuts": len(rows), "calls_ok": len(ok), "errors": len(rows) - len(ok),
            "blocks_transcribed": blocks,
            "cuts_with_no_text": len(empty_cuts),
            "roster_size": len(roster),
            "roster": {k: {"n_cuts": len(set(v)), "cuts": sorted(set(v))[:12]}
                       for k, v in sorted(roster.items())},
            "named": {x.get("local_id"): x.get("name") for x in nr if isinstance(x, dict)},
            "n_named": sum(1 for x in nr if isinstance(x, dict) and x.get("name")),
            "n_null": sum(1 for x in nr if isinstance(x, dict) and not x.get("name")),
            "guard_rejections": rej,
            "guard_reasons": {r: sum(1 for x in rej if x["reason"] == r)
                              for r in {x["reason"] for x in rej}},
            "avg_latency_s": round(sum(r["latency_s"] for r in ok) / len(ok), 1) if ok else None,
        }
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="익명 로스터 하네스 (Claude 동일 조건, qwen 실측)")
    ap.add_argument("--webtoon", type=int, default=23)
    ap.add_argument("--episodes", default="10")
    ap.add_argument("--model", default="qwen-base")
    ap.add_argument("--datasets", default=str(Path(__file__).resolve().parents[1] / "datasets"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--no-think", action="store_true",
                    help="chat_template_kwargs.enable_thinking=false 전송 (qwen-vl 등 reasoning 모델 필수)")
    ap.add_argument("--naming-only", action="store_true", help="컷 콜 생략, 명명만 재실행(⚠️ LLM 재호출 — 가드 효과와 재샘플링이 섞인다)")
    ap.add_argument("--reguard", action="store_true",
                    help="LLM 재호출 없이 저장된 raw에 가드만 재적용(가드 효과만 격리)")
    args = ap.parse_args()

    nos = (list(range(int(args.episodes.split("-")[0]), int(args.episodes.split("-")[1]) + 1))
           if "-" in args.episodes else [int(x) for x in args.episodes.split(",")])
    out_dir = Path(args.datasets) / "anon" / f"{args.model}_{_phash()}"
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.report:
        print(json.dumps(report(out_dir, args.webtoon, nos), ensure_ascii=False, indent=2))
        return

    meta = out_dir / "meta.json"
    if not meta.exists():
        meta.write_text(json.dumps({"cut_system": CUT_SYSTEM, "name_system": NAME_SYSTEM,
                                    "model": args.model, "max_dim": MAX_DIM,
                                    "ctx_tail_cuts": CTX_TAIL_CUTS,
                                    "schema_version": SCHEMA_VERSION}, ensure_ascii=False, indent=2))
    ctx = _ctx(args.model)
    logger.info("out=%s model=%s", out_dir, args.model)
    for no in nos:
        if args.reguard:
            reguard(args.webtoon, no, out_dir)
        elif args.naming_only:
            naming(args.webtoon, no, out_dir, ctx, no_think=args.no_think)
        else:
            run_episode(args.webtoon, no, out_dir, ctx, limit=args.limit, no_think=args.no_think)
    print(json.dumps(report(out_dir, args.webtoon, nos), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
