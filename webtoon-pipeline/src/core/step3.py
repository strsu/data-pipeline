"""Step 3 코어 — LLM 멀티모달 장면/화자 분석 (faust-free, 컷 단위 슬라이딩 윈도우).

활성 웹툰만(phase3) 실행. 컷 N에 대해:
  1) 컷 N의 paddle 텍스트 + 식별된 얼굴 로드
  2) 이미지 N-2/N-1/N 다운로드 + 현재 컷(N)에 얼굴 bbox/ID 오버레이
  3) LLM 1회 호출 → blocks(type/speaker/corrected_text) + scene_meta + name_discoveries
  4) 저장: TextAnnotation(source='llm'), cut_scene_meta, name_discoveries→Character, cut.llm_*
반환: 다음 컷에 넘길 prev_context(직전 컷 요약/마지막 대사).
LLM 모델/endpoint는 DB(LLMModel)에서 해석 — 코드에 모델명 하드코딩 없음(default=GLM).
"""
from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

from PIL import Image
from psycopg2.extras import Json

from src.config.db import db_cursor
from src.config.s3 import fetch_cut_image
from src.operators import narrative_context
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import resolve_llm_model
from src.operators.overlay import overlay_faces

logger = logging.getLogger(__name__)

_NAME_AUTO_CONFIDENCE = 0.85

# Pass-1(컷별 provisional 추출) 상수 — 비전 1콜 throughput/품질 가드.
_PASS1_MAX_DIM = 1280          # 다운스케일 상한(긴 변, px)
_PASS1_MIN_MAX_TOKENS = 16384  # Req 7.2 — 절단 방지(>=4096 → 8192 → 16384로 재상향).
# glm-4.6v 등 추론형 모델은 답 이전에 reasoning_content로 사고과정을 먼저 소모하는데,
# max_tokens는 reasoning+content 합산 예산이라 4096으로는 컷이 복잡하면(블록 많음 등)
# 본문이 잘려 JSON 파싱이 깨진다(실사례: ep=11 cut=2 "Unterminated string" — 2026-07-04).
# 8192로도 naver/820097 전 회차에서 finish_reason='length' 절단이 7건 잔존(2026-07-05 실측)
# → 16384로 재상향. GLM-4.6v 비전 컨텍스트 32K 중 입력(이미지 ~1.3K + 프롬프트)이 작아 여유 있음.
# DB(llm_model.params)에 명시적 max_tokens가 없어 이 floor가 그대로 적용되는 게 확인됨.
_PASS1_MAX_TEMPERATURE = 0.2   # Req 7.5 — 추출 단계 저온도(0.0~0.2)
_PASS1_RETRIES = 2             # Req 7.4 — 파싱/일시 오류 1회 재시도(총 2회 시도)
# 컷 간 belief 의존성 없음(연속성은 Pass-2 담당 — Req 1.2) → extract_cut 호출 자체는 컷 간
# 병렬 처리 가능. 다만 실제 동시 요청 수는 llm_client._LLM_SEMAPHORE(LLM_MAX_CONCURRENCY,
# 기본 1 — 모델 서버 동시호출 제한)가 최종적으로 가드하므로, 여기 워커 수를 올려도
# LLM_MAX_CONCURRENCY를 같이 올리지 않으면 실질 동시성은 늘지 않는다.
_PASS1_WORKERS = int(os.getenv("PASS1_WORKERS", "4") or "4")
_BLOCK_TYPES = ("speech", "monologue", "narration", "system", "other")
_SPEAKER_BASES = ("tail", "face", "context", "none")
_PROMINENCE = ("main", "minor", "extra")
_SPEAKER_TYPES = ("speech", "monologue")  # 화자 귀속 대상 type
# belief state: 이 임계값 미만이거나 face_label/name이 모두 없는 화자 후보는 'pending'으로
# 적재해 Pass-2a가 맥락으로 해소하게 둔다(Req 1.5 과확신 금지 / Req 2.4 해소 대상).
_PENDING_SPEAKER_MAX_CONFIDENCE = 0.5
_PASS1_STAGE = "vision"  # LLMUsage.stage — Stage V 컷 비전 콜(step3a).
# LLMUsage.stage 허용값(service `LLMStage` enum과 일치) — usage 적재의 단일 진실원천.
# vision=컷 비전(V), resolve=정체·화자 해소(R), narrative=서사 분석(N), arc=아크 종합(A).
_USAGE_STAGES = ("vision", "resolve", "narrative", "arc")

# ── Pass-1 (컷별 provisional 추출) ────────────────────────────────────────────
# 정본: qwen-vl/_pass1.py SYS 프롬프트를 그대로 이관(분류 먼저→화자 나중, strict JSON,
# 1:1 바인딩, SFX→cut_summary 흡수, prominence, name_evidence, type_confidence,
# speaker.basis, "지어내지 마"). 연속성은 Pass-1이 아니라 Pass-2가 담당(Req 1.2).
_PASS1_SYSTEM_PROMPT = (
    "당신은 웹툰 컷 분석기입니다. 입력: 현재 컷 이미지(얼굴 bbox에 F0/F1 라벨 오버레이), "
    "identified_faces(F라벨+알려진 이름+confirmed), ocr_blocks(index+text). 현재 컷만 분석해 **JSON만** 출력.\n"
    "identified_faces의 confirmed=true는 **사람이 확정한 정체성(진실)** — 그대로 신뢰한다. "
    "confirmed=false는 얼굴인식 추정값이라 이미지/대사와 어긋나 보이면 name_evidence로 이의만 남긴다.\n"
    "⚠️ **모든 자연어 출력(cut_summary/key_objects/name_evidence 등)은 반드시 한국어로 작성한다** "
    "(예외: corrected_text만 OCR 원문 언어 유지). 영어 등 다른 언어로 답하지 말 것.\n"
    "규칙:\n"
    "1) blocks는 ocr_blocks의 모든 index에 1:1(병합·분할·생략·재번호 금지), index 유지.\n"
    "2) **[1단계: 분류 먼저]** 모든 블록의 type을 먼저 정한다. type: speech|monologue|narration|system|other. "
    "speech=입밖 대사(말풍선), monologue=속마음(구름/각진 말풍선), narration=화자없는 해설(사각박스), "
    "system=상태창/시스템/캡션, other=그외(효과음 원문 등). 각 블록에 type_confidence(0~1).\n"
    "3) **[2단계: 화자 나중]** speech/monologue 블록에만 speaker를 정한다. 말풍선 꼬리가 가리키는 오버레이 "
    "얼굴 라벨을 face_label에 그대로. 얼굴 없지만 문맥상 분명하면 name에. 모르면 둘 다 null. "
    "confidence(0~1), basis(tail=꼬리방향|face=얼굴위치|context=문맥|none), "
    "tail_hint(F0|offpanel_left|offpanel_right|ambiguous|none). **확신 없으면 confidence 낮게(과확신 금지).**\n"
    "4) 효과음/의성어는 type=other로 두되 의미는 cut_summary에 흡수('쿠루룽'→'천둥이 친다'). "
    "corrected_text는 OCR 원문 언어 유지.\n"
    "5) characters: 이 컷에 보이는 인물. face_label, prominence(main|minor|extra: 전경/크기/디테일/대사인접), emotion.\n"
    "6) name_evidence: 대사/나레이션에서 인물 실제 이름이 드러나면 [{face_label,name,confidence,evidence}].\n"
    "7) 없는 사물/인물/이름 지어내지 마. 모르면 null/생략. **자연어는 반드시 한국어(corrected_text 제외).**\n"
    "스키마: {\"cut_summary\":\"\",\"key_objects\":[],"
    "\"characters\":[{\"face_label\":\"F0\",\"prominence\":\"main|minor|extra\",\"emotion\":\"\"}],"
    "\"blocks\":[{\"index\":0,\"type\":\"speech|monologue|narration|system|other\",\"type_confidence\":0,"
    "\"corrected_text\":\"\",\"speaker\":{\"face_label\":null,\"name\":null,\"confidence\":0,\"basis\":\"none\",\"tail_hint\":\"none\"}}],"
    "\"name_evidence\":[]}"
)


@dataclass
class Pass1Record:
    """컷 1개의 Pass-1 provisional 추출 결과(비-영속 전이 객체).

    `result`는 sanitize/1:1 검증을 통과한 Pass-1 JSON
    (`cut_summary`, `key_objects`, `characters`, `blocks`, `name_evidence`).
    `faces`/`name_evidence`/`provisional_speakers`는 step3b(extract_episode) belief state
    누적용 캐리오버(roster/pending/name_evidence). `usage`는 LLMUsage 적재용(Req 6.7).
    `skipped`(empty|no_image) 또는 `error`가 채워지면 비전 콜이 생략/실패한 컷이다.
    """

    cut_number: int
    cut_id: Optional[int]
    result: dict = field(default_factory=dict)
    faces: list[dict] = field(default_factory=list)
    name_evidence: list[dict] = field(default_factory=list)
    provisional_speakers: list[dict] = field(default_factory=list)
    usage: dict = field(default_factory=dict)
    skipped: Optional[str] = None
    error: Optional[str] = None


@dataclass
class ExtractResult:
    """에피소드 Pass-1 추출(step3a) 결과 — step3b(resolve_episode) 입력용 전이 객체(비-영속).

    `records`는 컷 읽기순 `Pass1Record` 목록(스킵/에러 컷 포함 — 추적용). `belief`는 컷을 가로질러
    누적한 belief state(`character_roster`/`pending_speakers`/`name_evidence`/`open_questions`)로,
    Pass-2a 윈도우 캐리오버에 그대로 넘긴다(design Belief State). `usage_total`은 비전 콜 토큰 집계
    (prompt/completion/total/calls)로, 영속 per-call 행은 `llm_usage`에 별도 적재된다(Req 6.7).
    """

    webtoon_episode_id: int
    records: list["Pass1Record"] = field(default_factory=list)
    belief: dict = field(default_factory=dict)
    cuts_total: int = 0
    cuts_analyzed: int = 0   # 비전 콜이 실제 일어난 컷 수
    cuts_skipped: int = 0    # 빈컷/이미지없음/에러로 콜을 못 한 컷 수
    usage_total: dict = field(default_factory=dict)


# ── 입력 로드 ─────────────────────────────────────────────────────────────────

def _get_webtoon_id(webtoon_episode_id: int) -> int:
    with db_cursor() as cur:
        cur.execute("SELECT webtoon_id FROM webtoon_episode WHERE id = %s", (webtoon_episode_id,))
        return cur.fetchone()[0]


def _episode_info(webtoon_episode_id: int) -> dict:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT w.source, w.title_id, we.no
            FROM webtoon_episode we JOIN webtoon w ON we.webtoon_id = w.id
            WHERE we.id = %s
            """,
            (webtoon_episode_id,),
        )
        row = cur.fetchone()
        return {"source": row[0], "title_id": row[1], "episode_no": row[2]}


def prepare_episode_scene(webtoon_episode_id: int) -> None:
    """Step3 재실행용 정리 — 에피소드의 기존 'llm' 어노테이션 + scene_meta 삭제(paddle 보존).

    재실행이 '완전 교체'가 되도록(이전 run이 다뤘다가 이번엔 빠뜨린 region의 stale llm 제거).
    Character 이름/제안은 누적 지식이라 건드리지 않는다.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            DELETE FROM analysis_text_annotation
            WHERE source = 'llm' AND region_id IN (
                SELECT tr.id FROM analysis_text_region tr
                JOIN webtoon_cut wc ON tr.cut_id = wc.id
                WHERE wc.episode_id = %s
            )
            """,
            (webtoon_episode_id,),
        )
        cur.execute(
            """
            DELETE FROM analysis_cut_scene_meta
            WHERE cut_id IN (SELECT id FROM webtoon_cut WHERE episode_id = %s)
            """,
            (webtoon_episode_id,),
        )


def _cut_id(webtoon_episode_id: int, cut_number: int) -> Optional[int]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT id FROM webtoon_cut WHERE episode_id = %s AND cut_number = %s",
            (webtoon_episode_id, cut_number),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _load_regions(cut_id: int) -> list[dict]:
    """컷의 text_region + paddle 텍스트(index 순)."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT tr.id, tr.index, tr.bbox_x1, tr.bbox_y1, tr.bbox_x2, tr.bbox_y2,
                   ta.text
            FROM analysis_text_region tr
            LEFT JOIN analysis_text_annotation ta ON ta.region_id = tr.id AND ta.source = 'paddle'
            WHERE tr.cut_id = %s AND tr.is_excluded = false
            ORDER BY tr.index
            """,
            (cut_id,),
        )
        return [
            {"region_id": r[0], "index": r[1], "bbox": [r[2], r[3], r[4], r[5]], "text": r[6] or ""}
            for r in cur.fetchall()
        ]


def _load_faces(cut_id: int) -> list[dict]:
    """컷의 식별된 얼굴: id=F{face_idx}, bbox, 현재 캐릭터명, appearance_id, character_id, confirmed.

    `confirmed`는 human이 그 얼굴↔캐릭터 매칭을 확정(`face_record.is_confirmed`)했는지다 —
    Pass-1/2a 프롬프트가 "is_confirmed는 진실로 동결"을 지시하므로 **모델 입력에 실제로 실어야**
    작동한다(이전엔 프롬프트만 있고 데이터가 없었음 — 2026-07-05 수정).
    """
    with db_cursor() as cur:
        # 정체는 face_identity 레이어에서 human > step2 우선으로 해석한다(v4.0 §17.2).
        # DISTINCT ON: detection당 우선순위 최상 1행. 'human' < 'step2' (알파벳) → human 우선.
        cur.execute(
            """
            SELECT fr.face_idx, fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fi.appearance_id, c.name, c.id, (fi.source = 'human') AS confirmed
            FROM analysis_face_detection fr
            LEFT JOIN LATERAL (
                SELECT source, appearance_id
                FROM analysis_face_identity
                WHERE detection_id = fr.id AND deleted_at IS NULL
                ORDER BY source ASC
                LIMIT 1
            ) fi ON true
            LEFT JOIN analysis_character_appearance ca ON fi.appearance_id = ca.id
            LEFT JOIN analysis_character c ON ca.character_id = c.id
            WHERE fr.cut_id = %s AND fr.is_used = true
            ORDER BY fr.face_idx
            """,
            (cut_id,),
        )
        return [
            {"id": f"F{r[0]}", "bbox": [r[1], r[2], r[3], r[4]],
             "appearance_id": r[5],
             # 클러스터는 name=""(미명명) — 프롬프트에는 None으로 실어 "이름 모름"을 명시.
             "name": (r[6] or None),
             "character_id": r[7],
             "confirmed": bool(r[8])}
            for r in cur.fetchall()
        ]


# ── 저장 ──────────────────────────────────────────────────────────────────────

def _find_character_by_name(webtoon_id: int, name: str) -> Optional[int]:
    """웹툰 내 기존 명명 인물(kind=character)을 이름/alias로 찾는다. 확정 캐릭터 우선."""
    n = (name or "").strip()
    if not n:
        return None
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT id FROM analysis_character
            WHERE webtoon_id = %s AND deleted_at IS NULL
              AND kind = 'character' AND name <> ''
              AND (LOWER(name) = LOWER(%s)
                   OR EXISTS (
                        SELECT 1 FROM jsonb_array_elements_text(COALESCE(aliases, '[]')::jsonb) a
                        WHERE LOWER(a) = LOWER(%s)))
            ORDER BY is_confirmed DESC, id ASC
            LIMIT 1
            """,
            (webtoon_id, n, n),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _upsert_scene_meta(cut_id: int, scene_meta: dict, run_id: Optional[int] = None) -> None:
    """cut_scene_meta upsert(OneToOne) — Stage V 산출(action_summary/key_objects) + run 귀속."""
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_cut_scene_meta
                (cut_id, action_summary, key_objects, run_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (cut_id)
            DO UPDATE SET action_summary = EXCLUDED.action_summary,
                          key_objects = EXCLUDED.key_objects, run_id = EXCLUDED.run_id,
                          updated_at = EXCLUDED.updated_at
            """,
            (cut_id, scene_meta.get("action_summary", ""),
             Json(scene_meta.get("key_objects", [])), run_id, now, now),
        )


# ── Pass-1 추출 헬퍼 ──────────────────────────────────────────────────────────

def _clampf(v, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return lo
    if f != f:  # NaN
        return lo
    return max(lo, min(hi, f))


def _downscale(image_bytes: bytes, max_dim: int = _PASS1_MAX_DIM) -> bytes:
    """긴 변이 max_dim을 넘으면 비율 유지 축소(JPEG q88). 토큰/대역 절감."""
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    w, h = img.size
    scale = max_dim / float(max(w, h))
    if scale < 1.0:
        img = img.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.LANCZOS)
    bio = BytesIO()
    img.save(bio, format="JPEG", quality=88)
    return bio.getvalue()


def _pass1_ctx(ctx: dict) -> dict:
    """추출 콜 전용 ctx 사본 — max_tokens>=4096(Req 7.2), temperature<=0.2(Req 7.5) 강제."""
    params = dict(ctx.get("params") or {})
    try:
        temp = float(params.get("temperature", _PASS1_MAX_TEMPERATURE))
    except (TypeError, ValueError):
        temp = _PASS1_MAX_TEMPERATURE
    params["temperature"] = max(0.0, min(_PASS1_MAX_TEMPERATURE, temp))
    try:
        mt = int(params.get("max_tokens") or 0)
    except (TypeError, ValueError):
        mt = 0
    params["max_tokens"] = max(mt, _PASS1_MIN_MAX_TOKENS)
    out = dict(ctx)
    out["params"] = params
    return out


def _sanitize_speaker(raw, btype) -> dict:
    """화자는 speech/monologue 블록에만 귀속(Req 1.5). 그 외 type/비정형 입력은 전부 null."""
    null = {"face_label": None, "name": None, "confidence": 0.0, "basis": "none", "tail_hint": "none"}
    if btype not in _SPEAKER_TYPES or not isinstance(raw, dict):
        return null
    basis = raw.get("basis")
    if basis not in _SPEAKER_BASES:
        basis = "none"
    fl = raw.get("face_label")
    nm = raw.get("name")
    th = raw.get("tail_hint")
    return {
        "face_label": fl if isinstance(fl, str) and fl.strip() else None,
        "name": nm if isinstance(nm, str) and nm.strip() else None,
        "confidence": _clampf(raw.get("confidence", 0)),
        "basis": basis,
        "tail_hint": th if isinstance(th, str) and th.strip() else "none",
    }


def _sanitize_pass1(result: dict, regions: list[dict]) -> dict:
    """LLM 원시 출력 → Pass-1 계약 JSON으로 정규화 + **1:1 바인딩 강제**(Property 1).

    출력 `blocks`의 index 집합은 입력 OCR region index 집합과 정확히 일치한다(병합·분할·
    생략·재번호 금지). LLM이 누락한 index는 OCR 원문으로 채우고(type=None), 입력에 없는
    index는 버린다. type/basis/prominence는 허용 enum으로 강등, confidence는 [0,1] 클램프.
    """
    result = result if isinstance(result, dict) else {}
    by_index: dict[int, dict] = {}
    raw_blocks = result.get("blocks")
    if isinstance(raw_blocks, list):
        for b in raw_blocks:
            if not isinstance(b, dict):
                continue
            idx = b.get("index")
            if isinstance(idx, bool) or not isinstance(idx, int):
                continue
            by_index.setdefault(idx, b)  # 중복 index는 첫 등장만(재번호/분할 방어)

    out_blocks: list[dict] = []
    for r in regions:
        idx = r["index"]
        rb = by_index.get(idx) or {}
        btype = rb.get("type")
        if btype not in _BLOCK_TYPES:
            btype = None
        text = rb.get("corrected_text")
        if not isinstance(text, str) or not text.strip():
            text = r.get("text", "") or ""
        out_blocks.append({
            "index": idx,
            "type": btype,
            "type_confidence": _clampf(rb.get("type_confidence", 0)),
            "corrected_text": text,
            "speaker": _sanitize_speaker(rb.get("speaker"), btype),
        })

    out_chars: list[dict] = []
    raw_chars = result.get("characters")
    if isinstance(raw_chars, list):
        for c in raw_chars:
            if not isinstance(c, dict):
                continue
            prom = c.get("prominence")
            if prom not in _PROMINENCE:
                prom = None
            fl = c.get("face_label")
            emo = c.get("emotion")
            out_chars.append({
                "face_label": fl if isinstance(fl, str) and fl.strip() else None,
                "prominence": prom,
                "emotion": emo if isinstance(emo, str) else "",
            })

    out_ne: list[dict] = []
    raw_ne = result.get("name_evidence")
    if isinstance(raw_ne, list):
        for n in raw_ne:
            if not isinstance(n, dict):
                continue
            name = n.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            fl = n.get("face_label")
            ev = n.get("evidence")
            out_ne.append({
                "face_label": fl if isinstance(fl, str) and fl.strip() else None,
                "name": name.strip(),
                "confidence": _clampf(n.get("confidence", 0)),
                "evidence": ev if isinstance(ev, str) else "",
            })

    key_objects = result.get("key_objects")
    if not isinstance(key_objects, list):
        key_objects = []
    cut_summary = result.get("cut_summary")
    if not isinstance(cut_summary, str):
        cut_summary = ""

    return {
        "cut_summary": cut_summary,
        "key_objects": key_objects,
        "characters": out_chars,
        "blocks": out_blocks,
        "name_evidence": out_ne,
    }


def _upsert_provisional_annotation(
    region_id: int, block: dict, model_name: str, speaker_id: Optional[int] = None,
    run_id: Optional[int] = None,
) -> None:
    """Pass-1 블록을 provisional(`resolution_status='unresolved'`)로 적재.

    화자 후보도 함께 영속한다(2026-07-05 설계 변경): Pass-1이 얼굴 라벨 기반으로 확신
    (confidence>=임계값)한 화자는 `speaker_id`로 저장하되 `resolution_status='unresolved'`를
    유지한다 — 확정(resolved 전이)은 여전히 Pass-2b만 한다. 이전 설계(speaker_id 항상 NULL,
    후보는 belief로만 캐리오버)는 Pass-2a가 재출력하지 않은 확신 화자가 전부 유실돼 화자
    매칭률이 1~2%에 그치는 원인이었다(naver/820097 전 회차 실측). type은 허용목록 외면 None.
    """
    now = datetime.now(timezone.utc)
    btype = block.get("type")
    if btype not in _BLOCK_TYPES:
        btype = None
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_text_annotation
                (region_id, source, text, type, speaker_id, resolution_status,
                 model_version, run_id, created_at, updated_at)
            VALUES (%s, 'llm', %s, %s, %s, 'unresolved', %s, %s, %s, %s)
            ON CONFLICT (region_id, source)
            DO UPDATE SET text = EXCLUDED.text, type = EXCLUDED.type,
                          speaker_id = EXCLUDED.speaker_id, resolution_status = 'unresolved',
                          model_version = EXCLUDED.model_version, run_id = EXCLUDED.run_id,
                          updated_at = EXCLUDED.updated_at
            """,
            (region_id, block.get("corrected_text", ""), btype, speaker_id,
             model_name, run_id, now, now),
        )


def _provisional_speaker_id(block: dict, faces: list[dict]) -> Optional[int]:
    """Pass-1 블록의 얼굴 기반 화자 후보 → character_id (provisional 영속용).

    speech/monologue 블록에서 speaker.face_label이 이 컷 얼굴에 매핑되고
    confidence >= `_PENDING_SPEAKER_MAX_CONFIDENCE`(0.5)일 때만 반환한다.
    그 미만/얼굴 없음은 None — Pass-2a가 맥락으로 해소할 몫으로 남긴다.
    """
    if block.get("type") not in _SPEAKER_TYPES:
        return None
    sp = block.get("speaker") or {}
    label = sp.get("face_label")
    try:
        conf = float(sp.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0.0
    if not label or conf < _PENDING_SPEAKER_MAX_CONFIDENCE:
        return None
    for f in faces or []:
        if f.get("id") == label:
            return f.get("character_id")
    return None


def extract_cut(
    webtoon_episode_id: int,
    cut_number: int,
    faces: Optional[list[dict]] = None,
    regions: Optional[list[dict]] = None,
    belief: Optional[dict] = None,
    *,
    ep_info: Optional[dict] = None,
    webtoon_id: Optional[int] = None,
    ctx: Optional[dict] = None,
    persist: bool = True,
    run_id: Optional[int] = None,
) -> Pass1Record:
    """컷 1개를 비전 LLM 1콜로 분석해 Pass-1 provisional 레코드를 만든다(step3a 단위).

    흐름: regions/faces 로드(미전달 시) → 현재 컷 얼굴 오버레이 → 다운스케일 →
    비전 LLM 1콜(분류 먼저→화자 나중, strict JSON) → sanitize/1:1 검증 →
    provisional 적재(`resolution_status=unresolved`). 연속성용 이웃 컷 이미지는 동봉하지
    않는다(Req 1.2 — 연속성은 Pass-2 담당).

    faces/regions는 호출부(extract_episode, 5.2)가 미리 로드해 넘길 수 있고, 미전달 시
    `_load_faces`/`_load_regions`로 로드한다. `belief`(roster/pending/name_evidence)는
    5.2 윈도우 캐리오버 배선용 예약 파라미터다(현재 콜은 컷 국소 분석 — Req 1.2).
    반환 `Pass1Record`는 `result`(검증된 JSON) + belief 캐리오버(faces/name_evidence/
    provisional_speakers) + `usage`(LLMUsage 적재용)를 노출한다.

    Req 7.4 — 파싱/일시 오류는 1회 재시도하고, 그래도 실패하면 해당 컷만 빈 결과로 스킵
    (run 중단 금지). Req 1.10 — OCR·얼굴이 모두 없으면 비전 콜을 생략한다.
    """
    cut_id = _cut_id(webtoon_episode_id, cut_number)
    if cut_id is None:
        return Pass1Record(cut_number=cut_number, cut_id=None, skipped="no_cut")

    if regions is None:
        regions = _load_regions(cut_id)
    if faces is None:
        faces = _load_faces(cut_id)

    # Req 1.10 — OCR 텍스트도 얼굴도 없으면 비전 콜 생략(throughput).
    if not regions and not faces:
        return Pass1Record(cut_number=cut_number, cut_id=cut_id, faces=faces, skipped="empty")

    info = ep_info or _episode_info(webtoon_episode_id)
    img = fetch_cut_image(info["source"], info["title_id"], info["episode_no"], cut_number)
    if img is None:
        return Pass1Record(cut_number=cut_number, cut_id=cut_id, faces=faces, skipped="no_image")

    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)
    call_ctx = _pass1_ctx(ctx or resolve_llm_model(webtoon_id))

    overlay_img = _downscale(overlay_faces(img, faces), _PASS1_MAX_DIM)
    user_text = json.dumps({
        "identified_faces": [
            {"id": f["id"], "name": f.get("name"), "character_id": f.get("character_id"),
             "confirmed": bool(f.get("confirmed"))}
            for f in faces
        ],
        "ocr_blocks": [{"index": r["index"], "text": r["text"]} for r in regions],
    }, ensure_ascii=False)

    # Req 7.4 — 1회 재시도(총 2회). 모두 실패하면 빈 결과로 스킵.
    raw_result: dict = {}
    usage: dict = {}
    err: Optional[str] = None
    for _attempt in range(_PASS1_RETRIES):
        try:
            call = call_llm_json(call_ctx, _PASS1_SYSTEM_PROMPT, user_text, [overlay_img])
            raw_result = call.result if isinstance(call.result, dict) else {}
            usage = call.usage or {}
            err = None
            break
        except Exception as e:  # noqa: BLE001 — 컷 단위 격리(run 중단 금지)
            err = str(e)
            raw_result = {}
    if err is not None:
        logger.warning(
            "[step3.pass1] %s/%s ep=%s cut=%s — 비전 콜 실패(스킵): %s",
            info["source"], info["title_id"], info["episode_no"], cut_number, err,
        )
        return Pass1Record(cut_number=cut_number, cut_id=cut_id, faces=faces,
                           usage=usage, error=err)

    result = _sanitize_pass1(raw_result, regions)

    # provisional 적재(Req 1.9) — 블록은 1:1로 region에 매핑, scene meta는 cut_scene_meta.
    if persist:
        region_by_index = {r["index"]: r["region_id"] for r in regions}
        for block in result["blocks"]:
            rid = region_by_index.get(block["index"])
            if rid is not None:
                _upsert_provisional_annotation(
                    rid, block, call_ctx["name"],
                    speaker_id=_provisional_speaker_id(block, faces),
                    run_id=run_id,
                )
        _upsert_scene_meta(cut_id, {
            "action_summary": result["cut_summary"],
            "key_objects": result["key_objects"],
        }, run_id=run_id)

    # belief 캐리오버용 화자 후보(pending) — speech/monologue 블록만.
    provisional_speakers = [
        {"cut": cut_number, "block_index": b["index"],
         "face_label": b["speaker"]["face_label"], "name": b["speaker"]["name"],
         "confidence": b["speaker"]["confidence"], "basis": b["speaker"]["basis"]}
        for b in result["blocks"] if b["type"] in _SPEAKER_TYPES
    ]

    logger.info(
        "[step3.pass1] %s/%s ep=%s cut=%s — blocks=%s chars=%s name_evidence=%s",
        info["source"], info["title_id"], info["episode_no"], cut_number,
        len(result["blocks"]), len(result["characters"]), len(result["name_evidence"]),
    )

    return Pass1Record(
        cut_number=cut_number, cut_id=cut_id, result=result, faces=faces,
        name_evidence=result["name_evidence"], provisional_speakers=provisional_speakers,
        usage=usage,
    )


# ── Pass-1 에피소드 순회 (step3a) ─────────────────────────────────────────────

def _insert_llm_usage(
    webtoon_id: int,
    episode_id: Optional[int],
    cut_id: Optional[int],
    llm_model_id: Optional[int],
    usage: dict,
    *,
    stage: str = _PASS1_STAGE,
    image_count: Optional[int] = 1,
    extra: Optional[dict] = None,
    run_id: Optional[int] = None,
) -> None:
    """LLM 콜 1회당 `llm_usage` 1행 적재 — usage 적재의 **단일 진실원천**(Req 6.7).

    stage-aware: 두 콜 경로가 이 헬퍼 하나만 쓴다.
      - 비전 컷콜(step3a): stage='pass1', episode_id=<에피소드>, cut_id=<컷>, image_count=1.
      - 텍스트 에피소드콜(step3b): stage='pass2_resolve', episode_id=<에피소드>, cut_id=NULL,
        image_count=NULL.
    키 채움 규칙: 컷 단위 콜이면 cut_id를 채우고, 에피소드 단위 콜이면 cut_id=NULL로 둔다(task 10.1).

    `stage`는 service `LLMStage` enum(`pass1|pass2_resolve|step4`, `_USAGE_STAGES`)과 일치해야 한다.
    벗어난 값이면 경고만 남기고 그대로 적재한다(enum 위반은 DB가 막으며, run을 중단하지 않는다 — Req 7.4).
    토큰/finish는 `Pass1Record.usage`(= `llm_client` 반환 shape: prompt_tokens/completion_tokens/
    total_tokens/finish_reason). `extra`(jsonb, NULL 허용)는 부가 메타 passthrough다.
    `llm_model_id`는 NOT NULL/PROTECT FK이므로 미해석(None)이면 적재를 건너뛴다(폴백 모델).
    """
    if llm_model_id is None:
        logger.warning(
            "[step3.usage] llm_model_id 미해석 — usage 적재 생략 (webtoon=%s episode=%s cut=%s)",
            webtoon_id, episode_id, cut_id,
        )
        return
    if stage not in _USAGE_STAGES:
        logger.warning(
            "[step3.usage] 알 수 없는 stage=%r (허용: %s) — 그대로 적재 (webtoon=%s episode=%s cut=%s)",
            stage, "/".join(_USAGE_STAGES), webtoon_id, episode_id, cut_id,
        )
    now = datetime.now(timezone.utc)
    u = usage or {}
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_llm_usage
                (webtoon_id, episode_id, cut_id, stage, llm_model_id,
                 prompt_tokens, completion_tokens, total_tokens, image_count,
                 finish_reason, extra, run_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                webtoon_id, episode_id, cut_id, stage, llm_model_id,
                int(u.get("prompt_tokens", 0) or 0),
                int(u.get("completion_tokens", 0) or 0),
                int(u.get("total_tokens", 0) or 0),
                image_count,
                u.get("finish_reason"),
                Json(extra) if extra else None,
                run_id,
                now, now,
            ),
        )


def _init_belief() -> dict:
    """비어 있는 belief state(design Belief State). 비-영속(전이) — DB 저장 안 함."""
    return {
        "character_roster": [],   # [{character_id, known_name, status, last_seen_cut}]
        "pending_speakers": [],   # [{cut, block_index, candidates, confidence}]
        "name_evidence": [],      # [{character_id, face_label, name, confidence, source}]
        "open_questions": [],     # [str] — Pass-2a가 채움
    }


def _accumulate_belief(belief: dict, rec: "Pass1Record") -> None:
    """컷 1개의 Pass-1 결과를 belief state에 누적(in-place). Pass-2a 윈도우 캐리오버용.

    - character_roster: 이 컷에서 식별된 얼굴 중 character_id가 있는 인물을 webtoon 글로벌 키로
      upsert하고 last_seen_cut/known_name(placeholder 제외)을 갱신한다.
    - pending_speakers: face_label/name이 모두 없거나 confidence가 임계값 미만인 화자 후보를
      적재해 Pass-2a가 맥락으로 해소하게 둔다(과확신 금지 — Req 1.5/2.4).
    - name_evidence: Pass-1이 대사/나레이션에서 포착한 이름 증거를 누적한다(Req 1.8).
    """
    roster = belief["character_roster"]
    roster_by_id = {e["character_id"]: e for e in roster}
    for f in rec.faces or []:
        cid = f.get("character_id")
        if cid is None:
            continue
        fname = (f.get("name") or "").strip()
        known = fname if (fname and not fname.startswith("NEW_CHAR_")) else None
        entry = roster_by_id.get(cid)
        if entry is None:
            entry = {"character_id": cid, "known_name": known,
                     "status": "active", "last_seen_cut": rec.cut_number}
            roster.append(entry)
            roster_by_id[cid] = entry
        else:
            entry["last_seen_cut"] = rec.cut_number
            if known and not entry.get("known_name"):
                entry["known_name"] = known

    for ps in rec.provisional_speakers or []:
        face_label = ps.get("face_label")
        name = ps.get("name")
        conf = _clampf(ps.get("confidence", 0))
        if (face_label or name) and conf >= _PENDING_SPEAKER_MAX_CONFIDENCE:
            continue  # 충분히 확실한 화자 후보는 pending에 담지 않음
        candidates = [c for c in (face_label, name) if c]
        belief["pending_speakers"].append({
            "cut": ps.get("cut", rec.cut_number),
            "block_index": ps.get("block_index"),
            "candidates": candidates,
            "confidence": conf,
        })

    for ne in rec.name_evidence or []:
        belief["name_evidence"].append({
            "character_id": None,  # face_label→character_id 해소는 Pass-2a 담당
            "face_label": ne.get("face_label"),
            "name": ne.get("name"),
            "confidence": _clampf(ne.get("confidence", 0)),
            "source": f"cut{rec.cut_number}:pass1",
        })


def extract_episode(
    webtoon_episode_id: int,
    heartbeat_cb=None,
    *,
    prepare: bool = True,
    run_id: Optional[int] = None,
) -> ExtractResult:
    """에피소드의 모든 컷을 Pass-1(비전 1콜)로 순회 추출하고 belief state를 누적한다(step3a).

    DB의 webtoon_cut 순서(cut_number)대로 돌며 컷마다 `extract_cut`을 호출한다. 빈 컷
    (OCR·얼굴 모두 없음)은 `extract_cut`이 비전 콜을 생략하고 `skipped='empty'`로 반환하므로
    자연히 스킵된다(Req 1.10). 모델 ctx는 에피소드당 1회만 해석(`resolve_llm_model`)해 각 컷
    호출에 `ctx=`로 재사용한다(컷마다 재해석 방지). 비전 콜이 실제 일어난 컷마다 `llm_usage`에
    per-call 1행을 적재한다(stage='pass1', episode_id/cut_id 채움 — Req 6.7).

    belief state(character_roster/pending_speakers/name_evidence/open_questions)는 컷을 가로질러
    in-memory로 누적되며 반환 `ExtractResult.belief`로 Pass-2a 윈도잉에 전달된다(비-영속, Req 8.2).
    `heartbeat_cb`는 컷 1개를 처리할 때마다 누적 처리 컷 수로 호출해 액티비티 타임아웃 타이머를
    갱신한다(Req 9.4).

    prepare: True면 시작 시 `prepare_episode_scene`으로 기존 'llm' 어노테이션/scene_meta를 정리해
    이번 추출을 '완전 교체'로 만든다(에피소드 단위 재처리 — Req 10.1). 부분 재실행에선 False.
    옵션 K컷 배칭은 본 구현 범위 밖(필요 시 후속 — throughput 통제용).
    """
    info = _episode_info(webtoon_episode_id)
    webtoon_id = _get_webtoon_id(webtoon_episode_id)
    ctx = resolve_llm_model(webtoon_id)  # 에피소드당 1회 해석 → 컷마다 재사용
    llm_model_id = ctx.get("id")

    if prepare:
        prepare_episode_scene(webtoon_episode_id)

    with db_cursor() as cur:
        cur.execute(
            "SELECT cut_number FROM webtoon_cut WHERE episode_id = %s ORDER BY cut_number",
            (webtoon_episode_id,),
        )
        cut_numbers = [r[0] for r in cur.fetchall()]

    belief = _init_belief()
    analyzed = 0
    skipped = 0
    agg = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "calls": 0}

    # 컷 간 belief 의존 없음(연속성은 Pass-2 담당 — Req 1.2)이라 extract_cut 호출 자체는
    # 컷을 가로질러 동시 처리한다(step2._fetch_and_embed_all과 같은 패턴). 실제 동시 요청 수는
    # llm_client._LLM_SEMAPHORE가 최종 가드. belief는 컷 국소 분석에 안 쓰이는 예약
    # 파라미터라 병렬 호출에는 넘기지 않는다(넘겨봐야 애초에 "직전 컷 반영"과 병렬은 상충).
    # 완료 순서는 뒤섞이므로, 순서 의존 후처리(belief 누적/usage 집계)는 cut_number로
    # 재정렬한 뒤 단일 스레드로 수행한다.
    records_by_cut: dict[int, Pass1Record] = {}
    processed = 0
    workers = min(_PASS1_WORKERS, len(cut_numbers)) or 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(extract_cut, webtoon_episode_id, cn,
                        ep_info=info, webtoon_id=webtoon_id, ctx=ctx, run_id=run_id): cn
            for cn in cut_numbers
        }
        for future in as_completed(futures):
            cn = futures[future]
            records_by_cut[cn] = future.result()
            processed += 1
            if heartbeat_cb:
                heartbeat_cb(processed)

    records = [records_by_cut[cn] for cn in cut_numbers]
    for rec in records:
        if rec.skipped is not None:
            skipped += 1
        else:
            # 비전 콜이 일어난 컷 — provisional 결과를 belief에 누적. _accumulate_belief의
            # last_seen_cut 갱신이 순서에 의존하므로 반드시 cut_number 순으로 호출한다.
            _accumulate_belief(belief, rec)

        # 비전 콜이 실제 완료된 컷만 per-call usage 적재(Req 6.7). 빈컷/이미지없음/에러는 제외.
        if rec.skipped is None and rec.usage:
            analyzed += 1
            _insert_llm_usage(webtoon_id, webtoon_episode_id, rec.cut_id,
                              llm_model_id, rec.usage, stage=_PASS1_STAGE, image_count=1,
                              run_id=run_id)
            agg["prompt_tokens"] += int(rec.usage.get("prompt_tokens", 0) or 0)
            agg["completion_tokens"] += int(rec.usage.get("completion_tokens", 0) or 0)
            agg["total_tokens"] += int(rec.usage.get("total_tokens", 0) or 0)
            agg["calls"] += 1

    logger.info(
        "[step3.pass1] episode %s — %s컷 중 분석=%s 스킵=%s, roster=%s pending=%s name_evidence=%s tokens=%s",
        webtoon_episode_id, len(cut_numbers), analyzed, skipped,
        len(belief["character_roster"]), len(belief["pending_speakers"]),
        len(belief["name_evidence"]), agg["total_tokens"],
    )

    return ExtractResult(
        webtoon_episode_id=webtoon_episode_id,
        records=records,
        belief=belief,
        cuts_total=len(cut_numbers),
        cuts_analyzed=analyzed,
        cuts_skipped=skipped,
        usage_total=agg,
    )


# ── Pass-2a (에피소드 전역 해소, step3b) ─────────────────────────────────────
# 정본: qwen-vl/_pass2.py SYS 프롬프트를 그대로 이관(character_id 정체성, mis-ID distrust +
# label_conflict, 텍스트 진실성 등급 + deceptions, beats/episode). 추가: threads[](떡밥, design
# 계약). 이미지 없이 에피소드 전체 Pass-1 레코드(읽기순) + 누적 서사 컨텍스트(prior)를 텍스트 LLM
# 1콜로 해소한다(Req 2.1). 윈도우 분할/belief 캐리오버(Req 8)는 task 6.2가 본 단일콜 경로를
# 감싼다 — 본 태스크는 단일콜 경로만 구현한다.
_PASS2_STAGE = "resolve"        # LLMUsage.stage — Stage R 정체·화자 해소 콜(step3b 1/2).
_NARRATIVE_STAGE = "narrative"  # LLMUsage.stage — Stage N 서사 분석 콜(step3b 2/2).
_PASS2_MIN_MAX_TOKENS = 16384    # 대용량 구조화 출력(characters/beats/...) 절단 방지(넉넉히).
# speaker_resolution이 '불확실 블록만'에서 '모든 speech/monologue 전수 테이블'로 바뀌어(2026-07-05)
# 출력량이 크게 늘었고, 추론형 모델의 reasoning 소모까지 감안해 16384로 상향.
_PASS2_MAX_TEMPERATURE = 0.2     # 해소는 결정론 지향(0.0~0.2).
_PASS2_RETRIES = _PASS1_RETRIES  # 파싱/일시 오류 1회 재시도(총 2회 시도).
_SIGNIFICANCE = ("main", "supporting", "minor_functional", "extra")  # Req 2.3

_RESOLVE_SYSTEM_PROMPT = (
    "당신은 웹툰 에피소드 전체를 보고 컷별 provisional 분석의 **인물 정체와 화자를 전역 해소**하는 "
    "분석기입니다(Stage R). ⚠️ **모든 자연어 출력은 반드시 한국어로 작성한다.** "
    "정체성 기준은 character_id(Step2가 부여한 안정적 인물/클러스터 id)입니다. 컷 내 F라벨은 그 컷 한정. "
    "이름이 없는 character_id는 아직 명명되지 않은 얼굴 클러스터다 — 대사/호칭/나레이션 근거로 실명을 "
    "확정할 수 있으면 name에 제시하라. "
    "⚠️ **중요**: faces의 confirmed=false인 character_id는 Step2 **얼굴인식 추정값**이며 정답이 아니다. "
    "**대사·호칭·맥락 증거가 얼굴 라벨보다 우선**한다. "
    "특히 **서사와 모순되는 얼굴 라벨**(예: 이미 죽은 인물이 다른 시대에 살아 등장, 나이/시대 불일치)은 "
    "**오인식(mis-ID)으로 의심**하고, name은 대사 근거로 정한 뒤 label_conflict에 사유를 적어라. "
    "단 **confirmed=true 얼굴과 confirmed_roster_prior는 human 확정 정답으로 동결**한다(강등·재라벨 금지, "
    "화자 판정에도 그대로 신뢰). "
    "에피소드 전체 맥락(앞뒤 컷)을 활용해 다음을 **JSON만** 출력:\n"
    "1) characters: 에피소드에 등장한 character_id별로 "
    "{character_id, name(대사/나레이션/호칭 근거로 확정된 실제 이름 또는 null), "
    "significance(main|supporting|minor_functional|extra: 재등장/대사량/서사역할 기준; "
    "minor_functional은 '화산파 제자A'식 기능라벨, extra는 행인), "
    "name_confidence(0~1), evidence, "
    "label_conflict(얼굴 라벨과 대사/맥락이 충돌하면 'Step2는 X로 인식했으나 대사상 Y' 식 설명, 없으면 null), "
    "merge_suggestion([같은 인물로 보이는 다른 character_id들])}. "
    "병합은 **제안만**(확신 있을 때만).\n"
    "2) speaker_resolution: **모든 speech/monologue 블록에 대한 전수 화자 테이블** → "
    "[{cut, block_index, character_id(또는 null), confidence, reason}]. "
    "블록의 spk_face/spk_cid(Pass-1 얼굴 기반 후보)가 맥락과 맞으면 그 인물로 **확인**하고, "
    "앞뒤 맥락·대화 흐름상 다른 인물이면 **교체**하라(교차 대화에서 말풍선 꼬리가 없는 블록은 "
    "직전/직후 발화자와의 문답 관계로 추론). 진짜 판단 불가일 때만 character_id null. "
    "**speech/monologue 블록을 빠뜨리지 마라.**\n"
    "3) face_reassignments: confirmed=false 얼굴 중 **그 컷의 대사/호칭/맥락상 현재 cid 배정이 "
    "명백히 틀린 얼굴**만 → [{cut, face(그 컷의 F라벨), "
    "to_character_id(올바른 인물의 character_id — 로스터에 실재하는 id만, 누군지 모르면 null), "
    "evidence(컷·대사 근거), confidence(0~1)}]. "
    "인물 전반의 의심(특정 얼굴을 못 짚음)은 여기가 아니라 characters[].label_conflict에 적어라. "
    "확신 있는 것만, 없으면 빈 배열.\n"
    "없는 정보는 지어내지 말 것(null). **자연어는 반드시 한국어.**"
)

# Stage N — 서사 분석(정체·화자가 정정된 트랜스크립트 입력, 이미지 없음). §17.4/§17.5.
_NARRATIVE_SYSTEM_PROMPT = (
    "당신은 화자·정체가 확정된 웹툰 에피소드 트랜스크립트를 읽고 **서사를 분석**하는 분석기입니다(Stage N). "
    "⚠️ **모든 자연어 출력은 반드시 한국어로 작성한다. 영어 등 다른 언어로 답하지 말 것.** "
    "📜 **텍스트 진실성 등급**: narration/system=작가의 객관적 진실(전개 골격), monologue=인물의 진짜 속마음/의도(정직), "
    "speech=남에게 한 말로 **거짓·과장·책략일 수 있는 주장**. "
    "confirmed_roster_prior(이전 화까지의 인물 도감)와 open_threads(미회수 떡밥)를 서사 기준선으로 삼아라. "
    "먼저 인물별 speech/monologue 타임라인을 정리하고 narration으로 전개를 잡은 뒤, **주장 vs 진실 괴리를 적극 탐색**하라. "
    "다음을 **JSON만** 출력:\n"
    "1) beats: 연속 컷을 서사 단위로 묶음 → [{cut_start, cut_end, hook_type(자유 텍스트), "
    "appeal_point(소구포인트 한 줄), intensity(0~1)}]. 비트 개수 제약 없음(에피소드 전체가 1비트일 수도).\n"
    "2) episode: {summary, teaser, appeal_point(핵심 소구포인트), cliffhanger, foreshadowing:[...]}. "
    "**summary(정보성, 스포 허용, 2~3문장)**: narration과 컷에서 실제 일어난 사건에만 근거. "
    "roster에 이미 있는 인물은 **수식어 없이 이름만** 쓴다('파문당한 귀족 에드' 금지 — 소개는 첫 등장 화에서 끝났다). "
    "근거 없는 낙인·동기 추측·강한 단정(예: '분탕', '배신자' 같은 평가어) 금지. "
    "**teaser(궁금증 유발 카피, 1~2문장, 스포 금지)**: 이번 화에서 밝혀진 진실(해소된 떡밥의 답, "
    "폭로된 거짓)은 절대 언급하지 말고, 미회수 떡밥은 암시만 하라. 질문형/여운형 문장 허용.\n"
    "3) deceptions: 인물이 **다른 인물을 속이려는 의도로** speech로 한 거짓/과장/책략(진실과 충돌) → "
    "[{cut, character_id, claim(주장 내용), contradicts(어떤 진실과 충돌하는지), confidence}]. "
    "⚠️ monologue(속마음)·혼잣말·자조·한탄·수사적 표현은 **deception이 아니다**(속일 상대가 없음). "
    "확실한 것만, 없으면 빈 배열.\n"
    "4) threads: 떡밥/복선/미스터리/목표 등 단위 경계를 가로지르는 서사 실 → "
    "[{description, type(foreshadowing|mystery|goal|...), status(open|resolved), "
    "planted_episode, planted_cut, resolved_episode, resolved_cut, confidence}]. "
    "이번 화에서 심긴 것은 open, 이번 화에서 해소된 것은 resolved. 없으면 빈 배열.\n"
    "5) profiles: 이번 화에 **근거가 있는** 인물 메타 갱신 → [{character_id, "
    "profile{gender(남|여|null), age_group(아동|10대|20대|30대|중년|노년|null), "
    "affiliation(소속 집단/가문/조직 또는 null), role(서사 역할 한 줄), "
    "personality([성격 키워드 1~4개]), traits({그 외 장르 특이 정보, 예: 신분/직업/능력 — 자유 key:value}), "
    "key_facts([이번 화에서 확인된 사실 문장들])}]. 모르면 항목 생략(지어내기 금지).\n"
    "없는 정보는 지어내지 말 것(null). **자연어는 반드시 한국어.**"
)


@dataclass
class ResolveResult:
    """에피소드 전역 해소(step3b, Pass-2a) 결과 — step3c(apply_resolution) 입력용 전이 객체.

    정본 계약(`_pass2.py`) + design 추가 `threads[]`:
      - characters: [{character_id, name, significance, name_confidence, evidence,
                      label_conflict, merge_suggestion}] (Req 2.3, 3.2)
      - speaker_resolution: [{cut, block_index, character_id, confidence, reason}] (Req 2.4)
      - face_reassignments: [{cut, face(F라벨), to_character_id, evidence, confidence}]
        — 얼굴 단위 재배정 제안(suggestion type=face_reassign 원료, §17.8 후속 구현)
      - beats: [{cut_start, cut_end, hook_type, appeal_point, intensity}] (Req 2.5)
      - episode: {summary, appeal_point, cliffhanger, foreshadowing} (Req 2.6)
      - deceptions: [{cut, character_id, claim, contradicts, confidence}] (Req 2.8)
      - threads: [{description, type, status, planted_episode, planted_cut,
                   resolved_episode, resolved_cut, confidence}] (Req 11.2)
    `usage`는 LLMUsage 적재용(Req 6.7, stage='pass2_resolve'). `error`가 채워지면 텍스트 콜이
    재시도 후에도 실패해 빈 결과로 스킵된 에피소드다(run 중단 금지 — Req 7.4).
    """

    webtoon_episode_id: Optional[int]
    characters: list[dict] = field(default_factory=list)
    speaker_resolution: list[dict] = field(default_factory=list)
    face_reassignments: list[dict] = field(default_factory=list)  # Stage R 얼굴 재배정 제안
    beats: list[dict] = field(default_factory=list)
    episode: dict = field(default_factory=dict)
    deceptions: list[dict] = field(default_factory=list)
    threads: list[dict] = field(default_factory=list)
    profiles: list[dict] = field(default_factory=list)  # Stage N 산출 — 인물 메타 delta(v4.0)
    usage: dict = field(default_factory=dict)
    error: Optional[str] = None


# ── Pass-2a 입력/출력 헬퍼 ────────────────────────────────────────────────────

def _opt_int(v) -> Optional[int]:
    """정수 또는 None(bool은 거부)."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _opt_str(v) -> Optional[str]:
    """비어 있지 않은 문자열이면 trim, 아니면 None."""
    return v.strip() if isinstance(v, str) and v.strip() else None


def _str_or_empty(v) -> str:
    """문자열이면 그대로, 아니면 빈 문자열(null 자연어 필드 정규화)."""
    return v if isinstance(v, str) else ""


def _prior_to_dict(prior) -> dict:
    """prior_context → Pass-2a prior dict. CumulativeContext면 to_prompt_dict() 사용.

    Accept either a `CumulativeContext`(narrative_context.load_prior 반환) 또는 그 dict, 또는 None.
    프로토타입 계약(`confirmed_roster_prior` 키)을 항상 유지한다.
    """
    if prior is None:
        return {"confirmed_roster_prior": []}
    to_prompt = getattr(prior, "to_prompt_dict", None)
    if callable(to_prompt):
        d = to_prompt()
        return d if isinstance(d, dict) else {"confirmed_roster_prior": []}
    if isinstance(prior, dict):
        return prior
    return {"confirmed_roster_prior": []}


def _build_pass2_user_payload(records: list["Pass1Record"], prior) -> dict:
    """Pass-1 레코드(읽기순) + prior → Pass-2a user payload(텍스트 트랜스크립트).

    `_pass2.py`의 build_transcript 정본을 Pass1Record로 이관: 컷별 cut_summary, blocks
    (index/type/corrected_text/provisional speaker), name_evidence, faces(F→character_id),
    그리고 등장 character_id roster(등장 컷 수)를 직렬화한다. prior(`confirmed_roster_prior`/
    open_threads/running_summary)를 합쳐 넘긴다. 스킵/에러/빈 컷은 제외한다.

    task 6.2(적응형 윈도우)는 이 헬퍼를 records 서브셋으로 반복 호출해 윈도우 payload를 만들 수
    있다(belief 캐리오버는 6.2에서 prior에 주입). 본 태스크는 전체 records 1콜만 사용한다.
    """
    transcript: list[dict] = []
    roster: dict = {}
    for rec in records:
        if rec.skipped or rec.error:
            continue
        res = rec.result or {}
        if not res:
            continue
        cut = rec.cut_number
        faces = rec.faces or []
        for f in faces:
            cid = f.get("character_id")
            if cid is None:
                continue
            entry = roster.setdefault(
                cid, {"character_id": cid, "known_name": None, "cuts": set()}
            )
            entry["cuts"].add(cut)
            fname = (f.get("name") or "").strip()
            if fname and not fname.startswith("NEW_CHAR_") and not entry["known_name"]:
                entry["known_name"] = fname
        blocks = []
        for b in (res.get("blocks") or []):
            sp = b.get("speaker") or {}
            blocks.append({
                "i": b.get("index"),
                "type": b.get("type"),
                "text": b.get("corrected_text"),
                "spk_face": sp.get("face_label"),
                "spk_name": sp.get("name"),
                "spk_cid": sp.get("character_id"),  # provisional 화자(character_id) — 재해소 경로 복원값
                "conf": sp.get("confidence"),
                "tail": sp.get("tail_hint"),
            })
        transcript.append({
            "cut": cut,
            "summary": res.get("cut_summary"),
            "faces": [{"F": f.get("id"), "cid": f.get("character_id"), "name": f.get("name"),
                       "confirmed": bool(f.get("confirmed"))}
                      for f in faces],
            "chars": res.get("characters") or [],
            "blocks": blocks,
            "name_evidence": res.get("name_evidence") or [],
        })
    roster_list = [
        {"character_id": r["character_id"], "known_name": r["known_name"],
         "appears_in_cuts": len(r["cuts"])}
        for r in roster.values()
    ]
    payload = dict(_prior_to_dict(prior))  # confirmed_roster_prior(+open_threads/running_summary)
    payload.setdefault("confirmed_roster_prior", [])
    payload["character_roster"] = roster_list
    payload["cuts"] = transcript
    return payload


def _pass2_ctx(ctx: dict) -> dict:
    """텍스트 해소 콜 전용 ctx 사본 — max_tokens 넉넉히, temperature<=0.2 강제.

    현 `resolve_llm_model`은 stage(비전/텍스트)별 해석을 지원하지 않으므로 호출부가 해석한 기본
    ctx를 그대로 받아 파라미터만 보정한다(코드에 모델명 하드코딩 없음 — Req 7.1). 텍스트 모델
    분리 해석은 후속(llm_resolver stage 인자) 과제다.
    """
    params = dict(ctx.get("params") or {})
    try:
        temp = float(params.get("temperature", _PASS2_MAX_TEMPERATURE))
    except (TypeError, ValueError):
        temp = _PASS2_MAX_TEMPERATURE
    params["temperature"] = max(0.0, min(_PASS2_MAX_TEMPERATURE, temp))
    try:
        mt = int(params.get("max_tokens") or 0)
    except (TypeError, ValueError):
        mt = 0
    params["max_tokens"] = max(mt, _PASS2_MIN_MAX_TOKENS)
    out = dict(ctx)
    out["params"] = params
    return out


def _sanitize_profile(raw) -> dict:
    """characters[].profile 정규화 — 인물도감용 범용 메타(Req: 장르 불문 공통 키 + 자유 traits).

    알려진 키(gender/age_group/affiliation/role)는 비어있지 않은 문자열만, personality는
    문자열 리스트, traits는 str→str dict만 통과. 근거 없는(빈) 값은 키 자체를 뺀다.
    """
    if not isinstance(raw, dict):
        return {}
    out: dict = {}
    for key in ("gender", "age_group", "affiliation", "role"):
        v = _opt_str(raw.get(key))
        if v:
            out[key] = v
    pers = raw.get("personality")
    if isinstance(pers, list):
        pers = [p.strip() for p in pers if isinstance(p, str) and p.strip()]
        if pers:
            out["personality"] = pers[:8]
    traits = raw.get("traits")
    if isinstance(traits, dict):
        clean = {str(k)[:40]: str(v)[:200] for k, v in traits.items()
                 if v not in (None, "") and str(k).strip()}
        if clean:
            out["traits"] = clean
    return out


def _sanitize_resolve_characters(raw) -> list[dict]:
    out: list[dict] = []
    if not isinstance(raw, list):
        return out
    for c in raw:
        if not isinstance(c, dict):
            continue
        sig = c.get("significance")
        if sig not in _SIGNIFICANCE:
            sig = None
        merge = c.get("merge_suggestion")
        if isinstance(merge, list):
            merge = [m for m in (_opt_int(x) for x in merge) if m is not None]
        else:
            mi = _opt_int(merge)
            merge = [mi] if mi is not None else []
        out.append({
            "character_id": _opt_int(c.get("character_id")),
            "name": _opt_str(c.get("name")),
            "significance": sig,
            "name_confidence": _clampf(c.get("name_confidence", 0)),
            "evidence": _str_or_empty(c.get("evidence")),
            "label_conflict": _opt_str(c.get("label_conflict")),
            "merge_suggestion": merge,
        })
    return out


def _sanitize_speaker_resolution(raw) -> list[dict]:
    out: list[dict] = []
    if not isinstance(raw, list):
        return out
    for s in raw:
        if not isinstance(s, dict):
            continue
        out.append({
            "cut": _opt_int(s.get("cut")),
            "block_index": _opt_int(s.get("block_index")),
            "character_id": _opt_int(s.get("character_id")),
            "confidence": _clampf(s.get("confidence", 0)),
            "reason": _str_or_empty(s.get("reason")),
        })
    return out


def _sanitize_face_reassignments(raw) -> list[dict]:
    """Stage R face_reassignments 정규화 — [{cut, face("F0"), to_character_id|None, evidence,
    confidence}]. face는 F라벨 문자열로 통일(정수 face_idx도 수용), (cut, face) 중복은
    confidence 높은 쪽만 유지. 실재/동결 검증은 커밋부(_commit_suggestions)가 DB 대조로 수행."""
    if not isinstance(raw, list):
        return []
    best: dict[tuple, dict] = {}
    order: list[tuple] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        cut = _opt_int(r.get("cut"))
        face = r.get("face")
        if isinstance(face, bool):
            face = None
        elif isinstance(face, int):
            face = f"F{face}"
        face = _opt_str(face)
        if cut is None or not face:
            continue
        face = face.upper()
        if not face.startswith("F"):
            face = f"F{face}"
        if not face[1:].isdigit():
            continue
        item = {
            "cut": cut,
            "face": face,
            "to_character_id": _opt_int(r.get("to_character_id")),
            "evidence": _str_or_empty(r.get("evidence")),
            "confidence": _clampf(r.get("confidence", 0)),
        }
        key = (cut, face)
        if key not in best:
            best[key] = item
            order.append(key)
        elif item["confidence"] > best[key]["confidence"]:
            best[key] = item
    return [best[k] for k in order]


def _sanitize_beats(raw) -> list[dict]:
    out: list[dict] = []
    if not isinstance(raw, list):
        return out
    for b in raw:
        if not isinstance(b, dict):
            continue
        out.append({
            "cut_start": _opt_int(b.get("cut_start")),
            "cut_end": _opt_int(b.get("cut_end")),
            "hook_type": _str_or_empty(b.get("hook_type")),  # free-form (Req 6.6)
            "appeal_point": _str_or_empty(b.get("appeal_point")),
            "intensity": _clampf(b.get("intensity", 0)),
        })
    return out


def _sanitize_episode_meta(raw) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    fs = raw.get("foreshadowing")
    foreshadowing: list = []
    if isinstance(fs, list):
        for x in fs:
            if isinstance(x, str) and x.strip():
                foreshadowing.append(x.strip())
            elif isinstance(x, dict):
                foreshadowing.append(x)
    elif isinstance(fs, str) and fs.strip():
        foreshadowing = [fs.strip()]
    return {
        "summary": _str_or_empty(raw.get("summary")),
        "teaser": _str_or_empty(raw.get("teaser")),
        "appeal_point": _str_or_empty(raw.get("appeal_point")),
        "cliffhanger": _str_or_empty(raw.get("cliffhanger")),
        "foreshadowing": foreshadowing,
    }


def _sanitize_deceptions(raw) -> list[dict]:
    out: list[dict] = []
    if not isinstance(raw, list):
        return out
    for d in raw:
        if not isinstance(d, dict):
            continue
        out.append({
            "cut": _opt_int(d.get("cut")),
            "character_id": _opt_int(d.get("character_id")),
            "claim": _str_or_empty(d.get("claim")),
            "contradicts": _str_or_empty(d.get("contradicts")),
            "confidence": _clampf(d.get("confidence", 0)),
        })
    return out


def _sanitize_threads(raw) -> list[dict]:
    out: list[dict] = []
    if not isinstance(raw, list):
        return out
    for t in raw:
        if not isinstance(t, dict):
            continue
        status = t.get("status")
        if status not in ("open", "resolved"):
            status = "open"
        out.append({
            "description": _str_or_empty(t.get("description")),
            "type": _str_or_empty(t.get("type")),
            "status": status,
            "planted_episode": _opt_int(t.get("planted_episode")),
            "planted_cut": _opt_int(t.get("planted_cut")),
            "resolved_episode": _opt_int(t.get("resolved_episode")),
            "resolved_cut": _opt_int(t.get("resolved_cut")),
            "confidence": _clampf(t.get("confidence", 0)),
        })
    return out


def _sanitize_profiles(raw) -> list[dict]:
    """Stage N profiles 산출 정규화 — [{character_id, profile{...}}]."""
    out: list[dict] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        cid = _opt_int(item.get("character_id"))
        prof = _sanitize_profile(item.get("profile") if isinstance(item.get("profile"), dict) else item)
        # key_facts는 profile 밖에 실려올 수도 있어 별도 수용.
        kf = item.get("key_facts") or (item.get("profile") or {}).get("key_facts") if isinstance(item.get("profile"), dict) else item.get("key_facts")
        if isinstance(kf, list):
            kf = [str(x).strip() for x in kf if str(x).strip()]
            if kf:
                prof["key_facts"] = kf[:12]
        if cid is not None and prof:
            out.append({"character_id": cid, "profile": prof})
    return out


def _sanitize_resolve(result: dict) -> dict:
    """Stage R 원시 출력 → 계약 dict 정규화(characters + 전수 speaker_resolution)."""
    result = result if isinstance(result, dict) else {}
    return {
        "characters": _sanitize_resolve_characters(result.get("characters")),
        "speaker_resolution": _sanitize_speaker_resolution(result.get("speaker_resolution")),
        "face_reassignments": _sanitize_face_reassignments(result.get("face_reassignments")),
    }


def _sanitize_narrative(result: dict) -> dict:
    """Stage N 원시 출력 → 계약 dict 정규화(beats/episode(teaser)/deceptions/threads/profiles)."""
    result = result if isinstance(result, dict) else {}
    return {
        "beats": _sanitize_beats(result.get("beats")),
        "episode": _sanitize_episode_meta(result.get("episode")),
        "deceptions": _sanitize_deceptions(result.get("deceptions")),
        "threads": _sanitize_threads(result.get("threads")),
        "profiles": _sanitize_profiles(result.get("profiles")),
    }


def resolve_episode(
    ep: "ExtractResult | int",
    prior_context=None,
    *,
    records: Optional[list["Pass1Record"]] = None,
    webtoon_id: Optional[int] = None,
    ctx: Optional[dict] = None,
    persist_usage: bool = True,
    run_id: Optional[int] = None,
) -> ResolveResult:
    """에피소드 전체 Pass-1 레코드를 텍스트 LLM 1콜로 전역 해소한다(step3b, Pass-2a).

    입력: 에피소드 전체 Pass-1 레코드(읽기순) + 누적 서사 컨텍스트(prior). **이미지 없이**
    텍스트 LLM으로 화자/이름/중요도를 전역 해소하고 beats/episode/deceptions/threads를 산출한다
    (Req 2.1~2.9, 3.1~3.3). 시스템 프롬프트는 `_RESOLVE_SYSTEM_PROMPT`(Stage R, v4.0 §17.4).

    `ep`는 step3a 산출 `ExtractResult`(records + webtoon_episode_id 보유) 또는 webtoon_episode_id
    정수다. 정수면 `records=`로 Pass-1 레코드를 명시해야 한다(본 단계는 Pass-1을 재실행하지 않음).
    prior_context는 `CumulativeContext`(narrative_context.load_prior 반환) 또는 그 dict 또는 None을
    받는다(to_prompt_dict() 우선 — Req 4.1).

    모델 ctx는 `resolve_llm_model`로 해석한다(코드 하드코딩 없음 — Req 7.1). 현 resolver는
    stage(비전/텍스트) 분리 인자를 지원하지 않아 기본 모델을 텍스트 콜에 그대로 쓴다(텍스트 모델
    분리 해석은 후속). max_tokens는 대용량 구조화 출력에 맞춰 넉넉히, temperature는 0.0~0.2.

    Req 7.4 — 파싱/일시 오류는 1회 재시도하고, 그래도 실패하면 빈 결과(+error)로 반환해 run을
    중단하지 않는다. 콜이 완료되면 `llm_usage`에 per-call 1행 적재(stage='pass2_resolve',
    episode_id=<에피소드>, cut_id=NULL — Req 6.7). 적응형 윈도우/belief 캐리오버(Req 8)는 task
    6.2가 본 단일콜 경로를 감싼다.
    """
    if isinstance(ep, ExtractResult):
        webtoon_episode_id = ep.webtoon_episode_id
        if records is None:
            records = ep.records
    elif isinstance(ep, int):
        webtoon_episode_id = ep
        if records is None:
            raise ValueError("resolve_episode: ep가 webtoon_episode_id 정수면 records= 를 명시해야 합니다")
    else:
        raise TypeError(f"resolve_episode: ep는 ExtractResult 또는 int 여야 합니다(got {type(ep)!r})")

    records = records or []
    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)
    if ctx is None:
        ctx = resolve_llm_model(webtoon_id)
    call_ctx = _pass2_ctx(ctx)

    payload = _build_pass2_user_payload(records, prior_context)
    user_text = json.dumps(payload, ensure_ascii=False)

    # Req 7.4 — 1회 재시도(총 2회). 모두 실패하면 빈 결과(+error)로 스킵. 텍스트 콜이므로 images=[].
    raw_result: dict = {}
    usage: dict = {}
    err: Optional[str] = None
    for _attempt in range(_PASS2_RETRIES):
        try:
            call = call_llm_json(call_ctx, _RESOLVE_SYSTEM_PROMPT, user_text, [])
            raw_result = call.result if isinstance(call.result, dict) else {}
            usage = call.usage or {}
            err = None
            break
        except Exception as e:  # noqa: BLE001 — 에피소드 단위 격리(run 중단 금지)
            err = str(e)
            raw_result = {}

    if err is not None:
        logger.warning(
            "[step3.pass2] episode %s — 텍스트 해소 콜 실패(스킵): %s", webtoon_episode_id, err,
        )
        return ResolveResult(webtoon_episode_id=webtoon_episode_id, usage=usage, error=err)

    sanitized = _sanitize_resolve(raw_result)

    # per-call usage 적재(Req 6.7) — 에피소드 텍스트 콜: episode_id 채움, cut_id=NULL, image_count=NULL.
    if persist_usage:
        _insert_llm_usage(
            webtoon_id, webtoon_episode_id, None, ctx.get("id"), usage,
            stage=_PASS2_STAGE, image_count=None, run_id=run_id,
        )

    logger.info(
        "[step3.resolve] episode %s — characters=%s speaker_resolution=%s tokens=%s",
        webtoon_episode_id, len(sanitized["characters"]), len(sanitized["speaker_resolution"]),
        (usage or {}).get("total_tokens"),
    )

    return ResolveResult(
        webtoon_episode_id=webtoon_episode_id,
        characters=sanitized["characters"],
        speaker_resolution=sanitized["speaker_resolution"],
        face_reassignments=sanitized["face_reassignments"],
        usage=usage,
    )


# ── Pass-2a 적응형 윈도우 + belief 캐리오버 (step3b, task 6.2) ───────────────────
# `resolve_episode`(단일 텍스트 콜)를 **감싸는** 적응형 윈도잉 래퍼. 모델 토큰 예산이 크면
# 에피소드 전체를 1콜로(= 기존 단일콜 경로 그대로 위임), 작으면(예: 16K) 레코드를 읽기순 윈도우로
# 자동 분할해 각 윈도우를 해소하고 belief state를 경계 너머로 캐리오버한다(Req 8.1~8.3). 동일 해소
# 로직(`resolve_episode`)이 16K/130K 양쪽에서 1급으로 동작한다(로컬 폴백 = 1급 경로). 윈도우당
# 1콜 = `llm_usage` 1행(resolve_episode가 적재) — 중복/누락 없음.

_CHARS_PER_TOKEN = 2.0  # narrative_context와 동일한 한글 위주 보수적 토큰 추정 계수.
# 현 resolver(params)에 명시 예산 키가 없을 때의 기본값. 크게 잡아 **기본은 단일콜**(기존 동작
# 보존) — 작은 예산은 호출부가 token_budget= 또는 params(context_window/max_context/token_budget)로
# 명시한다(코드에 모델명 하드코딩 없음 — Req 7.1).
_DEFAULT_TOKEN_BUDGET = 128_000
# 윈도우 분할 적합성 판단 시 belief 캐리오버가 더해질 여유(다음 윈도우 prior에 carried roster/threads가
# 누적되므로 base prior 기준 추정에 헤드룸을 둔다).
_WINDOW_BUDGET_MARGIN = 0.85


def _resolve_token_budget(ctx: dict, token_budget: Optional[int]) -> int:
    """Pass-2a 윈도우 크기를 정하는 모델 토큰 예산을 해석(Req 8.1).

    우선순위: 명시 인자(token_budget) > ctx.params의 예산 키(context_window|max_context|
    token_budget) > 모듈 기본값(_DEFAULT_TOKEN_BUDGET, 단일콜 보존). 모델명 하드코딩 없음.
    """
    if token_budget is not None:
        try:
            b = int(token_budget)
            if b > 0:
                return b
        except (TypeError, ValueError):
            pass
    params = (ctx or {}).get("params") or {}
    for key in ("context_window", "max_context", "token_budget"):
        v = params.get(key)
        if v:
            try:
                b = int(v)
                if b > 0:
                    return b
            except (TypeError, ValueError):
                continue
    return _DEFAULT_TOKEN_BUDGET


def _estimate_payload_tokens(records: list["Pass1Record"], prior) -> int:
    """윈도우 payload(system prompt + user payload)의 대략적 토큰 수(chars/2.0 휴리스틱).

    `_build_pass2_user_payload`로 실제 직렬화될 payload를 만들어 길이를 추정한다(narrative_context의
    approx_tokens와 동일 계수). 분할/단일콜 분기 판단에 쓴다.
    """
    payload = _build_pass2_user_payload(records, prior)
    text = json.dumps(payload, ensure_ascii=False)
    return int((len(text) + len(_RESOLVE_SYSTEM_PROMPT)) / _CHARS_PER_TOKEN)


def _active_records(records: list["Pass1Record"]) -> list["Pass1Record"]:
    """payload에 실제 기여하는 레코드만(스킵/에러/빈결과 제외 — payload builder와 동일 필터)."""
    return [r for r in records if not (r.skipped or r.error) and (r.result or {})]


def _split_records_to_windows(
    records: list["Pass1Record"], prior, budget: int,
) -> list[list["Pass1Record"]]:
    """active 레코드를 읽기순 윈도우로 분할 — 각 윈도우가 (margin 적용) 예산에 맞도록 greedy.

    belief 캐리오버가 다음 윈도우 prior에 누적되므로 base prior 기준 추정에 `_WINDOW_BUDGET_MARGIN`
    헤드룸을 둔다. 단일 레코드가 예산을 넘더라도 자기 윈도우 1개로 둔다(레코드는 더 쪼개지 않음).
    """
    fit = max(1, int(budget * _WINDOW_BUDGET_MARGIN))
    windows: list[list[Pass1Record]] = []
    current: list[Pass1Record] = []
    for rec in _active_records(records):
        trial = current + [rec]
        if current and _estimate_payload_tokens(trial, prior) > fit:
            windows.append(current)
            current = [rec]
        else:
            current = trial
    if current:
        windows.append(current)
    return windows


def _carried_from_result(result: "ResolveResult") -> dict:
    """한 윈도우 해소 결과에서 다음 윈도우로 넘길 belief를 추출.

    resolved roster(character_id→name/significance/evidence)와 threads를 캐리오버한다 → 다음
    윈도우가 동일 인물/떡밥을 같은 정체성으로 이어받아 경계에서 이름/정체성이 흔들리지 않게 한다
    (Req 8.2).
    """
    roster: list[dict] = []
    for c in result.characters or []:
        cid = c.get("character_id")
        if cid is None:
            continue
        roster.append({
            "character_id": cid,
            "name": c.get("name"),
            "significance": c.get("significance"),
            "key_facts": c.get("evidence") or "",
        })
    return {"roster": roster, "threads": list(result.threads or [])}


def _accumulate_carried(carried: dict, result: "ResolveResult") -> dict:
    """윈도우 간 누적 belief 갱신 — roster는 character_id로 union(이름 있는 갱신 우선), threads union.

    단조적으로 누적해 윈도우 N+1이 N까지의 resolved 인물/떡밥을 모두 본다(경계 정보 손실 방지).
    """
    out_roster: dict = {}
    order: list = []
    for e in carried.get("roster", []):
        cid = e.get("character_id")
        out_roster[cid] = dict(e)
        order.append(cid)
    new = _carried_from_result(result)
    for e in new["roster"]:
        cid = e.get("character_id")
        if cid in out_roster:
            for k, v in e.items():
                if v not in (None, ""):
                    out_roster[cid][k] = v
        else:
            out_roster[cid] = dict(e)
            order.append(cid)

    thr_by_key: dict = {}
    thr_order: list = []
    for t in carried.get("threads", []) + new["threads"]:
        key = _thread_merge_key(t)
        if key in thr_by_key:
            for k, v in t.items():
                if v not in (None, ""):
                    thr_by_key[key][k] = v
        else:
            thr_by_key[key] = dict(t)
            thr_order.append(key)

    return {
        "roster": [out_roster[c] for c in order],
        "threads": [thr_by_key[k] for k in thr_order],
    }


def _prior_with_belief(base_prior: dict, carried: dict) -> dict:
    """다음 윈도우용 prior dict — base prior + 캐리오버 belief 주입(Req 8.2).

    carried roster(이름 확정분)를 `confirmed_roster_prior`에 character_id로 union해 진실 기준선에
    포함시키고(다음 윈도우가 이전 윈도우의 정체성을 신뢰), 원본 belief는 `window_belief`로도 노출해
    프롬프트가 '윈도우 경계 캐리오버'임을 인지하게 한다.
    """
    prior = dict(base_prior)
    existing = list(prior.get("confirmed_roster_prior") or [])
    by_id: dict = {}
    order: list = []
    for e in existing:
        cid = e.get("character_id")
        key = ("cid", cid) if cid is not None else ("name", (e.get("name") or "").lower())
        by_id[key] = dict(e)
        order.append(key)
    for e in carried.get("roster", []):
        if not e.get("name"):
            continue  # 이름 미확정은 진실 기준선에 넣지 않음
        cid = e.get("character_id")
        key = ("cid", cid) if cid is not None else ("name", (e.get("name") or "").lower())
        if key in by_id:
            for k, v in e.items():
                if v not in (None, ""):
                    by_id[key][k] = v
        else:
            by_id[key] = dict(e)
            order.append(key)
    prior["confirmed_roster_prior"] = [by_id[k] for k in order]
    prior["window_belief"] = {
        "carried_roster": carried.get("roster", []),
        "carried_threads": carried.get("threads", []),
    }
    return prior


def _thread_merge_key(thread: dict):
    """thread 병합 키 — thread_id 우선(전역 안정 id), 없으면 정규화 description 폴백."""
    tid = thread.get("thread_id")
    if tid is not None:
        return ("id", tid)
    return ("desc", (thread.get("description") or "").strip().lower())


def _refine_character(target: dict, incoming: dict) -> None:
    """후속 윈도우가 동일 character_id를 보강(in-place) — 이름/중요도/충돌은 non-null override,
    name_confidence는 max, merge_suggestion은 union, evidence는 non-empty 우선."""
    for k in ("name", "significance", "label_conflict"):
        v = incoming.get(k)
        if v not in (None, ""):
            target[k] = v
    nc = incoming.get("name_confidence")
    if isinstance(nc, (int, float)):
        target["name_confidence"] = max(float(target.get("name_confidence", 0) or 0), float(nc))
    ev = incoming.get("evidence")
    if isinstance(ev, str) and ev.strip() and not (target.get("evidence") or "").strip():
        target["evidence"] = ev
    merge = incoming.get("merge_suggestion")
    if isinstance(merge, list) and merge:
        seen = list(target.get("merge_suggestion") or [])
        for m in merge:
            if m not in seen:
                seen.append(m)
        target["merge_suggestion"] = seen


def _merge_resolve_results(
    webtoon_episode_id: Optional[int], results: list["ResolveResult"],
) -> "ResolveResult":
    """윈도우별 ResolveResult를 에피소드 1개 ResolveResult로 결정론적 병합(Req 8.2/8.3).

    - characters: character_id로 union/dedup(후속 윈도우가 refine/add), id 없는 항목은 그대로 append.
    - speaker_resolution/beats/deceptions: 순서 유지 concat(비트는 윈도우를 넘지 않음 — 허용).
    - face_reassignments: (cut, face)로 dedup concat(윈도우는 컷을 나눠 가지므로 사실상 concat).
    - episode: 필드별 마지막 non-empty 채택, foreshadowing은 dedup concat.
    - threads: thread_id/description 키로 union(후속이 refine).
    - usage: 윈도우 토큰 합(calls=윈도우 수), error는 모든 윈도우 실패 시에만 채움.
    """
    chars_by_id: dict = {}
    chars_order: list = []
    chars_no_id: list[dict] = []
    speaker_resolution: list[dict] = []
    fr_by_key: dict = {}
    fr_order: list = []
    beats: list[dict] = []
    deceptions: list[dict] = []
    thr_by_key: dict = {}
    thr_order: list = []
    episode: dict = {"summary": "", "appeal_point": "", "cliffhanger": "", "foreshadowing": []}
    fs_seen: set = set()
    usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0,
             "finish_reason": None, "calls": 0}
    errors: list[str] = []
    ok = 0

    for res in results:
        if res.error:
            errors.append(res.error)
        else:
            ok += 1
        for c in res.characters or []:
            cid = c.get("character_id")
            if cid is None:
                chars_no_id.append(dict(c))
                continue
            if cid in chars_by_id:
                _refine_character(chars_by_id[cid], c)
            else:
                chars_by_id[cid] = dict(c)
                chars_order.append(cid)
        speaker_resolution.extend(res.speaker_resolution or [])
        for fr in res.face_reassignments or []:
            key = (fr.get("cut"), fr.get("face"))
            if key not in fr_by_key:
                fr_by_key[key] = fr
                fr_order.append(key)
            elif (fr.get("confidence") or 0) > (fr_by_key[key].get("confidence") or 0):
                fr_by_key[key] = fr
        beats.extend(res.beats or [])
        deceptions.extend(res.deceptions or [])
        for t in res.threads or []:
            key = _thread_merge_key(t)
            if key in thr_by_key:
                for k, v in t.items():
                    if v not in (None, ""):
                        thr_by_key[key][k] = v
            else:
                thr_by_key[key] = dict(t)
                thr_order.append(key)
        ep = res.episode or {}
        for f in ("summary", "appeal_point", "cliffhanger"):
            if ep.get(f):
                episode[f] = ep[f]
        for fs in ep.get("foreshadowing") or []:
            marker = fs if isinstance(fs, str) else json.dumps(fs, ensure_ascii=False, sort_keys=True)
            if marker not in fs_seen:
                fs_seen.add(marker)
                episode["foreshadowing"].append(fs)
        u = res.usage or {}
        usage["prompt_tokens"] += int(u.get("prompt_tokens", 0) or 0)
        usage["completion_tokens"] += int(u.get("completion_tokens", 0) or 0)
        usage["total_tokens"] += int(u.get("total_tokens", 0) or 0)
        if u.get("finish_reason"):
            usage["finish_reason"] = u["finish_reason"]
        usage["calls"] += 1

    characters = [chars_by_id[c] for c in chars_order] + chars_no_id
    threads = [thr_by_key[k] for k in thr_order]
    # 모든 윈도우가 실패했을 때만 에피소드를 error로 표시(부분 성공은 결과 유지).
    error = "; ".join(errors) if (errors and ok == 0) else None

    return ResolveResult(
        webtoon_episode_id=webtoon_episode_id,
        characters=characters,
        speaker_resolution=speaker_resolution,
        face_reassignments=[fr_by_key[k] for k in fr_order],
        beats=beats,
        episode=episode,
        deceptions=deceptions,
        threads=threads,
        usage=usage,
        error=error,
    )


def resolve_episode_windowed(
    ep: "ExtractResult | int",
    prior_context=None,
    *,
    token_budget: Optional[int] = None,
    records: Optional[list["Pass1Record"]] = None,
    webtoon_id: Optional[int] = None,
    ctx: Optional[dict] = None,
    persist_usage: bool = True,
    run_id: Optional[int] = None,
) -> ResolveResult:
    """컨텍스트 적응형 Stage R 해소 — 토큰 예산에 따라 단일콜/다중윈도우 자동 선택.

    `resolve_episode`(단일 텍스트 콜)를 감싼다. 모델 토큰 예산을 해석하고(Req 8.1) 에피소드 전체
    payload 추정이 예산 이내면 **기존 단일콜 경로로 그대로 위임**(동작/결과 동일), 초과하면 레코드를
    읽기순 윈도우로 분할해 각각 해소하고 belief state(resolved roster/threads)를 윈도우 경계 너머로
    캐리오버한 뒤(Req 8.2) 결과를 에피소드 1개로 병합한다. 동일 해소 로직이 16K/130K 양쪽에서 1급으로
    동작한다(Req 8.3 — 로컬 폴백 = 1급 경로).

    예산 해석: `token_budget=` 명시 > `ctx.params`(context_window|max_context|token_budget) >
    모듈 기본값(_DEFAULT_TOKEN_BUDGET, 크게 잡아 기본은 단일콜). 모델명 하드코딩 없음(Req 7.1).

    윈도우당 1콜이며 `resolve_episode`가 콜마다 `llm_usage` 1행을 적재한다(stage='pass2_resolve',
    cut_id=NULL — Req 6.7) → 중복/누락 없음. 병합 결과의 `usage`는 윈도우 토큰 합(인메모리 집계).

    다운스트림(task 9.1 step3b activity)은 이 함수를 해소 진입점으로 호출한다.
    """
    if isinstance(ep, ExtractResult):
        webtoon_episode_id = ep.webtoon_episode_id
        if records is None:
            records = ep.records
    elif isinstance(ep, int):
        webtoon_episode_id = ep
        if records is None:
            raise ValueError(
                "resolve_episode_windowed: ep가 webtoon_episode_id 정수면 records= 를 명시해야 합니다"
            )
    else:
        raise TypeError(
            f"resolve_episode_windowed: ep는 ExtractResult 또는 int 여야 합니다(got {type(ep)!r})"
        )

    records = records or []
    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)
    if ctx is None:
        ctx = resolve_llm_model(webtoon_id)

    budget = _resolve_token_budget(ctx, token_budget)
    estimate = _estimate_payload_tokens(records, prior_context)

    # 예산 이내 → 단일콜 경로로 위임(기존 resolve_episode 동작/시그니처/결과 그대로 보존).
    if estimate <= budget:
        logger.info(
            "[step3.pass2.window] episode %s — 단일콜(추정 %s tok <= 예산 %s)",
            webtoon_episode_id, estimate, budget,
        )
        return resolve_episode(
            webtoon_episode_id, prior_context,
            records=records, webtoon_id=webtoon_id, ctx=ctx, persist_usage=persist_usage,
            run_id=run_id,
        )

    # 예산 초과 → 읽기순 윈도우 분할 + belief 캐리오버.
    windows = _split_records_to_windows(records, prior_context, budget)
    base_prior = _prior_to_dict(prior_context)
    logger.info(
        "[step3.pass2.window] episode %s — 다중윈도우 %s개(추정 %s tok > 예산 %s)",
        webtoon_episode_id, len(windows), estimate, budget,
    )

    carried: dict = {"roster": [], "threads": []}
    window_results: list[ResolveResult] = []
    for wi, win in enumerate(windows):
        win_prior = _prior_with_belief(base_prior, carried)  # 경계 belief 주입
        res = resolve_episode(
            webtoon_episode_id, win_prior,
            records=win, webtoon_id=webtoon_id, ctx=ctx, persist_usage=persist_usage,
            run_id=run_id,
        )
        window_results.append(res)
        carried = _accumulate_carried(carried, res)  # 다음 윈도우로 belief 캐리오버
        logger.info(
            "[step3.pass2.window] episode %s — 윈도우 %s/%s 컷=%s..%s chars=%s",
            webtoon_episode_id, wi + 1, len(windows),
            win[0].cut_number, win[-1].cut_number, len(res.characters),
        )

    return _merge_resolve_results(webtoon_episode_id, window_results)


# ── Stage N (서사 분석, step3b 2/2) — 정정된 트랜스크립트 입력, 이미지 없음 ──────

def _build_narrative_payload(
    records: list["Pass1Record"], resolve_result: "ResolveResult", prior,
    id_to_name: dict[int, str],
) -> dict:
    """Stage N 입력 — Stage R로 화자·정체가 **정정된** 컷 트랜스크립트(v4.0 §17.4).

    R의 speaker_resolution((cut,block)→character_id)과 확정 이름 테이블을 블록에 주석해,
    N이 "누가 말했는지 확정된 상태"의 이야기를 읽게 한다. provisional 화자(spk_cid)는 R이
    다루지 않은 블록의 폴백. 이미지/얼굴 bbox는 싣지 않는다(서사 분석에 불필요 — 토큰 절약).
    """
    spk_map: dict[tuple, Optional[int]] = {}
    for sr in resolve_result.speaker_resolution or []:
        if sr.get("cut") is not None and sr.get("block_index") is not None:
            spk_map[(sr["cut"], sr["block_index"])] = sr.get("character_id")
    # R characters의 확정 이름을 id_to_name 위에 덮음(이번 화 신규 확정 반영).
    names = dict(id_to_name)
    for c in resolve_result.characters or []:
        if c.get("character_id") is not None and c.get("name"):
            names[c["character_id"]] = c["name"]

    cuts = []
    for rec in records:
        if rec.skipped or rec.error or not rec.result:
            continue
        res = rec.result
        blocks = []
        for b in res.get("blocks") or []:
            btype = b.get("type")
            entry = {"i": b.get("index"), "type": btype, "text": b.get("corrected_text")}
            if btype in _SPEAKER_TYPES:
                cid = spk_map.get((rec.cut_number, b.get("index")))
                if cid is None:
                    cid = (b.get("speaker") or {}).get("character_id")
                entry["speaker_id"] = cid
                entry["speaker"] = names.get(cid) if cid is not None else None
            blocks.append(entry)
        cuts.append({"cut": rec.cut_number, "summary": res.get("cut_summary"), "blocks": blocks})

    payload = dict(_prior_to_dict(prior))
    payload["characters"] = [
        {"character_id": cid, "name": nm} for cid, nm in sorted(names.items()) if nm
    ]
    payload["cuts"] = cuts
    return payload


def narrate_episode(
    webtoon_episode_id: int,
    records: list["Pass1Record"],
    resolve_result: "ResolveResult",
    prior_context=None,
    *,
    webtoon_id: Optional[int] = None,
    ctx: Optional[dict] = None,
    persist_usage: bool = True,
    run_id: Optional[int] = None,
) -> dict:
    """Stage N — 서사 분석 텍스트 1콜(beats/episode(summary+teaser)/deceptions/threads/profiles).

    입력은 Stage R로 정정된 트랜스크립트(§17.4 — 서사 분석이 정체 정정 **후의** 텍스트를 읽는 것이
    R/N 분리의 핵심 이점). 실패 시 빈 dict(+error 로그)로 격리한다 — R 산출(화자)은 이미 확보돼
    있으므로 N 실패가 화자 데이터를 잃게 하지 않는다(실패 격리).
    """
    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)
    if ctx is None:
        ctx = resolve_llm_model(webtoon_id)
    call_ctx = _pass2_ctx(ctx)

    id_to_name = _webtoon_character_names(webtoon_id)
    payload = _build_narrative_payload(records, resolve_result, prior_context, id_to_name)
    user_text = json.dumps(payload, ensure_ascii=False)

    raw_result: dict = {}
    usage: dict = {}
    err: Optional[str] = None
    for _attempt in range(_PASS2_RETRIES):
        try:
            call = call_llm_json(call_ctx, _NARRATIVE_SYSTEM_PROMPT, user_text, [])
            raw_result = call.result if isinstance(call.result, dict) else {}
            usage = call.usage or {}
            err = None
            break
        except Exception as e:  # noqa: BLE001 — 스테이지 단위 격리
            err = str(e)
            raw_result = {}

    if err is not None:
        logger.warning("[step3.narrative] episode %s — 서사 콜 실패(스킵): %s", webtoon_episode_id, err)
        return {"beats": [], "episode": {}, "deceptions": [], "threads": [], "profiles": [],
                "usage": usage, "error": err}

    sanitized = _sanitize_narrative(raw_result)
    if persist_usage:
        _insert_llm_usage(webtoon_id, webtoon_episode_id, None, ctx.get("id"), usage,
                          stage=_NARRATIVE_STAGE, image_count=None, run_id=run_id)
    logger.info(
        "[step3.narrative] episode %s — beats=%s deceptions=%s threads=%s profiles=%s tokens=%s",
        webtoon_episode_id, len(sanitized["beats"]), len(sanitized["deceptions"]),
        len(sanitized["threads"]), len(sanitized["profiles"]), (usage or {}).get("total_tokens"),
    )
    return {**sanitized, "usage": usage, "error": None}


def _webtoon_character_names(webtoon_id: int) -> dict[int, str]:
    """웹툰의 명명된 인물 id→이름(클러스터 제외) — Stage N 트랜스크립트 주석용."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT id, name FROM analysis_character
            WHERE webtoon_id = %s AND deleted_at IS NULL AND kind = 'character' AND name <> ''
            """,
            (webtoon_id,),
        )
        return {r[0]: r[1] for r in cur.fetchall()}


def resolve_and_narrate(
    ep: "ExtractResult | int",
    prior_context=None,
    *,
    records: Optional[list["Pass1Record"]] = None,
    webtoon_id: Optional[int] = None,
    ctx: Optional[dict] = None,
    token_budget: Optional[int] = None,
    run_id: Optional[int] = None,
) -> ResolveResult:
    """step3b 진입점(v4.0 §17.4) — Stage R(정체·화자, 윈도우 가능) → Stage N(서사) 순차 2콜.

    반환은 두 스테이지 산출을 합친 단일 `ResolveResult`(step3c apply 입력). R이 통째로 실패하면
    error를 채워 반환(스킵), N만 실패하면 화자 데이터는 유지된 채 서사 필드만 빈 값(실패 격리).
    """
    if isinstance(ep, ExtractResult):
        webtoon_episode_id = ep.webtoon_episode_id
        if records is None:
            records = ep.records
    else:
        webtoon_episode_id = int(ep)
        if records is None:
            raise ValueError("resolve_and_narrate: ep가 정수면 records= 를 명시해야 합니다")
    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)
    if ctx is None:
        ctx = resolve_llm_model(webtoon_id)

    r = resolve_episode_windowed(
        webtoon_episode_id, prior_context,
        records=records, webtoon_id=webtoon_id, ctx=ctx,
        token_budget=token_budget, run_id=run_id,
    )
    if r.error:
        return r

    n = narrate_episode(
        webtoon_episode_id, records or [], r, prior_context,
        webtoon_id=webtoon_id, ctx=ctx, run_id=run_id,
    )
    usage = dict(r.usage or {})
    n_usage = n.get("usage") or {}
    for k in ("prompt_tokens", "completion_tokens", "total_tokens"):
        usage[k] = int(usage.get(k, 0) or 0) + int(n_usage.get(k, 0) or 0)

    return ResolveResult(
        webtoon_episode_id=webtoon_episode_id,
        characters=r.characters,
        speaker_resolution=r.speaker_resolution,
        face_reassignments=r.face_reassignments,
        beats=n.get("beats") or [],
        episode=n.get("episode") or {},
        deceptions=n.get("deceptions") or [],
        threads=n.get("threads") or [],
        profiles=n.get("profiles") or [],
        usage=usage,
    )


# ── Pass-2b (결정론 커밋, step3c) ──────────────────────────────────────────────
# LLM 없이 Pass-2a `ResolveResult`를 에피소드 전체 DB에 결정론적으로 투영(소급 전파 포함)한다.
# 핵심 보장:
#   - 소급(backward) 전파: 이름은 **Character 행 1곳**에만 저장하고, 모든 컷의 TextAnnotation은
#     `speaker_id`(=character_id) FK로 그 행을 가리킨다. 따라서 뒤 컷 단서로 확정된 이름이
#     character_id 키 조인을 통해 앞 컷에도 자동 반영된다(별도 per-cut 이름 쓰기 불필요). Req 5.2.
#   - 멱등(Property 3): 모든 쓰기는 upsert(ON CONFLICT)/안정키/스코프 delete-reinsert로 동일
#     ResolveResult 재적용 시 동일 상태를 낸다. Req 5.3.
#   - 동결(Property 4): source='human' 어노테이션과 is_confirmed=true Character는 절대 건드리지
#     않는다(speaker 커밋은 source='llm'만, Character 갱신은 is_confirmed=false만). Req 5.4/3.4/10.5.
#   - 정체성 일관성(Property 5): 이름이 Character 1행에 단일 저장 → 동일 character_id의 모든 resolved
#     주석은 단일 확정 이름을 공유.
#   - status 전이(Property 6): resolution_status는 unresolved→resolved 단방향. resolved는 speaker_id가
#     non-null(speech/monologue 해소)이거나 명시적 화자 없음(narration/system/other).
#   - significance 정합(Property 7): significance='extra' ⇒ is_match_excluded=true(역방향 자동화
#     없음 — human override 허용).
# LLM 호출 없음. fold(11.4)는 task 8.2가 본 함수 반환 episode_meta로 별도 호출(여기선 호출 안 함).

_APPLY_SPEAKER_MIN_CONFIDENCE = 0.5      # speaker_resolution 커밋 임계값(미만은 provisional 유지 — Req 10.3)
_SPEAKERLESS_TYPES = ("narration", "system", "other")  # 화자 없는 블록 — resolved(speaker NULL)


def _webtoon_character_ids(webtoon_id: int) -> set[int]:
    """웹툰의 유효(미삭제) character id 집합 — speaker_id/claim FK 가드용(존재하지 않는 id 무시)."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT id FROM analysis_character WHERE webtoon_id = %s AND deleted_at IS NULL",
            (webtoon_id,),
        )
        return {r[0] for r in cur.fetchall()}


def _episode_cut_id_map(webtoon_episode_id: int) -> dict[int, int]:
    """cut_number → cut_id (해당 에피소드)."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT cut_number, id FROM webtoon_cut WHERE episode_id = %s",
            (webtoon_episode_id,),
        )
        return {r[0]: r[1] for r in cur.fetchall()}


def _episode_region_map(webtoon_episode_id: int) -> dict[tuple[int, int], int]:
    """(cut_number, text_region.index) → region_id (is_excluded=false). speaker_resolution 매핑용.

    speaker_resolution의 (cut, block_index)는 그 컷의 text_region.index에 대응한다(Pass-1 블록 index =
    region index). 에피소드 전체를 1쿼리로 적재한다.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT wc.cut_number, tr.index, tr.id
            FROM analysis_text_region tr
            JOIN webtoon_cut wc ON tr.cut_id = wc.id
            WHERE wc.episode_id = %s AND tr.is_excluded = false
            """,
            (webtoon_episode_id,),
        )
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}


def _episode_no_id_map(webtoon_id: int) -> dict[int, int]:
    """에피소드 회차번호(no) → webtoon_episode.id (threads의 planted/resolved 에피소드 매핑용)."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT no, id FROM webtoon_episode WHERE webtoon_id = %s AND deleted_at IS NULL",
            (webtoon_id,),
        )
        return {r[0]: r[1] for r in cur.fetchall()}


def _project_characters(
    webtoon_id: int, characters: list[dict], valid_ids: set[int], now: datetime,
) -> list[dict]:
    """이름/중요도 투영 + **클러스터→캐릭터 승격**(v4.0 §17.2) — 소급 전파의 핵심.

    결정론 규칙:
      - is_confirmed=true Character는 **동결**(이름/중요도/매칭 무변경 — Property 4).
      - 명명·승격(rename+kind=character)은 confidence>=_NAME_AUTO_CONFIDENCE(0.85) &
        현재 kind='cluster'(미명명 기계 산출물) & 동명 기존 인물 부재일 때만 자동 수행
        (is_name_auto_assigned=true). 그 외 이름 후보는 **제안**(suggestion type=name)으로만.
      - significance는 허용 enum이면 갱신, 'extra'면 is_match_excluded=true도 함께(Property 7,
        가역: 역방향 자동 해제 없음 — human override 허용).
    이름은 Character 행 1곳에만 쓰므로 speaker_id FK를 통해 전 컷에 소급 반영된다(Req 5.2).

    Returns: suggestion(type=name) 적재 대상 [{character_id, name, confidence, evidence}].
    """
    suggestions: list[dict] = []
    claimed_names: set[str] = set()  # 이번 apply에서 확정한 이름(동명 중복 승격 방지)
    for c in characters or []:
        cid = c.get("character_id")
        if cid is None or cid not in valid_ids:
            continue
        name = c.get("name")  # _sanitize에서 trim/None 처리됨
        sig = c.get("significance")
        conf = float(c.get("name_confidence", 0) or 0)
        evidence = c.get("evidence") or ""

        # 동명 기존 인물 탐색은 DB 커밋 전에 미리(읽기 전용).
        existing = _find_character_by_name(webtoon_id, name) if name else None

        with db_cursor() as cur:
            cur.execute(
                """
                SELECT name, kind, is_confirmed, significance, is_match_excluded
                FROM analysis_character WHERE id = %s AND webtoon_id = %s AND deleted_at IS NULL
                """,
                (cid, webtoon_id),
            )
            row = cur.fetchone()
            if not row:
                continue
            _cur_name, cur_kind, is_confirmed, _cur_sig, cur_excl = row
            if is_confirmed:
                continue  # 동결 — human 확정 Character는 LLM이 건드리지 않음(Property 4)

            sets: list[str] = []
            params: list = []

            # significance 투영(Property 7) — 가역적·human-override 가능 라벨.
            if sig in _SIGNIFICANCE:
                sets.append("significance = %s")
                params.append(sig)
                if sig == "extra" and not cur_excl:
                    sets.append("is_match_excluded = true")

            # 명명·승격 — 유일한 비가역성 식별 액션이므로 고신뢰 + 클러스터 + 동명 부재일 때만
            # 자동 수행하고, 그 외는 suggestion(type=name)으로만 둔다(Req 10.3/10.4).
            if name:
                dup_in_run = name.lower() in claimed_names
                can_promote = (
                    conf >= _NAME_AUTO_CONFIDENCE
                    and cur_kind == "cluster"
                    and (existing is None or existing == cid)
                    and not dup_in_run
                )
                if can_promote:
                    sets.append("name = %s")
                    params.append(name[:64])
                    sets.append("kind = 'character'")
                    sets.append("is_name_auto_assigned = true")
                    claimed_names.add(name.lower())
                else:
                    suggestions.append({
                        "character_id": cid, "name": name,
                        "confidence": conf, "evidence": evidence,
                    })

            if sets:
                sets.append("updated_at = %s")
                params.append(now)
                params.append(cid)
                cur.execute(
                    f"UPDATE analysis_character SET {', '.join(sets)} WHERE id = %s", params,
                )
    return suggestions


def _episode_face_detection_map(webtoon_episode_id: int) -> dict[tuple[int, int], dict]:
    """에피소드의 (cut_number, face_idx) → 얼굴 현황 — face_reassign 제안의 대상 해석용.

    detection_id(suggestion FK), 현재 배정 character_id(human>step2 우선 — `_load_faces`와 동일
    해석), confirmed(human 배정 여부 — 동결 대상 판별)를 담는다.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT wc.cut_number, fr.face_idx, fr.id, c.id, (fi.source = 'human') AS confirmed
            FROM analysis_face_detection fr
            JOIN webtoon_cut wc ON fr.cut_id = wc.id
            LEFT JOIN LATERAL (
                SELECT source, appearance_id
                FROM analysis_face_identity
                WHERE detection_id = fr.id AND deleted_at IS NULL
                ORDER BY source ASC
                LIMIT 1
            ) fi ON true
            LEFT JOIN analysis_character_appearance ca ON fi.appearance_id = ca.id
            LEFT JOIN analysis_character c ON ca.character_id = c.id
            WHERE wc.episode_id = %s AND fr.is_used = true
            """,
            (webtoon_episode_id,),
        )
        return {
            (r[0], r[1]): {"detection_id": r[2], "character_id": r[3], "confirmed": bool(r[4])}
            for r in cur.fetchall()
        }


def _commit_suggestions(
    webtoon_id: int, episode_id: int, name_suggestions: list[dict],
    characters: list[dict], now: datetime, run_id: Optional[int] = None,
    face_reassignments: Optional[list[dict]] = None,
    face_map: Optional[dict] = None, valid_ids: Optional[set[int]] = None,
) -> int:
    """AI 제안을 통합 `suggestion` 큐에 적재(v4.0 §17.2) — 에피소드 스코프 pending 재적재로 멱등.

    적재 대상:
      - name: `_project_characters`가 자동 승격하지 않은 이름 후보.
      - merge: Stage R characters[].merge_suggestion(자동 병합 금지 — 제안만, Req 10.4).
      - label_conflict: Stage R characters[].label_conflict(얼굴 라벨↔대사 충돌, 인물 단위 —
        특정 얼굴을 못 짚은 의심 신호. 자동 face_identity 변경은 하지 않는다).
      - face_reassign: Stage R face_reassignments(얼굴 단위 — detection_id로 대상 고정, 수락 시
        service가 human FaceIdentity를 생성). human 확정(confirmed) 얼굴·실재하지 않는 (cut,face)·
        현재 배정과 동일한 제안은 버린다. to_character_id가 웹툰에 없는 id면 null로 강등
        ("현재 배정이 틀렸다" 신호만 유지).
    이 에피소드가 만든 pending 제안만 지우고 재적재한다(수락/거부된 것은 보존 — human 판단 존중).
    """
    inserted = 0
    face_map = face_map or {}
    valid_ids = valid_ids or set()
    with db_cursor() as cur:
        cur.execute(
            "DELETE FROM analysis_suggestion WHERE episode_id = %s AND status = 'pending'",
            (episode_id,),
        )

        def _insert(stype: str, character_id, payload: dict, confidence,
                    detection_id=None, cut=None) -> None:
            nonlocal inserted
            cur.execute(
                """
                INSERT INTO analysis_suggestion
                    (webtoon_id, type, character_id, detection_id, episode_id, cut,
                     payload, confidence, run_id, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
                """,
                (webtoon_id, stype, character_id, detection_id, episode_id, cut,
                 Json(payload), confidence, run_id, now, now),
            )
            inserted += 1

        for sug in name_suggestions or []:
            ev = sug.get("evidence")
            ev_list = [ev] if isinstance(ev, str) and ev.strip() else (ev if isinstance(ev, list) else [])
            _insert("name", sug["character_id"],
                    {"name": (sug.get("name") or "")[:64], "evidence": ev_list},
                    sug.get("confidence"))

        for c in characters or []:
            cid = c.get("character_id")
            if cid is None:
                continue
            merge = c.get("merge_suggestion") or []
            if merge:
                _insert("merge", cid,
                        {"other_character_ids": merge, "evidence": c.get("evidence") or ""},
                        c.get("name_confidence"))
            conflict = c.get("label_conflict")
            if conflict:
                _insert("label_conflict", cid, {"description": conflict}, None)

        for fr in face_reassignments or []:
            face = fr.get("face") or ""
            try:
                face_idx = int(face[1:])
            except (TypeError, ValueError):
                continue
            target = face_map.get((fr.get("cut"), face_idx))
            if not target or target["confirmed"]:
                continue  # 실재하지 않는 얼굴(모델 착오) / human 확정 얼굴은 동결(Property 4)
            to_cid = fr.get("to_character_id")
            if to_cid is not None and to_cid not in valid_ids:
                to_cid = None
            if to_cid == target["character_id"]:
                continue  # 현재 배정과 동일 — 재배정 아님
            if to_cid is None and target["character_id"] is None:
                continue  # 미배정 얼굴 + 대상 미상 — 액션 불가한 제안
            _insert("face_reassign", target["character_id"],
                    {"to_character_id": to_cid, "evidence": fr.get("evidence") or ""},
                    fr.get("confidence"),
                    detection_id=target["detection_id"], cut=fr.get("cut"))
    return inserted


def _commit_profiles(
    webtoon_id: int, profiles: list[dict], valid_ids: set[int], now: datetime,
    run_id: Optional[int] = None,
) -> int:
    """Stage N profiles → `character_profile` llm 행 병합 upsert(v4.0 §17.2).

    human 행은 절대 건드리지 않는다(source 레이어링 — 서빙이 필드 단위 human 우선 병합).
    스칼라는 최신값 우선, personality는 합집합(캡 8), traits는 dict 병합, key_facts는
    append-dedup(캡 12) — 인물의 누적 사실 저장처(구 narrative_state key_facts 흡수).
    """
    updated = 0
    with db_cursor() as cur:
        for item in profiles or []:
            cid = item.get("character_id")
            prof = item.get("profile") or {}
            if cid is None or cid not in valid_ids or not prof:
                continue
            cur.execute(
                """
                SELECT gender, age_group, affiliation, role, personality, traits, key_facts
                FROM analysis_character_profile
                WHERE character_id = %s AND source = 'llm' AND deleted_at IS NULL
                """,
                (cid,),
            )
            row = cur.fetchone()
            cur_vals = {
                "gender": row[0] if row else "", "age_group": row[1] if row else "",
                "affiliation": row[2] if row else "", "role": row[3] if row else "",
                "personality": (row[4] if row and isinstance(row[4], list) else []),
                "traits": (row[5] if row and isinstance(row[5], dict) else {}),
                "key_facts": (row[6] if row and isinstance(row[6], list) else []),
            }
            merged = dict(cur_vals)
            for k in ("gender", "age_group", "affiliation", "role"):
                if prof.get(k):
                    merged[k] = str(prof[k])[:256 if k == "role" else 128]
            for p in prof.get("personality") or []:
                if p not in merged["personality"]:
                    merged["personality"].append(p)
            merged["personality"] = merged["personality"][-8:]
            merged["traits"] = {**merged["traits"], **(prof.get("traits") or {})}
            for f in prof.get("key_facts") or []:
                if f not in merged["key_facts"]:
                    merged["key_facts"].append(f)
            merged["key_facts"] = merged["key_facts"][-12:]

            cur.execute(
                """
                INSERT INTO analysis_character_profile
                    (character_id, source, gender, age_group, affiliation, role,
                     personality, traits, key_facts, run_id, created_at, updated_at)
                VALUES (%s, 'llm', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT uniq_character_profile_character_source
                DO UPDATE SET gender = EXCLUDED.gender, age_group = EXCLUDED.age_group,
                              affiliation = EXCLUDED.affiliation, role = EXCLUDED.role,
                              personality = EXCLUDED.personality, traits = EXCLUDED.traits,
                              key_facts = EXCLUDED.key_facts, run_id = EXCLUDED.run_id,
                              deleted_at = NULL, updated_at = EXCLUDED.updated_at
                """,
                (cid, merged["gender"][:16], merged["age_group"][:16],
                 merged["affiliation"][:128], merged["role"][:256],
                 Json(merged["personality"]), Json(merged["traits"]),
                 Json(merged["key_facts"]), run_id, now, now),
            )
            updated += 1
    return updated


def _commit_speaker_resolution(
    webtoon_episode_id: int,
    region_map: dict[tuple[int, int], int],
    speaker_resolution: list[dict],
    valid_ids: set[int],
    now: datetime,
) -> int:
    """speaker_resolution을 TextAnnotation에 커밋 + 화자없는 블록 resolved 처리(Property 6).

    - speech/monologue 해소: character_id 유효 & confidence>=임계값인 (cut, block_index)의 region을
      찾아 source='llm' 주석의 speaker_id + resolution_status='resolved' 설정(단방향). 임계값 미만/
      무효 id는 provisional 유지(Req 10.3). source='human'은 절대 갱신 안 함(동결 — Property 4).
    - **provisional 화자 승격(2026-07-05)**: Pass-2a가 명시 해소하지 않았지만 Pass-1이 얼굴 기반으로
      확신해 영속한 provisional speaker_id가 남아있는 speech/monologue 블록은 그 화자로 resolved
      승격한다. Pass-2a가 일부 블록을 빠뜨려도(전수 테이블 미준수) 얼굴 근거 화자가 유실되지 않는
      안전망 — 종전엔 이 유실이 화자 매칭률 1~2%의 주원인이었다.
    - 화자없는 블록(narration/system/other)은 speaker 없이 resolved로 전이(명시적 화자 없음 —
      Property 6). 역시 source='llm'만.
    Returns: speaker_id가 실제 커밋된 블록 수(명시 해소 + provisional 승격).
    """
    resolved = 0
    with db_cursor() as cur:
        for s in speaker_resolution or []:
            cid = s.get("character_id")
            cut = s.get("cut")
            bidx = s.get("block_index")
            conf = float(s.get("confidence", 0) or 0)
            if cid is None or cid not in valid_ids:
                continue
            if conf < _APPLY_SPEAKER_MIN_CONFIDENCE:
                continue
            rid = region_map.get((cut, bidx))
            if rid is None:
                continue
            cur.execute(
                """
                UPDATE analysis_text_annotation
                SET speaker_id = %s, resolution_status = 'resolved', updated_at = %s
                WHERE region_id = %s AND source = 'llm'
                """,
                (cid, now, rid),
            )
            resolved += cur.rowcount or 0

        # provisional 화자 승격 — Pass-2a가 다루지 않은 speech/monologue 중 Pass-1 화자 보유 블록.
        cur.execute(
            """
            UPDATE analysis_text_annotation ta
            SET resolution_status = 'resolved', updated_at = %s
            FROM analysis_text_region tr
            JOIN webtoon_cut wc ON tr.cut_id = wc.id
            WHERE ta.region_id = tr.id
              AND wc.episode_id = %s
              AND ta.source = 'llm'
              AND ta.type = ANY(%s)
              AND ta.speaker_id IS NOT NULL
              AND ta.resolution_status <> 'resolved'
            """,
            (now, webtoon_episode_id, list(_SPEAKER_TYPES)),
        )
        promoted = cur.rowcount or 0
        if promoted:
            logger.info(
                "[step3.apply] episode %s — provisional 화자 %s블록 resolved 승격(Pass-1 얼굴 근거)",
                webtoon_episode_id, promoted,
            )
        resolved += promoted

        # 화자없는 블록 일괄 resolved(speaker NULL) — 에피소드 스코프, source='llm'만.
        cur.execute(
            """
            UPDATE analysis_text_annotation ta
            SET resolution_status = 'resolved', updated_at = %s
            FROM analysis_text_region tr
            JOIN webtoon_cut wc ON tr.cut_id = wc.id
            WHERE ta.region_id = tr.id
              AND wc.episode_id = %s
              AND ta.source = 'llm'
              AND ta.type = ANY(%s)
              AND ta.resolution_status <> 'resolved'
            """,
            (now, webtoon_episode_id, list(_SPEAKERLESS_TYPES)),
        )
    return resolved


def _beat_stable_key(cut_start, cut_end, hook_type: str) -> str:
    """비트 안정키(멱등) — cut 범위 + 정규화 hook으로 결정론적 도출(unique(episode,stable_key))."""
    hook = (hook_type or "").strip().lower()
    return f"{cut_start}:{cut_end}:{hook}"[:128]


def _commit_beats(webtoon_episode_id: int, beats: list[dict], now: datetime,
                  run_id: Optional[int] = None) -> int:
    """EpisodeBeat 커밋 — stable_key upsert + 이번 결과에 없는 stale 비트 삭제(결정론 집합).

    stable_key = f(cut_start, cut_end, hook)로 동일 결과 재적용 시 동일 행을 in-place 갱신(멱등).
    cut_start/cut_end가 없는 비트는 적재 불가(NOT NULL)라 스킵한다.
    """
    keys: list[str] = []
    with db_cursor() as cur:
        for b in beats or []:
            cs = b.get("cut_start")
            ce = b.get("cut_end")
            if cs is None or ce is None:
                continue
            hook = (b.get("hook_type") or "").strip()
            key = _beat_stable_key(cs, ce, hook)
            keys.append(key)
            cur.execute(
                """
                INSERT INTO analysis_episode_beat
                    (episode_id, cut_start, cut_end, hook_type, appeal_point, intensity,
                     stable_key, run_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (episode_id, stable_key)
                DO UPDATE SET cut_start = EXCLUDED.cut_start, cut_end = EXCLUDED.cut_end,
                              hook_type = EXCLUDED.hook_type, appeal_point = EXCLUDED.appeal_point,
                              intensity = EXCLUDED.intensity, run_id = EXCLUDED.run_id,
                              updated_at = EXCLUDED.updated_at
                """,
                (webtoon_episode_id, cs, ce, hook, b.get("appeal_point") or "",
                 b.get("intensity"), key, run_id, now, now),
            )
        # 이번 결과에 없는 stale 비트 제거 → 비트 집합이 result의 순수 함수(멱등).
        if keys:
            cur.execute(
                "DELETE FROM analysis_episode_beat WHERE episode_id = %s AND stable_key <> ALL(%s)",
                (webtoon_episode_id, keys),
            )
        else:
            cur.execute("DELETE FROM analysis_episode_beat WHERE episode_id = %s", (webtoon_episode_id,))
    return len(keys)


def _commit_episode_report(
    webtoon_episode_id: int, episode: dict, character_timeline: list[dict], now: datetime,
    run_id: Optional[int] = None,
) -> None:
    """EpisodeReport OneToOne upsert — summary/**teaser**/appeal/cliffhanger/foreshadowing/타임라인."""
    ep = episode or {}
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_episode_report
                (episode_id, summary, teaser, appeal_point, cliffhanger, foreshadowing,
                 character_timeline, run_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (episode_id)
            DO UPDATE SET summary = EXCLUDED.summary, teaser = EXCLUDED.teaser,
                          appeal_point = EXCLUDED.appeal_point,
                          cliffhanger = EXCLUDED.cliffhanger,
                          foreshadowing = EXCLUDED.foreshadowing,
                          character_timeline = EXCLUDED.character_timeline,
                          run_id = EXCLUDED.run_id,
                          updated_at = EXCLUDED.updated_at
            """,
            (webtoon_episode_id, ep.get("summary") or "", ep.get("teaser") or "",
             ep.get("appeal_point") or "",
             ep.get("cliffhanger") or "", Json(ep.get("foreshadowing") or []),
             Json(character_timeline), run_id, now, now),
        )


def _commit_threads(
    webtoon_id: int,
    this_ep_id: int,
    this_ep_no: int,
    no_id_map: dict[int, int],
    threads: list[dict],
    now: datetime,
    run_id: Optional[int] = None,
) -> int:
    """NarrativeThread 커밋(webtoon 글로벌, plant→payoff). 멱등 전략:

      - 이 에피소드가 **심은(planted)** 떡밥: planted_episode_id=this_ep로 스코프 delete 후 재적재
        (이 에피소드 소유 행만 clear-reinsert → 멱등).
      - 이전 화에서 심겨 **이번에 해소된(resolved)** 떡밥: webtoon 내 동일 description(정규화)으로
        기존 행을 찾아 status='resolved'로 갱신(없으면 1회 insert). 재적용 시 이미 resolved 행을
        다시 찾아 갱신 → 중복 생성 없음(멱등). description이 비면 신뢰 매칭 불가라 스킵.

    planted/resolved_episode(회차번호)는 no_id_map으로 webtoon_episode.id에 매핑(미상이면 NULL).
    planted_episode 미지정(None)/this_ep_no는 '이번 화에 심음'으로 간주한다.
    """
    inserted = 0
    with db_cursor() as cur:
        cur.execute(
            "DELETE FROM analysis_narrative_thread WHERE webtoon_id = %s AND planted_episode_id = %s",
            (webtoon_id, this_ep_id),
        )
        for t in threads or []:
            desc = t.get("description") or ""
            ttype = (t.get("type") or "")[:32]
            status = t.get("status") or "open"
            planted_no = t.get("planted_episode")
            resolved_no = t.get("resolved_episode")
            planted_cut = t.get("planted_cut")
            resolved_cut = t.get("resolved_cut")
            conf = t.get("confidence")

            planted_here = planted_no is None or planted_no == this_ep_no
            resolved_id = (
                no_id_map.get(resolved_no) if resolved_no is not None else None
            )

            if status == "resolved" and not planted_here:
                # 이전 화에 심긴 떡밥의 해소 — 동일 description 기존 행 갱신(멱등). desc 없으면 스킵.
                if not desc.strip():
                    continue
                cur.execute(
                    """
                    SELECT id FROM analysis_narrative_thread
                    WHERE webtoon_id = %s AND lower(btrim(description)) = lower(btrim(%s))
                    ORDER BY id ASC LIMIT 1
                    """,
                    (webtoon_id, desc),
                )
                m = cur.fetchone()
                if m:
                    cur.execute(
                        """
                        UPDATE analysis_narrative_thread
                        SET status = 'resolved', resolved_episode_id = %s, resolved_cut = %s,
                            confidence = COALESCE(%s, confidence), updated_at = %s
                        WHERE id = %s
                        """,
                        (resolved_id or this_ep_id, resolved_cut, conf, now, m[0]),
                    )
                    continue
                # 매칭 없음 → 해소 떡밥 1회 insert(plant 정보는 알 수 있는 만큼).
                planted_id = no_id_map.get(planted_no) if planted_no is not None else None
                cur.execute(
                    """
                    INSERT INTO analysis_narrative_thread
                        (webtoon_id, description, type, status, planted_episode_id, planted_cut,
                         resolved_episode_id, resolved_cut, confidence, run_id, created_at, updated_at)
                    VALUES (%s, %s, %s, 'resolved', %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (webtoon_id, desc, ttype, planted_id, planted_cut,
                     resolved_id or this_ep_id, resolved_cut, conf, run_id, now, now),
                )
                inserted += 1
            else:
                # 이번 화에 심은 떡밥(open 또는 동일 화 내 resolved) → 재적재.
                cur.execute(
                    """
                    INSERT INTO analysis_narrative_thread
                        (webtoon_id, description, type, status, planted_episode_id, planted_cut,
                         resolved_episode_id, resolved_cut, confidence, run_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (webtoon_id, desc, ttype, status, this_ep_id, planted_cut,
                     resolved_id if status == "resolved" else None,
                     resolved_cut if status == "resolved" else None, conf, run_id, now, now),
                )
                inserted += 1
    return inserted


def _commit_claims(
    webtoon_episode_id: int,
    cut_map: dict[int, int],
    deceptions: list[dict],
    valid_ids: set[int],
    now: datetime,
    run_id: Optional[int] = None,
) -> int:
    """CharacterClaim 커밋(deceptions) — 이 에피소드 컷 스코프 clear-and-reinsert로 멱등.

    자연 unique 키가 없으므로 이 에피소드 컷들의 claim을 모두 지우고 재적재한다. cut 번호는
    cut_map으로 cut_id에 매핑(미상이면 스킵), character_id는 유효성 검사 후 무효면 NULL(FK SET_NULL).
    is_deception=true로 적재(deceptions = 책략 — Req 2.8).
    """
    inserted = 0
    with db_cursor() as cur:
        cur.execute(
            """
            DELETE FROM analysis_character_claim
            WHERE cut_id IN (SELECT id FROM webtoon_cut WHERE episode_id = %s)
            """,
            (webtoon_episode_id,),
        )
        for d in deceptions or []:
            cut_id = cut_map.get(d.get("cut"))
            if cut_id is None:
                continue
            cid = d.get("character_id")
            if cid is not None and cid not in valid_ids:
                cid = None
            cur.execute(
                """
                INSERT INTO analysis_character_claim
                    (cut_id, character_id, claim, contradicts, is_deception, confidence,
                     run_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, true, %s, %s, %s, %s)
                """,
                (cut_id, cid, d.get("claim") or "", d.get("contradicts") or "",
                 d.get("confidence"), run_id, now, now),
            )
            inserted += 1
    return inserted


def apply_resolution(ep: "ExtractResult | int", result: "ResolveResult", *,
                     webtoon_id: Optional[int] = None,
                     run_id: Optional[int] = None,
                     refresh_suggestions: bool = True) -> dict:
    """R+N `ResolveResult`를 에피소드 전체 DB에 결정론적으로 투영·커밋한다(step3c, **LLM 없음**).

    `refresh_suggestions=False`는 reapply(LLM 없는 재투영) 전용 — suggestion 큐를 건드리지
    않는다. 큐 원료 일부(name 후보 confidence, face_reassignments)는 스냅샷에 비영속이라
    재투영 시 delete-reinsert하면 직전 run의 pending 제안이 유실되기 때문.

    소급 전파(Req 5.2)·멱등(Req 5.3)·동결(Req 5.4/3.4)을 보장한다. 커밋 대상(v4.0 §17.4):
      1) characters → 명명·승격(kind=character)/significance 투영 + 잔여 이름 후보/병합/충돌은
         통합 suggestion 큐로. 이름은 Character 1행에만 저장되고 speaker_id FK 조인으로 전 컷에
         **소급** 반영된다. 자동 병합/자동 얼굴 재배정은 하지 않는다(제안만 — Req 10.4).
      2) speaker_resolution → TextAnnotation.speaker_id + resolution_status='resolved'(임계값 이상),
         provisional 화자 승격 안전망, 화자없는 블록(narration/system/other) resolved.
      3) beats → EpisodeBeat, 4) episode → EpisodeReport(summary+teaser),
      5) threads → NarrativeThread, 6) deceptions → CharacterClaim,
      7) profiles → CharacterProfile llm 행 병합(human 행 불가침).
    모든 산출물에 run_id를 귀속시킨다(§17.1 — 서빙/폐기 단위).

    동결: source='human' 주석과 is_confirmed=true Character는 절대 변경하지 않는다(Property 4).
    진행/stale 마킹은 하지 않는다 — run 원장에서 도출(§17.1).

    `ep`는 step3a 산출 `ExtractResult` 또는 webtoon_episode_id 정수. `result.error`가 있으면(해소
    실패) 아무것도 커밋하지 않고 빈 meta를 반환한다.

    Returns: 커밋 통계 episode_meta dict(액티비티 반환/run stats용).
    """
    if isinstance(ep, ExtractResult):
        webtoon_episode_id = ep.webtoon_episode_id
    elif isinstance(ep, int):
        webtoon_episode_id = ep
    else:
        raise TypeError(f"apply_resolution: ep는 ExtractResult 또는 int 여야 합니다(got {type(ep)!r})")

    info = _episode_info(webtoon_episode_id)
    episode_no = info["episode_no"]
    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)

    # 해소 실패(빈 결과) → 커밋 없이 빈 meta 반환(run 유지 — Req 7.4).
    if result is None or getattr(result, "error", None):
        logger.warning(
            "[step3.apply] episode %s — ResolveResult error/none, 커밋 스킵: %s",
            webtoon_episode_id, getattr(result, "error", None),
        )
        return {
            "webtoon_id": webtoon_id, "episode_no": episode_no,
            "episode_id": webtoon_episode_id,
            "stats": {"skipped": True},
        }

    now = datetime.now(timezone.utc)
    valid_ids = _webtoon_character_ids(webtoon_id)
    cut_map = _episode_cut_id_map(webtoon_episode_id)
    region_map = _episode_region_map(webtoon_episode_id)
    no_id_map = _episode_no_id_map(webtoon_id)

    # 1) 이름·승격 투영(소급) + 통합 suggestion 큐 적재(name/merge/label_conflict/face_reassign).
    #    refresh_suggestions=False(reapply)면 큐를 건드리지 않는다 — 재투영은 LLM 산출이 아니므로
    #    직전 run이 만든 pending 제안(특히 name/face_reassign — 스냅샷에 비영속)을 지우면 안 된다.
    name_suggestions = _project_characters(webtoon_id, result.characters, valid_ids, now)
    if refresh_suggestions:
        face_map = (
            _episode_face_detection_map(webtoon_episode_id)
            if result.face_reassignments else {}
        )
        n_suggestions = _commit_suggestions(
            webtoon_id, webtoon_episode_id, name_suggestions, result.characters, now, run_id,
            face_reassignments=result.face_reassignments,
            face_map=face_map, valid_ids=valid_ids,
        )
    else:
        n_suggestions = 0

    # 2) 화자 해소 커밋 + provisional 승격 + 화자없는 블록 resolved.
    n_speakers = _commit_speaker_resolution(
        webtoon_episode_id, region_map, result.speaker_resolution, valid_ids, now,
    )

    # 3) 비트.
    n_beats = _commit_beats(webtoon_episode_id, result.beats, now, run_id)

    # 4) 회차 리포트(인물 타임라인 = characters 요약 — 검토 UI/재적용 재구성용 스냅샷).
    character_timeline = [
        {"character_id": c.get("character_id"), "name": c.get("name"),
         "significance": c.get("significance"), "evidence": c.get("evidence") or "",
         "label_conflict": c.get("label_conflict"),
         "merge_suggestion": c.get("merge_suggestion") or []}
        for c in (result.characters or []) if c.get("character_id") is not None
    ]
    _commit_episode_report(webtoon_episode_id, result.episode, character_timeline, now, run_id)

    # 5) 떡밥(threads).
    n_threads = _commit_threads(
        webtoon_id, webtoon_episode_id, episode_no, no_id_map, result.threads, now, run_id,
    )

    # 6) 거짓/책략(deceptions → claims).
    n_claims = _commit_claims(webtoon_episode_id, cut_map, result.deceptions, valid_ids, now, run_id)

    # 7) 인물도감 프로필(llm 행 병합 — human 불가침).
    n_profiles = _commit_profiles(webtoon_id, result.profiles, valid_ids, now, run_id)

    stats = {
        "speakers_resolved": n_speakers,
        "beats": n_beats,
        "suggestions": n_suggestions,
        "threads": n_threads,
        "claims": n_claims,
        "profiles": n_profiles,
    }
    logger.info("[step3.apply] episode %s — %s", webtoon_episode_id, stats)

    return {
        "webtoon_id": webtoon_id,
        "episode_no": episode_no,
        "episode_id": webtoon_episode_id,
        "stats": stats,
    }


# ── 재처리 / Human-in-the-loop (v4.0 §17.1) ──────────────────────────────────
# staleness는 저장하지 않는다: human 수정 API(service)가 webtoon_cut.human_modified_at을 찍고,
# "재해소 필요"는 runs.episode_needs_reresolve(human_modified_at > 최신 succeeded resolve run)로
# 도출한다. 재해소 실행은 reresolve_episode(아래) / src.tools.reresolve CLI.

# ── Pass-1 레코드 DB 재구성 (비전 재실행 없는 재해소용) ────────────────────────

def _load_scene_meta(cut_id: int) -> dict:
    """cut_scene_meta(action_summary, key_objects) 로드 — Pass-1 cut_summary/key_objects 재구성용."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT action_summary, key_objects FROM analysis_cut_scene_meta WHERE cut_id=%s",
            (cut_id,),
        )
        row = cur.fetchone()
    if not row:
        return {"action_summary": "", "key_objects": []}
    return {
        "action_summary": row[0] or "",
        "key_objects": row[1] if isinstance(row[1], list) else [],
    }


def _load_provisional_blocks(cut_id: int) -> list[dict]:
    """영속된 provisional 어노테이션(source='llm')으로 Pass-1 blocks 재구성(index 순, 1:1 유지).

    corrected_text(text)/type/화자 후보(speaker_id, 2026-07-05부터 영속)는 복원된다 —
    speaker.character_id로 실어 Pass-2a 페이로드의 `spk_cid`가 된다. face_label/basis/tail_hint/
    type_confidence는 비-영속이라 null shape(Pass-2a가 맥락으로 재해소).
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT tr.index, ta.type, ta.text, ta.speaker_id
            FROM analysis_text_region tr
            JOIN analysis_text_annotation ta ON ta.region_id = tr.id AND ta.source = 'llm'
            WHERE tr.cut_id = %s AND tr.is_excluded = false
            ORDER BY tr.index
            """,
            (cut_id,),
        )
        rows = cur.fetchall()
    blocks: list[dict] = []
    for idx, btype, text, speaker_id in rows:
        if btype not in _BLOCK_TYPES:
            btype = None
        blocks.append({
            "index": idx,
            "type": btype,
            "type_confidence": 0.0,
            "corrected_text": text or "",
            "speaker": {"face_label": None, "name": None, "character_id": speaker_id,
                        "confidence": 0.0, "basis": "none", "tail_hint": "none"},
        })
    return blocks


def _load_pass1_records_from_db(webtoon_episode_id: int) -> list["Pass1Record"]:
    """영속된 provisional 어노테이션 + faces + cut_scene_meta에서 컷 읽기순 Pass-1 레코드를 재구성한다.

    비전(step3a) 재실행 없이 step3b(resolve_episode_windowed)를 돌리기 위한 로더다(Req 10.1 —
    재해소 단위=에피소드, 비전 결과가 유효할 때 비용 절감). `extract_episode`가 만드는 전이
    ExtractResult.records와 동형(同形)의 `Pass1Record` 목록을 돌려준다.

    보존: blocks.corrected_text/type, faces(character_id+이름), cut_summary/key_objects.
    손실(비-영속): provisional 화자 후보 confidence/basis/tail, characters.prominence/emotion,
    name_evidence — Pass-2a가 트랜스크립트·faces·prior로 재도출하므로 전역 해소에 본질적 손실 없음.
    OCR 어노테이션도 얼굴도 없는 컷은 `skipped='empty'`로 둔다(payload builder가 제외).
    """
    with db_cursor() as cur:
        cur.execute(
            "SELECT id, cut_number FROM webtoon_cut WHERE episode_id=%s ORDER BY cut_number",
            (webtoon_episode_id,),
        )
        cuts = cur.fetchall()

    records: list[Pass1Record] = []
    for cut_id, cut_number in cuts:
        faces = _load_faces(cut_id)
        blocks = _load_provisional_blocks(cut_id)
        if not blocks and not faces:
            records.append(Pass1Record(cut_number=cut_number, cut_id=cut_id,
                                       faces=faces, skipped="empty"))
            continue
        scene = _load_scene_meta(cut_id)
        result = {
            "cut_summary": scene["action_summary"],
            "key_objects": scene["key_objects"],
            # prominence/emotion은 비-영속 → faces에서 face_label만 복원(Pass-2a가 재도출).
            "characters": [{"face_label": f["id"], "prominence": None, "emotion": ""}
                           for f in faces],
            "blocks": blocks,
            "name_evidence": [],
        }
        records.append(Pass1Record(cut_number=cut_number, cut_id=cut_id,
                                    result=result, faces=faces))
    return records


# ── ResolveResult DB 재구성 (이름 테이블만 변경 시 step3c-only 재적용용) ───────

def _load_resolve_result_from_db(webtoon_episode_id: int, webtoon_id: int) -> "ResolveResult":
    """영속된 Pass-2a 산출(EpisodeReport/EpisodeBeat/NarrativeThread/CharacterClaim + 해소 주석)을
    `ResolveResult`로 재구성한다 — LLM 없는 step3c 재적용(`reapply_episode`, Req 10.2)용.

    이름/중요도(characters.name/significance)는 **현재 Character 테이블(라이브 진실)** 에서 끌어온다.
    그래야 human이 수락/수정한 이름이 비정규화 스냅샷(character_timeline)과 누적 서사 상태에 다시
    투영된다(Req 10.2 — 이름 테이블 변경의 일괄 재적용). evidence/label_conflict/merge_suggestion은
    LLM 파생값이라 character_timeline 스냅샷에서 가져온다.

    beats/episode/threads/deceptions/speaker_resolution은 영속 테이블에서 **빠짐없이** 재구성한다.
    apply_resolution의 _commit_* 들이 스코프 delete-reinsert(멱등)라, 부분 재구성은 기존 데이터를
    지울 수 있기 때문이다(전체 재구성 = 멱등 무손실 재적용 — Property 3).
    """
    with db_cursor() as cur:
        # EpisodeReport — episode 메타 + character_timeline(스냅샷).
        cur.execute(
            """
            SELECT summary, teaser, appeal_point, cliffhanger, foreshadowing, character_timeline
            FROM analysis_episode_report WHERE episode_id=%s
            """,
            (webtoon_episode_id,),
        )
        rep = cur.fetchone()
        if rep:
            summary, teaser, appeal_point, cliffhanger, foreshadowing, timeline = rep
            episode = {
                "summary": summary or "",
                "teaser": teaser or "",
                "appeal_point": appeal_point or "",
                "cliffhanger": cliffhanger or "",
                "foreshadowing": foreshadowing if isinstance(foreshadowing, list) else [],
            }
            timeline = timeline if isinstance(timeline, list) else []
        else:
            episode = {}
            timeline = []

        # EpisodeBeat — 비트.
        cur.execute(
            """
            SELECT cut_start, cut_end, hook_type, appeal_point, intensity
            FROM analysis_episode_beat WHERE episode_id=%s ORDER BY cut_start, cut_end
            """,
            (webtoon_episode_id,),
        )
        beats = [
            {"cut_start": cs, "cut_end": ce, "hook_type": hook or "",
             "appeal_point": ap or "", "intensity": inten}
            for cs, ce, hook, ap, inten in cur.fetchall()
        ]

        # NarrativeThread — 이 에피소드가 심은(planted) 떡밥(스코프 delete가 planted_episode 기준이라
        # 동일 스코프만 재구성 → 멱등). planted/resolved 에피소드는 회차번호(no)로.
        cur.execute(
            """
            SELECT nt.description, nt.type, nt.status, pe.no, nt.planted_cut,
                   re.no, nt.resolved_cut, nt.confidence
            FROM analysis_narrative_thread nt
            LEFT JOIN webtoon_episode pe ON nt.planted_episode_id = pe.id
            LEFT JOIN webtoon_episode re ON nt.resolved_episode_id = re.id
            WHERE nt.webtoon_id=%s AND nt.planted_episode_id=%s AND nt.deleted_at IS NULL
            ORDER BY nt.id
            """,
            (webtoon_id, webtoon_episode_id),
        )
        threads = [
            {"description": desc or "", "type": ttype or "", "status": status or "open",
             "planted_episode": p_no, "planted_cut": p_cut,
             "resolved_episode": r_no, "resolved_cut": r_cut, "confidence": conf}
            for desc, ttype, status, p_no, p_cut, r_no, r_cut, conf in cur.fetchall()
        ]

        # CharacterClaim — deceptions(이 에피소드 컷).
        cur.execute(
            """
            SELECT wc.cut_number, cc.character_id, cc.claim, cc.contradicts, cc.confidence
            FROM analysis_character_claim cc
            JOIN webtoon_cut wc ON cc.cut_id = wc.id
            WHERE wc.episode_id=%s AND cc.is_deception = true
            ORDER BY wc.cut_number
            """,
            (webtoon_episode_id,),
        )
        deceptions = [
            {"cut": cut, "character_id": cid, "claim": claim or "",
             "contradicts": contradicts or "", "confidence": conf}
            for cut, cid, claim, contradicts, conf in cur.fetchall()
        ]

        # 해소 주석 → speaker_resolution 재구성(현재 화자 재확인 — 멱등). speaker_id=character_id는
        # rename에 영향받지 않으므로 이름은 FK 조인으로 소급, 여기선 resolved 상태만 안정 유지.
        cur.execute(
            """
            SELECT wc.cut_number, tr.index, ta.speaker_id
            FROM analysis_text_annotation ta
            JOIN analysis_text_region tr ON ta.region_id = tr.id
            JOIN webtoon_cut wc ON tr.cut_id = wc.id
            WHERE wc.episode_id=%s AND ta.source='llm'
              AND ta.resolution_status='resolved' AND ta.speaker_id IS NOT NULL
            ORDER BY wc.cut_number, tr.index
            """,
            (webtoon_episode_id,),
        )
        speaker_resolution = [
            {"cut": cut, "block_index": idx, "character_id": sid,
             "confidence": 1.0, "reason": "reapply: 기존 해소 화자 재확인"}
            for cut, idx, sid in cur.fetchall()
        ]

        # characters — 현재 Character(라이브 이름/중요도) + timeline 스냅샷(evidence/label_conflict/
        # merge_suggestion). 이름 테이블 변경을 일괄 재투영(Req 10.2).
        cids = [e.get("character_id") for e in timeline if e.get("character_id") is not None]
        current: dict = {}
        if cids:
            cur.execute(
                "SELECT id, name, kind, significance FROM analysis_character "
                "WHERE id = ANY(%s) AND deleted_at IS NULL",
                (cids,),
            )
            for cid, name, kind, sig in cur.fetchall():
                current[cid] = (name, kind, sig)

    characters: list[dict] = []
    for e in timeline:
        cid = e.get("character_id")
        if cid is None:
            continue
        cur_name, cur_kind, cur_sig = current.get(cid, (None, None, None))
        live_name = cur_name if (cur_name and cur_kind == "character") else None
        characters.append({
            "character_id": cid,
            "name": live_name,
            # 현재 명명 인물(kind=character)의 이름만 소급 전파 — 클러스터는 미명명 유지.
            # 이미 명명된 이름은 _project_characters의 frozen/cluster 규칙으로 보존된다.
            "name_confidence": 1.0,
            "significance": cur_sig if cur_sig in _SIGNIFICANCE else e.get("significance"),
            "evidence": e.get("evidence") or "",
            "label_conflict": e.get("label_conflict"),
            "merge_suggestion": e.get("merge_suggestion") or [],
        })

    return ResolveResult(
        webtoon_episode_id=webtoon_episode_id,
        characters=characters,
        speaker_resolution=speaker_resolution,
        beats=beats,
        episode=episode,
        deceptions=deceptions,
        threads=threads,
    )


# ── 재처리 진입점 ─────────────────────────────────────────────────────────────

def reresolve_episode(
    webtoon_episode_id: int,
    *,
    rerun_extract: bool = False,
    prior_context=None,
    token_budget: Optional[int] = None,
    heartbeat_cb=None,
    webtoon_id: Optional[int] = None,
) -> dict:
    """에피소드 단위 재해소(Req 10.1) — human 수정 후 R→N→apply를 새 run으로 재실행(v4.0).

    재해소 단위는 **에피소드**다(컷 아님 — 전역 해소 특성상 한 컷만 다시 푸는 건 불가능).
    HITL 흐름: human이 수정(service가 human_modified_at 마킹) → `runs.episode_needs_reresolve`가
    True → 이 함수(또는 src.tools.reresolve CLI)로 재해소. 새 resolve run(성공 시 succeeded)이
    생기면 human_modified_at < run.finished_at이 되어 stale 도출이 자연히 해소된다 — 플래그 클리어
    불필요(§17.1).

    rerun_extract=False(기본): 비전(Stage V)을 재실행하지 **않고** 영속 provisional 레코드를
        `_load_pass1_records_from_db`로 재구성해 R부터 재해소한다(비용/시간 절감).
    rerun_extract=True: 비전까지 재실행(새 vision run) — OCR/얼굴 입력 자체가 바뀐 경우
        (**human이 얼굴↔캐릭터 매칭을 고친 경우 필수** — identified_faces 입력이 바뀜).

    Returns: 재해소 요약 dict(records 수, 해소 에러 여부, run_id, episode_meta).
    """
    from src.core import runs

    info = _episode_info(webtoon_episode_id)
    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)
    ctx = resolve_llm_model(webtoon_id)
    llm_model_id = ctx.get("id")

    vision_run_id = None
    if rerun_extract:
        vision_run_id = runs.start_run(webtoon_id, webtoon_episode_id, runs.KIND_VISION,
                                       llm_model_id=llm_model_id)
        ext = extract_episode(webtoon_episode_id, heartbeat_cb=heartbeat_cb, prepare=True,
                              run_id=vision_run_id)
        runs.finish_run(vision_run_id, stats={
            "cuts_total": ext.cuts_total, "cuts_analyzed": ext.cuts_analyzed,
            "cuts_skipped": ext.cuts_skipped, "usage": ext.usage_total,
        })
        records = ext.records
    else:
        vision_run_id = runs.latest_succeeded_run_id(webtoon_episode_id, runs.KIND_VISION)
        records = _load_pass1_records_from_db(webtoon_episode_id)

    if prior_context is None:
        prior_context = narrative_context.load_prior(webtoon_id, info["episode_no"])

    run_id = runs.start_run(webtoon_id, webtoon_episode_id, runs.KIND_RESOLVE,
                            llm_model_id=llm_model_id, vision_run_id=vision_run_id)
    result = resolve_and_narrate(
        webtoon_episode_id, prior_context,
        records=records, webtoon_id=webtoon_id, ctx=ctx,
        token_budget=token_budget, run_id=run_id,
    )
    episode_meta = apply_resolution(webtoon_episode_id, result, webtoon_id=webtoon_id,
                                    run_id=run_id)
    if result.error:
        runs.finish_run(run_id, status="failed", error=result.error)
    else:
        runs.finish_run(run_id, stats=episode_meta.get("stats") or {})

    logger.info(
        "[step3.reresolve] episode %s — rerun_extract=%s records=%s error=%s run=%s",
        webtoon_episode_id, rerun_extract, len(records), result.error, run_id,
    )
    return {
        "webtoon_episode_id": webtoon_episode_id,
        "rerun_extract": rerun_extract,
        "records": len(records),
        "resolve_error": result.error,
        "run_id": run_id,
        "episode_meta": episode_meta,
    }


def reapply_episode(webtoon_episode_id: int, *, webtoon_id: Optional[int] = None) -> dict:
    """이름 테이블만 변경된 경우의 부분 재처리(Req 10.2) — **LLM 없이 apply만 재실행**.

    근거(소급은 조인으로 자동): 확정 이름은 Character 행 1곳에만 저장되고 TextAnnotation은
    speaker_id FK로 그 행을 가리키므로, human 이름 수락/수정은 조인으로 즉시 전 컷에 반영된다.
    조인으로 갱신되지 않는 비정규화 스냅샷(EpisodeReport.character_timeline의 이름)과
    significance/is_match_excluded 투영만 재적용이 필요하다. LLM 콜 0회 — run은 만들지 않는다
    (LLM 산출이 아니라 기존 run 산출의 재투영이므로).

    경계: EpisodeReport가 없으면(한 번도 해소된 적 없음) 재적용할 산출이 없으므로 스킵한다.
    """
    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)

    with db_cursor() as cur:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM analysis_episode_report WHERE episode_id=%s)",
            (webtoon_episode_id,),
        )
        has_report = bool(cur.fetchone()[0])

    if not has_report:
        logger.info(
            "[step3.reapply] episode %s — 영속 해소 산출 없음(EpisodeReport 부재), 재적용 스킵 "
            "(최초 해소는 reresolve_episode 필요).",
            webtoon_episode_id,
        )
        return {
            "webtoon_episode_id": webtoon_episode_id,
            "reapplied": False,
            "reason": "no_resolution",
        }

    from src.core import runs

    run_id = runs.latest_succeeded_run_id(webtoon_episode_id, runs.KIND_RESOLVE)
    result = _load_resolve_result_from_db(webtoon_episode_id, webtoon_id)
    # refresh_suggestions=False — 재투영은 제안 큐를 재생성하지 않는다(직전 run의 pending 보존).
    episode_meta = apply_resolution(webtoon_episode_id, result, webtoon_id=webtoon_id,
                                    run_id=run_id, refresh_suggestions=False)

    logger.info(
        "[step3.reapply] episode %s — apply-only 재적용(LLM 없음) chars=%s beats=%s threads=%s claims=%s",
        webtoon_episode_id, len(result.characters), len(result.beats),
        len(result.threads), len(result.deceptions),
    )
    return {
        "webtoon_episode_id": webtoon_episode_id,
        "reapplied": True,
        "episode_meta": episode_meta,
    }
