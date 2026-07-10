"""캐릭터 재분석(재도출) — mode=profile 원천 재도출 코어 (prd §20, 2026-07-10 설계).

병합/얼굴교정 후 프로필을 "재봉합(stitch)"이 아니라 **원천 근거로 다시 계산(re-derive)** 한다:
귀속 대사(speaker) + 등장 컷 장면 서술(현재 FaceIdentity 기준) + 과거 프로필 조각(흡수분 포함)을
전량 주입해 LLM 1콜로 프로필 전체를 재생성하고 **무캡 replace** upsert 한다(§20.6).

- 모델: 특별 배선 없이 `resolve_llm_model(webtoon_id, TEXT)`(DB 기본 glm-5.2 + self-FK fallback).
- 프롬프트: v3(§20.6 확정) — role=항상적 정체, progression=변천사(자유형), 장르 addendum 합성,
  교차 인물 혼동 가드("이 인물 아님" 목록 주입).
- run 원장: kind='profile'(episode NULL, 웹툰 단위 — arc와 동일 패턴). stats.character_id/mode로
  프론트 진행표시가 캐릭터별 running 여부를 도출한다. supersede는 같은 character_id의 running
  행만(웹툰 내 다른 캐릭터의 재도출과 독립).
- mode=reresolve(화자 재귀속)는 이 모듈이 아니라 기존 `step3.reresolve_episode(rerun_extract=True)`
  를 에피소드 순차 실행(Temporal 워크플로가 오케스트레이션) 후, 마지막에 이 재도출로 프로필을
  clean 근거 위에서 다시 뽑는다.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2.extras import Json

from src.config.db import db_cursor
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import TEXT, resolve_llm_model

logger = logging.getLogger(__name__)

_PROFILE_STAGE = "profile"  # LLMUsage.stage / AnalysisRun.kind — 재도출 전용
_REGEN_RETRIES = 2          # 파싱/일시 오류 1회 재시도(step3 _PASS2_RETRIES와 동일)
_REGEN_MAX_TEMPERATURE = 0.2

# ── v3 프롬프트 (§20.6 확정 원문 — prd.md가 정본) ──────────────────────────────

_BASE_PROMPT = (
    "당신은 웹툰 캐릭터의 모든 근거 자료를 읽고 철저하고 상세한 인물 도감 프로필을 재구성하는 "
    "분석기입니다. 한국어. 마지막에 JSON만 출력.\n"
    "입력 근거 3종: 1) dialogue(귀속 대사/독백 — speech 거짓·과장 가능, monologue 속마음, "
    "narration/system 객관진실) 2) scenes(등장 컷 행동 서술, 객관진실) "
    "3) prior_profile_fragments(과거 조각, 중복·구식 가능).\n"
    "[절차] 1단계 전수추출: dialogue·scenes 처음~끝, 모든 개별 사실 빠짐없이 회차(ep)순. "
    "2단계 중복만 병합(서로 다른 사실 생략 금지). 3단계 스키마 정리.\n"
    "[role 규칙] role은 항상적 정체·서사적 위치(장기 불변). 특정 회차 사건·상황 금지 — 사건은 "
    "key_facts로. 어느 시점에 봐도 성립하는 한 문장.\n"
    "[progression] 시간에 따라 변하는 값은 traits로 뭉개지 말고 progression에 연대순: "
    '[{"when":"회차/국면","change":"무엇이 어떻게 바뀌었나"}]. traits엔 최근 현재값만.\n'
    "[분량] 주요 인물이라 근거 풍부 — key_facts 15+, traits 8+, progression 변하는 값 모두. "
    "요약으로 날리지 말되 근거 없는 창작 금지.\n"
    "[교차 인물 혼동 금지] 다른 등장인물 이름·정체·행적을 이 인물에 섞지 말 것 — 입력의 "
    "not_this_character 목록은 **이 인물이 아니다**. 그들의 정체/행적을 이 프로필에 쓰지 말 것.\n"
    "[출력 JSON] {gender, age_group, affiliation, role(항상적 한 문장), personality[3~6], "
    "traits{현재값 8+}, key_facts[연대순 15+], progression[{when,change}]}"
)

# 장르 addendum(§20.6-3) — webtoon.genre 버킷으로 base에 이어붙인다. 스키마 포크 없음.
_GENRE_ADDENDA = {
    "fantasy": (
        "\n[장르 지침 — 판타지/게임/무협] 스탯 수치·스킬/능력 습득·아이템/장비·정수/파워 시스템·"
        "전투력 성장을 빠짐없이. 수치·능력 변화 시점은 progression에 연대순."
    ),
    "romance": (
        "\n[장르 지침 — 로맨스/로판] 관계·호감/애정 변화·감정선·오해와 화해 중심. 스탯 만들지 말 것. "
        "관계 진전/후퇴는 progression에."
    ),
    "drama": (
        "\n[장르 지침 — 스릴러/드라마] 비밀·진짜 동기·처지·심경·관계 갈등·반전 중심. 스탯 만들지 말 것. "
        "처지·내면 변화는 progression에."
    ),
}

_GENRE_BUCKETS = (
    ("fantasy", ("판타지", "게임", "무협", "액션")),
    ("romance", ("로맨스", "로판", "순정")),
    ("drama", ("스릴러", "드라마", "미스터리", "공포")),
)


def _genre_addendum(genre: str) -> str:
    g = (genre or "").lower()
    for bucket, keys in _GENRE_BUCKETS:
        if any(k in g for k in keys):
            return _GENRE_ADDENDA[bucket]
    return ""


# ── 근거 수집 ─────────────────────────────────────────────────────────────────

def _character_info(character_id: int) -> Optional[dict]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT c.webtoon_id, c.name, c.aliases, c.kind, c.significance,
                   w.genre, w.source, w.title_id
            FROM analysis_character c
            JOIN webtoon w ON c.webtoon_id = w.id
            WHERE c.id = %s AND c.deleted_at IS NULL
            """,
            (character_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "webtoon_id": row[0], "name": row[1] or "", "aliases": row[2] or [],
        "kind": row[3], "significance": row[4],
        "genre": row[5] or "", "source": row[6], "title_id": row[7],
    }


def _gather_dialogue(character_id: int) -> list[dict]:
    """귀속 대사/독백 — region당 유효(human>llm) 어노테이션의 speaker가 이 캐릭터인 것 전부."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.no, wc.cut_number, ta.type, ta.text
            FROM analysis_text_region tr
            JOIN webtoon_cut wc ON tr.cut_id = wc.id
            JOIN webtoon_episode we ON wc.episode_id = we.id
            JOIN LATERAL (
                SELECT type, text, speaker_id
                FROM analysis_text_annotation
                WHERE region_id = tr.id AND deleted_at IS NULL AND source IN ('human', 'llm')
                ORDER BY source ASC
                LIMIT 1
            ) ta ON true
            WHERE tr.deleted_at IS NULL AND tr.is_excluded = false
              AND wc.deleted_at IS NULL AND we.deleted_at IS NULL
              AND ta.speaker_id = %s AND ta.type IN ('speech', 'monologue')
            ORDER BY we.no, wc.cut_number, tr.index
            """,
            (character_id,),
        )
        return [
            {"ep": r[0], "cut": r[1], "type": r[2], "text": r[3]}
            for r in cur.fetchall() if (r[3] or "").strip()
        ]


def _appearing_cut_ids(character_id: int) -> list[tuple[int, int, int]]:
    """캐릭터 얼굴이 등장한 컷 — 현재 유효 정체(human>step2) 기준. [(cut_id, ep_no, cut_number)]."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT wc.id, we.no, wc.cut_number
            FROM analysis_face_detection fd
            JOIN webtoon_cut wc ON fd.cut_id = wc.id
            JOIN webtoon_episode we ON wc.episode_id = we.id
            JOIN LATERAL (
                SELECT appearance_id
                FROM analysis_face_identity
                WHERE detection_id = fd.id AND deleted_at IS NULL
                ORDER BY source ASC
                LIMIT 1
            ) fi ON true
            JOIN analysis_character_appearance ca ON fi.appearance_id = ca.id
            WHERE fd.is_used = true AND fd.deleted_at IS NULL
              AND wc.deleted_at IS NULL AND we.deleted_at IS NULL
              AND ca.character_id = %s
            ORDER BY we.no, wc.cut_number
            """,
            (character_id,),
        )
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def _gather_scenes(cut_rows: list[tuple[int, int, int]]) -> list[dict]:
    """등장 컷의 장면 서술(action_summary, 객관진실) + 그 컷의 narration/system 텍스트."""
    if not cut_rows:
        return []
    cut_ids = [c[0] for c in cut_rows]
    actions: dict[int, str] = {}
    texts: dict[int, list[str]] = {}
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT cut_id, action_summary FROM analysis_cut_scene_meta
            WHERE cut_id = ANY(%s) AND deleted_at IS NULL
            """,
            (cut_ids,),
        )
        for cid, action in cur.fetchall():
            if (action or "").strip():
                actions[cid] = action
        cur.execute(
            """
            SELECT tr.cut_id, ta.text
            FROM analysis_text_region tr
            JOIN LATERAL (
                SELECT type, text
                FROM analysis_text_annotation
                WHERE region_id = tr.id AND deleted_at IS NULL AND source IN ('human', 'llm')
                ORDER BY source ASC
                LIMIT 1
            ) ta ON true
            WHERE tr.cut_id = ANY(%s) AND tr.deleted_at IS NULL AND tr.is_excluded = false
              AND ta.type IN ('narration', 'system')
            ORDER BY tr.cut_id, tr.index
            """,
            (cut_ids,),
        )
        for cid, text in cur.fetchall():
            if (text or "").strip():
                texts.setdefault(cid, []).append(text)

    scenes = []
    for cid, ep_no, cut_number in cut_rows:
        if cid not in actions and cid not in texts:
            continue
        scene = {"ep": ep_no, "cut": cut_number}
        if cid in actions:
            scene["action"] = actions[cid]
        if cid in texts:
            scene["texts"] = texts[cid]
        scenes.append(scene)
    return scenes


def _gather_prior_fragments(character_id: int, absorbed_ids: list[int]) -> list[dict]:
    """과거 프로필 조각 — 본인 활성(llm/human) 행 + 흡수(absorbed) soft-delete 행(§20.4)."""
    ids = [character_id] + [i for i in (absorbed_ids or []) if i and i != character_id]
    frags: list[dict] = []
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT character_id, source, role, personality, traits, key_facts, progression,
                   (deleted_at IS NOT NULL) AS soft_deleted
            FROM analysis_character_profile
            WHERE character_id = ANY(%s)
            ORDER BY character_id, source
            """,
            (ids,),
        )
        for cid, source, role, personality, traits, key_facts, progression, soft_deleted in cur.fetchall():
            if cid == character_id and soft_deleted:
                continue  # 본인 폐기분은 노이즈 — 활성 행만
            frag = {"source": source, "absorbed": cid != character_id}
            if (role or "").strip():
                frag["role"] = role
            if personality:
                frag["personality"] = personality
            if traits:
                frag["traits"] = traits
            if key_facts:
                frag["key_facts"] = key_facts
            if progression:
                frag["progression"] = progression
            if len(frag) > 2:
                frags.append(frag)
    return frags


def _gather_not_this_character(webtoon_id: int, character_id: int) -> list[str]:
    """교차 인물 혼동 가드 — 같은 웹툰의 다른 명명 인물 이름/별칭(§20.6-4)."""
    names: list[str] = []
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT name, aliases FROM analysis_character
            WHERE webtoon_id = %s AND id != %s AND kind = 'character'
              AND deleted_at IS NULL AND name != ''
            ORDER BY id
            """,
            (webtoon_id, character_id),
        )
        for name, aliases in cur.fetchall():
            names.append(name)
            for a in aliases or []:
                if isinstance(a, str) and a.strip() and a not in names:
                    names.append(a)
    return names


# ── 산출 정제 ─────────────────────────────────────────────────────────────────

def _str_list(raw) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [str(x).strip() for x in raw if isinstance(x, (str, int, float)) and str(x).strip()]


def _sanitize_regen_profile(raw: dict) -> dict:
    """재도출 산출 JSON → 저장 shape. 무캡(§20.6 — 슬라이딩 윈도우는 지식 폐기)."""
    prof = raw if isinstance(raw, dict) else {}
    # 모델이 {"profile": {...}}로 감쌀 수 있음 — 한 겹 벗긴다.
    if isinstance(prof.get("profile"), dict):
        prof = prof["profile"]

    traits = {}
    if isinstance(prof.get("traits"), dict):
        for k, v in prof["traits"].items():
            key = str(k).strip()
            if not key:
                continue
            traits[key] = v if isinstance(v, (str, int, float, bool)) else json.dumps(v, ensure_ascii=False)

    progression = []
    if isinstance(prof.get("progression"), list):
        for item in prof["progression"]:
            if not isinstance(item, dict):
                continue
            when = str(item.get("when") or "").strip()
            change = str(item.get("change") or "").strip()
            if change:
                progression.append({"when": when, "change": change})

    return {
        "gender": str(prof.get("gender") or "")[:16],
        "age_group": str(prof.get("age_group") or "")[:16],
        "affiliation": str(prof.get("affiliation") or "")[:128],
        "role": str(prof.get("role") or "")[:256],
        "personality": _str_list(prof.get("personality")),
        "traits": traits,
        "key_facts": _str_list(prof.get("key_facts")),
        "progression": progression,
    }


# ── run 원장 (kind='profile', episode NULL) ───────────────────────────────────

def begin_profile_run(webtoon_id: int, character_id: int, mode: str,
                      llm_model_id: Optional[int] = None,
                      episodes_total: int = 0) -> int:
    """재분석 umbrella run 시작 — 같은 캐릭터의 잔여 running 행만 supersede(다른 캐릭터와 독립)."""
    now = datetime.now(timezone.utc)
    stats = {"character_id": character_id, "mode": mode, "episodes_total": episodes_total,
             "episodes_done": 0}
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE analysis_run
            SET status='failed', error='superseded by new regen', finished_at=%s, updated_at=%s
            WHERE kind='profile' AND status='running' AND webtoon_id=%s
              AND (stats->>'character_id')::int = %s
            """,
            (now, now, webtoon_id, character_id),
        )
        cur.execute(
            """
            INSERT INTO analysis_run
                (webtoon_id, episode_id, kind, status, llm_model_id, started_at,
                 stats, error, created_at, updated_at)
            VALUES (%s, NULL, 'profile', 'running', %s, %s, %s, '', %s, %s)
            RETURNING id
            """,
            (webtoon_id, llm_model_id, now, Json(stats), now, now),
        )
        run_id = cur.fetchone()[0]
    logger.info("[regen] start profile run=%s character=%s mode=%s", run_id, character_id, mode)
    return run_id


def bump_profile_run_progress(run_id: int, episodes_done: int) -> None:
    """reresolve 모드 진행도 갱신 — stats.episodes_done(프론트 진행표시용)."""
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE analysis_run
            SET stats = stats || %s, updated_at = %s
            WHERE id = %s
            """,
            (Json({"episodes_done": episodes_done}), now, run_id),
        )


# ── 재도출 본체 (mode=profile) ────────────────────────────────────────────────

def regenerate_character_profile(
    character_id: int,
    *,
    absorbed_character_ids: Optional[list[int]] = None,
    run_id: Optional[int] = None,
) -> dict:
    """캐릭터 프로필 원천 재도출 — 근거 전량 주입 LLM 1콜 → llm 행 **무캡 replace** upsert(§20.6).

    human 행은 절대 건드리지 않는다(source 레이어링). 서빙은 필드 단위 human 우선 병합 그대로.
    absorbed_character_ids: 병합 직후 훅이 넘기는 흡수 캐릭터 id들 — soft-delete된 프로필
    조각(key_facts 등)을 prior_profile_fragments로 회수한다(§20.4). 수동 버튼 경로면 생략.
    반환: {"character_id", "profile", "usage", "error"}.
    """
    from src.core.step3 import _insert_llm_usage, _pass2_ctx  # 지연 import(무거운 모듈)

    info = _character_info(character_id)
    if info is None:
        logger.warning("[regen] character=%s 없음/삭제됨 — 재도출 건너뜀", character_id)
        return {"character_id": character_id, "profile": None, "usage": {},
                "error": "character not found"}
    webtoon_id = info["webtoon_id"]

    dialogue = _gather_dialogue(character_id)
    cut_rows = _appearing_cut_ids(character_id)
    scenes = _gather_scenes(cut_rows)
    fragments = _gather_prior_fragments(character_id, absorbed_character_ids or [])
    not_this = _gather_not_this_character(webtoon_id, character_id)

    payload = {
        "character": {
            "name": info["name"] or None,
            "aliases": info["aliases"],
            "significance": info["significance"],
        },
        "genre": info["genre"] or None,
        "dialogue": dialogue,
        "scenes": scenes,
        "prior_profile_fragments": fragments,
        "not_this_character": not_this,
    }
    user_text = json.dumps(payload, ensure_ascii=False)
    system_prompt = _BASE_PROMPT + _genre_addendum(info["genre"])

    ctx = resolve_llm_model(webtoon_id, TEXT)
    call_ctx = _pass2_ctx(ctx)
    logger.info(
        "[regen] character=%s(%s) — dialogue=%d scenes=%d fragments=%d payload=%d자 model=%s",
        character_id, info["name"], len(dialogue), len(scenes), len(fragments),
        len(user_text), ctx.get("model_id"),
    )

    raw_result: dict = {}
    usage: dict = {}
    err: Optional[str] = None
    for _attempt in range(_REGEN_RETRIES):
        try:
            call = call_llm_json(call_ctx, system_prompt, user_text, [])
            raw_result = call.result if isinstance(call.result, dict) else {}
            usage = call.usage or {}
            err = None
            break
        except Exception as e:  # noqa: BLE001 — 단일 콜 스테이지, 실패는 반환값으로 격리
            err = str(e)
            raw_result = {}

    _insert_llm_usage(webtoon_id, None, None, ctx.get("id"), usage,
                      stage=_PROFILE_STAGE, image_count=None, run_id=run_id,
                      extra={"character_id": character_id})

    if err is not None:
        logger.error("[regen] character=%s 재도출 실패: %s", character_id, err)
        return {"character_id": character_id, "profile": None, "usage": usage, "error": err}

    profile = _sanitize_regen_profile(raw_result)
    if not any((profile["role"], profile["key_facts"], profile["personality"], profile["traits"])):
        err = "재도출 산출이 비어 있음(파싱은 성공) — 기존 프로필 유지"
        logger.error("[regen] character=%s %s", character_id, err)
        return {"character_id": character_id, "profile": None, "usage": usage, "error": err}

    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_character_profile
                (character_id, source, gender, age_group, affiliation, role,
                 personality, traits, key_facts, progression, run_id, created_at, updated_at)
            VALUES (%s, 'llm', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uniq_character_profile_character_source
            DO UPDATE SET gender = EXCLUDED.gender, age_group = EXCLUDED.age_group,
                          affiliation = EXCLUDED.affiliation, role = EXCLUDED.role,
                          personality = EXCLUDED.personality, traits = EXCLUDED.traits,
                          key_facts = EXCLUDED.key_facts, progression = EXCLUDED.progression,
                          run_id = EXCLUDED.run_id, deleted_at = NULL,
                          updated_at = EXCLUDED.updated_at
            """,
            (character_id, profile["gender"], profile["age_group"], profile["affiliation"],
             profile["role"], Json(profile["personality"]), Json(profile["traits"]),
             Json(profile["key_facts"]), Json(profile["progression"]), run_id, now, now),
        )
    logger.info(
        "[regen] character=%s(%s) 재도출 완료 — key_facts=%d personality=%d traits=%d "
        "progression=%d tokens=%s",
        character_id, info["name"], len(profile["key_facts"]), len(profile["personality"]),
        len(profile["traits"]), len(profile["progression"]), usage.get("total_tokens"),
    )
    return {"character_id": character_id, "profile": profile, "usage": usage, "error": None}


# ── reresolve 모드 대상 에피소드 ──────────────────────────────────────────────

def character_episode_ids(character_id: int) -> list[dict]:
    """캐릭터 등장 에피소드(얼굴 등장 ∪ 화자 귀속) — reresolve 모드 대상 집합. 회차순."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.id, we.no
            FROM webtoon_episode we
            WHERE we.deleted_at IS NULL AND we.id IN (
                SELECT wc.episode_id
                FROM analysis_face_detection fd
                JOIN webtoon_cut wc ON fd.cut_id = wc.id
                JOIN LATERAL (
                    SELECT appearance_id
                    FROM analysis_face_identity
                    WHERE detection_id = fd.id AND deleted_at IS NULL
                    ORDER BY source ASC
                    LIMIT 1
                ) fi ON true
                JOIN analysis_character_appearance ca ON fi.appearance_id = ca.id
                WHERE fd.is_used = true AND fd.deleted_at IS NULL AND ca.character_id = %s
                UNION
                SELECT wc2.episode_id
                FROM analysis_text_region tr
                JOIN webtoon_cut wc2 ON tr.cut_id = wc2.id
                JOIN LATERAL (
                    SELECT speaker_id
                    FROM analysis_text_annotation
                    WHERE region_id = tr.id AND deleted_at IS NULL AND source IN ('human', 'llm')
                    ORDER BY source ASC
                    LIMIT 1
                ) ta ON true
                WHERE ta.speaker_id = %s AND tr.deleted_at IS NULL
            )
            ORDER BY we.no
            """,
            (character_id, character_id),
        )
        return [{"episode_id": r[0], "episode_no": r[1]} for r in cur.fetchall()]
