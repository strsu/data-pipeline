"""누적 서사 컨텍스트 (Cumulative Narrative Context) — Pass-2a prior 조립/갱신.

에피소드 N의 Pass-2a(전역 해소)는 ep1..ep(N-1)의 **누적 상태**를 prior로 받는다
(= 확정 character roster + 미해결 떡밥 + running 요약). (Req 11.3)

이 prior의 **정본(source of truth)은 확정 테이블**(`character` / `narrative_thread`(open) /
직전 `episode_report`·`story_arc`)이며, 빠른 prompt 조립과 토큰 예산을 위해 웹툰당
materialized 캐시 `webtoon_narrative_state`(roster/open_threads/running_summary)를 병용한다.

`load_prior`는 확정 테이블 + 캐시에서 prior를 조립하고, **토큰 예산을 초과하면 오래된 회차
원문 누적 대신 아크/부(StoryArc) 요약으로 압축**해서 전달한다(Req 11.7). 원문 전체 누적 금지.

DB 접근은 레포 표준(`src.config.db.db_cursor` 직접 SQL)을 따른다. service 스키마(이미 마이그레이션)
테이블/컬럼명을 그대로 사용한다.

> `fold(state, episode_meta)`(처리 후 갱신, Req 11.4)는 task 7.2에서 이 모듈에 추가된다.
"""
from __future__ import annotations

import copy
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from psycopg2.extras import Json

from src.config.db import db_cursor

logger = logging.getLogger(__name__)

# NEW_CHAR placeholder(이름 미확정) 제외 정규식 — step3._find_character_by_name과 동일 규칙.
_NEW_CHAR_RE = r"^NEW_CHAR_[0-9]+$"
_NEW_CHAR_PAT = re.compile(_NEW_CHAR_RE)

# Pass-2a 텍스트 모델 토큰 예산의 기본값. prior는 에피소드 트랜스크립트와 예산을 나눠 쓰므로,
# 전체 컨텍스트가 아니라 prior 몫만 보수적으로 잡는다(GLM 130k 중 prior ~6k). 호출부가 override.
DEFAULT_PRIOR_TOKEN_BUDGET = 6000

# 한글 위주 텍스트의 대략적 토큰 추정 계수(프로토타입 _pass2.py: len//2와 동일 보수치).
_CHARS_PER_TOKEN = 2.0


@dataclass
class CumulativeContext:
    """ep1..ep(upto_episode-1) 누적 서사 컨텍스트 — Pass-2a 입력 prior.

    - confirmed_roster: 확정/명명된 인물 [{character_id, name, significance, key_facts}].
      Pass-2a의 `confirmed_roster_prior`(진실 기준선, Req 4.1).
    - open_threads: 미해결 떡밥 [{thread_id, description, type, planted_episode, planted_cut, confidence}].
    - running_summary: 직전까지 누적 요약(materialized 캐시 정본). 압축 시 아크/부 요약으로 대체.
    - arc_summaries: 상위 단위(아크/부) 요약 [{level, ordinal, title, episode_start, episode_end,
      summary, appeal_point}]. compressed=True일 때 running 원문 대체용으로 채워진다.
    - compressed: 토큰 예산 초과로 압축이 적용됐는지(Req 11.7).
    """

    webtoon_id: int
    upto_episode: int
    confirmed_roster: list[dict] = field(default_factory=list)
    open_threads: list[dict] = field(default_factory=list)
    running_summary: str = ""
    arc_summaries: list[dict] = field(default_factory=list)
    compressed: bool = False
    last_resolved_episode: int | None = None

    def to_prompt_dict(self) -> dict:
        """Pass-2a user payload에 합칠 prior dict. `confirmed_roster_prior` 키는 프로토타입 계약 유지."""
        payload: dict = {
            "confirmed_roster_prior": self.confirmed_roster,
            "open_threads": self.open_threads,
            "running_summary": self.running_summary,
        }
        if self.arc_summaries:
            payload["arc_summaries"] = self.arc_summaries
        return payload

    def approx_tokens(self) -> int:
        """prior payload의 대략적 토큰 수(예산 판단용)."""
        text = json.dumps(self.to_prompt_dict(), ensure_ascii=False)
        return int(len(text) / _CHARS_PER_TOKEN)


# ── 확정 테이블/캐시 로드 ───────────────────────────────────────────────────────


def _load_narrative_state(webtoon_id: int) -> dict | None:
    """materialized 캐시 `webtoon_narrative_state` (roster/open_threads/running_summary)."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT wns.roster, wns.open_threads, wns.running_summary, le.no
            FROM webtoon_narrative_state wns
            LEFT JOIN webtoon_episode le ON wns.last_resolved_episode_id = le.id
            WHERE wns.webtoon_id = %s AND wns.deleted_at IS NULL
            LIMIT 1
            """,
            (webtoon_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    roster, open_threads, running_summary, last_no = row
    return {
        "roster": roster if isinstance(roster, list) else [],
        "open_threads": open_threads if isinstance(open_threads, list) else [],
        "running_summary": running_summary or "",
        "last_resolved_episode": last_no,
    }


def _load_confirmed_roster(webtoon_id: int, upto_episode: int, cache_roster: list[dict]) -> list[dict]:
    """확정 테이블(`character`) 기준 로스터. key_facts는 캐시 roster/extra에서 보강.

    정본은 character 테이블(이름/significance/확정여부). 캐시 roster는 key_facts(누적 사실) 보강용.
    upto_episode 이전에 최초 등장한(또는 first_seen 미상의 human-확정) 명명 인물만 prior에 포함한다.
    """
    cache_by_id: dict[int, dict] = {}
    for r in cache_roster or []:
        cid = r.get("character_id")
        if cid is not None:
            cache_by_id[cid] = r

    with db_cursor() as cur:
        cur.execute(
            f"""
            SELECT c.id, c.name, c.significance, c.extra, c.is_confirmed
            FROM character c
            LEFT JOIN webtoon_episode fe ON c.first_seen_episode_id = fe.id
            WHERE c.webtoon_id = %s
              AND c.deleted_at IS NULL
              AND c.name !~ '{_NEW_CHAR_RE}'
              AND (fe.no IS NULL OR fe.no < %s)
            ORDER BY c.is_confirmed DESC, c.id ASC
            """,
            (webtoon_id, upto_episode),
        )
        rows = cur.fetchall()

    roster: list[dict] = []
    for cid, name, significance, extra, is_confirmed in rows:
        extra = extra if isinstance(extra, dict) else {}
        cached = cache_by_id.get(cid, {})
        # key_facts 우선순위: 캐시 roster(key_facts|note) > human extra(key_facts|note).
        key_facts = (
            cached.get("key_facts")
            or cached.get("note")
            or extra.get("key_facts")
            or extra.get("note")
            or ""
        )
        roster.append(
            {
                "character_id": cid,
                "name": name,
                "significance": significance,
                "key_facts": key_facts,
            }
        )
    return roster


def _load_open_threads(webtoon_id: int, upto_episode: int) -> list[dict]:
    """미해결(open) 떡밥 — 정본 `narrative_thread`. upto_episode 이전에 심긴 것만."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT nt.id, nt.description, nt.type, nt.confidence, pe.no, nt.planted_cut
            FROM narrative_thread nt
            LEFT JOIN webtoon_episode pe ON nt.planted_episode_id = pe.id
            WHERE nt.webtoon_id = %s
              AND nt.deleted_at IS NULL
              AND nt.status = 'open'
              AND (pe.no IS NULL OR pe.no < %s)
            ORDER BY nt.id ASC
            """,
            (webtoon_id, upto_episode),
        )
        rows = cur.fetchall()
    return [
        {
            "thread_id": tid,
            "description": description or "",
            "type": ttype or "",
            "confidence": confidence,
            "planted_episode": planted_no,
            "planted_cut": planted_cut,
        }
        for tid, description, ttype, confidence, planted_no, planted_cut in rows
    ]


def _load_arc_summaries(webtoon_id: int, upto_episode: int) -> list[dict]:
    """상위 단위(아크/부) 요약 — 정본 `story_arc`. 토큰 예산 초과 시 압축 전달용(Req 11.7)."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT level, ordinal, title, episode_start, episode_end, summary, appeal_point
            FROM story_arc
            WHERE webtoon_id = %s
              AND deleted_at IS NULL
              AND (episode_start IS NULL OR episode_start < %s)
            ORDER BY level ASC, ordinal ASC
            """,
            (webtoon_id, upto_episode),
        )
        rows = cur.fetchall()
    return [
        {
            "level": level,
            "ordinal": ordinal,
            "title": title or "",
            "episode_start": episode_start,
            "episode_end": episode_end,
            "summary": summary or "",
            "appeal_point": appeal_point or "",
        }
        for level, ordinal, title, episode_start, episode_end, summary, appeal_point in rows
    ]


# ── 압축 ────────────────────────────────────────────────────────────────────────


def _summary_char_budget(ctx: CumulativeContext, token_budget: int) -> int:
    """running_summary를 뺀 나머지(roster/threads/arc) overhead를 제외한 running 요약용 잔여 char 예산."""
    budget_chars = int(token_budget * _CHARS_PER_TOKEN)
    saved = ctx.running_summary
    ctx.running_summary = ""
    try:
        overhead = len(json.dumps(ctx.to_prompt_dict(), ensure_ascii=False))
    finally:
        ctx.running_summary = saved
    return max(0, budget_chars - overhead)


def _compress(ctx: CumulativeContext, token_budget: int) -> None:
    """토큰 예산 초과 시 in-place 압축(Req 11.7): 원문 running 요약 → 아크/부 요약으로 대체.

    원문 전체 누적 금지. 오래된 회차 디테일을 상위 단위(StoryArc) 요약으로 갈음한다.
    아크/부 요약이 없으면(또는 그래도 초과하면) running 요약을 잔여 예산 내로 절단한다.
    roster/open_threads는 교차에피소드 정체성/떡밥 보존을 위해 유지한다.
    """
    if ctx.approx_tokens() <= token_budget:
        return

    # 1) 아크/부 요약으로 원문 running 요약 대체.
    if ctx.arc_summaries:
        ctx.running_summary = " / ".join(
            s["summary"] for s in ctx.arc_summaries if s.get("summary")
        )
    ctx.compressed = True

    # 2) 여전히 초과하면 running 요약을 잔여 char 예산 내로 절단.
    if ctx.approx_tokens() > token_budget:
        remaining = _summary_char_budget(ctx, token_budget)
        ctx.running_summary = (ctx.running_summary or "")[:remaining]


# ── 진입점 ──────────────────────────────────────────────────────────────────────


def load_prior(
    webtoon_id: int,
    upto_episode: int,
    token_budget: int = DEFAULT_PRIOR_TOKEN_BUDGET,
) -> CumulativeContext:
    """ep1..ep(upto_episode-1)의 누적 서사 컨텍스트(prior)를 조립한다.

    Args:
        webtoon_id: 대상 웹툰 id.
        upto_episode: 해소 대상 에피소드 번호(회차 `no`). prior는 이 회차 **미만**(이전 화)으로 구성.
        token_budget: prior에 허용할 대략적 토큰 예산. 초과 시 아크/부 요약으로 압축(Req 11.7).

    Returns:
        CumulativeContext — confirmed_roster / open_threads / running_summary (+ 압축 시 arc_summaries).
        consumed by Pass-2a(`resolve_episode`)의 `confirmed_roster_prior`/prior.
    """
    cache = _load_narrative_state(webtoon_id) or {}
    cache_roster = cache.get("roster", [])

    roster = _load_confirmed_roster(webtoon_id, upto_episode, cache_roster)
    # open_threads는 정본 테이블 우선, 비어 있으면 캐시 폴백(캐시만 갱신된 과도기 대비).
    threads = _load_open_threads(webtoon_id, upto_episode)
    if not threads and cache.get("open_threads"):
        threads = cache["open_threads"]
    arc_summaries = _load_arc_summaries(webtoon_id, upto_episode)

    ctx = CumulativeContext(
        webtoon_id=webtoon_id,
        upto_episode=upto_episode,
        confirmed_roster=roster,
        open_threads=threads,
        running_summary=cache.get("running_summary", ""),
        arc_summaries=[],
        compressed=False,
        last_resolved_episode=cache.get("last_resolved_episode"),
    )

    # 토큰 예산 초과 시 압축(원문 누적 금지 → 아크/부 요약 대체).
    if ctx.approx_tokens() > token_budget:
        ctx.arc_summaries = arc_summaries
        _compress(ctx, token_budget)

    logger.info(
        "[narrative_context] load_prior webtoon=%s upto_ep=%s roster=%s threads=%s "
        "approx_tok=%s compressed=%s",
        webtoon_id, upto_episode, len(ctx.confirmed_roster), len(ctx.open_threads),
        ctx.approx_tokens(), ctx.compressed,
    )
    return ctx


# ════════════════════════════════════════════════════════════════════════════
# fold(state, episode_meta) — 처리 후 누적 서사 상태 갱신 (Req 4.3, 11.4)
# ════════════════════════════════════════════════════════════════════════════
#
# 설계 결정(Property 9 보장):
#   - fold는 **순수 함수(PURE)** 다. 입력 state를 변형하지 않고(deepcopy) 새 NarrativeState를
#     반환한다. DB I/O는 하지 않는다. 영속화는 별도 `persist_state`(또는 호출부 step3c 8.2)가 맡는다.
#     → state(N) = fold(state(N-1), meta(N)) 를 인메모리로 체인 가능 = Property 9를 DB 없이 검증.
#   - 정체성은 **webtoon 글로벌**: roster/threads 병합 키는 character_id / thread_id(전역 안정 id).
#   - 단조성(Property 9):
#       roster: 인물은 **절대 제거하지 않는다**. character_id로 병합해 status/name/significance/
#               key_facts만 갱신·누적(append-only). → roster character_id 집합은 단조 증가.
#       threads: open→resolved **단방향**. 한 번 resolved된 thread는 resolved_threads에 들어가고
#                resolved_keys로 차단되어 다시 open_threads로 돌아갈 수 없다(재오픈 금지).
#
# episode_meta 형태(= resolve_episode(6.1)/apply_resolution(8.x) 산출물의 영속 projection):
#   {
#     "webtoon_id": int | None,         # 선택(state에 없을 때 보강용)
#     "episode_no": int | None,         # 회차 no(running_summary 태깅 + last_resolved_episode 갱신)
#     "episode_id": int | None,         # WebtoonEpisode.id(persist 시 last_resolved_episode_id FK)
#     "characters": [                   # Pass-2a characters[] (+ 선택 status/key_facts/relationships)
#       {character_id, name, significance, name_confidence?, evidence?, label_conflict?,
#        merge_suggestion?, status?, key_facts?|facts?, relationships?}
#     ],
#     "threads": [                      # Pass-2a threads[] (신규/해소)
#       {thread_id?, description, type, status("open"|"resolved"),
#        planted_episode?, planted_cut?, resolved_episode?, resolved_cut?, confidence?}
#     ],
#     "episode": {summary, appeal_point?, cliffhanger?, foreshadowing?},  # running 요약 누적용
#   }
#
# state 형태(materialized 캐시 webtoon_narrative_state와 정합 + resolved_threads 추적):
#   NarrativeState(webtoon_id, roster[], open_threads[], resolved_threads[],
#                  running_summary, last_resolved_episode)
#   - roster entry      : {character_id, name, significance, status?, key_facts(list), relationships?}
#   - open/resolved thread: {thread_id?, description, type, status, planted_episode, planted_cut,
#                            resolved_episode?, resolved_cut?, confidence}
#   resolved_threads는 캐시 컬럼이 아니라(정본은 narrative_thread 테이블) fold 체인 내 단방향성을
#   보장하기 위한 인메모리 추적이다. persist_state는 open_threads/roster/running_summary만 캐시에 쓴다.


@dataclass
class NarrativeState:
    """누적 서사 상태(fold의 입출력). webtoon_narrative_state 캐시와 정합.

    PURE fold의 단위. roster/open_threads는 jsonb 캐시 컬럼과 1:1, resolved_threads는 fold 체인
    내 open→resolved 단방향성(Property 9) 보장을 위한 인메모리 추적.
    """

    webtoon_id: int | None = None
    roster: list[dict] = field(default_factory=list)
    open_threads: list[dict] = field(default_factory=list)
    resolved_threads: list[dict] = field(default_factory=list)
    running_summary: str = ""
    last_resolved_episode: int | None = None

    @classmethod
    def empty(cls, webtoon_id: int | None = None) -> "NarrativeState":
        """초기(state(0)) — 빈 누적 상태."""
        return cls(webtoon_id=webtoon_id)

    @classmethod
    def from_dict(cls, data: dict | None, webtoon_id: int | None = None) -> "NarrativeState":
        """캐시 dict(또는 _load_narrative_state 반환) → NarrativeState."""
        data = data or {}
        roster = data.get("roster")
        open_threads = data.get("open_threads")
        resolved = data.get("resolved_threads")
        return cls(
            webtoon_id=data.get("webtoon_id", webtoon_id),
            roster=[dict(r) for r in roster] if isinstance(roster, list) else [],
            open_threads=[dict(t) for t in open_threads] if isinstance(open_threads, list) else [],
            resolved_threads=[dict(t) for t in resolved] if isinstance(resolved, list) else [],
            running_summary=data.get("running_summary") or "",
            last_resolved_episode=data.get("last_resolved_episode"),
        )

    def to_dict(self) -> dict:
        """jsonb 캐시/직렬화용 dict."""
        return {
            "roster": self.roster,
            "open_threads": self.open_threads,
            "resolved_threads": self.resolved_threads,
            "running_summary": self.running_summary,
            "last_resolved_episode": self.last_resolved_episode,
        }


def _coerce_state(state: "NarrativeState | dict | None") -> NarrativeState:
    """state를 NarrativeState로 정규화(dict/None 허용)."""
    if isinstance(state, NarrativeState):
        return state
    if state is None:
        return NarrativeState.empty()
    return NarrativeState.from_dict(state)


def _clean_name(name) -> str | None:
    """확정 이름만 채택 — null/공백/NEW_CHAR placeholder는 미확정으로 간주(None)."""
    if not name or not isinstance(name, str):
        return None
    name = name.strip()
    if not name or _NEW_CHAR_PAT.match(name):
        return None
    return name


def _as_fact_list(value) -> list[str]:
    """key_facts/facts/evidence 입력을 정규화된 문자열 리스트로."""
    if not value:
        return []
    if isinstance(value, str):
        v = value.strip()
        return [v] if v else []
    if isinstance(value, (list, tuple)):
        out: list[str] = []
        for item in value:
            if isinstance(item, str):
                s = item.strip()
                if s:
                    out.append(s)
            elif item is not None:
                out.append(str(item))
        return out
    return [str(value)]


def _merge_facts(existing, new_facts: list[str]) -> list[str]:
    """key_facts 누적(append-only, 중복 제거, 순서 보존) — 단조 증가."""
    merged = _as_fact_list(existing)
    seen = set(merged)
    for f in new_facts:
        if f not in seen:
            merged.append(f)
            seen.add(f)
    return merged


def _thread_key(thread: dict):
    """thread 병합 키 — thread_id 우선(webtoon 글로벌 안정 id), 없으면 description 정규화 폴백."""
    tid = thread.get("thread_id")
    if tid is not None:
        return ("id", tid)
    desc = (thread.get("description") or "").strip().lower()
    return ("desc", desc)


def _normalize_thread(thread: dict, *, status: str | None = None) -> dict:
    """thread를 캐시/상태 표준 형태로 정규화."""
    out = {
        "thread_id": thread.get("thread_id"),
        "description": thread.get("description") or "",
        "type": thread.get("type") or "",
        "status": status or (thread.get("status") or "open").lower(),
        "planted_episode": thread.get("planted_episode"),
        "planted_cut": thread.get("planted_cut"),
        "confidence": thread.get("confidence"),
    }
    if thread.get("resolved_episode") is not None:
        out["resolved_episode"] = thread.get("resolved_episode")
    if thread.get("resolved_cut") is not None:
        out["resolved_cut"] = thread.get("resolved_cut")
    return out


def _update_open_thread(target: dict, incoming: dict) -> None:
    """open 상태 유지하며 메타만 보강(설명/타입/근거 갱신). status는 open 유지."""
    for k in ("description", "type", "planted_episode", "planted_cut", "confidence"):
        v = incoming.get(k)
        if v not in (None, ""):
            target[k] = v


def _fold_roster(state: NarrativeState, characters: list[dict]) -> None:
    """roster 갱신(in-place on the *new* state) — character_id 병합, 제거 금지(단조 증가)."""
    by_id: dict = {}
    for r in state.roster:
        cid = r.get("character_id")
        if cid is not None:
            by_id[cid] = r

    for ch in characters or []:
        cid = ch.get("character_id")
        if cid is None:
            continue
        name = _clean_name(ch.get("name"))
        sig = ch.get("significance")
        status = ch.get("status")
        facts = _as_fact_list(ch.get("key_facts") or ch.get("facts") or ch.get("evidence"))
        rels = ch.get("relationships")

        if cid in by_id:
            entry = by_id[cid]
            if name:
                entry["name"] = name
            if sig:
                entry["significance"] = sig
            if status:
                entry["status"] = status
            if facts:
                entry["key_facts"] = _merge_facts(entry.get("key_facts"), facts)
            if rels:
                entry["relationships"] = rels
        else:
            entry = {"character_id": cid, "name": name, "significance": sig}
            if status:
                entry["status"] = status
            if facts:
                entry["key_facts"] = facts
            if rels:
                entry["relationships"] = rels
            state.roster.append(entry)  # append-only — 절대 제거하지 않음
            by_id[cid] = entry


def _fold_threads(state: NarrativeState, threads: list[dict], ep_no: int | None) -> None:
    """떡밥 갱신 — open→resolved 단방향. 재오픈 금지(Property 9)."""
    open_by_key: dict = {_thread_key(t): t for t in state.open_threads}
    resolved_keys = {_thread_key(t) for t in state.resolved_threads}

    for t in threads or []:
        key = _thread_key(t)
        status = (t.get("status") or "open").lower()

        # 이미 resolved된 thread는 재오픈 불가(단방향). 멱등하게 무시.
        if key in resolved_keys:
            continue

        if status == "resolved":
            existing = open_by_key.pop(key, None)
            base = dict(existing) if existing else {}
            base.update({k: v for k, v in _normalize_thread(t, status="resolved").items()
                         if v not in (None, "") or k == "status"})
            base["status"] = "resolved"
            if base.get("resolved_episode") is None and ep_no is not None:
                base["resolved_episode"] = ep_no
            if t.get("resolved_cut") is not None:
                base["resolved_cut"] = t.get("resolved_cut")
            state.resolved_threads.append(base)
            resolved_keys.add(key)
        else:  # open
            if key in open_by_key:
                _update_open_thread(open_by_key[key], t)
            else:
                open_by_key[key] = _normalize_thread(t, status="open")

    # resolved로 빠진 thread는 open 집합에서 제외된 상태로 재구성.
    state.open_threads = list(open_by_key.values())


def _fold_summary(state: NarrativeState, episode: dict, ep_no: int | None) -> None:
    """running 요약 누적(직전 + ep N). 원문 압축은 load_prior에서 예산 기준으로 수행."""
    summary = (episode.get("summary") or "").strip() if isinstance(episode, dict) else ""
    if not summary:
        return
    tag = f"[ep{ep_no}] " if ep_no is not None else ""
    piece = f"{tag}{summary}"
    state.running_summary = (
        f"{state.running_summary}\n{piece}".strip() if state.running_summary else piece
    )


def fold(state: "NarrativeState | dict | None", episode_meta: dict) -> NarrativeState:
    """누적 서사 상태 갱신: state(N) = state(N-1) ⊕ episodeMeta(N). (Req 4.3, 11.4)

    **PURE** — 입력 state를 변형하지 않고 새 NarrativeState를 반환한다(DB I/O 없음). 에피소드
    해소 후(apply_resolution, 8.2) 누적 로스터/떡밥/요약을 갱신한다. 정체성은 webtoon 글로벌.

    단조성 불변식(Property 9):
      - roster: character_id로 병합, **제거 없이** status/name/significance/key_facts만 갱신·누적.
      - threads: open→resolved **단방향**(재오픈 금지). 새 open만 추가, 해소는 open에서 제외.

    Args:
        state: 직전 누적 상태(NarrativeState | 캐시 dict | None=초기).
        episode_meta: 해소된 에피소드 출력 projection(characters/threads/episode + episode_no 등).

    Returns:
        새 NarrativeState — state(N). 호출부(8.2)가 `persist_state`로 캐시에 영속화할 수 있다.
    """
    base = _coerce_state(state)
    meta = episode_meta or {}

    new_state = NarrativeState(
        webtoon_id=base.webtoon_id if base.webtoon_id is not None else meta.get("webtoon_id"),
        roster=copy.deepcopy(base.roster),
        open_threads=copy.deepcopy(base.open_threads),
        resolved_threads=copy.deepcopy(base.resolved_threads),
        running_summary=base.running_summary,
        last_resolved_episode=base.last_resolved_episode,
    )

    ep_no = meta.get("episode_no")

    _fold_roster(new_state, meta.get("characters") or [])
    _fold_threads(new_state, meta.get("threads") or [], ep_no)
    _fold_summary(new_state, meta.get("episode") or {}, ep_no)

    # last_resolved_episode 단조 증가.
    if ep_no is not None and (
        new_state.last_resolved_episode is None or ep_no > new_state.last_resolved_episode
    ):
        new_state.last_resolved_episode = ep_no

    logger.info(
        "[narrative_context] fold webtoon=%s ep=%s roster=%s open_threads=%s resolved_threads=%s",
        new_state.webtoon_id, ep_no, len(new_state.roster),
        len(new_state.open_threads), len(new_state.resolved_threads),
    )
    return new_state


# ── 영속화(선택) — fold는 순수, 캐시 쓰기는 여기서/또는 호출부(step3c 8.2) ──────────────


def persist_state(
    webtoon_id: int,
    state: "NarrativeState | dict",
    last_resolved_episode_id: int | None = None,
) -> None:
    """fold 결과를 materialized 캐시 `webtoon_narrative_state`에 upsert(웹툰당 OneToOne).

    fold 자체는 순수하므로 영속화는 분리한다(8.2 step3c가 apply 후 호출 가능). 캐시는 prompt 조립/
    예산용이며 정본은 확정 테이블(character/narrative_thread)이다. resolved_threads는 캐시 컬럼이
    아니므로 쓰지 않는다(정본 = narrative_thread.status='resolved').
    """
    st = _coerce_state(state)
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO webtoon_narrative_state
                (webtoon_id, last_resolved_episode_id, roster, open_threads,
                 running_summary, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (webtoon_id)
            DO UPDATE SET last_resolved_episode_id = EXCLUDED.last_resolved_episode_id,
                          roster = EXCLUDED.roster,
                          open_threads = EXCLUDED.open_threads,
                          running_summary = EXCLUDED.running_summary,
                          updated_at = EXCLUDED.updated_at
            """,
            (
                webtoon_id,
                last_resolved_episode_id,
                Json(st.roster),
                Json(st.open_threads),
                st.running_summary,
                now,
                now,
            ),
        )
