"""누적 서사 컨텍스트(prior) 조립 — v4.0(§17.3): 캐시 없이 **정본 테이블 조인**으로만.

에피소드 N의 Stage R/N은 ep1..ep(N-1)의 누적 상태를 prior로 받는다:
  - confirmed_roster: 명명된 인물(kind=character) + 인물도감(character_profile,
    human>llm 필드 병합) — 정체성 진실 기준선 + 도감 컨텍스트.
  - open_threads: 미회수 떡밥(narrative_thread.status='open').
  - recent_summaries: 최근 N화 episode_report.summary(회차별 row가 정본 —
    구 running_summary/webtoon_narrative_state 캐시는 폐기됨).

웹툰당 에피소드 1회 처리라 조인 비용은 무시 가능하다. 토큰 예산 초과 시 recent_summaries를
오래된 것부터 줄이고, 그래도 초과하면 roster의 minor 인물부터 덜어낸다(원문 무한 누적 금지).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from src.config.db import db_cursor

logger = logging.getLogger(__name__)

# prior 몫의 기본 토큰 예산(전체 컨텍스트가 아니라 prior 몫만 보수적으로). 호출부가 override.
DEFAULT_PRIOR_TOKEN_BUDGET = 6000
# 한글 위주 텍스트의 대략적 토큰 추정 계수(프로토타입 _pass2.py: len//2와 동일 보수치).
_CHARS_PER_TOKEN = 2.0
# prior에 싣는 최근 회차 요약 수 기본값.
DEFAULT_RECENT_SUMMARIES = 10

_PROFILE_SCALARS = ("gender", "age_group", "affiliation", "role")


@dataclass
class CumulativeContext:
    """ep1..ep(upto_episode-1) 누적 서사 컨텍스트 — Stage R/N 입력 prior."""

    webtoon_id: int
    upto_episode: int
    confirmed_roster: list[dict] = field(default_factory=list)
    open_threads: list[dict] = field(default_factory=list)
    recent_summaries: list[dict] = field(default_factory=list)  # [{episode, summary}]
    compressed: bool = False
    last_resolved_episode: int | None = None

    def to_prompt_dict(self) -> dict:
        """R/N user payload에 합칠 prior dict. `confirmed_roster_prior` 키는 프롬프트 계약."""
        return {
            "confirmed_roster_prior": self.confirmed_roster,
            "open_threads": self.open_threads,
            "recent_episode_summaries": self.recent_summaries,
        }

    def approx_tokens(self) -> int:
        text = json.dumps(self.to_prompt_dict(), ensure_ascii=False)
        return int(len(text) / _CHARS_PER_TOKEN)


def _merge_profile_rows(rows: list[tuple]) -> dict:
    """(source, gender, age_group, affiliation, role, personality, traits, key_facts) 행들을
    필드 단위 human 우선으로 병합(v4.0 §17.2 — service serializer와 동일 규칙)."""
    by_source = {r[0]: r for r in rows}
    out: dict = {}
    for idx, key in enumerate(_PROFILE_SCALARS, start=1):
        for src in ("human", "llm"):
            r = by_source.get(src)
            if r and r[idx]:
                out[key] = r[idx]
                break
    for idx, key in ((5, "personality"), (6, "traits"), (7, "key_facts")):
        for src in ("human", "llm"):
            r = by_source.get(src)
            v = r[idx] if r else None
            if v:
                out[key] = v
                break
    return out


def _load_confirmed_roster(webtoon_id: int, upto_episode: int) -> list[dict]:
    """명명된 인물(kind=character) + 도감 병합 — prior의 정체성 기준선.

    upto_episode 이전에 최초 등장한(또는 first_seen 미상의) 인물만 포함한다.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT c.id, c.name, c.significance, c.is_confirmed
            FROM analysis_character c
            LEFT JOIN webtoon_episode fe ON c.first_seen_episode_id = fe.id
            WHERE c.webtoon_id = %s
              AND c.deleted_at IS NULL
              AND c.kind = 'character'
              AND c.name <> ''
              AND (fe.no IS NULL OR fe.no < %s)
            ORDER BY c.is_confirmed DESC, c.id ASC
            """,
            (webtoon_id, upto_episode),
        )
        chars = cur.fetchall()
        ids = [r[0] for r in chars]
        profiles_by_char: dict[int, list[tuple]] = {}
        if ids:
            cur.execute(
                """
                SELECT character_id, source, gender, age_group, affiliation, role,
                       personality, traits, key_facts
                FROM analysis_character_profile
                WHERE character_id = ANY(%s) AND deleted_at IS NULL
                """,
                (ids,),
            )
            for row in cur.fetchall():
                profiles_by_char.setdefault(row[0], []).append(row[1:])

    roster: list[dict] = []
    for cid, name, significance, is_confirmed in chars:
        entry = {
            "character_id": cid,
            "name": name,
            "significance": significance,
            "is_confirmed": bool(is_confirmed),
        }
        prof = _merge_profile_rows(profiles_by_char.get(cid, []))
        if prof:
            entry["profile"] = prof
        roster.append(entry)
    return roster


def _load_open_threads(webtoon_id: int, upto_episode: int) -> list[dict]:
    """미회수(open) 떡밥 — 정본 `narrative_thread`. upto_episode 이전에 심긴 것만."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT nt.id, nt.description, nt.type, nt.confidence, pe.no, nt.planted_cut
            FROM analysis_narrative_thread nt
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


def _load_recent_summaries(webtoon_id: int, upto_episode: int, limit: int) -> list[dict]:
    """최근 N화 회차 요약(episode_report.summary) — 구 running_summary 캐시 대체(정본 조인)."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.no, er.summary
            FROM analysis_episode_report er
            JOIN webtoon_episode we ON er.episode_id = we.id
            WHERE we.webtoon_id = %s AND we.no < %s
              AND er.deleted_at IS NULL AND er.summary <> ''
            ORDER BY we.no DESC
            LIMIT %s
            """,
            (webtoon_id, upto_episode, limit),
        )
        rows = cur.fetchall()
    return [{"episode": no, "summary": summary} for no, summary in reversed(rows)]


def _last_resolved_episode_no(webtoon_id: int) -> int | None:
    """최신 succeeded resolve run이 있는 회차 번호(정보성 — run 원장에서 도출)."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT MAX(we.no)
            FROM analysis_run ar
            JOIN webtoon_episode we ON ar.episode_id = we.id
            WHERE ar.webtoon_id = %s AND ar.kind = 'resolve' AND ar.status = 'succeeded'
              AND ar.deleted_at IS NULL
            """,
            (webtoon_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _compress(ctx: CumulativeContext, token_budget: int) -> None:
    """토큰 예산 초과 시 in-place 압축: 오래된 요약부터 제거 → minor 인물부터 roster 축소.

    원문 무한 누적 금지(Req 11.7). 장기 줄거리의 정본은 episode_report(회차별)와 story_arc(아크,
    Stage A 산출)이며, prior는 "직전 맥락 + 정체성 기준선"만 담는다.
    """
    if ctx.approx_tokens() <= token_budget:
        return
    ctx.compressed = True
    # 1) 오래된 요약부터 제거(최근 3개는 보존).
    while len(ctx.recent_summaries) > 3 and ctx.approx_tokens() > token_budget:
        ctx.recent_summaries.pop(0)
    # 2) 여전히 초과 → minor/extra 인물부터 roster 축소(main/supporting/확정 인물 보존).
    if ctx.approx_tokens() > token_budget:
        keep = [r for r in ctx.confirmed_roster
                if r.get("is_confirmed") or r.get("significance") in ("main", "supporting")]
        ctx.confirmed_roster = keep


def load_prior(
    webtoon_id: int,
    upto_episode: int,
    token_budget: int = DEFAULT_PRIOR_TOKEN_BUDGET,
    recent_summaries: int = DEFAULT_RECENT_SUMMARIES,
) -> CumulativeContext:
    """ep1..ep(upto_episode-1)의 누적 서사 컨텍스트(prior)를 정본 테이블에서 조립한다(v4.0).

    Args:
        webtoon_id: 대상 웹툰 id.
        upto_episode: 해소 대상 회차 번호(no). prior는 이 회차 **미만**으로 구성.
        token_budget: prior에 허용할 대략적 토큰 예산(초과 시 압축).
        recent_summaries: 싣는 최근 회차 요약 수.
    """
    ctx = CumulativeContext(
        webtoon_id=webtoon_id,
        upto_episode=upto_episode,
        confirmed_roster=_load_confirmed_roster(webtoon_id, upto_episode),
        open_threads=_load_open_threads(webtoon_id, upto_episode),
        recent_summaries=_load_recent_summaries(webtoon_id, upto_episode, recent_summaries),
        compressed=False,
        last_resolved_episode=_last_resolved_episode_no(webtoon_id),
    )
    _compress(ctx, token_budget)

    logger.info(
        "[narrative_context] load_prior webtoon=%s upto_ep=%s roster=%s threads=%s "
        "summaries=%s approx_tok=%s compressed=%s",
        webtoon_id, upto_episode, len(ctx.confirmed_roster), len(ctx.open_threads),
        len(ctx.recent_summaries), ctx.approx_tokens(), ctx.compressed,
    )
    return ctx
