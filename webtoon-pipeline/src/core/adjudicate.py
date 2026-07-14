"""제안검토 심판(§22.4) — 정리 패스의 LLM 심판 + 결정론 가드/교차대조.

역할 분리(§22.3): **판정·가드는 여기(data-pipeline), 실행은 service 수락 경로**
(§19 병합 시맨틱·name수락=병합·face_reassign수락 재사용 — §20 자동 훅이 따라옴).

드라이런 실측(2026-07-12, prd §22.4 — 화산귀환 pending 749건 + 사용자 눈검증 GT)으로
캘리브레이션된 규칙을 그대로 구현한다:
  - **sid 기반 교차대조**: 심판 출력의 cluster_id 필드는 신뢰 불가(묶음 판정 시 대상 id
    오기입·rename으로 병합 우회 실측) — 판정은 suggestion_ids로 수집하고 액션 의미는
    suggestion 원본에서 복원한다.
  - rename-to-기존이름 = 병합으로 정규화(§19.3과 동일 시맨틱).
  - 자동 수락 가드(전부 만족): 무명 클러스터→named 방향 / 어떤 서류철에서도 혼합(mixed)
    플래그 없음 / 다대상 accept 없음 / human reject 블록리스트 아님 /
    **표결 accept >= 2×(reject+needs_human)** (GT: 오병합 0·참병합 9 유일 규칙군 —
    단순 다수결은 오병합 27%).
  - 만장일치 reject → 자동 기각(payload.judge.by='ai' 마커 — human reject과 구분해
    블록리스트 가드가 ai 기각을 human 판단으로 오인하지 않게).
  - 그 외 판정은 payload['judge'] 권고로 영속 — human 검토 큐 정렬/배지용.

의도적 중복 배치: 한 클러스터를 관련 named 인물 서류철마다 노출시켜 다관점 표결을
얻는다(모순이 표결로 드러남 — c1712 조걸자칭 vs 청명 실측). 서류철 크기는 캡으로 제한
(88k자 서류철이 게이트웨이 재시도로 2h+ 걸린 실측 — 분할이 정답).
"""
from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Optional

from psycopg2.extras import Json

from src.config.db import db_cursor
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import TEXT, resolve_llm_model

logger = logging.getLogger(__name__)

_JUDGE_STAGE = "judge"
# 서류철당 제안 상한 — 초과 시 분할(대형 서류철 1콜이 게이트웨이 재시도 지옥 실측).
_MAX_SUGGESTIONS_PER_DOSSIER = 60
_EVIDENCE_SNIPPET = 500          # 제안 근거 전문 상한(자)
_ACCEPT_MULTIPLIER = 2.0         # accept >= MULT × (reject+needs_human)
_JUDGE_RETRIES = 2               # 서류철 콜 실패 시 재시도(콜 내부 재시도와 별개)

JUDGE_SYSTEM = (
    "너는 웹툰 인물 정체성 데이터의 '제안 검토 심판'이다. Step2(얼굴 임베딩 매칭)가 만든 얼굴 클러스터와 "
    "Step3(서사 분석)가 낸 정정 제안(merge/name/face_reassign/label_conflict)이 입력이다. 제안마다 실행 여부를 판정하라.\n"
    "판단 원칙:\n"
    "1) 서로 다른 회차에서 독립적으로 반복된 근거, confirmed=true(사람 확정) 얼굴 앵커, 대사 속 자기지칭(예: '본도 청명')은 강한 긍정 신호.\n"
    "2) 닮은꼴이지만 서로 다른 인물(같은 문파 도복, 사제지간)을 병합하는 오류를 경계하라 — 근거가 '외형 유사'(step2/CCIP)뿐이고 "
    "서사 근거가 없으면 accept 금지(reject 또는 needs_human).\n"
    "3) 혼합 클러스터(한 클러스터의 얼굴이 컷별로 서로 다른 인물의 대사를 함 — label_conflict 다수)는 통째 병합 불가 "
    "→ needs_human(얼굴 단위 재배정 필요)으로 판정하고 이유에 명시.\n"
    "4) ⚠️ 이름이 있는 캐릭터(kind=character)를 다른 캐릭터로 통째 병합하는 accept는 금지 — 근거가 '일부 회차 얼굴이 딴 사람'이라면 "
    "그건 병합이 아니라 face_reassign(해당 얼굴만 이동)이다. 너는 그 캐릭터의 전체 회차 얼굴 분포를 못 보므로, "
    "보이는 근거 밖의 얼굴까지 딸려가는 통째 병합은 액션을 face_reassign으로 바꾸거나 needs_human으로 판정하라.\n"
    "5) 확신이 없으면 accept가 아니라 needs_human. 잘못된 병합은 되돌릴 수 없다. 반대로 근거가 명백히 무효인 제안은 "
    "needs_human이 아니라 reject로 큐에서 제거하라.\n"
    "6) rename 제안은 기존 이름이 오명(다른 인물 이름)인 근거가 여러 회차에서 일관될 때만 accept.\n"
    "한국어. 마지막에 JSON만 출력. cluster_id/target_id는 반드시 서류철에 등장한 숫자 id(cXXXX의 XXXX)만 사용:\n"
    '{"decisions":[{"suggestion_ids":[..],"cluster_id":0,"action":"merge|rename|promote|face_reassign",'
    '"target_id":0또는null,"target_name":"","verdict":"accept|reject|needs_human","confidence":0.0,"reason":""}],'
    '"cluster_notes":[{"cluster_id":0,"mixed":true,"note":"혼합/단일 판단과 권고"}]}'
)


# ── 데이터 로드 ────────────────────────────────────────────────────────────────

def _load_characters(webtoon_id: int) -> dict[int, dict]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT c.id, c.name, c.kind, c.is_confirmed, c.significance, c.is_match_excluded,
                   COALESCE(f.faces, 0), COALESCE(f.eps, 0), COALESCE(f.human_faces, 0)
            FROM analysis_character c
            LEFT JOIN (
                SELECT a.character_id,
                       count(fi.id) AS faces,
                       count(DISTINCT wc.episode_id) AS eps,
                       count(*) FILTER (WHERE fi.source = 'human') AS human_faces
                FROM analysis_character_appearance a
                JOIN analysis_face_identity fi ON fi.appearance_id = a.id AND fi.deleted_at IS NULL
                JOIN analysis_face_detection fd ON fd.id = fi.detection_id
                JOIN webtoon_cut wc ON wc.id = fd.cut_id
                WHERE a.deleted_at IS NULL
                GROUP BY a.character_id
            ) f ON f.character_id = c.id
            WHERE c.webtoon_id = %s AND c.deleted_at IS NULL
            """,
            (webtoon_id,),
        )
        return {
            r[0]: {"id": r[0], "name": r[1], "kind": r[2], "confirmed": r[3], "sig": r[4],
                   "excluded": r[5], "faces": r[6], "eps": r[7], "human_faces": r[8]}
            for r in cur.fetchall()
        }


def _load_suggestions(webtoon_id: int) -> list[dict]:
    """pending(판정 대상) + rejected(human 블록리스트 원료) 전부.

    regen_origin: 제안을 만든 resolve run이 regen 재해소(stats.origin='regen')였는지 —
    수렴 가드(§22.6)가 이런 pending을 자동판정에서 제외한다(수락→reresolve→새 제안→수락…
    자가 증폭 루프 차단; 이런 제안은 human 검토 큐로만 흐른다).
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT s.id, s.type, s.status, s.character_id, e.no, s.confidence, s.payload,
                   COALESCE(r.stats->>'origin', '') = 'regen' AS regen_origin
            FROM analysis_suggestion s
            LEFT JOIN webtoon_episode e ON e.id = s.episode_id
            LEFT JOIN analysis_run r ON r.id = s.run_id
            WHERE s.webtoon_id = %s AND s.deleted_at IS NULL
              AND s.status IN ('pending', 'rejected')
            """,
            (webtoon_id,),
        )
        return [
            {"id": r[0], "type": r[1], "status": r[2], "cid": r[3], "ep": r[4],
             "conf": float(r[5]) if r[5] is not None else None, "payload": r[6] or {},
             "regen_origin": bool(r[7])}
            for r in cur.fetchall()
        ]


def _is_named(chars: dict, cid) -> bool:
    c = chars.get(cid)
    return bool(c and c["kind"] == "character" and c["name"])


def _suggestion_pairs(chars: dict, by_name: dict, s: dict) -> list[tuple]:
    """suggestion → 액션 의미 복원. ("pair", 무명cl, named) / ("NN", a, b) /
    ("CC", a, b) / ("PROMOTE", cl, name) / ("RENAME", cid, name)."""
    out: list[tuple] = []
    if s["type"] == "merge":
        for o in (s["payload"].get("other_character_ids") or []):
            a, b = s["cid"], o
            if a not in chars or b not in chars:
                continue
            na, nb = _is_named(chars, a), _is_named(chars, b)
            if na and not nb:
                out.append(("pair", b, a))
            elif nb and not na:
                out.append(("pair", a, b))
            elif na and nb:
                out.append(("NN", min(a, b), max(a, b)))
            else:
                out.append(("CC", min(a, b), max(a, b)))
    elif s["type"] == "name":
        nm = (s["payload"].get("name") or "").strip()
        if nm and s["cid"] in chars:
            ex = by_name.get(nm)
            if ex and ex != s["cid"] and not _is_named(chars, s["cid"]):
                out.append(("pair", s["cid"], ex))  # §19.3 — 동명 존재 name 수락 = 병합
            elif ex is None and not _is_named(chars, s["cid"]):
                out.append(("PROMOTE", s["cid"], nm))
            else:
                out.append(("RENAME", s["cid"], nm))
    return out


# ── 서류철 구성 ────────────────────────────────────────────────────────────────

def _build_groups(chars: dict, pending: list[dict]) -> dict[str, dict]:
    """named 인물별 + 대형/다경합 클러스터 전용 + 신규명명 배치 서류철(§22.4).

    의도적 중복(한 클러스터가 여러 named 서류철에 등장)은 교차 검증 장치.
    """
    by_name: dict[str, int] = {}
    for c in chars.values():
        if _is_named(chars, c["id"]):
            by_name.setdefault(c["name"], c["id"])

    targets: dict[int, set] = defaultdict(set)       # cluster -> {named target}
    cluster_rel: dict[int, set] = defaultdict(set)   # named -> {관련 cluster/named}
    promote: set[int] = set()
    for s in pending:
        for p in _suggestion_pairs(chars, by_name, s):
            if p[0] == "pair":
                targets[p[1]].add(p[2])
                cluster_rel[p[2]].add(p[1])
            elif p[0] == "NN":
                cluster_rel[p[1]].add(p[2])
                cluster_rel[p[2]].add(p[1])
            elif p[0] == "PROMOTE":
                promote.add(p[1])
            elif p[0] == "RENAME":
                cluster_rel[p[1]].add(p[1])

    groups: dict[str, dict] = {}
    for nid, rel in sorted(cluster_rel.items()):
        if not _is_named(chars, nid):
            continue
        rivals: set[int] = set()
        for cl in rel:
            rivals |= targets.get(cl, set())
        groups[f"char_{nid}"] = {
            "group_ids": {nid} | set(rel),
            "related_ids": rivals - {nid},
            "group_names": {chars[nid]["name"]},
        }
    for cid, tg in sorted(targets.items()):
        c = chars.get(cid)
        if c and not _is_named(chars, cid) and (len(tg) >= 3 or c["faces"] >= 50):
            groups[f"cluster_{cid}"] = {"group_ids": {cid}, "related_ids": set(tg), "group_names": set()}
    if promote:
        groups["promote_batch"] = {"group_ids": promote, "related_ids": set(), "group_names": set()}
    return groups


def _group_suggestions(chars: dict, pending: list[dict], group_ids: set, group_names: set) -> list[dict]:
    out = []
    for s in pending:
        others = set(s["payload"].get("other_character_ids") or [])
        hit = s["cid"] in group_ids or (others & group_ids)
        if not hit and s["type"] == "name" and (s["payload"].get("name") or "").strip() in group_names:
            hit = True
        if hit:
            out.append(s)
    return out


def _dossier_text(chars: dict, group_ids: set, related_ids: set, suggs: list[dict]) -> str:
    ids = set(group_ids) | set(related_ids)
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT character_id, role, personality, key_facts
            FROM analysis_character_profile
            WHERE character_id = ANY(%s) AND source = 'llm' AND deleted_at IS NULL
            """,
            (list(ids),),
        )
        profiles = {r[0]: {"role": r[1], "personality": r[2], "key_facts": (r[3] or [])[:8]}
                    for r in cur.fetchall()}

    lines = ["=== 등장인물(도감) ==="]
    for cid in sorted(ids):
        c = chars.get(cid)
        if not c:
            continue
        lines.append(
            f"- c{cid} '{c['name'] or '(무명 클러스터)'}' kind={c['kind']} sig={c['sig']} "
            f"얼굴 {c['faces']}개/{c['eps']}개 회차, human확정얼굴 {c['human_faces']}"
        )
        p = profiles.get(cid)
        if p:
            lines.append(f"    role: {p.get('role')}")
            if p.get("key_facts"):
                lines.append(f"    key_facts: {' / '.join(map(str, p['key_facts']))}")
    named_others = [f"c{c['id']} {c['name']}" for c in chars.values()
                    if _is_named(chars, c["id"]) and c["id"] not in ids]
    lines.append(f"\n=== 이 웹툰의 다른 명명 인물(혼동 금지 목록) ===\n{', '.join(named_others)}")

    lines.append(f"\n=== pending 제안 {len(suggs)}건 (전문) ===")
    for s in sorted(suggs, key=lambda x: (x["type"], x["ep"] or 0, x["id"])):
        pl = dict(s["payload"])
        ev = pl.pop("evidence", "")
        if isinstance(ev, list):
            ev = " / ".join(map(str, ev))
        lines.append(
            f"[id={s['id']} {s['type']} ep{s['ep']} conf={s['conf']}] 대상 c{s['cid']}"
            f"{' → ' + str(pl.get('other_character_ids')) if pl.get('other_character_ids') else ''}"
            f"{' 이름후보=' + repr(pl['name']) if pl.get('name') else ''}"
            f"{' [step2/CCIP]' if pl.get('source') == 'step2' else ''}\n"
            f"  근거: {str(ev)[:_EVIDENCE_SNIPPET]}"
            f"{chr(10) + '  설명: ' + str(pl['description'])[:_EVIDENCE_SNIPPET] if pl.get('description') else ''}"
        )
    return "\n".join(lines)


# ── 심판 실행 + 교차대조 ──────────────────────────────────────────────────────
# 패스는 3단계로 분해돼 각각 별도 Temporal 액티비티로 돈다(중간 저장 — 완료된 서류철
# 판정은 재시작/배포에도 재실행되지 않는다):
#   plan_webtoon(계획 스냅샷) → judge_dossier(서류철 1개=콜 1개) × N → reconcile_pass(취합).
# 단계 간 데이터는 JSON 직렬화 가능해야 한다(set 금지, 정렬 list 사용).
#
# 표(votes)는 워크플로 히스토리가 아니라 suggestion payload['judge_votes'] 스크래치로
# 흐른다 — 대형 패스(서류철 수십 × 표 수십 × 한글 reason)를 reconcile 입력 페이로드
# 하나로 모으면 Temporal 단일 페이로드 한도(2MB)를 초과할 수 있다(ensure_ascii로 한글이
# 자당 6바이트, 2026-07-14 리뷰 실측 산술 3.7MB). judge가 서류철 단위로 영속(키 교체
# 멱등)하고 reconcile이 pending에서 취합 후 스크래치를 청소한다. 소멸한 sid의 표는
# 행이 사라지므로 자연 탈락(스냅샷 가드와 동일 시맨틱).

def _pending_and_held(all_suggs: list[dict]) -> tuple[list[dict], int]:
    """수렴 가드(§22.6): regen 재해소가 만든 pending은 자동판정 제외(human 검토로만) —
    심판 수락→자동 reresolve→새 제안→다시 수락…의 자가 증폭 루프를 사이클 1에서 끊는다
    (2026-07-13 화산귀환 16h 백로그 재발 방지). 정규 체인 resolve가 같은 에피소드를 다시
    돌면 제안이 새 run으로 재생성되므로 그때 자동판정 자격을 회복한다."""
    pending_all = [s for s in all_suggs if s["status"] == "pending"]
    pending = [s for s in pending_all if not s.get("regen_origin")]
    return pending, len(pending_all) - len(pending)


def _by_name(chars: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    for c in chars.values():
        if _is_named(chars, c["id"]):
            out.setdefault(c["name"], c["id"])
    return out


def _clear_judge_votes_scratch(webtoon_id: int) -> int:
    """payload['judge_votes'] 스크래치 일괄 제거 — plan(직전 패스 크래시 잔재)과
    reconcile(취합 완료 후)이 호출한다. 잔재를 안 지우면 다음 패스의 취합에 이전 패스
    표가 섞인다."""
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE analysis_suggestion
            SET payload = payload - 'judge_votes', updated_at = %s
            WHERE webtoon_id = %s AND payload ? 'judge_votes'
            """,
            (datetime.now(timezone.utc), webtoon_id),
        )
        return cur.rowcount


def _persist_dossier_votes(gname: str, votes: list[list], snapshot_sids: list[int]) -> int:
    """서류철 1개의 표를 pending payload['judge_votes'][서류철명]에 영속(모듈 주석 참조).

    재실행(Temporal attempt 재시도) 멱등은 **서류철 단위**: 먼저 계획 스냅샷의 전체 sid에서
    이 서류철 키를 지우고 새 표를 쓴다 — sid 단위 교체만 하면 attempt1이 표를 남긴 sid에
    attempt2가 표를 안 냈을 때 attempt1 잔표가 섞인다(2026-07-14 라운드2 리뷰).
    status='pending' 가드로 판정 도중 소멸/처리된 sid에는 쓰지 않는다.
    """
    by_sid: dict[int, list] = defaultdict(list)
    for sid, _gname, action, verdict, conf, reason in votes:
        by_sid[sid].append([action, verdict, conf, reason])
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE analysis_suggestion
            SET payload = jsonb_set(payload, '{judge_votes}', (payload->'judge_votes') - %s),
                updated_at = %s
            WHERE id = ANY(%s) AND status = 'pending'
              AND jsonb_typeof(payload->'judge_votes') = 'object'
              AND payload->'judge_votes' ? %s
            """,
            (gname, now, list(snapshot_sids), gname),
        )
        if by_sid:
            cur.executemany(
                """
                UPDATE analysis_suggestion
                SET payload = jsonb_set(payload, '{judge_votes}',
                                        COALESCE(payload->'judge_votes', '{}'::jsonb) || %s::jsonb),
                    updated_at = %s
                WHERE id = %s AND status = 'pending'
                """,
                [(Json({gname: v}), now, sid) for sid, v in by_sid.items()],
            )
    return len(by_sid)


def _load_scratch_votes(pending: list[dict]) -> dict[int, list]:
    """pending payload['judge_votes'] → votes 취합. 형식: sid -> [(group, action, verdict, conf, reason)]."""
    votes: dict[int, list] = defaultdict(list)
    for s in pending:
        jv = s["payload"].get("judge_votes") or {}
        if not isinstance(jv, dict):
            continue
        for gname, vlist in jv.items():
            for v in vlist if isinstance(vlist, list) else []:
                if isinstance(v, list) and len(v) >= 4:
                    votes[s["id"]].append((gname, v[0], v[1], v[2], v[3]))
    return votes


def plan_webtoon(webtoon_id: int) -> dict:
    """1단계 — 서류철 계획 스냅샷. LLM 콜 없음, DB 조회만.

    반환(JSON 직렬화): {"dossiers": [{"name", "group_ids", "related_ids", "sids"}...],
                        "pending": n, "regen_held": n}
    sids를 계획 시점에 고정해두면, 패스 도중 apply가 pending을 재생성해도 각 서류철은
    자기 스냅샷 기준으로 판정한다(소멸 sid는 판정/취합 단계 가드가 버린다).
    부수효과: 직전 패스가 취합 전에 죽으며 남긴 judge_votes 스크래치를 청소한다.
    """
    cleared = _clear_judge_votes_scratch(webtoon_id)
    if cleared:
        logger.info("[adjudicate] w%s — 직전 패스 judge_votes 잔재 %d건 청소", webtoon_id, cleared)
    chars = _load_characters(webtoon_id)
    pending, regen_held = _pending_and_held(_load_suggestions(webtoon_id))
    if regen_held:
        logger.info("[adjudicate] w%s — regen-origin pending %d건 자동판정 보류(수렴 가드, human 검토로)",
                    webtoon_id, regen_held)
    if not pending:
        return {"dossiers": [], "pending": 0, "regen_held": regen_held}

    groups = _build_groups(chars, pending)

    # 서류철 크기 캡 — 초과 그룹은 제안 청크로 분할(도감 컨텍스트는 공유).
    dossiers: list[dict] = []

    def _spec(name: str, spec: dict, suggs: list[dict]) -> dict:
        return {"name": name,
                "group_ids": sorted(spec["group_ids"]),
                "related_ids": sorted(spec["related_ids"]),
                "sids": sorted(s["id"] for s in suggs)}

    for gname, spec in groups.items():
        suggs = _group_suggestions(chars, pending, spec["group_ids"], spec["group_names"])
        if not suggs:
            continue
        if len(suggs) <= _MAX_SUGGESTIONS_PER_DOSSIER:
            dossiers.append(_spec(gname, spec, suggs))
        else:
            suggs = sorted(suggs, key=lambda x: (x["ep"] or 0, x["id"]))
            for i in range(0, len(suggs), _MAX_SUGGESTIONS_PER_DOSSIER):
                dossiers.append(_spec(f"{gname}_p{i // _MAX_SUGGESTIONS_PER_DOSSIER + 1}",
                                      spec, suggs[i:i + _MAX_SUGGESTIONS_PER_DOSSIER]))

    logger.info(
        "[adjudicate] w%s 심판 계획 — 인물 %d, pending %d건(regen 보류 %d), 그룹 %d개 → 서류철 %d개",
        webtoon_id, len(chars), len(pending), regen_held, len(groups), len(dossiers),
    )
    return {"dossiers": dossiers, "pending": len(pending), "regen_held": regen_held}


def judge_dossier(webtoon_id: int, run_id: Optional[int], dossier: dict,
                  index: int, total: int) -> dict:
    """2단계 — 서류철 1개 = LLM 심판 콜 1개. 텍스트는 DB에서 재구성(히스토리 비대 방지).

    표는 payload['judge_votes']에 영속하고(모듈 주석 — 2MB 페이로드 한도 회피),
    반환은 소형 요약만: {"dossier", "votes": n, "mixed_cluster_ids": [...]}.
    실패는 예외로 전파 — Temporal 재시도가 처리하고, 소진되면 워크플로가 call_error로
    기록하고 다음 서류철로 넘어간다(한 콜 실패가 패스를 죽이지 않음).
    """
    from src.core.step3 import _insert_llm_usage, _pass2_ctx  # 지연 import(무거운 모듈)

    gname = dossier["name"]
    chars = _load_characters(webtoon_id)
    pending, _held = _pending_and_held(_load_suggestions(webtoon_id))
    sid_set = set(dossier["sids"])
    suggs = [s for s in pending if s["id"] in sid_set]
    if not suggs:
        # 계획 이후 apply/human 처리로 sid가 전부 소멸 — 콜 없이 빈 판정.
        logger.info("[adjudicate] w%s 서류철 %d/%d %s — sid 전부 소멸, 건너뜀", webtoon_id, index, total, gname)
        return {"dossier": gname, "votes": 0, "mixed_cluster_ids": []}

    text = _dossier_text(chars, set(dossier["group_ids"]), set(dossier["related_ids"]), suggs)
    ctx = resolve_llm_model(webtoon_id, TEXT)
    call_ctx = _pass2_ctx(ctx)
    logger.info("[adjudicate] w%s 서류철 %d/%d %s — 제안 %d건(계획 %d), %d자, model=%s — 심판 콜 시작",
                webtoon_id, index, total, gname, len(suggs), len(sid_set), len(text), ctx.get("name"))

    call_t0 = time.perf_counter()
    last_exc: Exception = RuntimeError("unreachable")
    result = None
    for attempt in range(1, _JUDGE_RETRIES + 1):
        try:
            call = call_llm_json(call_ctx, JUDGE_SYSTEM, text, [])
            result = call.result if isinstance(call.result, dict) else {}
            _insert_llm_usage(webtoon_id, None, None, ctx.get("id"), call.usage or {},
                              stage=_JUDGE_STAGE, image_count=None, run_id=run_id,
                              extra={"dossier": gname})
            break
        except Exception as e:  # noqa: BLE001
            # 콜 성공 후 usage 적재가 실패한 경우까지 포함해 이번 attempt 결과를 버린다
            # (리셋 없이는 attempt1의 절반 성공 상태로 진행해 usage 행이 누락된다).
            result = None
            last_exc = e
            logger.warning("[adjudicate] w%s %s 콜 실패(attempt %d/%d, %.0fs 경과): %s",
                           webtoon_id, gname, attempt, _JUDGE_RETRIES,
                           time.perf_counter() - call_t0, e)
    if result is None:
        raise last_exc

    valid_sids = {s["id"] for s in suggs}
    votes: list[list] = []
    vcount = {"accept": 0, "reject": 0, "needs_human": 0}
    for d in result.get("decisions", []):
        v = d.get("verdict")
        if v in vcount:
            vcount[v] += 1
        for sid in (d.get("suggestion_ids") or []):
            if isinstance(sid, int) and sid in valid_sids:
                votes.append([sid, gname, d.get("action"), d.get("verdict"),
                              d.get("confidence"), str(d.get("reason") or "")[:300]])
    mixed_ids = sorted({
        note["cluster_id"] for note in result.get("cluster_notes", [])
        if isinstance(note.get("cluster_id"), int) and note["cluster_id"] in chars and note.get("mixed")
    })
    persisted = _persist_dossier_votes(gname, votes, dossier["sids"])
    logger.info(
        "[adjudicate] w%s 서류철 %d/%d %s 완료 — %.0fs (판정 %d건: accept %d/reject %d/"
        "needs_human %d, 유효 sid 표 %d — %d sid 영속, mixed %d)",
        webtoon_id, index, total, gname, time.perf_counter() - call_t0,
        len(result.get("decisions", []) or []), vcount["accept"], vcount["reject"],
        vcount["needs_human"], len(votes), persisted, len(mixed_ids),
    )
    return {"dossier": gname, "votes": len(votes), "mixed_cluster_ids": mixed_ids}


def reconcile_pass(webtoon_id: int, run_id: Optional[int], plan: dict,
                   summaries: list[Optional[dict]], call_errors: list[str]) -> dict:
    """3단계 — 표결 취합 → 쌍 단위 교차대조/가드 → 권고 영속 → 결과/stats.

    반환 계약은 종전 adjudicate_webtoon과 동일:
    {"accept_suggestion_ids", "reject_suggestion_ids", "stats", "error"}.
    표는 pending payload['judge_votes'] 스크래치에서 취합한다(모듈 주석 — summaries에는
    mixed 플래그/카운트 요약만 온다). pending은 취합 시점 기준으로 다시 읽으므로 계획
    이후 소멸한 sid의 표는 행과 함께 자연 소멸하고, 남은 표도 smap 가드가 거른다.
    ⚠️ 스크래치 청소는 여기서 하지 않는다 — 청소 커밋 후 액티비티 완료 보고 전에 워커가
    죽으면 재시도가 빈 표를 읽어 이번 패스 실행분이 통째로 noop이 된다(라운드2 리뷰).
    청소는 결과가 워크플로 히스토리에 실린 뒤(consolidation_finish) 또는 합성 경로의
    reconcile 직후(adjudicate_webtoon)가 담당하고, 남은 잔재는 다음 plan이 치운다.
    """
    chars = _load_characters(webtoon_id)
    all_suggs = _load_suggestions(webtoon_id)
    pending, _held = _pending_and_held(all_suggs)
    by_name = _by_name(chars)

    # human reject 블록리스트. 주의: 판정 권고(payload.judge)는 human행 제안에도 남으므로
    # "judge가 봤는가"가 아니라 "누가 기각했는가"로 갈라야 한다 — ai 자동 기각만
    # verdict='auto_reject'로 실행되므로, rejected인데 auto_reject가 아니면 human 기각.
    human_reject_pairs: set[tuple] = set()
    for s in all_suggs:
        if s["status"] == "rejected" and (s["payload"].get("judge") or {}).get("verdict") != "auto_reject":
            for p in _suggestion_pairs(chars, by_name, s):
                if p[0] == "pair":
                    human_reject_pairs.add((p[1], p[2]))

    votes = _load_scratch_votes(pending)              # sid -> [(group, action, verdict, conf, reason)]
    mixed_flag: dict[int, list] = defaultdict(list)   # cluster -> [group]
    for r in summaries:
        if not r:
            continue
        for ncid in r.get("mixed_cluster_ids", []):
            mixed_flag[ncid].append(r.get("dossier", "_"))

    decisions = _reconcile(chars, by_name, pending, votes, mixed_flag, human_reject_pairs)
    _persist_advisory(pending, votes, mixed_flag, decisions, run_id)

    smap = {s["id"]: s for s in pending}
    stats = {
        "pending": plan.get("pending", len(pending)), "dossiers": len(plan.get("dossiers", [])),
        "judged_sids": len(votes),
        "auto_accept": len(decisions["accept_suggestion_ids"]),
        "auto_reject": len(decisions["reject_suggestion_ids"]),
        "auto_accept_pairs": [
            {"sid": sid, "cluster": p[1], "target": p[2], "target_name": chars[p[2]]["name"]}
            for sid in decisions["accept_suggestion_ids"]
            for p in _suggestion_pairs(chars, by_name, smap[sid]) if p[0] == "pair"
        ],
        "mixed_clusters": sorted(mixed_flag),
        "call_errors": call_errors,
        "regen_held": plan.get("regen_held", 0),
    }
    logger.info("[adjudicate] w%s 완료 — %s", webtoon_id, json.dumps(stats, ensure_ascii=False)[:400])
    return {"accept_suggestion_ids": decisions["accept_suggestion_ids"],
            "reject_suggestion_ids": decisions["reject_suggestion_ids"],
            "stats": stats, "error": None}


def adjudicate_webtoon(
    webtoon_id: int,
    run_id: Optional[int] = None,
    heartbeat: Optional[Callable[[str], None]] = None,
) -> dict:
    """웹툰 단위 심판 1패스(단일 프로세스 합성) — plan→judge×N→reconcile.

    프로덕션 경로는 ConsolidateWebtoonWorkflow가 3단계를 각각 액티비티로 돌리지만
    (중간 저장), 드라이런/구버전 액티비티(consolidation_adjudicate) 호환용으로 유지한다.
    """
    plan = plan_webtoon(webtoon_id)
    if not plan["dossiers"]:
        return {"accept_suggestion_ids": [], "reject_suggestion_ids": [],
                "stats": {"pending": plan["pending"], "dossiers": 0,
                          "regen_held": plan["regen_held"]},
                "error": None}
    summaries: list[Optional[dict]] = []
    call_errors: list[str] = []
    for di, dossier in enumerate(plan["dossiers"], 1):
        if heartbeat:
            heartbeat(f"judge {di}/{len(plan['dossiers'])} {dossier['name']}")
        try:
            summaries.append(judge_dossier(webtoon_id, run_id, dossier, di, len(plan["dossiers"])))
        except Exception:  # noqa: BLE001 — 서류철 단위 격리
            call_errors.append(dossier["name"])
    result = reconcile_pass(webtoon_id, run_id, plan, summaries, call_errors)
    _clear_judge_votes_scratch(webtoon_id)  # 합성 경로는 결과를 손에 쥔 뒤라 즉시 청소 안전
    return result


def _reconcile(chars, by_name, pending, votes, mixed_flag, human_reject_pairs) -> dict:
    """sid 표결 → 쌍 단위 교차대조 + 가드 → 자동 수락/기각 sid 목록(§22.4)."""
    pair_votes: dict[tuple, dict] = defaultdict(lambda: {"acc": 0, "rej": 0, "hum": 0, "sids": set()})
    smap = {s["id"]: s for s in pending}

    for sid, vs in votes.items():
        s = smap.get(sid)
        if not s:
            continue
        for p in _suggestion_pairs(chars, by_name, s):
            if p[0] != "pair":
                continue
            book = pair_votes[(p[1], p[2])]
            book["sids"].add(sid)
            for _g, _a, v, _c, _r in vs:
                key = "acc" if v == "accept" else "rej" if v == "reject" else "hum"
                book[key] += 1

    # 다대상 가드용: 클러스터별 accept 표가 있는 대상 수.
    accepted_targets: dict[int, set] = defaultdict(set)
    for (cl, tg), b in pair_votes.items():
        if b["acc"] > 0:
            accepted_targets[cl].add(tg)

    accept_sids: list[int] = []
    for (cl, tg), b in sorted(pair_votes.items()):
        if b["acc"] < 1:
            continue
        if _is_named(chars, cl):                      # 무명→named 한정
            continue
        if cl in mixed_flag:                          # 혼합 플래그
            continue
        if len(accepted_targets[cl]) >= 2:            # 다대상 모순
            continue
        if (cl, tg) in human_reject_pairs:            # human reject 블록리스트
            continue
        if b["acc"] < _ACCEPT_MULTIPLIER * (b["rej"] + b["hum"]):  # 표결 가드
            continue
        # 대표 sid 1건만 수락 — 병합이 성사되면 §19.2가 같은 쌍의 나머지 pending을 정리한다.
        # merge 타입 우선(수락 시맨틱이 직접적), 그다음 conf 높은 순.
        cand = sorted(
            (smap[sid] for sid in b["sids"]),
            key=lambda s: (0 if s["type"] == "merge" else 1, -(s["conf"] or 0)),
        )
        accept_sids.append(cand[0]["id"])

    # 만장일치 reject → 자동 기각(ai 마커는 실행측이 남김). smap 필터: 취합 전에 소멸한
    # sid가 실행 목록에 실리지 않게(실행측 PENDING 필터가 있어도 여기서 거르는 게 정본).
    reject_sids = [sid for sid, vs in votes.items()
                   if sid in smap and vs
                   and all(v == "reject" for _g, _a, v, _c, _r in vs)
                   and sid not in set(accept_sids)]
    return {"accept_suggestion_ids": accept_sids, "reject_suggestion_ids": reject_sids}


def _persist_advisory(pending, votes, mixed_flag, decisions, run_id) -> None:
    """판정 요약을 pending payload['judge']에 영속 — human 큐 배지/정렬용(실행과 무관)."""
    now = datetime.now(timezone.utc)
    accept_set = set(decisions["accept_suggestion_ids"])
    reject_set = set(decisions["reject_suggestion_ids"])
    rows = []
    for s in pending:
        vs = votes.get(s["id"])
        if not vs:
            continue
        acc = sum(1 for _g, _a, v, _c, _r in vs if v == "accept")
        rej = sum(1 for _g, _a, v, _c, _r in vs if v == "reject")
        hum = len(vs) - acc - rej
        verdict = ("auto_accept" if s["id"] in accept_set
                   else "auto_reject" if s["id"] in reject_set
                   else "needs_human" if hum or (acc and rej)
                   else "accept_advisory" if acc
                   else "reject_advisory")
        # 대표 사유: conf 최고 표의 reason.
        top = max(vs, key=lambda x: (x[3] or 0))
        rows.append((Json({"judge": {
            "by": "ai", "run_id": run_id, "verdict": verdict,
            "votes": {"accept": acc, "reject": rej, "needs_human": hum},
            "reason": top[4],
        }}), now, s["id"]))
    if not rows:
        return
    with db_cursor() as cur:
        cur.executemany(
            "UPDATE analysis_suggestion SET payload = payload || %s, updated_at = %s "
            "WHERE id = %s AND status = 'pending'",
            rows,
        )
    logger.info("[adjudicate] 판정 권고 %d건 payload.judge 영속", len(rows))
