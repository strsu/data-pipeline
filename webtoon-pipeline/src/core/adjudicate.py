"""제안검토 심판(§22.4) — 정리 패스의 LLM 심판 + 결정론 가드/교차대조.

역할 분리(§22.3): **판정·가드는 여기(data-pipeline), 실행은 service 수락 경로**
(§19 병합 시맨틱·name수락=병합·face_reassign수락 재사용 — §20 자동 훅이 따라옴).

드라이런 실측(2026-07-12, prd §22.4 — 화산귀환 pending 749건 + 사용자 눈검증 GT)으로
캘리브레이션된 규칙을 그대로 구현한다:
  - **sid 기반 교차대조**: 심판 출력의 cluster_id 필드는 신뢰 불가(묶음 판정 시 대상 id
    오기입·rename으로 병합 우회 실측) — 판정은 suggestion_ids로 수집하고 액션 의미는
    suggestion 원본에서 복원한다.
  - rename-to-기존이름 = 병합으로 정규화(§19.3과 동일 시맨틱).
  - 자동 수락 가드(전부 만족): 무명 클러스터→named 방향 / 어떤 도시에서도 혼합(mixed)
    플래그 없음 / 다대상 accept 없음 / human reject 블록리스트 아님 /
    **표결 accept >= 2×(reject+needs_human)** (GT: 오병합 0·참병합 9 유일 규칙군 —
    단순 다수결은 오병합 27%).
  - 만장일치 reject → 자동 기각(payload.judge.by='ai' 마커 — human reject과 구분해
    블록리스트 가드가 ai 기각을 human 판단으로 오인하지 않게).
  - 그 외 판정은 payload['judge'] 권고로 영속 — human 검토 큐 정렬/배지용.

의도적 중복 배치: 한 클러스터를 관련 named 인물 도시에마다 노출시켜 다관점 표결을
얻는다(모순이 표결로 드러남 — c1712 조걸자칭 vs 청명 실측). 도시에 크기는 캡으로 제한
(88k자 도시에가 게이트웨이 재시도로 2h+ 걸린 실측 — 분할이 정답).
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Optional

from psycopg2.extras import Json

from src.config.db import db_cursor
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import TEXT, resolve_llm_model

logger = logging.getLogger(__name__)

_JUDGE_STAGE = "judge"
# 도시에당 제안 상한 — 초과 시 분할(대형 도시에 1콜이 게이트웨이 재시도 지옥 실측).
_MAX_SUGGESTIONS_PER_DOSSIER = 60
_EVIDENCE_SNIPPET = 500          # 제안 근거 전문 상한(자)
_ACCEPT_MULTIPLIER = 2.0         # accept >= MULT × (reject+needs_human)
_JUDGE_RETRIES = 2               # 도시에 콜 실패 시 재시도(콜 내부 재시도와 별개)

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
    "한국어. 마지막에 JSON만 출력. cluster_id/target_id는 반드시 도시에에 등장한 숫자 id(cXXXX의 XXXX)만 사용:\n"
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
    """pending(판정 대상) + rejected(human 블록리스트 원료) 전부."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT s.id, s.type, s.status, s.character_id, e.no, s.confidence, s.payload
            FROM analysis_suggestion s
            LEFT JOIN webtoon_episode e ON e.id = s.episode_id
            WHERE s.webtoon_id = %s AND s.deleted_at IS NULL
              AND s.status IN ('pending', 'rejected')
            """,
            (webtoon_id,),
        )
        return [
            {"id": r[0], "type": r[1], "status": r[2], "cid": r[3], "ep": r[4],
             "conf": float(r[5]) if r[5] is not None else None, "payload": r[6] or {}}
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


# ── 도시에 구성 ────────────────────────────────────────────────────────────────

def _build_groups(chars: dict, pending: list[dict]) -> dict[str, dict]:
    """named 인물별 + 대형/다경합 클러스터 전용 + 신규명명 배치 도시에(§22.4).

    의도적 중복(한 클러스터가 여러 named 도시에에 등장)은 교차 검증 장치.
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

def adjudicate_webtoon(
    webtoon_id: int,
    run_id: Optional[int] = None,
    heartbeat: Optional[Callable[[str], None]] = None,
) -> dict:
    """웹툰 단위 심판 1패스 — 판정만 하고 실행은 하지 않는다(실행은 service).

    반환: {"accept_suggestion_ids", "reject_suggestion_ids", "stats", "error"}
    부수효과: 판정 요약을 pending suggestion payload['judge']에 영속(권고/정렬용),
              심판 콜마다 llm_usage(stage='judge') 적재.
    """
    from src.core.step3 import _insert_llm_usage, _pass2_ctx  # 지연 import(무거운 모듈)

    def beat(msg: str) -> None:
        if heartbeat:
            heartbeat(msg)

    chars = _load_characters(webtoon_id)
    all_suggs = _load_suggestions(webtoon_id)
    pending = [s for s in all_suggs if s["status"] == "pending"]
    by_name: dict[str, int] = {}
    for c in chars.values():
        if _is_named(chars, c["id"]):
            by_name.setdefault(c["name"], c["id"])

    # human reject 블록리스트. 주의: 판정 권고(payload.judge)는 human행 제안에도 남으므로
    # "judge가 봤는가"가 아니라 "누가 기각했는가"로 갈라야 한다 — ai 자동 기각만
    # verdict='auto_reject'로 실행되므로, rejected인데 auto_reject가 아니면 human 기각.
    human_reject_pairs: set[tuple] = set()
    for s in all_suggs:
        if s["status"] == "rejected" and (s["payload"].get("judge") or {}).get("verdict") != "auto_reject":
            for p in _suggestion_pairs(chars, by_name, s):
                if p[0] == "pair":
                    human_reject_pairs.add((p[1], p[2]))

    if not pending:
        return {"accept_suggestion_ids": [], "reject_suggestion_ids": [],
                "stats": {"pending": 0, "dossiers": 0}, "error": None}

    groups = _build_groups(chars, pending)

    # 도시에 크기 캡 — 초과 그룹은 제안 청크로 분할(도감 컨텍스트는 공유).
    dossiers: list[tuple[str, set, set, list[dict]]] = []
    for gname, spec in groups.items():
        suggs = _group_suggestions(chars, pending, spec["group_ids"], spec["group_names"])
        if not suggs:
            continue
        if len(suggs) <= _MAX_SUGGESTIONS_PER_DOSSIER:
            dossiers.append((gname, spec["group_ids"], spec["related_ids"], suggs))
        else:
            suggs = sorted(suggs, key=lambda x: (x["ep"] or 0, x["id"]))
            for i in range(0, len(suggs), _MAX_SUGGESTIONS_PER_DOSSIER):
                dossiers.append((f"{gname}_p{i // _MAX_SUGGESTIONS_PER_DOSSIER + 1}",
                                 spec["group_ids"], spec["related_ids"],
                                 suggs[i:i + _MAX_SUGGESTIONS_PER_DOSSIER]))

    ctx = resolve_llm_model(webtoon_id, TEXT)
    call_ctx = _pass2_ctx(ctx)

    votes: dict[int, list] = defaultdict(list)        # sid -> [(group, action, verdict, conf, reason)]
    mixed_flag: dict[int, list] = defaultdict(list)   # cluster -> [group]
    call_errors: list[str] = []

    for di, (gname, group_ids, related_ids, suggs) in enumerate(dossiers, 1):
        text = _dossier_text(chars, group_ids, related_ids, suggs)
        beat(f"judge {di}/{len(dossiers)} {gname}")
        logger.info("[adjudicate] w%s 도시에 %d/%d %s — 제안 %d건, %d자",
                    webtoon_id, di, len(dossiers), gname, len(suggs), len(text))
        result = None
        for _ in range(_JUDGE_RETRIES):
            try:
                call = call_llm_json(call_ctx, JUDGE_SYSTEM, text, [])
                result = call.result if isinstance(call.result, dict) else {}
                _insert_llm_usage(webtoon_id, None, None, ctx.get("id"), call.usage or {},
                                  stage=_JUDGE_STAGE, image_count=None, run_id=run_id,
                                  extra={"dossier": gname})
                break
            except Exception as e:  # noqa: BLE001 — 도시에 단위 격리(한 콜 실패가 패스를 죽이지 않음)
                logger.warning("[adjudicate] w%s %s 콜 실패: %s", webtoon_id, gname, e)
                result = None
        if result is None:
            call_errors.append(gname)
            continue

        valid_sids = {s["id"] for s in suggs}
        for d in result.get("decisions", []):
            for sid in (d.get("suggestion_ids") or []):
                if isinstance(sid, int) and sid in valid_sids:
                    votes[sid].append((gname, d.get("action"), d.get("verdict"),
                                       d.get("confidence"), str(d.get("reason") or "")[:300]))
        for note in result.get("cluster_notes", []):
            ncid = note.get("cluster_id")
            if isinstance(ncid, int) and ncid in chars and note.get("mixed"):
                mixed_flag[ncid].append(gname)

    decisions = _reconcile(chars, by_name, pending, votes, mixed_flag, human_reject_pairs)
    _persist_advisory(pending, votes, mixed_flag, decisions, run_id)

    smap = {s["id"]: s for s in pending}
    stats = {
        "pending": len(pending), "dossiers": len(dossiers), "judged_sids": len(votes),
        "auto_accept": len(decisions["accept_suggestion_ids"]),
        "auto_reject": len(decisions["reject_suggestion_ids"]),
        "auto_accept_pairs": [
            {"sid": sid, "cluster": p[1], "target": p[2], "target_name": chars[p[2]]["name"]}
            for sid in decisions["accept_suggestion_ids"]
            for p in _suggestion_pairs(chars, by_name, smap[sid]) if p[0] == "pair"
        ],
        "mixed_clusters": sorted(mixed_flag),
        "call_errors": call_errors,
    }
    logger.info("[adjudicate] w%s 완료 — %s", webtoon_id, json.dumps(stats, ensure_ascii=False)[:400])
    return {"accept_suggestion_ids": decisions["accept_suggestion_ids"],
            "reject_suggestion_ids": decisions["reject_suggestion_ids"],
            "stats": stats, "error": None}


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

    # 만장일치 reject → 자동 기각(ai 마커는 실행측이 남김).
    reject_sids = [sid for sid, vs in votes.items()
                   if vs and all(v == "reject" for _g, _a, v, _c, _r in vs)
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
