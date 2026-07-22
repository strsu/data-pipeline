"""연쇄 링커 v2 — 가드 추가. v1 스트레스 테스트가 드러낸 2결함(역할 드리프트·오류 compounding)을
설계 §8①②⑤ 가드로 막고 ep38~44 재연쇄해 개선을 실측한다.

가드:
  A. persona 매칭을 역할어(리더/POV/베테랑)가 아니라 **고유 어미·관계 지문**으로(프롬프트).
  B. new-name 게이트: 자칭 앵커 + **자모 dedup** — 기존 로스터/죽은 이름과 자모유사 ≥THR면
     병합(기존) 또는 거부(죽은). 단일 호명은 이름으로 안 씀.
  C. 죽은 이름 리스트(카락) 근처면 거부(라락~카락 0.8 차단).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "webtoon-pipeline"))
from exp_speaker_flow import call  # noqa: E402
from exp_e2e_correction import pull_dialogue  # noqa: E402
from exp_jamo_norm import jamo_ratio  # noqa: E402
import psycopg2  # noqa: E402

DATA = HERE.parent / "datasets" / "e2e_correction"
DEAD = ["카락"]          # 서사상 죽은/부재 인물 — 이 근처 새 이름은 오독으로 간주
DEDUP_THR = 0.70

CLUSTER_SYS = """웹툰 회차의 텍스트 블록만 보고(이미지·정체성 없음) **말하는 인물의 익명 슬롯**을 구성하라.
인물을 가르는 핵심은 **고유한 말끝 어미·말버릇**(예: ~냥/~당, ~시오/~소, ~구려)과 **관계**(누구에게
존대/하대, 누구를 뭐라 부름)다. ⚠️ '리더·POV·베테랑·탱커' 같은 **역할어는 여러 명이 공유하므로
정체성 근거로 쓰지 마라.** 각 슬롯 persona에 반드시 **특유 어미 지문**을 명시하라. 이름은
**자칭("나는 X"/"자칭 X") 또는 나레이션 명시**일 때만 anchor_name(호명만 된 이름 금지). JSON만:
{"slots": {"A": {"persona":"고유어미+관계 중심", "anchor_name": "자칭/나레이션명 또는 null"}, ...}}"""

LINK_SYS = """교차회차 인물 링커. prior 로스터(확정 인물 name + 고유 persona 지문)와 현재 회차 익명
슬롯(persona)을 받는다. **오직 고유 어미·말버릇·관계 지문으로** 매칭하라 — 역할어(리더/POV/탱커)는
근거로 쓰지 마라(여러 명이 공유). 확신 시 prior 이름, 애매하면 null. 한 prior 인물은 현재 슬롯 최대 1개.
JSON만: {"link": {"A": {"name":"prior이름 또는 null", "confidence":0~1, "matched_on":"일치한 고유지문"}}}"""


def gate_new_name(name: str, roster_names: set) -> tuple[str | None, str]:
    """새 이름 게이트. 반환 (확정이름 또는 None, 사유)."""
    if not name:
        return None, "no-anchor"
    for dead in DEAD:
        if jamo_ratio(name, dead) >= 0.65:
            return None, f"reject: '{name}'~죽은'{dead}'({jamo_ratio(name,dead):.2f})"
    for rn in roster_names:
        if jamo_ratio(name, rn) >= DEDUP_THR:
            return rn, f"merge: '{name}'→기존'{rn}'({jamo_ratio(name,rn):.2f})"
    return name, "new-ok"


def episode_roster(ep: int, prior: list[dict]) -> tuple[dict, list[dict], list[str]]:
    conn = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
                            dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
                            password=os.environ["POSTGRES_PASSWORD"])
    blocks = pull_dialogue(conn.cursor(), ep)
    lines = [f'{i}\t컷{b["cut"]}\t[{b["type"]}]\t{b["text"]}' for i, b in enumerate(blocks)]
    slots = call("glm-5.2", CLUSTER_SYS, "[블록]\n" + "\n".join(lines)).get("slots", {})

    prior_txt = "\n".join(f"  {p['name']}: {p['persona']}" for p in prior) or "  (없음)"
    slots_txt = "\n".join(
        f"  {s}: persona={v.get('persona','')} anchor={v.get('anchor_name')}" for s, v in slots.items())
    link = call("glm-5.2", LINK_SYS,
                f"[prior 로스터]\n{prior_txt}\n\n[현재 회차(ep{ep}) 슬롯]\n{slots_txt}").get("link", {})

    resolved, notes = {}, []
    prior_names = {p["name"] for p in prior}
    for s, v in slots.items():
        li = link.get(s, {})
        name, is_new = li.get("name"), False
        if not name and v.get("anchor_name"):     # 링크 실패 → 신규 후보(게이트)
            gated, why = gate_new_name(v["anchor_name"], prior_names)
            notes.append(f"{s}:{why}")
            name = gated
            is_new = bool(gated) and gated not in prior_names
        resolved[s] = {"name": name, "conf": li.get("confidence"), "is_new": is_new,
                       "persona": v.get("persona", "")}
        if is_new:
            prior.append({"name": name, "persona": v.get("persona", "")})
            prior_names.add(name)
    return resolved, prior, notes


def main() -> None:
    ep37 = json.loads((DATA / "ep37.json").read_text())
    prior = [{"name": (i.get("name") if isinstance(i, dict) else None),
              "persona": (i.get("desc", "") if isinstance(i, dict) else str(i))}
             for i in ep37["roster"].values()]
    prior = [p for p in prior if p["name"]]
    print(f"시드(ep37): {[p['name'] for p in prior]}\n")

    chain = {"37": [p["name"] for p in prior]}
    for ep in range(38, 45):
        resolved, prior, notes = episode_roster(ep, prior)
        print(f"=== ep{ep} ===  가드: {notes}")
        for s, r in resolved.items():
            tag = " [신규]" if r["is_new"] else ""
            print(f"  {s}: {r['name']!r} (conf={r['conf']}){tag}  {r['persona'][:42]}")
        chain[str(ep)] = {s: r["name"] for s, r in resolved.items()}
    (DATA / "chain_v2_summary.json").write_text(json.dumps(chain, ensure_ascii=False, indent=2))
    print("\n최종 누적 로스터:", [p["name"] for p in prior])


if __name__ == "__main__":
    main()
