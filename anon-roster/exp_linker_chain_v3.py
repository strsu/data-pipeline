"""연쇄 링커 v3 — dead-veto 제거 + 발화행위 게이트 + 3인칭 언급 분리.

사용자 지적 반영(2026-07-22): 하드코딩 dead-list는 반칙이고 회상·3인칭 언급을 깨뜨린다. 자모는
문지기가 아니라 확정된 이름의 표면형 통일 도구다. 죽음은 "지금 말하나?"로 흐름이 자연 처리한다.

v3 가드:
  ① 발화행위 게이트 — anchor_name은 evidence.kind ∈ {self, narration_subject}일 때만 슬롯 결합.
     (라락 같은 고립 나레이션 토큰·SFX 오독은 narration_subject 아님 → 탈락. dead-list 불필요.)
  ② 3인칭 언급/호격 분리 — kind=reference/vocative 이름은 '언급된 엔티티'로만 기록, 현재 슬롯 미결합.
     ("예전에 카락이 했었지" → 카락은 mentioned_entities에만, 어느 슬롯에도 안 붙음.)
  ③ persona 매칭 = 고유어미·관계(역할어 금지). 신규는 자모-dedup(기존 로스터만) 후 추가. dead-veto 없음.
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
DEDUP_THR = 0.70

CLUSTER_SYS = """웹툰 회차의 텍스트 블록만 보고(이미지·정체성 없음) **말하는 인물의 익명 슬롯**을 구성하라.
인물을 가르는 핵심은 **고유 말끝 어미·말버릇**(~냥/~당, ~시오/~소, ~구려)과 **관계**(누구에게 존대/하대,
누구를 뭐라 부름)다. '리더·POV·베테랑·탱커' 같은 역할어는 여러 명이 공유하므로 정체성 근거로 쓰지 마라.

각 대사 속 이름을 **역할**로 분류하라:
- self: 자칭("나는 X", "자칭 X", "X라고 한다") → 그 화자 슬롯의 이름
- narration_subject: 나레이션이 그 인물을 **주어로 삼아 행동·상태를 서술**("X가 앞장섰다", "X는 …했다")
- vocative: 상대를 부름("X야", "X님") → 화자 아니라 상대
- reference: 3인칭 언급("예전에 X가 했었지", "X 말로는…") → 현재 등장 인물이 아닐 수 있음
⚠️ 고립된 이름 토큰(문장 없이 이름만), 효과음처럼 보이는 것은 어느 분류도 아님(무시).

슬롯의 anchor_name은 **self 또는 narration_subject일 때만** 채운다(vocative/reference/고립 금지).
vocative·reference로 나온 이름은 mentioned에 모아라(슬롯 결합 안 함). JSON만:
{"slots": {"A": {"persona":"고유어미+관계", "anchor_name": "self/narration_subject 이름 또는 null"}, ...},
 "mentioned": ["vocative/reference로만 나온 이름들"]}"""

LINK_SYS = """교차회차 인물 링커. prior 로스터(확정 name + 고유 persona 지문)와 현재 회차 익명 슬롯을
받아, **오직 고유 어미·말버릇·관계로** 매칭하라(역할어·외모 금지). 확신 시 prior 이름, 애매하면 null.
한 prior 인물은 현재 슬롯 최대 1개. JSON만:
{"link": {"A": {"name":"prior이름 또는 null", "confidence":0~1, "matched_on":"일치 고유지문"}}}"""


def dedup(name: str, roster_names: set) -> tuple[str, str]:
    for rn in roster_names:
        if jamo_ratio(name, rn) >= DEDUP_THR:
            return rn, f"merge:{name}->{rn}({jamo_ratio(name,rn):.2f})"
    return name, "new"


def episode(ep: int, prior: list[dict]):
    conn = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
                            dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
                            password=os.environ["POSTGRES_PASSWORD"])
    blocks = pull_dialogue(conn.cursor(), ep)
    lines = [f'{i}\t컷{b["cut"]}\t[{b["type"]}]\t{b["text"]}' for i, b in enumerate(blocks)]
    cl = call("glm-5.2", CLUSTER_SYS, "[블록]\n" + "\n".join(lines))
    slots, mentioned = cl.get("slots", {}), cl.get("mentioned", [])

    prior_txt = "\n".join(f"  {p['name']}: {p['persona']}" for p in prior) or "  (없음)"
    slots_txt = "\n".join(f"  {s}: persona={v.get('persona','')} anchor={v.get('anchor_name')}"
                          for s, v in slots.items())
    link = call("glm-5.2", LINK_SYS,
                f"[prior 로스터]\n{prior_txt}\n\n[현재 회차(ep{ep}) 슬롯]\n{slots_txt}").get("link", {})

    resolved, notes = {}, []
    names = {p["name"] for p in prior}
    for s, v in slots.items():
        li = link.get(s, {})
        name, is_new = li.get("name"), False
        if not name and v.get("anchor_name"):          # 링크 실패 → 자칭/나레이션 앵커로 신규(게이트 통과분만)
            cand, why = dedup(v["anchor_name"], names)
            notes.append(f"{s}:{why}")
            name = cand
            is_new = cand not in names
        resolved[s] = {"name": name, "conf": li.get("confidence"), "is_new": is_new,
                       "persona": v.get("persona", "")}
        if is_new:
            prior.append({"name": name, "persona": v.get("persona", "")})
            names.add(name)
    return resolved, prior, notes, mentioned


def main():
    ep37 = json.loads((DATA / "ep37.json").read_text())
    prior = [{"name": (i.get("name") if isinstance(i, dict) else None),
              "persona": (i.get("desc", "") if isinstance(i, dict) else str(i))}
             for i in ep37["roster"].values()]
    prior = [p for p in prior if p["name"]]
    print(f"시드(ep37): {[p['name'] for p in prior]}\n")
    chain = {"37": [p["name"] for p in prior]}
    for ep in range(38, 45):
        resolved, prior, notes, mentioned = episode(ep, prior)
        print(f"=== ep{ep} ===  게이트:{notes}  언급엔티티:{mentioned}")
        for s, r in resolved.items():
            tag = " [신규]" if r["is_new"] else ""
            print(f"  {s}: {r['name']!r} (conf={r['conf']}){tag}  {r['persona'][:40]}")
        chain[str(ep)] = {"roster": {s: r["name"] for s, r in resolved.items()}, "mentioned": mentioned}
    (DATA / "chain_v3_summary.json").write_text(json.dumps(chain, ensure_ascii=False, indent=2))
    print("\n최종 누적 로스터:", [p["name"] for p in prior])


if __name__ == "__main__":
    main()
