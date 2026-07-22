"""연쇄 교차회차 링커 스트레스 테스트 — ep37 시드 → ep38~44 순차 전파.

매 회차: ①이름 없이 persona 슬롯 클러스터(+회차내 자칭/나레이션 앵커만 이름) → ②누적 prior
로스터에 persona로 링크(이름 전파) → ③미링크 슬롯 중 회차내 앵커 있으면 신규로 로스터에 추가.
관건: 7회차 연쇄에서 (a) 핵심 인물 이름 안정 유지 (b) 신규 인물 정상 추가 (c) 오류 전파 여부.
속도 위해 로스터만 출력(per-block assign 생략).
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
import psycopg2  # noqa: E402

DATA = HERE.parent / "datasets" / "e2e_correction"

CLUSTER_SYS = """웹툰 회차의 텍스트 블록만 보고(이미지·정체성 없음) **말하는 인물의 익명 슬롯**을 구성하라.
화법(반말/존댓말/특유 어미)·호칭·대사교대·나레이션 POV·역할·관계로 인물을 가른다.
각 슬롯을 재식별 가능한 persona로 서술(화법 지문·역할·관계). 이름은 **자칭("나는 X"/"자칭 X") 또는
나레이션이 그 인물을 X로 명시**할 때만 anchor_name에 넣고, 그 외엔 anchor_name=null(호명만 된 이름은
넣지 마라 — 그건 상대를 부른 것일 수 있다). JSON만:
{"slots": {"A": {"persona":"...", "anchor_name": "자칭/나레이션명 또는 null"}, ...}}"""

LINK_SYS = """교차회차 인물 링커. prior 로스터(이전까지 확정된 인물 name+persona)와 현재 회차 익명
슬롯(persona, 일부 anchor_name)을 받는다. 각 현재 슬롯이 prior의 누구인지 **persona(화법지문·역할·
관계)로만** 매칭하라(얼굴·외모 없음). 규칙: 확신 시 prior 이름, 애매하면 null. 한 prior 인물은 현재
슬롯 최대 1개. anchor_name이 있으면 그것도 참고(자칭은 강한 증거). JSON만:
{"link": {"A": {"name":"prior이름 또는 anchor_name 또는 null", "confidence":0~1, "is_new": true/false}, ...}}"""


def episode_roster(ep: int, prior: list[dict]) -> tuple[dict, list[dict]]:
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

    resolved = {}
    prior_names = {p["name"] for p in prior}
    for s, v in slots.items():
        li = link.get(s, {})
        name = li.get("name") or v.get("anchor_name")
        resolved[s] = {"name": name, "conf": li.get("confidence"),
                       "is_new": bool(li.get("is_new")) and name and name not in prior_names,
                       "persona": v.get("persona", "")}
        # 신규(prior에 없고 이름 있음) → 로스터에 추가
        if name and name not in prior_names:
            prior.append({"name": name, "persona": v.get("persona", "")})
            prior_names.add(name)
    return resolved, prior


def main() -> None:
    ep37 = json.loads((DATA / "ep37.json").read_text())
    prior = [{"name": (i.get("name") if isinstance(i, dict) else None),
              "persona": (i.get("desc", "") if isinstance(i, dict) else str(i))}
             for i in ep37["roster"].values()]
    prior = [p for p in prior if p["name"]]
    print(f"시드(ep37): {[p['name'] for p in prior]}\n")

    chain = {"37": [p["name"] for p in prior]}
    for ep in range(38, 45):
        resolved, prior = episode_roster(ep, prior)
        print(f"=== ep{ep} ===")
        for s, r in resolved.items():
            tag = " [신규]" if r["is_new"] else ""
            print(f"  {s}: {r['name']!r} (conf={r['conf']}){tag}  {r['persona'][:45]}")
        chain[str(ep)] = {s: r["name"] for s, r in resolved.items()}
        (DATA / f"chain_ep{ep}.json").write_text(json.dumps(resolved, ensure_ascii=False, indent=2))
    (DATA / "chain_summary.json").write_text(json.dumps(chain, ensure_ascii=False, indent=2))
    print("\n최종 누적 로스터:", [p["name"] for p in prior])


if __name__ == "__main__":
    main()
