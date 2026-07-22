"""E2E — 흐름-통합이 카락/로트밀러 오귀속을 교정하는가.

프로덕션 DB의 ep37~44 대사(이미 전사됨)를 순서대로 뽑아, 정체성 주입 없이 흐름-통합(glm-5.2)만
돌린다. 가설: 대사에 '카락' 호명이 0회이므로(실측), 흐름-통합 로스터엔 카락이 안 나오고
로트밀러·무라드·미샤·드왈키가 나와야 한다 = 프로덕션의 유령 카락(face-attribution 산물) 교정.
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
import psycopg2  # noqa: E402

SYSTEM = """너는 웹툰 회차의 텍스트 블록만 보고(이미지 없음, 정체성 정보 없음) ① 말하는 인물의
익명 로스터를 구성하고 ② 각 블록의 화자를 배정한다. 대사 자체에서 인물을 갈라내라:
화법(반말/존댓말), 호칭(누가 누구를 뭐라 부르나), 대사 교대, 나레이션 POV.
**이름은 대사에서 실제로 호명/자칭될 때만** 로스터에 붙여라(억지 명명 금지, 없으면 이름 없이 서술만).
효과음·화자불명은 null. 반드시 JSON만:
{"roster": {"A": {"desc":"화법/역할", "name":"대사에서 호명된 이름 또는 null"}, ...},
 "assign": {"0":"A", "1":null, ...}}"""


def pull_dialogue(cur, ep_no: int) -> list[dict]:
    cur.execute("""
    SELECT c.cut_number, ta.type, ta.text
    FROM analysis_text_annotation ta
    JOIN analysis_text_region r ON r.id = ta.region_id
    JOIN webtoon_cut c ON c.id = r.cut_id
    JOIN webtoon_episode e ON e.id = c.episode_id
    WHERE e.webtoon_id = 23 AND e.no = %s AND ta.source = 'llm'
      AND ta.type IN ('speech','monologue','narration')
    ORDER BY c.cut_number, r.index, ta.id
    """, (ep_no,))
    return [{"cut": cn, "type": t, "text": (x or "").strip()}
            for cn, t, x in cur.fetchall() if (x or "").strip()]


def main() -> None:
    ep_no = int(sys.argv[1]) if len(sys.argv) > 1 else 41
    conn = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
                            dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
                            password=os.environ["POSTGRES_PASSWORD"])
    cur = conn.cursor()
    blocks = pull_dialogue(cur, ep_no)
    lines = [f'{i}\t컷{b["cut"]}\t[{b["type"]}]\t{b["text"]}' for i, b in enumerate(blocks)]
    user = "[블록 (인덱스\\t컷\\t[type]\\t텍스트)]\n" + "\n".join(lines)
    print(f"ep{ep_no} 대사블록 {len(blocks)}개, 흐름-통합(glm-5.2)...")

    out = call("glm-5.2", SYSTEM, user)
    roster = out.get("roster", {})
    assign = out.get("assign", {})
    from collections import Counter
    cnt = Counter(str(v) for v in assign.values())
    print(f"\n=== ep{ep_no} 흐름-통합 로스터 (말하는 인물) ===")
    for slot, info in roster.items():
        if isinstance(info, dict):
            nm, desc = info.get("name"), info.get("desc", "")
        else:
            nm, desc = None, str(info)
        n = cnt.get(slot, 0)
        print(f"  {slot} [{n}블록] name={nm!r}  {desc[:70]}")
    print("  (배정분포:", dict(cnt.most_common()), ")")

    out_dir = HERE.parent / "datasets" / "e2e_correction"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"ep{ep_no}.json").write_text(
        json.dumps({"ep": ep_no, "roster": roster, "assign": assign, "blocks": blocks},
                   ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
