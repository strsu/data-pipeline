"""교차회차 링커 프로토타입 — 자칭 앵커 회차의 이름을 후속 회차 슬롯에 전파.

E2E 실측: ep41은 회차 내 이름 앵커(자칭/나레이션)가 없어 비요른/로트밀러를 슬롯에 못 붙였다.
그러나 ep37은 자칭으로 정확히 명명됐다. 링커의 역할: **얼굴·이름 없이 persona(화법·역할·관계)로
ep41 익명 슬롯을 ep37 확정 로스터에 매칭**해 이름을 전파한다(설계 §4③·C6, prd C4 링크=엣지).

파이프라인:
  1) ep41 재클러스터링 — 이름 없이 persona만(화법/역할/관계 지문).
  2) 링커 — prior 로스터(ep37 name+persona) + ep41 슬롯(persona) → 슬롯별 {name, confidence, reason}.
  3) 검증 — ep41.A(반말 바바리안 리더)가 비요른으로, 로트밀러 오결합이 사라지는가.
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

DATA = HERE.parent / "datasets"

CLUSTER_SYS = """너는 웹툰 회차의 텍스트 블록만 보고(이미지·정체성 없음) **말하는 인물의 익명 슬롯**을
구성한다. 대사에서 인물을 갈라내라: 화법(반말/존댓말/특유 어미), 호칭, 대사 교대, 나레이션 POV,
역할·관계. ⚠️ **이름은 절대 붙이지 마라(name 필드 없음).** 각 슬롯을 재식별 가능한 persona로만 서술:
화법 지문(특유 어미·말투), 역할(리더/탱커/네비게이터/마법사 등), 관계(누구에게 존대/하대, 누구를 부름).
효과음·화자불명 null. JSON만: {"slots": {"A": "persona 서술", ...}, "assign": {"0":"A", "1":null, ...}}"""

LINK_SYS = """너는 교차회차 인물 링커다. **prior 로스터**(이전 회차에서 이름이 확정된 인물들, 각자
persona 포함)와 **현재 회차의 익명 슬롯**(persona만, 이름 없음)을 받는다. 각 현재 슬롯이 prior의
누구인지 **persona(화법 지문·역할·관계)로만** 매칭하라. 얼굴·외모 정보는 없다 — 오직 말투와 역할.
규칙: 확신 있으면 prior 이름 전파, 애매하면 name=null(신규 인물이거나 판단 불가). 한 prior 인물은
현재 슬롯 최대 1개에만. JSON만: {"link": {"A": {"name": "전파된 이름 또는 null", "confidence": 0~1,
"reason": "매칭 근거(어떤 화법/역할이 일치)"}, ...}}"""


def main() -> None:
    conn = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
                            dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
                            password=os.environ["POSTGRES_PASSWORD"])
    cur = conn.cursor()

    # prior 로스터: ep37 확정 결과에서 name 있는 슬롯만
    ep37 = json.loads((DATA / "e2e_correction" / "ep37.json").read_text())
    prior = []
    for slot, info in ep37["roster"].items():
        nm = info.get("name") if isinstance(info, dict) else None
        desc = info.get("desc", "") if isinstance(info, dict) else str(info)
        if nm:
            prior.append({"name": nm, "persona": desc})
    print(f"prior 로스터(ep37 확정): {[p['name'] for p in prior]}")

    # 1) ep41 이름 없이 재클러스터링
    blocks = pull_dialogue(cur, 41)
    lines = [f'{i}\t컷{b["cut"]}\t[{b["type"]}]\t{b["text"]}' for i, b in enumerate(blocks)]
    out = call("glm-5.2", CLUSTER_SYS, "[블록]\n" + "\n".join(lines))
    slots = out.get("slots", {})
    print(f"\nep41 익명 슬롯(이름 없음) {len(slots)}개:")
    for s, p in slots.items():
        print(f"  {s}: {p[:75]}")

    # 2) 링커
    prior_txt = "\n".join(f"  {p['name']}: {p['persona']}" for p in prior)
    slots_txt = "\n".join(f"  {s}: {p}" for s, p in slots.items())
    link_user = f"[prior 로스터 (이름 확정)]\n{prior_txt}\n\n[현재 회차(ep41) 익명 슬롯]\n{slots_txt}"
    link = call("glm-5.2", LINK_SYS, link_user).get("link", {})

    print("\n=== 링커 결과 (ep41 슬롯 → 전파된 이름) ===")
    for s, info in link.items():
        persona = slots.get(s, "")[:45]
        print(f"  {s} ({persona}) → {info.get('name')!r} conf={info.get('confidence')} :: {info.get('reason','')[:55]}")

    (DATA / "e2e_correction" / "ep41_linked.json").write_text(
        json.dumps({"prior": prior, "slots": slots, "link": link}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
