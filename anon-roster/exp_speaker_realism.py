"""E1c — 흐름 화자배정의 현실성 검증.

E1a는 '깨끗한 수동 전사 + 정답 로스터'라는 상한 조건이었다. 이건 그 경계조건(b)를 닫는다:
**실제 파이프라인 전사**(anon glm-4.6v ep10 산출, 오독 포함)로 같은 흐름 화자배정을 돌리고,
각 블록을 수동정답(컷+텍스트 유사도)에 정렬해 채점한다. 흐름이 노이즈에도 버티는지가 관건.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from exp_speaker_flow import ROSTER, SYSTEM, call, parse_manual, score  # noqa: E402
from exp_jamo_norm import jamo_ratio  # noqa: E402

REPO = HERE.parent
ANON = REPO / "datasets" / "anon" / "glm-4.6v_dfa7453c37a6"


def load_anon(ep: int) -> list[dict]:
    """anon 산출에서 (cut, transcript, type) 읽기순으로."""
    recs = {r["cut_number"]: r for r in
            (json.loads(l) for l in open(ANON / f"w23_e{ep}.jsonl") if l.strip())}
    blocks = []
    for cn in sorted(recs):
        res = recs[cn].get("result") or {}
        for b in res.get("blocks", []):
            t = (b.get("transcript") or "").strip()
            if t:
                blocks.append({"cut": cn, "text": t, "type": b.get("type", "")})
    return blocks


def align_truth(anon_blocks: list[dict], manual: list[dict]) -> list[dict]:
    """anon 블록을 수동정답에 정렬 — 같은 컷 + 텍스트 유사도(자모) 최댓값, 0.5 이상만 채택."""
    by_cut: dict[int, list[dict]] = {}
    for m in manual:
        if m["spk_truth"] in ("A", "B") and m["text"]:
            by_cut.setdefault(m["cut"], []).append(m)
    out = []
    for b in anon_blocks:
        cands = by_cut.get(b["cut"], [])
        best, bestr = None, 0.0
        for m in cands:
            r = jamo_ratio(b["text"][:20], m["text"][:20])
            if r > bestr:
                best, bestr = m, r
        out.append({**b, "spk_truth": best["spk_truth"] if (best and bestr >= 0.5) else "?"})
    return out


def main() -> None:
    ep = 10
    model = "glm-5.2"
    anon = load_anon(ep)
    manual = parse_manual(ep)
    blocks = align_truth(anon, manual)
    ngold = sum(1 for b in blocks if b["spk_truth"] in ("A", "B"))
    print(f"E1c ep{ep}: anon블록 {len(anon)} / 정렬된 gold {ngold} / model {model}")

    lines = [f'{i}\t컷{b["cut"]}\t[{b["type"]}]\t{b["text"]}' for i, b in enumerate(blocks)]
    roster_txt = "\n".join(f"  {k}: {v}" for k, v in ROSTER[ep].items())
    user = f"[로스터]\n{roster_txt}\n\n[블록 (인덱스\\t컷\\t[type]\\t텍스트)]\n" + "\n".join(lines)

    out = call(model, SYSTEM, user)
    assign = out.get("assign", out)
    res = score(blocks, assign)
    out_dir = REPO / "datasets" / "speaker_flow"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"ep{ep}_{model}_realism.json").write_text(
        json.dumps({"score": res, "assign": out, "blocks": blocks}, ensure_ascii=False, indent=2))
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
