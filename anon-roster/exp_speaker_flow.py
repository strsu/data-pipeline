"""E1 — 흐름-only 화자배정 실측.

가설(사용자): "대사의 흐름을 보면 이 대사를 누가 했을법한지 추출 가능하지 않나?"
설계(redesign-flow-first-2026-07-22.md §4): 화자는 얼굴이 아니라 **익명 슬롯**에 귀속하고,
근거는 대사교대·화법(반말/존대)·호칭·POV = 순수 텍스트 흐름.

이 스크립트: ep10 수동분석(참조정답, 컷별 A/B/— 화자)을 파싱 → 화자 라벨을 떼고
**중립 시각 로스터 + 순서대로의 대사(type 포함)**만 모델에 주고, 각 블록의 화자를 흐름으로
배정시켜 참조정답과 대조한다. 이미지 없음(순수 흐름 검증). 모델 교체 가능(--model).

참조정답은 '강한 참조'지 검증된 정답이 아니다(prd-for-improve §4.9) — but 화자 흐름의
사람 기준 상한으로 쓴다.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

_PIPELINE = Path(__file__).resolve().parents[1] / "webtoon-pipeline"
sys.path.insert(0, str(_PIPELINE))
import httpx  # noqa: E402
from src.operators.llm_client import _resolve_api_key  # noqa: E402

REPO = Path(__file__).resolve().parents[1]

# 회차별 중립 시각 로스터 (register/POV 힌트 없음 — 흐름으로 추론하게)
ROSTER = {
    10: {
        "A": "근육질 남성. 짙은 갈색 뾰족머리, 파란 눈. 대형 망치 + 방패 + 배낭. 베이지 튜닉.",
        "B": "은발 장발. 금색 눈, 뾰족귀(엘프/요정). 활 + 화살통. 갈색 가죽옷.",
    },
}


def parse_manual(ep: int) -> list[dict]:
    """수동분석 md의 '컷별 기록' 표를 파싱. 반환: [{cut, text, type, spk_truth}] (읽기순)."""
    path = REPO / f"ep{ep}-manual-analysis-2026-07-16.md"
    rows: list[dict] = []
    in_table = False
    for line in path.read_text().splitlines():
        if line.startswith("| 컷 |"):
            in_table = True
            continue
        if in_table:
            if not line.startswith("|"):
                if rows:
                    break
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 4 or cells[0] in ("---", ""):
                continue
            cut_raw, text_raw, typ, spk = cells[0], cells[1], cells[2], cells[3]
            m = re.search(r"\d+", cut_raw)
            if not m:
                continue
            cut = int(m.group())
            # 전사 정제: **, ⭐, ← 주석 제거
            text = re.sub(r"\*\*|⭐", "", text_raw)
            text = re.sub(r"\s*←.*$", "", text).strip()
            rows.append({"cut": cut, "text": text, "type": typ, "spk_truth": spk})
    return rows


def build_input(blocks: list[dict], roster: dict) -> str:
    lines = []
    for i, b in enumerate(blocks):
        lines.append(f'{i}\t컷{b["cut"]}\t[{b["type"]}]\t{b["text"]}')
    roster_txt = "\n".join(f"  {k}: {v}" for k, v in roster.items())
    return roster_txt, "\n".join(lines)


SYSTEM = """너는 웹툰 회차의 대사 화자를 판정한다. 회차의 익명 인물 로스터(외형만 주어짐)와,
읽기 순서대로의 텍스트 블록(대사/독백/나레이션/효과음, 화자 미표기)을 받는다.
각 블록을 말한 인물을 로스터 기호(A/B/...) 중 하나로 배정하라. 근거는 오직 흐름이다:
- 대사 교대(질문↔대답의 리듬), 화법(반말 vs 존댓말), 호칭(누가 누구를 뭐라 부르나),
  POV(1인칭 나레이션·독백은 시점 인물), 문맥 연속.
효과음(other)이나 화자가 불분명하면 null. 억지 배정 금지 — 모르면 null.
반드시 JSON만 출력: {"assign": {"0":"A","1":"B","2":null, ...}}  (키는 블록 인덱스 문자열)"""


SYSTEM_CONSOLIDATE = """너는 웹툰 회차의 텍스트 블록만 보고(이미지 없음) ① 익명 인물 로스터를
구성하고 ② 각 블록의 화자를 배정한다. 로스터는 주어지지 않는다 — **대사 자체에서** 서로 다른
인물을 갈라내라. 단서: 화법(반말 vs 존댓말), 호칭(누가 누구를 '아저씨' 등으로 부르나), 대사 교대,
POV(1인칭 나레이션·독백의 시점 인물). 시각 정보가 없으니 **말하는 방식으로만** 인물을 구분한다.
군중/단역은 억지로 늘리지 말고, 효과음(other)은 화자 null. 모르면 null.
반드시 JSON만: {"roster": {"A":"화법/역할 서술", "B":"..."}, "assign": {"0":"A","1":null, ...}}"""


def call(model: str, system: str, user: str, no_think: bool = False) -> dict:
    """스트리밍 호출 — Cloudflare 터널 idle-timeout(524) 회피(파이프라인과 동일 패턴)."""
    host = os.environ["VLLM_API_HOST"].rstrip("/")
    ctx = {"provider": "vllm", "model_id": model, "params": {}}
    key = _resolve_api_key(ctx)
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "stream": True,
    }
    if no_think:
        body["chat_template_kwargs"] = {"enable_thinking": False}
    last_err = None
    for attempt in range(4):
        try:
            content = ""
            with httpx.Client(timeout=httpx.Timeout(600.0, connect=30.0)) as client:
                with client.stream("POST", f"{host}/v1/chat/completions", json=body,
                                   headers={"Authorization": f"Bearer {key}"}) as resp:
                    resp.raise_for_status()
                    for line in resp.iter_lines():
                        if not line or not line.startswith("data:"):
                            continue
                        data = line[5:].strip()
                        if data == "[DONE]":
                            break
                        try:
                            delta = json.loads(data)["choices"][0]["delta"]
                        except Exception:
                            continue
                        content += delta.get("content") or ""
            m = re.search(r"\{.*\}", content, re.DOTALL)
            return json.loads(m.group() if m else content)
        except Exception as e:  # noqa: BLE001
            last_err = e
            print(f"  attempt {attempt+1} failed: {type(e).__name__}: {str(e)[:80]}")
    raise last_err


def score(blocks: list[dict], assign: dict) -> dict:
    # 참조정답이 A 또는 B 인 블록만 채점(—, 고블린, 복합 제외)
    gold = [(i, b) for i, b in enumerate(blocks) if b["spk_truth"] in ("A", "B")]
    n = len(gold)
    correct = null_pred = wrong = 0
    confusion = {"A->B": 0, "B->A": 0}
    by_type = {}
    for i, b in gold:
        pred = assign.get(str(i))
        truth = b["spk_truth"]
        t = b["type"]
        by_type.setdefault(t, [0, 0])
        by_type[t][1] += 1
        if pred == truth:
            correct += 1
            by_type[t][0] += 1
        elif pred is None or pred == "null":
            null_pred += 1
        else:
            wrong += 1
            if truth == "A" and pred == "B":
                confusion["A->B"] += 1
            elif truth == "B" and pred == "A":
                confusion["B->A"] += 1
    return {
        "n_gold": n, "correct": correct, "wrong": wrong, "null_pred": null_pred,
        "accuracy": round(correct / n, 3) if n else 0,
        "acc_excl_null": round(correct / (correct + wrong), 3) if (correct + wrong) else 0,
        "confusion": confusion,
        "by_type": {k: f"{v[0]}/{v[1]}" for k, v in by_type.items()},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ep", type=int, default=10)
    ap.add_argument("--model", default="glm-5.2")
    ap.add_argument("--out", default=str(REPO / "datasets" / "speaker_flow"))
    ap.add_argument("--consolidate", action="store_true",
                    help="로스터 미제공 — 모델이 로스터 구성+화자배정(E1b)")
    args = ap.parse_args()

    blocks = parse_manual(args.ep)
    roster = ROSTER[args.ep]
    roster_txt, blocks_txt = build_input(blocks, roster)
    ngold = sum(1 for b in blocks if b['spk_truth'] in ('A', 'B'))
    print(f"ep{args.ep} blocks={len(blocks)} gold(A/B)={ngold} model={args.model} consolidate={args.consolidate}")

    if args.consolidate:
        user = f"[블록 (인덱스\\t컷\\t[type]\\t텍스트)]\n{blocks_txt}"
        out = call(args.model, SYSTEM_CONSOLIDATE, user)
        model_roster = out.get("roster", {})
        assign = out.get("assign", {})
        # 모델 슬롯 → gold(A/B) 매핑: 각 모델슬롯이 gold A/B 어느 쪽 블록을 더 많이 덮나
        overlap = {}
        for i, b in enumerate(blocks):
            if b["spk_truth"] not in ("A", "B"):
                continue
            ms = assign.get(str(i))
            if ms is None:
                continue
            overlap.setdefault(ms, {"A": 0, "B": 0})[b["spk_truth"]] += 1
        slot_map = {ms: max(v, key=v.get) for ms, v in overlap.items()}
        mapped = {k: slot_map.get(v) for k, v in assign.items()}
        res = score(blocks, mapped)
        res["model_roster_size"] = len(model_roster)
        res["slot_map"] = slot_map
        res["model_roster"] = model_roster
        assign = out
    else:
        user = f"[로스터]\n{roster_txt}\n\n[블록 (인덱스\\t컷\\t[type]\\t텍스트)]\n{blocks_txt}"
        assign = call(args.model, SYSTEM, user)
        res = score(blocks, assign["assign"] if "assign" in assign else assign)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    rec = {"ep": args.ep, "model": args.model, "score": res}
    tag = f"{args.model}_consolidate" if args.consolidate else args.model
    (out_dir / f"ep{args.ep}_{tag}.json").write_text(
        json.dumps({**rec, "assign": assign, "blocks": blocks}, ensure_ascii=False, indent=2))
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
