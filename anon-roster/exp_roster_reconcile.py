"""로스터-강화 reconcile 실험 — 프로덕션 로스터의 정확한 입력(payload+faces)에 강화 계약을 넣어
misid_character_ids(오귀속 인물 cid)를 뽑는지 검증. 이걸 통과하면 그 계약을 _ROSTER_SYSTEM_PROMPT에
반영한다. 읽기전용(DB 쓰기 없음).

    cd webtoon-pipeline && set -a && source ../prod.env && set +a && \
    PYTHONPATH=. .venv/bin/python ../anon-roster/exp_roster_reconcile.py --webtoon 43 --no 2 [--model qwen-vl-fp8]
"""
import os, sys, json, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "webtoon-pipeline"))
from src.core.step3 import (
    _load_pass1_records_from_db, _build_pass2_user_payload, _ROSTER_SYSTEM_PROMPT, _pass2_ctx,
)
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import resolve_llm_model, TEXT
import psycopg2


def eid_of(wid, no):
    c = psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
                         dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
                         password=os.environ["POSTGRES_PASSWORD"])
    cur = c.cursor(); cur.execute("SELECT id FROM webtoon_episode WHERE webtoon_id=%s AND no=%s", (wid, no))
    e = cur.fetchone()[0]; c.close(); return e


# 강화 계약: 현행 로스터 프롬프트 + reconcile 절 + misid 출력. (프로덕션 반영 후보 원문)
_RECONCILE_CLAUSES = (
    "\n\n[정체 조정(reconcile) — 추가 규칙]\n"
    "- **faces는 CCIP 얼굴인식 추측이다. confirmed=false는 저신뢰(점수 낮음)이며 정답이 아니다.**"
    " 진짜 등장인지 오귀속인지는 **대사·호칭·맥락으로만** 판정한다. 얼굴이 컷에 붙어 있다는 사실만으로"
    " present_now=true로 하지 마라.\n"
    "- **대사 증거가 얼굴보다 절대 우선**한다. 대사·나레이션이 어떤 인물의 **사망/부재**를 명시하는데"
    " (예 '죽었다','투신','안 보이네요','참수') CCIP가 그 인물(character_id)에 얼굴을 붙였다면,"
    " 그 얼굴들은 **오귀속(mis-ID)**이다 → 그 인물은 present_now=false로 하고, 그 character_id를"
    " **misid_character_ids**에 넣어라(그 얼굴이 실제 누구인지는 몰라도 된다).\n"
    "- 단 confirmed=true(human 확정) 얼굴은 동결 — 절대 misid에 넣지 마라.\n"
    "- 출력 JSON에 최상위로 **\"misid_character_ids\": [정수 cid, ...]** 를 추가하라(없으면 빈 배열)."
    " roster 항목에는 present_now·status를 대사 근거로 정확히 채워라."
)
_SYS = _ROSTER_SYSTEM_PROMPT + _RECONCILE_CLAUSES


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--webtoon", type=int, default=43); ap.add_argument("--no", type=int, default=2)
    ap.add_argument("--model", default=None)
    args = ap.parse_args()

    eid = eid_of(args.webtoon, args.no)
    records = _load_pass1_records_from_db(eid)
    payload = _build_pass2_user_payload(records, None)
    user = json.dumps(payload, ensure_ascii=False)
    print(f"[입력] pass1 records {len(records)}, payload {len(user)}자 (프로덕션 로스터와 동일 입력)")

    ctx = resolve_llm_model(args.webtoon, TEXT)
    if args.model:
        ctx = dict(ctx); ctx["model_id"] = args.model; ctx["name"] = args.model
    call = call_llm_json(_pass2_ctx(ctx), _SYS, user, [])
    out = call.result
    print(f"[모델] {ctx['model_id']} finish={call.usage.get('finish_reason')}\n")

    print("=== 로스터 (present_now/status) — named 위주 ===")
    for r in out.get("roster", []):
        nm = r.get("name", "")
        if nm and any(x in nm for x in ["대석", "화진", "학재", "경민", "준형", "병수"]) or not r.get("present_now"):
            print(f"  {nm}: present={r.get('present_now')} status={r.get('status','')[:40]}")
    print(f"\n=== misid_character_ids (오귀속 강등 대상 cid) ===\n  {out.get('misid_character_ids')}")

    # 정답 대조: 박대석 cid57이 misid에 있나
    if args.webtoon == 43 and args.no == 2:
        misid = out.get("misid_character_ids") or []
        print(f"\n[채점] 박대석(cid57) misid 포함: {'✅ YES' if 57 in misid else '❌ NO'}")
    Path(__file__).with_name(f"roster_reconcile_w{args.webtoon}_e{args.no}.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
