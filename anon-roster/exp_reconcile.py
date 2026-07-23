"""조정(reconcile) 스테이지 실험 — CCIP 오귀속이 서사를 오도하는 문제(박대석 케이스) 검증.

가설: 현행 R은 화자·이름·재배정(대상 필요)만 하고 '등장(presence) 재판정'과 '대상없는 얼굴 강등'이
없어 박대석 오귀속을 못 고친다. 강화된 계약(presence/status + demote)을 주면 대사 증거로 박대석을
사망/부재 판정하고 오귀속 얼굴을 강등할 수 있는가?

입력: w43 ep2 실제 트랜스크립트(대사+type+화자) + 얼굴원장(인물별 등장컷+CCIP점수). 프로덕션 R이
보는 것과 동일 + 점수 노출. 코드 수정/DB 쓰기 없음(읽기전용 실험).

    cd webtoon-pipeline && set -a && source ../prod.env && set +a && \
    PYTHONPATH=. .venv/bin/python ../anon-roster/exp_reconcile.py [--model glm-5.2]
"""
import os, sys, json, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "webtoon-pipeline"))
import psycopg2
from src.operators.llm_client import call_llm_json
from src.core.step3 import _pass2_ctx
from src.operators.llm_resolver import resolve_llm_model, TEXT


def conn():
    return psycopg2.connect(host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
                            dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
                            password=os.environ["POSTGRES_PASSWORD"])


def load(wid, no):
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id FROM webtoon_episode WHERE webtoon_id=%s AND no=%s", (wid, no))
    eid = cur.fetchone()[0]
    # 트랜스크립트: 컷·index 순서, type·화자명·텍스트
    cur.execute("""
        SELECT wc.cut_number, tr.index, ta.type, ch.name, ta.text
        FROM analysis_text_annotation ta
        JOIN analysis_text_region tr ON tr.id=ta.region_id
        JOIN webtoon_cut wc ON wc.id=tr.cut_id
        LEFT JOIN analysis_character ch ON ch.id=ta.speaker_id
        WHERE wc.episode_id=%s AND tr.is_excluded=false AND COALESCE(ta.text,'')<>''
        ORDER BY wc.cut_number, tr.index""", (eid,))
    transcript = [{"cut": cn, "type": tp, "speaker": sp, "text": tx}
                  for cn, ix, tp, sp, tx in cur.fetchall()]
    # 얼굴 원장: character_id별 등장 컷 + 점수(named 우선)
    cur.execute("""
        SELECT ch.id, ch.name, wc.cut_number, round(fi.score::numeric,3)
        FROM analysis_face_identity fi
        JOIN analysis_face_detection d ON d.id=fi.detection_id
        JOIN analysis_character_appearance a ON a.id=fi.appearance_id
        JOIN analysis_character ch ON ch.id=a.character_id
        JOIN webtoon_cut wc ON wc.id=d.cut_id
        WHERE wc.episode_id=%s AND fi.source='step2'
        ORDER BY ch.id, wc.cut_number""", (eid,))
    faces = {}
    for cid, nm, cn, sc in cur.fetchall():
        e = faces.setdefault(cid, {"cid": cid, "name": nm or "", "cuts": [], "scores": []})
        e["cuts"].append(cn)
        if sc is not None:
            e["scores"].append(float(sc))
    c.close()
    return eid, transcript, list(faces.values())


_SYS = (
    "당신은 웹툰 에피소드 분석의 **최종 정체 조정기(reconcile)**다. 컷별 추출/CCIP 얼굴인식이 끝난 뒤,"
    " 회차 전체 증거로 각 인물의 **실제 등장 여부와 생사/상태를 재판정**하고 오귀속 얼굴을 바로잡는다."
    " 모든 자연어는 한국어. JSON만 출력.\n"
    "\n입력:\n"
    "- transcript: [{cut, type, speaker(명명된 화자 또는 null), text}] — 회차 대사/나레이션 전체(읽기순)\n"
    "- face_ledger: [{cid, name, cuts(얼굴 탐지된 컷들), scores(CCIP 매칭점수)}] — CCIP가 각 인물에 붙인 얼굴\n"
    "\n⚠️ 핵심 규칙:\n"
    "1) **CCIP 점수는 전 인물이 낮다(대략 0.02~0.12) — 점수 크기로 진위를 구분하지 마라.** 진짜 등장인지"
    " 오귀속인지는 **대사·호칭·맥락(transcript)**으로만 판정한다.\n"
    "2) 각 인물의 **present(이 회차에 실제 등장하는가)** 와 **status(alive|dead|absent|unknown)** 를"
    " 증거 강도로 판정한다. **명시적 대사 증거(예 '죽었다','안 보이네요','투신')가 저신뢰 얼굴 등장보다"
    " 절대적으로 우선**한다.\n"
    "3) **서사와 모순되는 얼굴**(대사상 부재/사망인데 CCIP가 그 인물 얼굴을 컷에 붙임) = **오귀속(mis-ID)**"
    " 으로 판정하고 demote=true. 오귀속된 얼굴이 누구인지 몰라도 된다(그냥 '이 인물 것이 아님'만 판정).\n"
    "4) 없는 정보 지어내지 말 것.\n"
    "\n출력 JSON:\n"
    '{\n'
    '  "characters": [{"cid": int, "name": str, "present": bool, "status": "alive|dead|absent|unknown",'
    ' "evidence": "판정 근거(대사 인용)", "demote_faces": bool, "demote_reason": str}],\n'
    '  "summary": "정정된 회차 요약(오귀속 반영 — 실제 등장/사망/부재를 정확히)"\n'
    '}'
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--webtoon", type=int, default=43)
    ap.add_argument("--no", type=int, default=2)
    ap.add_argument("--model", default=None)  # None이면 프로덕션 TEXT 기본(glm-5.2)
    args = ap.parse_args()

    eid, transcript, faces = load(args.webtoon, args.no)
    # named 인물 우선 정렬 + 무명 요약(입력 압축)
    named = [f for f in faces if f["name"]]
    anon = [f for f in faces if not f["name"]]
    ledger = []
    for f in named:
        sc = f["scores"]
        ledger.append({"cid": f["cid"], "name": f["name"], "cuts": sorted(set(f["cuts"])),
                       "score_min": round(min(sc), 3) if sc else None, "score_max": round(max(sc), 3) if sc else None})
    payload = {"transcript": transcript,
               "face_ledger_named": ledger,
               "face_ledger_anon_count": len(anon)}
    user = json.dumps(payload, ensure_ascii=False)
    print(f"[입력] transcript {len(transcript)}블록, named 인물 {len(named)}, 무명 {len(anon)}, user_text {len(user)}자")

    ctx = resolve_llm_model(args.webtoon, TEXT)
    if args.model:
        ctx = dict(ctx); ctx["model_id"] = args.model; ctx["name"] = args.model
    call = call_llm_json(_pass2_ctx(ctx), _SYS, user, [])
    out = call.result

    print(f"\n[모델] {ctx['model_id']}  finish={call.usage.get('finish_reason')}\n")
    print("=== 인물 재판정 (named) ===")
    for ch in out.get("characters", []):
        if ch.get("name"):
            print(f"  {ch['name']}(cid{ch.get('cid')}): present={ch.get('present')} status={ch.get('status')}"
                  f" demote={ch.get('demote_faces')}")
            print(f"     근거: {ch.get('evidence','')[:120]}")
            if ch.get("demote_faces"):
                print(f"     강등사유: {ch.get('demote_reason','')[:120]}")
    print("\n=== 정정 요약 ===")
    print(out.get("summary", "(없음)"))
    # 저장
    Path(__file__).with_name(f"reconcile_w{args.webtoon}_e{args.no}.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
