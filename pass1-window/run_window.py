"""Pass-1 슬라이딩 윈도우 실험 — 앞뒤 컷을 CONTEXT로 넣고 중앙 컷만 예측(center-predict).

가설: qwen-base(9B)의 약점인 화자 귀속·인물 동일성은 **앞뒤 컷 맥락**이 있으면 개선된다.
(구조 결함 — 블록 1:1 병합/누락 — 은 모델 용량 문제라 개선 기대 없음. 악화 여부만 감시.)

설계:
- 입력은 DB가 아니라 `datasets/pass1/<hash>/w{wid}_e{no}.jsonl`을 재활용한다. 그 파일에 컷별
  오버레이 이미지(images/*.jpg)·프로덕션 입력(user_text)·glm 티처 결과·qwen 싱글 결과가 전부
  들어있어, 입력 파리티가 자동 보장되고 베이스라인 콜을 다시 할 필요가 없다.
- 윈도우는 유효(분석) 컷 시퀀스 위에서 잡는다. 타겟 컷 T에 대해 [T-R … T … T+R](R=--radius,
  기본 2 → 5장). 가장자리는 클리핑(창이 작아짐 — 타겟을 항상 실제 이웃 사이에 둔다).
- 출력 스키마는 프로덕션 Pass-1과 **완전 동일**(달라지는 건 입력 프롬프트뿐) → 기존
  `_sanitize_pass1`/`_structural_metrics`/`_agreement_metrics`를 그대로 재사용해 비교한다.
- 이미지 사이에 `[컷 N] (CONTEXT|TARGET)` 텍스트 마커를 끼워 넣어야 해서(이미지 혼동 방어)
  `call_llm_json`(텍스트 1개 + 이미지 몰아넣기)을 못 쓴다 → `_call_direct`로 직접 호출.

실행:
  cd webtoon-pipeline && set -a && source ../prod.env && set +a && \
  PYTHONPATH=. .venv/bin/python ../pass1-window/run_window.py --webtoon 23 --episodes 10
스모크(3컷):
  ... ../pass1-window/run_window.py --webtoon 23 --episodes 10 --limit 3
집계만:
  ... ../pass1-window/run_window.py --webtoon 23 --episodes 10 --report
"""
from __future__ import annotations

import os

# llm_client가 import 시점에 세마포어를 만든다 — import 전에 설정(로컬 9B라 낮게).
os.environ.setdefault("LLM_MAX_CONCURRENCY", "2")

import argparse
import hashlib
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_PIPELINE = Path(__file__).resolve().parents[1] / "webtoon-pipeline"
if str(_PIPELINE) not in sys.path:
    sys.path.insert(0, str(_PIPELINE))

from src.config.s3 import fetch_cut_image  # noqa: E402
from src.core.step3 import (  # noqa: E402
    _PASS1_SYSTEM_PROMPT,
    _episode_info,
    _pass1_ctx,
    _sanitize_pass1,
    build_pass1_input,
)
from src.operators.llm_client import (  # noqa: E402
    _data_url,
    _get_client,
    _parse_json_content_ex,
    _resolve_api_key,
    _resolve_endpoint,
)
from tools.pass1_bench import _agreement_metrics, _structural_metrics  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pass1_window")

SCHEMA_VERSION = 1
DEFAULT_SOURCE_HASH = "78c51d18a1d9"


# ── 윈도우 프롬프트 ───────────────────────────────────────────────────────────
# 프로덕션 프롬프트의 첫 문장("현재 컷 이미지 1장")만 교체하고, 나머지 규칙(1:1 바인딩, 분류→화자
# 2단계, 스키마)은 **글자 그대로 유지**한다 — 출력 계약이 같아야 싱글 이미지와 비교가 성립한다.
_PROD_HEADER = (
    "당신은 웹툰 컷 분석기입니다. 입력: 현재 컷 이미지(얼굴 bbox에 F0/F1 라벨 오버레이), "
    "identified_faces(F라벨+알려진 이름+confirmed), ocr_blocks(index+text). 현재 컷만 분석해 **JSON만** 출력.\n"
)

_WINDOW_HEADER = (
    "당신은 웹툰 컷 분석기입니다. 입력: **연속된 컷 이미지 여러 장**(웹툰 읽기 순서대로, 각 이미지 "
    "바로 앞에 '[컷 N] (CONTEXT|TARGET)' 표시, 얼굴 bbox에 F0/F1 라벨 오버레이). 그중 **정확히 한 장이 "
    "분석 대상(TARGET)**이고 나머지는 앞뒤 맥락(CONTEXT)입니다. identified_faces와 ocr_blocks는 "
    "**TARGET 컷의 것만** 주어집니다. **TARGET 컷만 분석해 JSON만** 출력.\n"
)

_WINDOW_RULES = (
    "\n[윈도우 규칙]\n"
    "W1) 출력은 오직 TARGET 컷에 대한 것이다. CONTEXT 컷의 대사·인물·사물을 TARGET 출력에 넣지 마라. "
    "blocks는 **TARGET의 ocr_blocks index와만** 1:1이다 — CONTEXT 컷에 보이는 글자는 절대 blocks에 "
    "추가하지 않는다(규칙 1의 1:1은 TARGET 기준으로 그대로 적용).\n"
    "W2) CONTEXT는 **판단 근거로만** 쓴다: 대화 흐름(직전 컷에서 말하던 인물이 이어 말하는가, 질문→대답 "
    "교대), 인물 동일성(같은 인물이 앞뒤 컷에 등장), 장면 연속성. TARGET에서 말풍선 꼬리가 애매하거나 "
    "화자 얼굴이 안 보일 때 앞뒤 컷의 대화 순서로 화자를 추론하라 — 그렇게 정한 화자는 basis=context.\n"
    "W3) F 라벨은 **TARGET 컷 오버레이 기준**이다. CONTEXT 컷에도 F0/F1이 보이지만 그건 그 컷의 번호이고 "
    "같은 인물이라는 보장이 없다 — CONTEXT의 라벨 숫자를 그대로 옮기지 말고, 얼굴 생김새로 대응시켜라. "
    "출력 face_label은 반드시 TARGET의 identified_faces에 있는 라벨만 쓴다.\n"
    "W4) cut_summary/key_objects/characters는 전부 TARGET 컷 기준이다(CONTEXT에만 나온 인물/사물 금지). "
    "CONTEXT 대사에서 알게 된 이름은 name_evidence에 쓸 수 있으며, evidence에 어느 컷 근거인지 밝힌다.\n"
)


def _window_system_prompt() -> str:
    if _PROD_HEADER not in _PASS1_SYSTEM_PROMPT:
        raise SystemExit(
            "프로덕션 Pass-1 프롬프트 헤더가 바뀌었다 — run_window.py의 _PROD_HEADER를 맞춰야 함")
    return _PASS1_SYSTEM_PROMPT.replace(_PROD_HEADER, _WINDOW_HEADER) + _WINDOW_RULES


def _prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:12]


# ── 소스 재활용 ───────────────────────────────────────────────────────────────

def _load_source(src_dir: Path, webtoon_id: int, episode_no: int,
                 include_empty: bool = False) -> list[dict]:
    """pass1 벤치 JSONL을 cut_number 순으로.

    include_empty=False면 유효 컷만(창이 '분석된 컷'의 이웃으로 잡힘 — 스킵 컷 자리에 구멍).
    True면 skipped 컷도 시퀀스에 남긴다(창이 실제 cut_number 연속이 됨). 스킵 컷은 이미지가
    벤치 산출물에 없으므로 `_ensure_images`가 S3에서 받아 같은 전처리로 캐시한다.
    """
    path = src_dir / f"w{webtoon_id}_e{episode_no}.jsonl"
    if not path.exists():
        raise SystemExit(f"소스 없음: {path} — 먼저 tools/pass1_bench.py로 생성해야 함")
    rows = []
    for line in path.read_text().splitlines():
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("skipped") and not include_empty:
            continue
        rows.append(r)
    rows.sort(key=lambda r: r["cut_number"])
    return rows


def _ensure_images(rows: list[dict], src_dir: Path, ctx_dir: Path) -> dict[int, Path]:
    """cut_number → 이미지 경로. 스킵 컷(벤치가 이미지를 안 남긴 것)은 S3에서 받아 캐시.

    전처리는 프로덕션과 동일 경로(`build_pass1_input`)를 타서 얼굴/OCR 없는 오버레이 + 1280
    다운스케일이 된다 — 분석 컷 이미지와 같은 규격이어야 컨텍스트로서 공정하다.
    받지 못한 컷(S3 부재 등)은 dict에서 빠지고, 호출부가 시퀀스에서 제외한다.
    """
    out: dict[int, Path] = {}
    need: list[dict] = []
    for r in rows:
        if not r.get("skipped"):
            out[r["cut_number"]] = src_dir / r["image_file"]
            continue
        p = ctx_dir / f"w{r['webtoon_id']}_e{r['episode_no']}_c{r['cut_number']}.jpg"
        if p.exists():
            out[r["cut_number"]] = p
        else:
            need.append(r)
    if not need:
        return out

    ctx_dir.mkdir(parents=True, exist_ok=True)
    info = _episode_info(need[0]["episode_id"])

    def _fetch(r: dict) -> tuple[int, Path | None]:
        img = fetch_cut_image(info["source"], info["title_id"], info["episode_no"], r["cut_number"])
        if img is None:
            return r["cut_number"], None
        overlay, _ = build_pass1_input(img, [], [])  # 얼굴·OCR 없음 → 오버레이 없는 다운스케일본
        p = ctx_dir / f"w{r['webtoon_id']}_e{r['episode_no']}_c{r['cut_number']}.jpg"
        p.write_bytes(overlay)
        return r["cut_number"], p

    logger.info("빈 컷 이미지 %d개 S3에서 수신 중…", len(need))
    with ThreadPoolExecutor(max_workers=4) as pool:
        for cn, p in pool.map(_fetch, need):
            if p is None:
                logger.warning("컷 %s 이미지 없음 — 컨텍스트에서 제외", cn)
                continue
            out[cn] = p
    return out


def _regions_of(row: dict) -> list[dict]:
    """벤치가 저장한 user_text(프로덕션 입력)에서 ocr_blocks 복원 — 구조/sanitize 비교의 기준."""
    ut = json.loads(row["input"]["user_text"])
    return [{"index": b["index"], "text": b.get("text", "")} for b in ut.get("ocr_blocks", [])]


def _faces_of(row: dict) -> list[dict]:
    return json.loads(row["input"]["user_text"]).get("identified_faces", [])


def _context_digest(row: dict) -> dict:
    """CONTEXT 컷 요약 — F 라벨은 **일부러 뺀다**(타겟 라벨과 섞이면 오염, W3 참고).

    이름은 사람 확정/추정을 구분해서 넘긴다(대화 흐름 추론용). 텍스트는 index 없는 평문 —
    모델이 blocks에 끌어다 쓰지 않도록 프로덕션 ocr_blocks 포맷과 의도적으로 다르게 준다.
    skipped(빈) 컷은 OCR·얼굴이 애초에 없으니 이미지만 보라고 명시한다.
    """
    if row.get("skipped"):
        return {"cut": row["cut_number"], "known_people": [], "texts": [],
                "note": "대사·인물 없는 컷(배경/전환) — 이미지만 참고"}
    ut = json.loads(row["input"]["user_text"])
    people = []
    for f in ut.get("identified_faces", []):
        name = f.get("name")
        if not name:
            continue
        people.append(f"{name}({'확정' if f.get('confirmed') else '추정'})")
    return {
        "cut": row["cut_number"],
        "known_people": people,
        "texts": [b.get("text", "") for b in ut.get("ocr_blocks", [])],
    }


def _build_window_input(rows: list[dict], i: int, radius: int,
                        img_paths: dict[int, Path]) -> dict:
    """타겟 i에 대한 윈도우 입력 — user_text JSON + (마커, 이미지경로) 순서열.

    이미지를 못 구한 컷은 창에서 빠진다(마커만 남기면 모델이 없는 이미지를 찾는다).
    """
    lo, hi = max(0, i - radius), min(len(rows) - 1, i + radius)
    members = [j for j in range(lo, hi + 1) if rows[j]["cut_number"] in img_paths]
    target = rows[i]
    ut = json.loads(target["input"]["user_text"])

    user = {
        "target_cut": target["cut_number"],
        "context_cuts": [_context_digest(rows[j]) for j in members if j != i],
        "identified_faces": ut.get("identified_faces", []),  # TARGET only
        "ocr_blocks": ut.get("ocr_blocks", []),              # TARGET only
    }
    parts = []
    for j in members:
        tag = "TARGET — 이 컷을 분석" if j == i else "CONTEXT — 참고만"
        parts.append({
            "marker": f"[컷 {rows[j]['cut_number']}] ({tag})",
            "image_path": str(img_paths[rows[j]["cut_number"]]),
            "cut_number": rows[j]["cut_number"],
            "is_target": j == i,
            "is_empty": bool(rows[j].get("skipped")),
        })
    return {
        "user_text": json.dumps(user, ensure_ascii=False),
        "parts": parts,
        "window_cuts": [rows[j]["cut_number"] for j in members],
        "context_empty_cuts": [rows[j]["cut_number"] for j in members
                               if j != i and rows[j].get("skipped")],
        "target_position": members.index(i) + 1,  # 1-based, 위치 편향 분석용
        "window_size": len(members),
    }


# ── 호출 ──────────────────────────────────────────────────────────────────────

def _call_direct(ctx: dict, system: str, user_text: str, parts: list[dict]) -> dict:
    """텍스트 마커와 이미지를 **번갈아** 실은 멀티이미지 콜.

    `call_llm_json`은 (텍스트 1개 + 이미지 전부 뒤에 몰기) 구조라 어느 이미지가 몇 번 컷인지
    표시할 자리가 없다 → 멀티이미지에서 컷 혼동이 난다. 그래서 직접 호출한다.
    """
    endpoint = _resolve_endpoint(ctx)
    headers = {"Authorization": f"Bearer {_resolve_api_key(ctx)}"}
    content: list[dict] = [{"type": "text", "text": user_text}]
    for p in parts:
        content.append({"type": "text", "text": p["marker"]})
        img = Path(p["image_path"]).read_bytes()
        content.append({"type": "image_url", "image_url": {"url": _data_url(img)}})

    body = {
        "model": ctx["model_id"],
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": content}],
        "temperature": (ctx.get("params") or {}).get("temperature", 0.0),
        "response_format": {"type": "json_object"},
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    chunks: list[str] = []
    usage: dict = {}
    finish: str | None = None
    with _get_client().stream("POST", endpoint, json=body, headers=headers) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if not line:
                continue
            if line.startswith("data:"):
                line = line[5:].strip()
            if line == "[DONE]":
                break
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("usage"):
                usage = obj["usage"]
            for ch in (obj.get("choices") or []):
                d = ch.get("delta") or {}
                if isinstance(d.get("content"), str):
                    chunks.append(d["content"])
                if ch.get("finish_reason"):
                    finish = ch["finish_reason"]
    text = "".join(chunks).strip()
    parsed, repaired = _parse_json_content_ex(text)
    return {
        "raw": parsed if isinstance(parsed, dict) else {},
        "raw_text": text,
        "repaired": bool(repaired),
        "usage": {
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
            "total_tokens": usage.get("total_tokens"),
            "finish_reason": finish,
        },
    }


def _run_one(ctx: dict, system: str, win: dict) -> dict:
    t0 = time.monotonic()
    try:
        res = _call_direct(ctx, system, win["user_text"], win["parts"])
        res["latency_s"] = round(time.monotonic() - t0, 2)
        res["error"] = None
        return res
    except Exception as e:  # noqa: BLE001 — 컷 단위 격리
        return {"raw": {}, "raw_text": "", "repaired": False, "usage": {},
                "latency_s": round(time.monotonic() - t0, 2), "error": str(e)}


# ── 에피소드 실행 ─────────────────────────────────────────────────────────────

def _done_cuts(path: Path) -> set[int]:
    done: set[int] = set()
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                done.add(json.loads(line)["cut_number"])
            except (json.JSONDecodeError, KeyError):
                continue
    return done


def run_episode(webtoon_id: int, episode_no: int, src_dir: Path, out_dir: Path,
                ctx: dict, system: str, radius: int, workers: int,
                limit: int | None = None, include_empty: bool = True) -> None:
    rows = _load_source(src_dir, webtoon_id, episode_no, include_empty=include_empty)
    img_paths = _ensure_images(rows, src_dir, out_dir / "ctx_images")
    out_path = out_dir / f"w{webtoon_id}_e{episode_no}.jsonl"
    done = _done_cuts(out_path)

    # 타겟은 **분석 컷만**(빈 컷은 예측할 blocks·faces가 없고 싱글 베이스라인도 없다).
    # 빈 컷은 시퀀스에 남아 CONTEXT 이미지로만 들어간다.
    targets = [i for i, r in enumerate(rows)
               if not r.get("skipped") and r["cut_number"] not in done]
    if limit:
        targets = targets[:limit]
    n_empty = sum(1 for r in rows if r.get("skipped"))
    logger.info("w%s ep%s — 시퀀스 %d컷(빈 컷 %d 포함), 타겟 %d (기존 %d)",
                webtoon_id, episode_no, len(rows), n_empty, len(targets), len(done))

    # 진행 상황을 뷰어가 바로 보도록 완료되는 즉시 flush(순서 무관하게 append).
    with out_path.open("a") as fh, ThreadPoolExecutor(max_workers=workers) as pool:
        futs = []
        for i in targets:
            win = _build_window_input(rows, i, radius, img_paths)
            futs.append((i, win, pool.submit(_run_one, ctx, system, win)))

        for n, (i, win, fut) in enumerate(futs, 1):
            row = rows[i]
            res = fut.result()
            regions = _regions_of(row)
            san = _sanitize_pass1(res["raw"], regions) if not res["error"] else {}
            rec = {
                "schema_version": SCHEMA_VERSION,
                "webtoon_id": webtoon_id, "episode_no": episode_no,
                "cut_id": row["cut_id"], "cut_number": row["cut_number"],
                "image_file": row["image_file"],
                "window": {
                    "cuts": win["window_cuts"],
                    "size": win["window_size"],
                    "radius": radius,
                    "target_position": win["target_position"],
                    "context_empty_cuts": win["context_empty_cuts"],
                },
                "input": {
                    "user_text": win["user_text"],
                    "n_regions": len(regions),
                    "n_faces": len(_faces_of(row)),
                },
                "window_call": {
                    "model": ctx["model_id"],
                    "raw": res["raw"], "raw_text": res["raw_text"],
                    "repaired": res["repaired"], "usage": res["usage"],
                    "latency_s": res["latency_s"], "error": res["error"],
                    "sanitized": san,
                    "structural": _structural_metrics(res["raw"], regions) if not res["error"] else None,
                },
            }
            # 티처(glm) 대비 일치 — 싱글 이미지 baseline은 소스 JSONL의 student가 이미 갖고 있다.
            t = row.get("teacher") or {}
            if not res["error"] and not t.get("error") and t.get("sanitized"):
                rec["agreement_vs_teacher"] = _agreement_metrics(t["sanitized"], san)
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fh.flush()
            if n % 5 == 0 or n == len(futs):
                logger.info("w%s ep%s — %d/%d", webtoon_id, episode_no, n, len(futs))


# ── 리포트 ────────────────────────────────────────────────────────────────────

def _rate(n: int, d: int) -> float | None:
    return round(n / d, 4) if d else None


def report(src_dir: Path, out_dir: Path, webtoon_id: int, episode_nos: list[int]) -> dict:
    out: dict = {"episodes": episode_nos, "arms": {}}
    win_rows: list[dict] = []
    for no in episode_nos:
        p = out_dir / f"w{webtoon_id}_e{no}.jsonl"
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            try:
                win_rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    win_by_cut = {(r["episode_no"], r["cut_number"]): r for r in win_rows}

    src_rows: list[dict] = []
    for no in episode_nos:
        src_rows.extend(_load_source(src_dir, webtoon_id, no))  # 분석 컷만 — 비교 대상

    # 윈도우가 처리한 컷만 비교(진행 중에도 공정한 대조).
    src_rows = [r for r in src_rows if (r["episode_no"], r["cut_number"]) in win_by_cut]

    def _arm(name: str, entries: list[dict], ags: list[dict]) -> dict:
        ok = [e for e in entries if not e.get("error")]
        st = [e["structural"] for e in ok if e.get("structural")]
        d = {
            "calls_ok": len(ok), "calls_error": len(entries) - len(ok),
            "repaired": sum(1 for e in ok if e.get("repaired")),
            "finish_not_stop": sum(1 for e in ok
                                   if (e.get("usage") or {}).get("finish_reason") not in (None, "stop")),
            "cuts_with_merge": sum(1 for s in st if s["merges"]),
            "cuts_with_missing_idx": sum(1 for s in st if s["missing_indices"]),
            "cuts_with_extra_idx": sum(1 for s in st if s["extra_indices"]),
            "merge_rate": _rate(sum(1 for s in st if s["merges"]), len(st)),
            "missing_rate": _rate(sum(1 for s in st if s["missing_indices"]), len(st)),
            "avg_latency_s": round(sum(e["latency_s"] for e in ok) / len(ok), 1) if ok else None,
            "avg_prompt_tokens": round(sum((e.get("usage") or {}).get("prompt_tokens") or 0
                                           for e in ok) / len(ok)) if ok else None,
        }
        if ags:
            sp = sum(a["speaker_total"] for a in ags)
            sa = sum(a["speaker_agree"] for a in ags)
            sap = sum(a["speaker_assigned_total"] for a in ags)
            saa = sum(a["speaker_assigned_agree"] for a in ags)
            tp = sum(a["type_pairs"] for a in ags)
            ta = sum(a["type_agree"] for a in ags)
            jac = [a["char_jaccard"] for a in ags if a["char_jaccard"] is not None]
            d["vs_teacher"] = {
                "cuts_compared": len(ags),
                "type_agreement": _rate(ta, tp),
                "speaker_agreement_all": _rate(sa, sp),
                "speaker_agreement_assigned": _rate(saa, sap),
                "char_jaccard_mean": round(sum(jac) / len(jac), 4) if jac else None,
            }
        out["arms"][name] = d
        return d

    _arm("single (qwen-base, 소스 재활용)",
         [r["student"] for r in src_rows if r.get("student")],
         [r["agreement"] for r in src_rows if r.get("agreement")])
    _arm("window (qwen-base, center-predict)",
         [r["window_call"] for r in win_rows],
         [r["agreement_vs_teacher"] for r in win_rows if r.get("agreement_vs_teacher")])
    _arm("teacher (glm-4.6v, 참고)",
         [r["teacher"] for r in src_rows if r.get("teacher") and not r["teacher"].get("error")],
         [])

    # 위치 편향 — 가장자리(창이 작거나 타겟이 중앙이 아닌 경우) 확인용.
    by_pos: dict = {}
    for r in win_rows:
        wc = r["window_call"]
        if wc.get("error") or not wc.get("structural"):
            continue
        key = f"{r['window']['target_position']}/{r['window']['size']}"
        b = by_pos.setdefault(key, {"n": 0, "merge": 0, "missing": 0})
        b["n"] += 1
        b["merge"] += 1 if wc["structural"]["merges"] else 0
        b["missing"] += 1 if wc["structural"]["missing_indices"] else 0
    out["by_target_position"] = by_pos
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Pass-1 슬라이딩 윈도우(center-predict) 실험")
    ap.add_argument("--webtoon", type=int, default=23)
    ap.add_argument("--episodes", default="10", help="예: 10 또는 10-12 또는 1,3")
    ap.add_argument("--model", default="qwen-base")
    ap.add_argument("--radius", type=int, default=2, help="앞뒤 컨텍스트 컷 수(2 → 창 5장)")
    ap.add_argument("--include-empty", action=argparse.BooleanOptionalAction, default=True,
                    help="OCR·얼굴 없는 빈 컷도 CONTEXT 이미지로 포함(창이 cut_number 연속). "
                         "--no-include-empty면 분석 컷 이웃으로만 창을 잡는다(구멍 생김)")
    ap.add_argument("--workers", type=int, default=1, help="동시 콜(로컬 9B — 기본 1)")
    ap.add_argument("--source-hash", default=DEFAULT_SOURCE_HASH)
    ap.add_argument("--datasets", default=str(Path(__file__).resolve().parents[1] / "datasets"))
    ap.add_argument("--limit", type=int, default=None, help="에피소드당 컷 제한(스모크)")
    ap.add_argument("--report", action="store_true", help="집계만(콜 없음)")
    args = ap.parse_args()

    if "-" in args.episodes:
        lo, hi = args.episodes.split("-")
        episode_nos = list(range(int(lo), int(hi) + 1))
    else:
        episode_nos = [int(x) for x in args.episodes.split(",")]

    datasets = Path(args.datasets)
    src_dir = datasets / "pass1" / args.source_hash
    system = _window_system_prompt()
    # 빈 컷 포함 여부는 창 구성 자체를 바꾼다 → 디렉터리를 분리해야 기록이 섞이지 않는다.
    tag = f"r{args.radius}{'_full' if args.include_empty else ''}_{_prompt_hash(system)}"
    out_dir = datasets / "pass1_window" / tag
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.report:
        print(json.dumps(report(src_dir, out_dir, args.webtoon, episode_nos),
                         ensure_ascii=False, indent=2))
        return

    meta = out_dir / "meta.json"
    if not meta.exists():
        meta.write_text(json.dumps({
            "system_prompt": system, "prompt_hash": _prompt_hash(system),
            "model": args.model, "radius": args.radius, "temperature": 0.0,
            "include_empty": args.include_empty,
            "source_hash": args.source_hash, "schema_version": SCHEMA_VERSION,
        }, ensure_ascii=False, indent=2))

    ctx = _pass1_ctx({
        "provider": "vllm", "model_id": args.model,
        "params": {"temperature": 0.0}, "supports_vision": True,
    })
    logger.info("out=%s model=%s radius=%s include_empty=%s",
                out_dir, args.model, args.radius, args.include_empty)
    for no in episode_nos:
        run_episode(args.webtoon, no, src_dir, out_dir, ctx, system,
                    args.radius, args.workers, limit=args.limit,
                    include_empty=args.include_empty)
    print(json.dumps(report(src_dir, out_dir, args.webtoon, episode_nos),
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
