"""에피소드 단위 세그먼트 vs 컷 비전 추출 비교(여러 웹툰). 각 회차의 **전체 세그먼트**와 **전체 컷**을
순수 비전 추출(병렬)해 truncation률·대사량을 집계. 컷(타일)이 콘텐츠를 쪼개 truncated↑·대사 파편화되는지,
세그먼트(콘텐츠 밴드)가 온전한지 에피소드 규모로 확인. 읽기전용.

    cd webtoon-pipeline && set -a && source ../prod.env && set +a && \
    LLM_MAX_CONCURRENCY=6 PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python PYTHONPATH=. \
    .venv/bin/python ../anon-roster/exp_segment_episode.py
"""
import os, sys, base64, bisect
from io import BytesIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "webtoon-pipeline"))
from PIL import Image
from src.config.s3 import fetch_cut_image
from src.core.step1 import _resize_cut_to_width, _scan_common_width
from src.config.db import db_cursor
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import resolve_llm_model, VISION
from src.core.step3 import _pass1_ctx

TARGETS = [(43, 2, "참교육/액션"), (17, 2, "화산귀환/무협"), (7, 1, "당골/스릴러")]
MODEL = "qwen-vl-fp8"

_SYS = (
    "이 웹툰 이미지 조각을 보고 보이는 그대로 추출하라. 한국어. JSON만.\n"
    '{"dialogues": ["말풍선/캡션 텍스트 원문(잘린 것은 뒤에 [잘림])"], '
    '"characters": <보이는 인물 수 정수>, '
    '"truncated": <상단/하단에서 말풍선·인물·효과음이 잘려나갔으면 true, 온전하면 false>}'
)


def _call(ctx, img_bytes):
    try:
        r = call_llm_json(_pass1_ctx(ctx), _SYS, "추출.", [img_bytes]).result
        return {"d": len(r.get("dialogues") or []), "c": int(r.get("characters") or 0),
                "t": bool(r.get("truncated"))}
    except Exception as e:
        return {"d": 0, "c": 0, "t": None, "err": str(e)[:40]}


def _jpeg(im):
    b = BytesIO(); im.save(b, "JPEG", quality=85); return b.getvalue()


def run_ep(wid, no, ctx):
    with db_cursor() as cur:
        cur.execute("SELECT w.source, w.title_id, e.id FROM webtoon w JOIN webtoon_episode e ON e.webtoon_id=w.id WHERE w.id=%s AND e.no=%s", (wid, no))
        src, tid, eid = cur.fetchone()
        cur.execute("SELECT strip_y1, strip_y2 FROM analysis_episode_segment WHERE episode_id=%s ORDER BY index", (eid,))
        segs = cur.fetchall()
    W, total = _scan_common_width(src, tid, no)
    parts, off = [], 0
    cut_imgs = {}
    for cn in range(1, total + 1):
        b = fetch_cut_image(src, tid, no, cn)
        if b is None:
            continue
        im = _resize_cut_to_width(Image.open(BytesIO(b)).convert("RGB"), W)
        cut_imgs[cn] = im; parts.append(np.asarray(im)); off += im.height
    strip = np.vstack(parts)
    seg_imgs = [Image.fromarray(strip[y1:y2]) for y1, y2 in segs]

    def agg(imgs):
        res = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = [pool.submit(_call, ctx, _jpeg(im)) for im in imgs]
            for f in as_completed(futs):
                res.append(f.result())
        ok = [r for r in res if r["t"] is not None]
        n = len(ok)
        trunc = sum(1 for r in ok if r["t"])
        dial = sum(r["d"] for r in ok)
        return {"n": n, "trunc": trunc, "trunc_pct": round(100 * trunc / n) if n else 0, "dial": dial,
                "err": len(res) - n}

    seg_m = agg(seg_imgs)
    cut_m = agg(list(cut_imgs.values()))
    return seg_m, cut_m


def main():
    ctx = dict(resolve_llm_model(43, VISION)); ctx["model_id"] = MODEL; ctx["name"] = MODEL
    print(f"모델={MODEL}\n{'웹툰':<16}{'단위':<8}{'개수':>5}{'truncated':>12}{'대사총':>8}")
    print("-" * 52)
    for wid, no, nm in TARGETS:
        try:
            seg_m, cut_m = run_ep(wid, no, ctx)
        except Exception as e:
            print(f"{nm}: 실패 {str(e)[:50]}"); continue
        print(f"{nm:<16}{'세그':<8}{seg_m['n']:>5}{str(seg_m['trunc'])+'('+str(seg_m['trunc_pct'])+'%)':>12}{seg_m['dial']:>8}")
        print(f"{'':<16}{'컷':<8}{cut_m['n']:>5}{str(cut_m['trunc'])+'('+str(cut_m['trunc_pct'])+'%)':>12}{cut_m['dial']:>8}")
        print(f"{'':<16}→ 세그 truncated {seg_m['trunc_pct']}% vs 컷 {cut_m['trunc_pct']}% / 대사 세그 {seg_m['dial']} vs 컷 {cut_m['dial']}")
        print("-" * 52, flush=True)


if __name__ == "__main__":
    main()
