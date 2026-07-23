"""세그먼트-단위 vs 컷-단위 비전 추출 비교 — 컷(다운로드 타일) 경계가 콘텐츠를 쪼개는 문제를
직접 실측. 컷 경계를 넘는 세그먼트를 골라, (a)세그먼트 이미지(온전한 콘텐츠 밴드) vs (b)그게
걸친 컷 이미지들(반토막)로 각각 vision 추출해 대조한다. 읽기전용(DB 쓰기 없음).

    cd webtoon-pipeline && set -a && source ../prod.env && set +a && \
    PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python PYTHONPATH=. \
    .venv/bin/python ../anon-roster/exp_segment_vision.py --webtoon 43 --no 2 [--pick 3]
"""
import os, sys, json, base64, argparse, bisect
from io import BytesIO
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "webtoon-pipeline"))
from PIL import Image
from src.config.s3 import fetch_cut_image
from src.core.step1 import _resize_cut_to_width, _scan_common_width
from src.config.db import db_cursor
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import resolve_llm_model, VISION
from src.core.step3 import _pass1_ctx

_SYS = (
    "이 웹툰 이미지 조각 하나를 보고 **보이는 그대로** 추출하라. 한국어. JSON만.\n"
    "{\"panels\": <몇 개의 구분된 패널/장면인가 정수>, "
    "\"dialogues\": [\"말풍선/캡션 텍스트 원문 그대로(잘린 것은 [잘림] 표기)\"], "
    "\"characters\": <보이는 인물 수 정수>, "
    "\"truncated\": <이미지 상단/하단에서 말풍선이나 인물이 잘려나간 게 있으면 true>, "
    "\"scene\": \"한 줄 상황 요약\"}"
)


def _b64(img_bytes):
    return "data:image/jpeg;base64," + base64.b64encode(img_bytes).decode()


def _call(ctx, img_bytes):
    # call_llm_json은 bytes 이미지를 받는다(내부에서 data-url화). 순수 비전(OCR 주입 없음).
    return call_llm_json(_pass1_ctx(ctx), _SYS, "이 조각을 추출하라.", [img_bytes]).result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--webtoon", type=int, default=43); ap.add_argument("--no", type=int, default=2)
    ap.add_argument("--pick", type=int, default=3, help="비교할 spanning 세그먼트 수")
    ap.add_argument("--model", default="qwen-vl-fp8")
    args = ap.parse_args()
    src = "naver" if args.webtoon != 28 else "kakao"
    # title_id
    with db_cursor() as cur:
        cur.execute("SELECT w.source, w.title_id, e.id FROM webtoon w JOIN webtoon_episode e ON e.webtoon_id=w.id WHERE w.id=%s AND e.no=%s", (args.webtoon, args.no))
        src, tid, eid = cur.fetchone()
        cur.execute("SELECT index, strip_y1, strip_y2 FROM analysis_episode_segment WHERE episode_id=%s ORDER BY index", (eid,))
        segs = cur.fetchall()
    W, total = _scan_common_width(src, tid, args.no)
    # 스트립 + 컷 경계
    parts, off, cut_bounds, cut_imgs = [], 0, [0], {}
    for cn in range(1, total + 1):
        b = fetch_cut_image(src, tid, args.no, cn)
        if b is None:
            cut_bounds.append(off); continue
        im = _resize_cut_to_width(Image.open(BytesIO(b)).convert("RGB"), W)
        cut_imgs[cn] = im; parts.append(__import__("numpy").asarray(im)); off += im.height; cut_bounds.append(off)
    strip = __import__("numpy").vstack(parts)

    # 컷 경계 넘는 세그먼트 중 중간 크기 몇 개
    spanning = []
    for ix, y1, y2 in segs:
        lo = bisect.bisect_right(cut_bounds, y1) - 1
        hi = bisect.bisect_right(cut_bounds, y2 - 1) - 1
        if hi > lo and 400 < (y2 - y1) < 1400:
            spanning.append((ix, y1, y2, list(range(lo + 1, hi + 2))))
    spanning = spanning[len(spanning) // 3: len(spanning) // 3 + args.pick]  # 중반부 표본
    ctx = dict(resolve_llm_model(args.webtoon, VISION)); ctx["model_id"] = args.model; ctx["name"] = args.model

    for ix, y1, y2, cuts in spanning:
        print(f"\n{'='*70}\n세그먼트 {ix} (strip {y1}~{y2}, {y2-y1}px) — 걸친 컷: {cuts}")
        seg_bytes = BytesIO(); Image.fromarray(strip[y1:y2]).save(seg_bytes, "JPEG", quality=85)
        so = _call(ctx, seg_bytes.getvalue())
        print(f"  [세그먼트-단위] panels={so.get('panels')} chars={so.get('characters')} truncated={so.get('truncated')}")
        print(f"     대사: {so.get('dialogues')}")
        print(f"     장면: {so.get('scene')}")
        for cn in cuts:
            if cn not in cut_imgs:
                continue
            cb = BytesIO(); cut_imgs[cn].save(cb, "JPEG", quality=85)
            co = _call(ctx, cb.getvalue())
            print(f"  [컷 {cn}] panels={co.get('panels')} chars={co.get('characters')} truncated={co.get('truncated')} 대사={co.get('dialogues')}")


if __name__ == "__main__":
    main()
