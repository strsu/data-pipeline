"""세그먼트 분할 시각화 — step1이 스트립을 어떻게 자르는지 눈으로 본다.

`segment-oversize-2026-07-17.md`의 수정(거대 세그먼트 재분할) 전/후를 나란히 렌더한다.
프로덕션과 **같은 경로**를 쓴다: `fetch_cut_image` → `_resize_cut_to_width` → vstack →
`_content_intervals` → (수정 후) `split_tall_interval`.

산출: 자체 완결 HTML(이미지 base64 인라인) — 서버 없이 더블클릭으로 열린다.

실행:
  cd webtoon-pipeline && set -a && source ../prod.env && set +a && \
  PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python PYTHONPATH=. \
  .venv/bin/python tools/segment_view.py --webtoon 43 --episode 1 --cuts 1-8
  # OCR 검출 수까지 보려면 --ocr (느림)
"""
from __future__ import annotations

import argparse
import base64
import io
import json
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

from src.config.db import db_cursor
from src.config.s3 import fetch_cut_image
from src.core.step1 import _resize_cut_to_width
from src.operators.cut_merger import (
    MAX_SEGMENT_PX,
    _content_intervals,
    split_tall_interval,
)

PALETTE = [(88, 166, 255), (63, 185, 80), (163, 113, 247), (240, 136, 62),
           (248, 81, 73), (255, 212, 59), (86, 211, 200), (255, 123, 114)]


def _build_strip(src: str, tid: str, ep: int, cuts: list[int]):
    """프로덕션과 동일하게 컷을 공통 폭(W)으로 리사이즈해 세로로 이어붙인다."""
    chunks, bounds, W = [], [0], None
    for cn in cuts:
        b = fetch_cut_image(src, tid, ep, cn)
        if b is None:
            continue
        im = Image.open(io.BytesIO(b)).convert("RGB")
        if W is None:
            W = im.size[0]
        im = _resize_cut_to_width(im, W)
        chunks.append(np.asarray(im))
        bounds.append(bounds[-1] + im.size[1])
    return np.vstack(chunks), bounds, W


def _render(strip: np.ndarray, segs: list[tuple[int, int]], cut_bounds: list[int],
            scale: float) -> str:
    """스트립 위에 세그먼트 밴드와 컷 경계를 그려 base64 PNG로."""
    h, w = strip.shape[:2]
    im = Image.fromarray(strip).convert("RGB")
    tw, th = int(w * scale), int(h * scale)
    im = im.resize((tw, th), Image.LANCZOS)
    ov = Image.new("RGBA", (tw, th), (0, 0, 0, 0))
    d = ImageDraw.Draw(ov)

    for i, (y0, y1) in enumerate(segs):
        c = PALETTE[i % len(PALETTE)]
        a0, a1 = int(y0 * scale), int(y1 * scale)
        d.rectangle([0, a0, tw - 1, a1 - 1], fill=(*c, 46), outline=(*c, 235), width=2)
        d.text((4, a0 + 2), f"#{i} {y1-y0}px", fill=(*c, 255))

    # 컷 경계 — 흰 점선
    for b in cut_bounds[1:-1]:
        yy = int(b * scale)
        for x in range(0, tw, 12):
            d.line([x, yy, x + 6, yy], fill=(255, 255, 255, 170), width=1)

    im = Image.alpha_composite(im.convert("RGBA"), ov).convert("RGB")
    bio = io.BytesIO()
    im.save(bio, "PNG")
    return base64.b64encode(bio.getvalue()).decode()


HTML = """<!doctype html><html lang="ko"><head><meta charset="utf-8">
<title>세그먼트 분할 — {title}</title><style>
body{{background:#0e1116;color:#e6edf3;font:14px/1.6 -apple-system,"Apple SD Gothic Neo",sans-serif;margin:0;padding:18px}}
h1{{font-size:17px;margin:0 0 4px}} .sub{{color:#8b949e;font-size:12.5px;margin-bottom:14px}}
.cols{{display:flex;gap:22px;align-items:flex-start}}
.col{{background:#161b22;border:1px solid #2a323d;border-radius:10px;padding:12px}}
.col h2{{font-size:13px;margin:0 0 8px}} .before h2{{color:#f85149}} .after h2{{color:#3fb950}}
.stat{{font-size:12px;color:#8b949e;margin-bottom:8px}} .stat b{{color:#e6edf3}}
img{{display:block;border-radius:6px}}
table{{border-collapse:collapse;font-size:12px;margin-top:10px;width:100%}}
th,td{{border:1px solid #2a323d;padding:3px 7px;text-align:right}} th{{background:#1c232d;color:#8b949e}}
td.l{{text-align:left}} .big{{color:#f85149;font-weight:700}} .ok{{color:#3fb950}}
.legend{{font-size:12px;color:#8b949e;margin-top:12px}}
.legend i{{display:inline-block;width:10px;height:10px;background:#fff;opacity:.6;margin-right:4px}}
</style></head><body>
<h1>세그먼트 분할 — {title}</h1>
<div class="sub">프로덕션과 동일 경로(fetch_cut_image → _resize_cut_to_width → vstack → _content_intervals).
스트립 {w}×{h}px · 컷 {ncuts}개 · 축소 {scale:.2f}× · MAX_SEGMENT_PX={maxseg}</div>
<div class="cols">
  <div class="col before"><h2>현행 (분할 없음)</h2>
    <div class="stat">세그먼트 <b>{n_before}</b>개 · 최대 <b class="big">{max_before}px</b>{ocr_before}</div>
    <img src="data:image/png;base64,{img_before}"></div>
  <div class="col after"><h2>수정 후 (split_tall_interval)</h2>
    <div class="stat">세그먼트 <b>{n_after}</b>개 · 최대 <b class="ok">{max_after}px</b>{ocr_after}</div>
    <img src="data:image/png;base64,{img_after}"></div>
  <div class="col"><h2>세그먼트 목록</h2>{table}
    <div class="legend"><i></i>흰 점선 = 컷 경계 &nbsp;|&nbsp; 색 밴드 = 세그먼트</div></div>
</div></body></html>"""


def main() -> None:
    ap = argparse.ArgumentParser(description="step1 세그먼트 분할 시각화")
    ap.add_argument("--webtoon", type=int, required=True)
    ap.add_argument("--episode", type=int, default=1)
    ap.add_argument("--cuts", default="1-8", help="예: 1-8")
    ap.add_argument("--scale", type=float, default=0.22)
    ap.add_argument("--ocr", action="store_true", help="세그먼트별 OCR 검출 수도 측정(느림)")
    ap.add_argument("--out", default="../datasets")
    args = ap.parse_args()

    lo, hi = (int(x) for x in args.cuts.split("-"))
    cuts = list(range(lo, hi + 1))
    with db_cursor() as cur:
        cur.execute("SELECT source, title_id, title_name FROM webtoon WHERE id=%s", (args.webtoon,))
        src, tid, name = cur.fetchone()

    strip, bounds, W = _build_strip(src, tid, args.episode, cuts)
    before = _content_intervals(strip)
    after: list[tuple[int, int]] = []
    for (y0, y1) in before:
        after += split_tall_interval(strip, y0, y1)

    ocr_b = ocr_a = None
    if args.ocr:
        from src.operators.ocr_yolo_client import run_ocr

        def _count(segs):
            n = 0
            for (y0, y1) in segs:
                bio = io.BytesIO()
                Image.fromarray(strip[y0:y1]).save(bio, "JPEG", quality=92)
                n += len(run_ocr(bio.getvalue()))
            return n
        ocr_b, ocr_a = _count(before), _count(after)

    def _cut_of(y):
        for i in range(len(bounds) - 1):
            if bounds[i] <= y < bounds[i + 1]:
                return cuts[i]
        return "-"

    rows = "".join(
        f'<tr><td>#{i}</td><td class="l">{y0}~{y1}</td>'
        f'<td class="{"big" if y1-y0 > MAX_SEGMENT_PX*1.2 else ""}">{y1-y0}</td>'
        f'<td>{_cut_of(y0)}</td></tr>'
        for i, (y0, y1) in enumerate(after))
    table = ("<table><tr><th>#</th><th>y 범위</th><th>높이</th><th>컷</th></tr>"
             + rows + "</table>")

    html = HTML.format(
        title=f"{name} ep{args.episode} (컷 {lo}~{hi})",
        w=strip.shape[1], h=strip.shape[0], ncuts=len(cuts), scale=args.scale,
        maxseg=MAX_SEGMENT_PX,
        n_before=len(before), max_before=max(b - a for a, b in before),
        n_after=len(after), max_after=max(b - a for a, b in after),
        ocr_before=f" · OCR <b class='big'>{ocr_b}개</b>" if ocr_b is not None else "",
        ocr_after=f" · OCR <b class='ok'>{ocr_a}개</b>" if ocr_a is not None else "",
        img_before=_render(strip, before, bounds, args.scale),
        img_after=_render(strip, after, bounds, args.scale),
        table=table,
    )
    out = Path(args.out) / f"segment_view_w{args.webtoon}_e{args.episode}.html"
    out.write_text(html)
    print(json.dumps({
        "out": str(out), "strip": list(strip.shape[:2]),
        "before": {"n": len(before), "max": max(b - a for a, b in before), "ocr": ocr_b},
        "after": {"n": len(after), "max": max(b - a for a, b in after), "ocr": ocr_a},
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
