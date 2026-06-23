"""Step 1 코어 — OCR / YOLO 로컬 추출 (faust-free, 순수 함수).

Temporal 액티비티가 호출한다. OCR과 YOLO는 **독립 경로**로 분리:
- process_cut_ocr  : 이미지 다운로드 → 세그먼트 분할 → OCR → text_region/text_annotation 저장
- process_cut_yolo : 이미지 다운로드 → 세그먼트 분할 → YOLO → face_record 저장 + crop S3 업로드

둘은 같은 컷 행(webtoon_cut)을 공유하므로 cut row는 `ensure_cut`로 멱등 보장하고,
에피소드 재처리용 정리는 `prepare_episode_*`가 에피소드 단위로 1회 수행한다.

이미지는 각 경로가 독립적으로 재다운로드한다(재다운 비용 무시 가능 — 일 ~1000컷).
"""
from __future__ import annotations

import time
from bisect import bisect_right
from datetime import datetime, timezone
from io import BytesIO

import numpy as np
from PIL import Image

from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import delete_face_crop, fetch_cut_image, upload_face_crop
from src.operators.cut_merger import (
    CutSegment, _content_intervals, adjust_bbox_to_cut, split_cut_pair,
)
from src.operators.ocr_yolo_client import run_ocr, run_yolo

FACE_PAD_RATIO = 0.15
FACE_CROP_SIZE = (112, 112)
_IOU_DEDUP_THRESHOLD = 0.5

# OCR 후처리: 저확신도 제거 + 같은 줄(세로 겹침 + 가로 근접) 병합
OCR_MIN_SCORE = 0.4       # 이 미만 검출은 버림(검출 노이즈 제거)
OCR_LINE_VOVERLAP = 0.4   # 두 박스의 세로 겹침이 작은 높이의 이 비율 이상이면 같은 줄 후보
OCR_XGAP_RATIO = 0.8      # 가로 간격이 (작은 글자높이 × 이 값) 이하일 때만 병합(멀면 별개 텍스트)


def _postprocess_ocr(blocks: list[dict], min_score: float = OCR_MIN_SCORE,
                     voverlap: float = OCR_LINE_VOVERLAP, xgap_ratio: float = OCR_XGAP_RATIO) -> list[dict]:
    """raw OCR 블록 → 저확신도 제거 + 같은 줄 병합.

    - score < min_score 인 블록 제거(예: 말풍선 조각/획 오검출).
    - 세로로 겹치고(같은 줄) **가로로 가까운**(간격 ≤ 글자높이×xgap_ratio) 박스만 묶어
      x 순으로 text 이어붙임. 멀리 떨어진(다른 말풍선) 박스는 합치지 않는다.
    - bbox_2d 없는 블록은 위치를 알 수 없어 병합 대상에서 제외하되 score 통과 시 보존.
    반환 블록: {"text", "score", "bbox_2d":[x1,y1,x2,y2]}
    """
    kept = [b for b in blocks if float(b.get("score", 0)) >= min_score]
    with_box = [b for b in kept if b.get("bbox_2d")]
    without_box = [b for b in kept if not b.get("bbox_2d")]

    # 같은 줄(세로 겹침) 후보로 1차 그룹핑
    with_box.sort(key=lambda b: (b["bbox_2d"][1] + b["bbox_2d"][3]) / 2)
    rows: list[list[dict]] = []
    row_span: list[tuple[int, int]] = []
    for b in with_box:
        x1, y1, x2, y2 = b["bbox_2d"]
        placed = False
        for ri, (ry1, ry2) in enumerate(row_span):
            ov = min(y2, ry2) - max(y1, ry1)
            if ov > 0 and ov >= voverlap * min(y2 - y1, ry2 - ry1):
                rows[ri].append(b)
                row_span[ri] = (min(ry1, y1), max(ry2, y2))
                placed = True
                break
        if not placed:
            rows.append([b])
            row_span.append((y1, y2))

    # 줄 내부에서 x 순 + 가로 간격 가까운 것만 연결(멀면 끊어 별개 블록)
    out: list[dict] = []
    for row in rows:
        row.sort(key=lambda b: b["bbox_2d"][0])
        group = [row[0]]
        for prev, cur in zip(row, row[1:]):
            gap = cur["bbox_2d"][0] - prev["bbox_2d"][2]
            hmin = min(prev["bbox_2d"][3] - prev["bbox_2d"][1], cur["bbox_2d"][3] - cur["bbox_2d"][1])
            if gap <= xgap_ratio * hmin:
                group.append(cur)
            else:
                out.append(_merge_group(group))
                group = [cur]
        out.append(_merge_group(group))

    out.extend({"text": (b.get("text") or "").strip(), "score": b.get("score")} for b in without_box)
    out.sort(key=lambda b: (b.get("bbox_2d", [0, 0])[1], b.get("bbox_2d", [0, 0])[0]))
    return out


def _merge_group(items: list[dict]) -> dict:
    text = " ".join((i.get("text") or "") for i in items).strip()
    xs = [i["bbox_2d"][0] for i in items] + [i["bbox_2d"][2] for i in items]
    ys = [i["bbox_2d"][1] for i in items] + [i["bbox_2d"][3] for i in items]
    score = min(float(i.get("score", 0)) for i in items)
    return {"text": text, "score": round(score, 4), "bbox_2d": [min(xs), min(ys), max(xs), max(ys)]}


# ── 기하 헬퍼 ─────────────────────────────────────────────────────────────────

def _iou(a: list[float], b: list[float]) -> float:
    xi1, yi1 = max(a[0], b[0]), max(a[1], b[1])
    xi2, yi2 = min(a[2], b[2]), min(a[3], b[3])
    inter = max(0.0, xi2 - xi1) * max(0.0, yi2 - yi1)
    if inter == 0.0:
        return 0.0
    union = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / union if union > 0.0 else 0.0


def _crop_face(image_bytes: bytes, bbox: list[float]) -> bytes:
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    x1, y1, x2, y2 = bbox
    w, h = x2 - x1, y2 - y1
    px, py = w * FACE_PAD_RATIO, h * FACE_PAD_RATIO
    crop = img.crop((
        max(0, x1 - px), max(0, y1 - py),
        min(img.width, x2 + px), min(img.height, y2 + py),
    )).resize(FACE_CROP_SIZE, Image.LANCZOS)
    buf = BytesIO()
    crop.save(buf, format="JPEG", quality=92)
    return buf.getvalue()


# ── 컷 row 보장 / 조회 ────────────────────────────────────────────────────────

def ensure_cut(webtoon_episode_id: int, cut_number: int) -> int:
    """webtoon_cut 행을 멱등 upsert하고 id 반환. 텍스트/얼굴은 건드리지 않는다."""
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO webtoon_cut
                (episode_id, cut_number, processed_at, is_stale, created_at, updated_at)
            VALUES (%s, %s, %s, false, %s, %s)
            ON CONFLICT ON CONSTRAINT uniq_webtoon_cut_episode_no DO UPDATE
                SET processed_at = EXCLUDED.processed_at,
                    updated_at   = EXCLUDED.updated_at
            RETURNING id
            """,
            (webtoon_episode_id, cut_number, now, now, now),
        )
        return cur.fetchone()[0]


def _cut_id(cur, webtoon_episode_id: int, cut_number: int) -> int | None:
    cur.execute(
        "SELECT id FROM webtoon_cut WHERE episode_id = %s AND cut_number = %s",
        (webtoon_episode_id, cut_number),
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_image_count(webtoon_episode_id: int) -> int:
    with db_cursor() as cur:
        cur.execute(
            "SELECT image_count FROM webtoon_episode WHERE id = %s",
            (webtoon_episode_id,),
        )
        row = cur.fetchone()
        return row[0] if row else 0


# ── 에피소드 재처리 정리 (에피소드 시작 시 1회) ───────────────────────────────

def prepare_episode_ocr(webtoon_episode_id: int) -> None:
    """에피소드의 모든 컷에서 기존 text_region/text_annotation 제거(재처리 멱등)."""
    with db_cursor() as cur:
        cur.execute("SELECT id FROM webtoon_cut WHERE episode_id = %s", (webtoon_episode_id,))
        cut_ids = [r[0] for r in cur.fetchall()]
        for cut_id in cut_ids:
            cur.execute(
                "DELETE FROM text_annotation WHERE region_id IN "
                "(SELECT id FROM text_region WHERE cut_id = %s)",
                (cut_id,),
            )
            cur.execute("DELETE FROM text_region WHERE cut_id = %s", (cut_id,))


def _cleanup_cut_faces(cut_id: int, source: str, title_id: str) -> None:
    """컷의 face_record + S3 crop + Chroma + face_embedding 제거."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fe.embedding_model, fe.chroma_doc_id
            FROM face_record fr
            LEFT JOIN face_embedding fe ON fe.face_record_id = fr.id
            WHERE fr.cut_id = %s
            """,
            (cut_id,),
        )
        rows = cur.fetchall()

    face_ids: set[int] = set()
    chroma_entries: dict[str, list[str]] = {}
    for face_id, model, doc_id in rows:
        face_ids.add(face_id)
        if model and doc_id:
            chroma_entries.setdefault(model, []).append(doc_id)

    for face_id in face_ids:
        try:
            delete_face_crop(face_id, source, title_id)
        except Exception as e:
            print(f"[step1] S3 crop 삭제 실패 face_id={face_id}: {e}")

    for model, doc_ids in chroma_entries.items():
        try:
            get_face_collection(source, title_id, model).delete(ids=doc_ids)
        except Exception as e:
            print(f"[step1] Chroma 삭제 실패 model={model}: {e}")

    if face_ids:
        with db_cursor() as cur:
            cur.execute("DELETE FROM face_embedding WHERE face_record_id = ANY(%s)", (list(face_ids),))
            cur.execute("DELETE FROM face_record WHERE id = ANY(%s)", (list(face_ids),))


def prepare_episode_yolo(webtoon_episode_id: int, source: str, title_id: str) -> None:
    """에피소드의 모든 컷에서 기존 face 데이터 일괄 정리(재처리 멱등)."""
    with db_cursor() as cur:
        cur.execute("SELECT id FROM webtoon_cut WHERE episode_id = %s", (webtoon_episode_id,))
        cut_ids = [r[0] for r in cur.fetchall()]
    for cut_id in cut_ids:
        _cleanup_cut_faces(cut_id, source, title_id)


# ── 세그먼트 단위 처리 (OCR / YOLO 분리) ──────────────────────────────────────

def _process_segment_ocr(
    segment: CutSegment, webtoon_episode_id: int, cut_number_n: int,
    source: str, title_id: str, episode_no: int, region_index: dict[int, int],
) -> int:
    ocr_blocks = run_ocr(
        segment.image_bytes, source=source, title_id=title_id, episode_no=episode_no, cut=cut_number_n
    )
    ocr_blocks = _postprocess_ocr(ocr_blocks)
    saved = 0
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        for block in ocr_blocks:
            raw = block.get("bbox_2d") or [0, 0, 0, 0]
            adjusted, _ = adjust_bbox_to_cut([raw[0], raw[1], raw[2], raw[3]], segment)
            abs_y_center = segment.y_offset + (raw[1] + raw[3]) / 2
            if abs_y_center >= segment.cut_n_height:
                continue
            cut_id = _cut_id(cur, webtoon_episode_id, cut_number_n)
            if cut_id is None:
                continue
            idx = region_index.get(cut_id, 0)
            region_index[cut_id] = idx + 1
            cur.execute(
                """
                INSERT INTO text_region
                    (cut_id, index, bbox_x1, bbox_y1, bbox_x2, bbox_y2, is_excluded, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, false, %s, %s)
                RETURNING id
                """,
                (cut_id, idx, adjusted[0], adjusted[1], adjusted[2], adjusted[3], now, now),
            )
            region_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO text_annotation
                    (region_id, source, text, confidence, created_at, updated_at)
                VALUES (%s, 'paddle', %s, %s, %s, %s)
                """,
                (region_id, block["text"], block.get("score"), now, now),
            )
            saved += 1
    return saved


def _process_segment_yolo(
    segment: CutSegment, webtoon_episode_id: int, cut_number_n: int,
    source: str, title_id: str, face_index: dict[int, int], saved_face_bboxes: dict[int, list],
) -> int:
    faces = run_yolo(segment.image_bytes, source=source, title_id=title_id, cut=cut_number_n)
    saved = 0
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        for face in faces:
            abs_y1 = segment.y_offset + face["bbox"][1]
            if abs_y1 >= segment.cut_n_height:
                continue
            adjusted, cut_offset = adjust_bbox_to_cut(face["bbox"], segment)
            actual_cut = cut_number_n + cut_offset
            cut_id = _cut_id(cur, webtoon_episode_id, actual_cut)
            if cut_id is None:
                continue
            norm = [adjusted[0], max(0.0, adjusted[1]), adjusted[2], max(0.0, adjusted[3])]
            if any(_iou(norm, s) >= _IOU_DEDUP_THRESHOLD for s in saved_face_bboxes.get(cut_id, [])):
                continue
            saved_face_bboxes.setdefault(cut_id, []).append(norm)
            face_idx = face_index.get(cut_id, 0)
            face_index[cut_id] = face_idx + 1
            b = adjusted
            cur.execute(
                """
                INSERT INTO face_record
                    (cut_id, face_idx, bbox_x1, bbox_y1, bbox_x2, bbox_y2,
                     conf, is_confirmed, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, false, %s, %s)
                ON CONFLICT ON CONSTRAINT uniq_face_record_cut_idx DO NOTHING
                RETURNING id
                """,
                (cut_id, face_idx, b[0], b[1], b[2], b[3], face["conf"], now, now),
            )
            res = cur.fetchone()
            if not res:
                continue
            face_record_id = res[0]
            saved += 1
            try:
                crop_bytes = _crop_face(segment.image_bytes, face["bbox"])
                upload_face_crop(face_record_id, source, title_id, crop_bytes)
            except Exception as e:
                print(f"[step1] face crop upload 실패 face_id={face_record_id}: {e}")
    return saved


# ── 컷 단위 진입점 (Temporal 액티비티가 호출) ─────────────────────────────────

def process_cut_ocr(
    source: str, title_id: str, episode_no: int, webtoon_episode_id: int, cut: int
) -> bool:
    """단일 컷 OCR 처리. 반환: 다음 컷 존재 여부(has_next)."""
    cur_bytes = fetch_cut_image(source, title_id, episode_no, cut)
    if cur_bytes is None:
        return False
    next_bytes = fetch_cut_image(source, title_id, episode_no, cut + 1)

    ensure_cut(webtoon_episode_id, cut)
    if next_bytes is not None:
        ensure_cut(webtoon_episode_id, cut + 1)

    region_index: dict[int, int] = {}
    total = 0
    for segment in split_cut_pair(cur_bytes, next_bytes):
        total += _process_segment_ocr(
            segment, webtoon_episode_id, cut, source, title_id, episode_no, region_index
        )
    print(f"[step1.ocr] {source}/{title_id} ep={episode_no} cut={cut} — 텍스트 {total}개")
    return next_bytes is not None


def process_cut_yolo(
    source: str, title_id: str, episode_no: int, webtoon_episode_id: int, cut: int
) -> bool:
    """단일 컷 YOLO 처리. 반환: 다음 컷 존재 여부(has_next)."""
    cur_bytes = fetch_cut_image(source, title_id, episode_no, cut)
    if cur_bytes is None:
        return False
    next_bytes = fetch_cut_image(source, title_id, episode_no, cut + 1)

    ensure_cut(webtoon_episode_id, cut)
    if next_bytes is not None:
        ensure_cut(webtoon_episode_id, cut + 1)

    face_index: dict[int, int] = {}
    saved_face_bboxes: dict[int, list] = {}
    total = 0
    for segment in split_cut_pair(cur_bytes, next_bytes):
        total += _process_segment_yolo(
            segment, webtoon_episode_id, cut, source, title_id, face_index, saved_face_bboxes
        )
    print(f"[step1.yolo] {source}/{title_id} ep={episode_no} cut={cut} — 얼굴 {total}개")
    return next_bytes is not None


# ── 에피소드 단위 처리 (스트립 결합 → 콘텐츠 세그먼트, 컷 페어링 없음) ──────────
#
# 에피소드의 모든 컷을 하나의 세로 스트립으로 이어붙인 뒤, 콘텐츠 구간(여백/단색 제거)
# 단위로 OCR/YOLO를 1회씩 돌린다. 컷 페어링(2컷 결합)이 없으므로 세그먼트 중복이 없고,
# 컷 경계에 걸친 얼굴/텍스트도 온전히 처리된다. 검출은 전역 y의 중심이 속한 컷에 귀속하고
# 그 컷의 로컬 좌표로 변환해 저장한다.
#
# 메모리: 에피소드 전체 스트립(+1회 배열)을 메모리에 올린다(컷 ~100장이면 수백 MB).
# 컷 단위 대비 메모리 사용이 크므로 워커 메모리를 고려할 것.


def _load_episode_strip(source: str, title_id: str, episode_no: int, webtoon_episode_id: int,
                        tag: str = "step1"):
    """에피소드 전체 컷 다운로드 → 공통 너비로 결합한 스트립 + 컷 경계/매핑 반환.

    반환: (strip PIL, W, cut_numbers[list], bounds[list], cut_id_map{cut_no:id})
          또는 컷이 없으면 None.  bounds[k]~bounds[k+1] 가 cut_numbers[k] 영역.
    """
    ep = f"{source}/{title_id} ep={episode_no}"
    # [1] 컷 이미지 S3 다운로드 (순차)
    t0 = time.perf_counter()
    print(f"[{tag}.load] {ep} — 이미지 다운로드 시작")
    imgs: list[tuple[int, Image.Image]] = []
    cut = 1
    while True:
        b = fetch_cut_image(source, title_id, episode_no, cut)
        if b is None:
            break
        imgs.append((cut, Image.open(BytesIO(b)).convert("RGB")))
        if cut % 10 == 0:
            print(f"[{tag}.load] {ep} — 다운로드 {cut}개 완료 ({time.perf_counter() - t0:.1f}s)")
        cut += 1
    if not imgs:
        print(f"[{tag}.load] {ep} — 컷 없음")
        return None
    print(f"[{tag}.load] {ep} — 다운로드 완료 {len(imgs)}개 ({time.perf_counter() - t0:.1f}s)")

    # [2] 공통 너비로 리사이즈 + 세로 결합(스트립 생성)
    t1 = time.perf_counter()
    print(f"[{tag}.load] {ep} — 스트립 결합 시작 ({len(imgs)}개)")
    W = min(im.width for _, im in imgs)
    cut_numbers: list[int] = []
    bounds: list[int] = [0]
    resized: list[Image.Image] = []
    for cn, im in imgs:
        if im.width != W:
            im = im.resize((W, round(im.height * W / im.width)), Image.LANCZOS)
        resized.append(im)
        cut_numbers.append(cn)
        bounds.append(bounds[-1] + im.height)

    strip = Image.new("RGB", (W, bounds[-1]))
    for im, y0 in zip(resized, bounds[:-1]):
        strip.paste(im, (0, y0))
    print(f"[{tag}.load] {ep} — 스트립 결합 완료 {W}x{bounds[-1]} ({time.perf_counter() - t1:.1f}s)")

    # [3] 컷 row 멱등 보장
    t2 = time.perf_counter()
    cut_id_map = {cn: ensure_cut(webtoon_episode_id, cn) for cn in cut_numbers}
    print(f"[{tag}.load] {ep} — cut row {len(cut_id_map)}개 ensure ({time.perf_counter() - t2:.1f}s), "
          f"load 총 {time.perf_counter() - t0:.1f}s")
    return strip, W, cut_numbers, bounds, cut_id_map


def _cut_index_at(bounds: list[int], y: float) -> int:
    """전역 y가 속한 컷 인덱스 k (bounds[k] <= y < bounds[k+1])."""
    k = bisect_right(bounds, y) - 1
    return max(0, min(k, len(bounds) - 2))


def _ensure_segment(cur, episode_id: int, index: int, y0: int, y1: int, width: int) -> int:
    """episode_segment 멱등 upsert(OCR/YOLO가 같은 분할을 공유). id 반환."""
    now = datetime.now(timezone.utc)
    cur.execute(
        """
        INSERT INTO episode_segment
            (episode_id, index, strip_y1, strip_y2, width, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT uniq_episode_segment_episode_index DO UPDATE
            SET strip_y1 = EXCLUDED.strip_y1, strip_y2 = EXCLUDED.strip_y2,
                width = EXCLUDED.width, updated_at = EXCLUDED.updated_at
        RETURNING id
        """,
        (episode_id, index, y0, y1, width, now, now),
    )
    return cur.fetchone()[0]


def _assign_line_groups(
    blocks: list[dict], voverlap: float = OCR_LINE_VOVERLAP, xgap_ratio: float = OCR_XGAP_RATIO
) -> list[tuple[dict, int]]:
    """raw OCR 블록(전량)에 같은 줄 그룹 id 부여. drop/merge 없이 (block, group_id) 반환.

    같은 줄 = 세로 겹침 + 가로 근접(글자높이×xgap_ratio). 멀리 떨어지면 다른 그룹.
    bbox 없는 블록은 group_id=-1.
    """
    with_box = [b for b in blocks if b.get("bbox_2d")]
    without_box = [b for b in blocks if not b.get("bbox_2d")]

    with_box.sort(key=lambda b: (b["bbox_2d"][1] + b["bbox_2d"][3]) / 2)
    rows: list[list[dict]] = []
    row_span: list[list[int]] = []
    for b in with_box:
        x1, y1, x2, y2 = b["bbox_2d"]
        placed = False
        for ri in range(len(rows)):
            ry1, ry2 = row_span[ri]
            ov = min(y2, ry2) - max(y1, ry1)
            if ov > 0 and ov >= voverlap * min(y2 - y1, ry2 - ry1):
                rows[ri].append(b)
                row_span[ri] = [min(ry1, y1), max(ry2, y2)]
                placed = True
                break
        if not placed:
            rows.append([b])
            row_span.append([y1, y2])

    out: list[tuple[dict, int]] = []
    gid = 0
    for row in rows:
        row.sort(key=lambda b: b["bbox_2d"][0])
        prev = None
        for cur_b in row:
            if prev is not None:
                gap = cur_b["bbox_2d"][0] - prev["bbox_2d"][2]
                hmin = min(prev["bbox_2d"][3] - prev["bbox_2d"][1],
                           cur_b["bbox_2d"][3] - cur_b["bbox_2d"][1])
                if gap > xgap_ratio * hmin:
                    gid += 1  # 멀면 새 그룹
            out.append((cur_b, gid))
            prev = cur_b
        gid += 1  # 줄이 바뀌면 새 그룹
    for b in without_box:
        out.append((b, -1))
    return out


def prepare_episode_segments(webtoon_episode_id: int) -> None:
    """에피소드의 episode_segment 행 제거(재처리 멱등). 검출은 prepare_episode_ocr/yolo가 먼저 정리."""
    with db_cursor() as cur:
        cur.execute("DELETE FROM episode_segment WHERE episode_id = %s", (webtoon_episode_id,))


def process_episode_ocr(
    source: str, title_id: str, episode_no: int, webtoon_episode_id: int,
    heartbeat_cb=None,
) -> int:
    """에피소드 단위 OCR. 스트립 콘텐츠 세그먼트별 OCR → raw 전량 저장(컷 귀속).

    검출은 전부 저장하고, 신뢰도(OCR_MIN_SCORE) 미만은 is_used=False로만 표시(드롭 아님).
    같은 줄 그룹은 line_group으로 기록(병합은 소비측에서 파생). 반환: 저장 텍스트 수.
    """
    loaded = _load_episode_strip(source, title_id, episode_no, webtoon_episode_id, tag="step1.ocr")
    if loaded is None:
        print(f"[step1.ocr] {source}/{title_id} ep={episode_no} — 컷 없음")
        return 0
    strip, W, cut_numbers, bounds, cut_id_map = loaded

    ep = f"{source}/{title_id} ep={episode_no}"
    t_seg = time.perf_counter()
    print(f"[step1.ocr] {ep} — 세그먼트 분할 시작 (strip {W}x{bounds[-1]})")
    arr = np.asarray(strip)
    intervals = _content_intervals(arr)
    del arr
    print(f"[step1.ocr] {ep} — 세그먼트 분할 완료 {len(intervals)}개 ({time.perf_counter() - t_seg:.1f}s)")

    region_index: dict[int, int] = {}
    total = 0
    now = datetime.now(timezone.utc)
    t_ocr = time.perf_counter()
    for si, (y0, y1) in enumerate(intervals):
        seg = strip.crop((0, y0, W, y1))
        buf = BytesIO()
        seg.save(buf, format="JPEG", quality=92)
        rep_cut = cut_numbers[_cut_index_at(bounds, y0)]
        raw_blocks = run_ocr(
            buf.getvalue(), source=source, title_id=title_id, episode_no=episode_no, cut=rep_cut
        )
        grouped = _assign_line_groups(raw_blocks)
        with db_cursor() as cur:
            segment_id = _ensure_segment(cur, webtoon_episode_id, si, y0, y1, W)
            for blk, gid in grouped:
                bb = blk.get("bbox_2d")
                if not bb:
                    continue
                gy1, gy2 = y0 + bb[1], y0 + bb[3]
                k = _cut_index_at(bounds, (gy1 + gy2) / 2)
                cut_id = cut_id_map[cut_numbers[k]]
                ystart = bounds[k]
                lb = [bb[0], gy1 - ystart, bb[2], gy2 - ystart]
                score = blk.get("score")
                is_used = score is not None and float(score) >= OCR_MIN_SCORE
                idx = region_index.get(cut_id, 0)
                region_index[cut_id] = idx + 1
                cur.execute(
                    """
                    INSERT INTO text_region
                        (cut_id, segment_id, index, bbox_x1, bbox_y1, bbox_x2, bbox_y2,
                         score, line_group, is_used, is_excluded, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, false, %s, %s)
                    RETURNING id
                    """,
                    (cut_id, segment_id, idx, lb[0], lb[1], lb[2], lb[3],
                     score, (gid if gid >= 0 else None), is_used, now, now),
                )
                region_id = cur.fetchone()[0]
                cur.execute(
                    """
                    INSERT INTO text_annotation
                        (region_id, source, text, confidence, created_at, updated_at)
                    VALUES (%s, 'paddle', %s, %s, %s, %s)
                    """,
                    (region_id, blk["text"], score, now, now),
                )
                total += 1
        if heartbeat_cb:
            heartbeat_cb(si + 1)
        print(f"[step1.ocr] {ep} — 세그먼트 {si + 1}/{len(intervals)} 처리 "
              f"(누적 텍스트 {total}개, {time.perf_counter() - t_ocr:.1f}s)")

    print(f"[step1.ocr] {ep} — 세그먼트 {len(intervals)}개, 텍스트 {total}개(raw) "
          f"OCR {time.perf_counter() - t_ocr:.1f}s")
    return total


def process_episode_yolo(
    source: str, title_id: str, episode_no: int, webtoon_episode_id: int,
    heartbeat_cb=None,
) -> int:
    """에피소드 단위 YOLO. 스트립 세그먼트별 얼굴 검출 → raw 전량 저장(컷 귀속).

    IOU 중복은 드롭하지 않고 is_duplicate=True/is_used=False로 표시. crop은 is_used 얼굴만 업로드.
    임베딩/매칭(Step2)도 is_used 얼굴만 대상으로 해야 함.
    """
    loaded = _load_episode_strip(source, title_id, episode_no, webtoon_episode_id, tag="step1.yolo")
    if loaded is None:
        print(f"[step1.yolo] {source}/{title_id} ep={episode_no} — 컷 없음")
        return 0
    strip, W, cut_numbers, bounds, cut_id_map = loaded

    ep = f"{source}/{title_id} ep={episode_no}"
    t_seg = time.perf_counter()
    print(f"[step1.yolo] {ep} — 세그먼트 분할 시작 (strip {W}x{bounds[-1]})")
    arr = np.asarray(strip)
    intervals = _content_intervals(arr)
    del arr
    print(f"[step1.yolo] {ep} — 세그먼트 분할 완료 {len(intervals)}개 ({time.perf_counter() - t_seg:.1f}s)")

    face_index: dict[int, int] = {}
    used_bboxes: dict[int, list] = {}  # cut_id -> 사용된 얼굴 bbox(IOU 중복 판정 기준)
    total = 0
    now = datetime.now(timezone.utc)
    t_yolo = time.perf_counter()
    for si, (y0, y1) in enumerate(intervals):
        seg = strip.crop((0, y0, W, y1))
        buf = BytesIO()
        seg.save(buf, format="JPEG", quality=92)
        seg_bytes = buf.getvalue()
        rep_cut = cut_numbers[_cut_index_at(bounds, y0)]
        faces = run_yolo(seg_bytes, source=source, title_id=title_id, episode_no=episode_no, cut=rep_cut)
        with db_cursor() as cur:
            segment_id = _ensure_segment(cur, webtoon_episode_id, si, y0, y1, W)
            for face in faces:
                fb = face["bbox"]  # 세그먼트 로컬 좌표
                gy1, gy2 = y0 + fb[1], y0 + fb[3]
                k = _cut_index_at(bounds, (gy1 + gy2) / 2)
                cut_id = cut_id_map[cut_numbers[k]]
                ystart = bounds[k]
                lb = [fb[0], max(0.0, gy1 - ystart), fb[2], max(0.0, gy2 - ystart)]
                is_dup = any(_iou(lb, s) >= _IOU_DEDUP_THRESHOLD for s in used_bboxes.get(cut_id, []))
                is_used = not is_dup
                if is_used:
                    used_bboxes.setdefault(cut_id, []).append(lb)
                fidx = face_index.get(cut_id, 0)
                face_index[cut_id] = fidx + 1
                cur.execute(
                    """
                    INSERT INTO face_record
                        (cut_id, segment_id, face_idx, bbox_x1, bbox_y1, bbox_x2, bbox_y2,
                         conf, is_used, is_duplicate, is_confirmed, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, false, %s, %s)
                    ON CONFLICT ON CONSTRAINT uniq_face_record_cut_idx DO NOTHING
                    RETURNING id
                    """,
                    (cut_id, segment_id, fidx, lb[0], lb[1], lb[2], lb[3],
                     face["conf"], is_used, is_dup, now, now),
                )
                res = cur.fetchone()
                if not res:
                    continue
                face_record_id = res[0]
                total += 1
                if is_used:
                    try:
                        crop_bytes = _crop_face(seg_bytes, fb)
                        upload_face_crop(face_record_id, source, title_id, crop_bytes)
                    except Exception as e:
                        print(f"[step1] face crop upload 실패 face_id={face_record_id}: {e}")
        if heartbeat_cb:
            heartbeat_cb(si + 1)
        print(f"[step1.yolo] {ep} — 세그먼트 {si + 1}/{len(intervals)} 처리 "
              f"(누적 얼굴 {total}개, {time.perf_counter() - t_yolo:.1f}s)")

    print(f"[step1.yolo] {ep} — 세그먼트 {len(intervals)}개, 얼굴 {total}개(raw) "
          f"YOLO {time.perf_counter() - t_yolo:.1f}s")
    return total
