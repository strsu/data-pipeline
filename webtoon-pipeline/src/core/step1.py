"""Step 1 코어 — OCR / YOLO 로컬 추출 (faust-free, 순수 함수).

Temporal 액티비티가 호출한다. OCR과 YOLO는 **독립 경로**로 분리:
- process_cut_ocr  : 이미지 다운로드 → 세그먼트 분할 → OCR → text_region/text_annotation 저장
- process_cut_yolo : 이미지 다운로드 → 세그먼트 분할 → YOLO → face_record 저장 + crop S3 업로드

둘은 같은 컷 행(webtoon_cut)을 공유하므로 cut row는 `ensure_cut`로 멱등 보장하고,
에피소드 재처리용 정리는 `prepare_episode_*`가 에피소드 단위로 1회 수행한다.

이미지는 각 경로가 독립적으로 재다운로드한다(재다운 비용 무시 가능 — 일 ~1000컷).
"""
from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO

from PIL import Image

from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import delete_face_crop, fetch_cut_image, upload_face_crop
from src.operators.cut_merger import CutSegment, adjust_bbox_to_cut, split_cut_pair
from src.operators.ocr_yolo_client import run_ocr, run_yolo

FACE_PAD_RATIO = 0.15
FACE_CROP_SIZE = (112, 112)
_IOU_DEDUP_THRESHOLD = 0.5


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
