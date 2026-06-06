"""Stage A Agent: OCR + YOLO → DB 저장 + face crop S3 업로드, episode.phase1a.complete 발행.

컷 처리 시 인접 컷(cut[N+1])을 미리 읽어 자연 구분선 기반 분할을 수행한다.
분할 후 각 세그먼트에 OCR + YOLO를 실행하고 bbox를 원본 컷 좌표로 변환해 저장한다.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from io import BytesIO

import faust
from PIL import Image

from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import delete_face_crop, fetch_cut_image, upload_face_crop
from src.operators.cut_merger import CutSegment, adjust_bbox_to_cut, split_cut_pair
from src.operators.ocr import run_ocr
from src.operators.yolo import detect_faces
from src.worker import app

FACE_PAD_RATIO = 0.15
FACE_CROP_SIZE = (112, 112)

# 컷 5개 처리 후 worker 교체 → paddle C++ 힙 메모리 전체 반납
# OOM이 컷 ~10개에서 발생하므로 5로 설정
_process_pool = ProcessPoolExecutor(max_workers=1, max_tasks_per_child=5)


# ── Kafka 메시지 스키마 ────────────────────────────────────────────────────────

class EpisodeStartMsg(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int


class EpisodePhase1aComplete(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    total_cuts: int


class EpisodePhase1aError(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    failed_cut: int
    error: str


# ── Kafka 토픽 ───────────────────────────────────────────────────────────────

cut_phase1_start = app.topic("cut.phase1.start", value_type=EpisodeStartMsg)
episode_phase1a_complete = app.topic("episode.phase1a.complete", value_type=EpisodePhase1aComplete)
episode_phase1a_error = app.topic("episode.phase1a.error", value_type=EpisodePhase1aError)


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

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


def _process_segment(
    segment: CutSegment,
    webtoon_episode_id: int,
    cut_number_n: int,
    source: str,
    title_id: str,
    region_index: dict[int, int],
    face_index: dict[int, int],
) -> tuple[int, int]:
    """단일 세그먼트에 OCR + YOLO를 실행하고 결과를 DB에 저장."""
    ocr_blocks = run_ocr(segment.image_bytes)
    faces = detect_faces(segment.image_bytes)

    saved_ocr = 0
    saved_faces = 0
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        for block in ocr_blocks:
            raw_bbox = block.get("bbox_2d") or [0, 0, 0, 0]
            adjusted_bbox, cut_offset = adjust_bbox_to_cut(
                [raw_bbox[0], raw_bbox[1], raw_bbox[2], raw_bbox[3]], segment
            )
            actual_cut = cut_number_n + cut_offset

            cur.execute(
                "SELECT id FROM webtoon_cut WHERE episode_id = %s AND cut_number = %s",
                (webtoon_episode_id, actual_cut),
            )
            row = cur.fetchone()
            if not row:
                continue
            cut_id = row[0]

            idx = region_index.get(cut_id, 0)
            region_index[cut_id] = idx + 1

            cur.execute(
                """
                INSERT INTO text_region
                    (cut_id, index, bbox_x1, bbox_y1, bbox_x2, bbox_y2, is_excluded, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, false, %s, %s)
                RETURNING id
                """,
                (cut_id, idx, adjusted_bbox[0], adjusted_bbox[1],
                 adjusted_bbox[2], adjusted_bbox[3], now, now),
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
            saved_ocr += 1

        for face in faces:
            adjusted_bbox, cut_offset = adjust_bbox_to_cut(face["bbox"], segment)
            actual_cut = cut_number_n + cut_offset

            cur.execute(
                "SELECT id FROM webtoon_cut WHERE episode_id = %s AND cut_number = %s",
                (webtoon_episode_id, actual_cut),
            )
            row = cur.fetchone()
            if not row:
                continue
            cut_id = row[0]

            face_idx = face_index.get(cut_id, 0)
            face_index[cut_id] = face_idx + 1

            b = adjusted_bbox
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
            result = cur.fetchone()
            if not result:
                continue
            face_record_id = result[0]
            saved_faces += 1

            try:
                crop_bytes = _crop_face(segment.image_bytes, face["bbox"])
                upload_face_crop(face_record_id, source, title_id, crop_bytes)
            except Exception as e:
                print(f"[ocr_yolo] face crop upload 실패 face_id={face_record_id}: {e}")

    return saved_ocr, saved_faces


def _cleanup_cut_faces(cut_id: int, source: str, title_id: str) -> None:
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
            print(f"[ocr_yolo] S3 crop 삭제 실패 face_id={face_id}: {e}")

    for model, doc_ids in chroma_entries.items():
        try:
            collection = get_face_collection(source, title_id, model)
            collection.delete(ids=doc_ids)
        except Exception as e:
            print(f"[ocr_yolo] Chroma 삭제 실패 model={model}: {e}")

    if face_ids:
        with db_cursor() as cur:
            cur.execute(
                "DELETE FROM face_embedding WHERE face_record_id = ANY(%s)",
                (list(face_ids),),
            )
            cur.execute(
                "DELETE FROM face_record WHERE id = ANY(%s)",
                (list(face_ids),),
            )


def _upsert_cut(webtoon_episode_id: int, cut_number: int, now: datetime, source: str, title_id: str) -> int:
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
        cut_id = cur.fetchone()[0]
        cur.execute(
            "DELETE FROM text_annotation WHERE region_id IN (SELECT id FROM text_region WHERE cut_id = %s)",
            (cut_id,),
        )
        cur.execute("DELETE FROM text_region WHERE cut_id = %s", (cut_id,))

    _cleanup_cut_faces(cut_id, source, title_id)
    return cut_id


def _update_phase1_status(webtoon_episode_id: int, status: str) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE webtoon_pipeline_state
            SET phase1_status = %s, updated_at = %s
            WHERE webtoon_id = (
                SELECT webtoon_id FROM webtoon_episode WHERE id = %s
            )
            """,
            (status, now, webtoon_episode_id),
        )


# ── 컷 단위 처리 (에피소드 루프는 agent에서 진행) ─────────────────────────────

def _process_single_cut(
    source: str,
    title_id: str,
    episode_no: int,
    webtoon_episode_id: int,
    cut_no: int,
) -> tuple[int, int] | None:
    """컷 1개의 OCR+YOLO 처리. 컷이 없으면 None, 있으면 (ocr_count, face_count) 반환."""
    cur_bytes = fetch_cut_image(source, title_id, episode_no, cut_no)
    if cur_bytes is None:
        return None

    next_bytes = fetch_cut_image(source, title_id, episode_no, cut_no + 1)

    now = datetime.now(timezone.utc)
    _upsert_cut(webtoon_episode_id, cut_no, now, source, title_id)
    if next_bytes is not None:
        _upsert_cut(webtoon_episode_id, cut_no + 1, now, source, title_id)

    segments = split_cut_pair(cur_bytes, next_bytes)
    region_index: dict[int, int] = {}
    face_index: dict[int, int] = {}
    ocr_total = 0
    face_total = 0
    for segment in segments:
        o, f = _process_segment(
            segment, webtoon_episode_id, cut_no,
            source, title_id, region_index, face_index,
        )
        ocr_total += o
        face_total += f

    return ocr_total, face_total


# ── Faust Agent ───────────────────────────────────────────────────────────────

@app.agent(cut_phase1_start)
async def ocr_yolo_agent(stream):
    loop = asyncio.get_running_loop()

    async for msg in stream:
        print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} — 메시지 수신")
        _update_phase1_status(msg.webtoon_episode_id, "running")

        cut = 1
        total = 0
        total_ocr = 0
        total_faces = 0
        error_msg = None

        try:
            while True:
                result = await loop.run_in_executor(
                    _process_pool,
                    _process_single_cut,
                    msg.source,
                    msg.title_id,
                    msg.episode_no,
                    msg.webtoon_episode_id,
                    cut,
                )
                if result is None:
                    break
                ocr_n, face_n = result
                total_ocr += ocr_n
                total_faces += face_n
                total += 1
                print(
                    f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} cut={cut}"
                    f" — 텍스트 영역 {ocr_n}개 | 얼굴 {face_n}개"
                )
                cut += 1
        except Exception as e:
            error_msg = str(e)
            print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} cut={cut} 오류: {e}")

        print(
            f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} —"
            f" 완료: {total}컷 | 텍스트 {total_ocr}개 | 얼굴 {total_faces}개"
        )

        if error_msg:
            await episode_phase1a_error.send(
                key=f"{msg.source}_{msg.title_id}",
                value=EpisodePhase1aError(
                    source=msg.source,
                    title_id=msg.title_id,
                    episode_no=msg.episode_no,
                    webtoon_episode_id=msg.webtoon_episode_id,
                    failed_cut=cut,
                    error=error_msg,
                ),
            )
            _update_phase1_status(msg.webtoon_episode_id, "error")
            continue

        _update_phase1_status(msg.webtoon_episode_id, "completed")
        await episode_phase1a_complete.send(
            key=f"{msg.source}_{msg.title_id}",
            value=EpisodePhase1aComplete(
                source=msg.source,
                title_id=msg.title_id,
                episode_no=msg.episode_no,
                webtoon_episode_id=msg.webtoon_episode_id,
                total_cuts=total,
            ),
        )
