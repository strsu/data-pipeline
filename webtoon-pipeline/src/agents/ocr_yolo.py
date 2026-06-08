"""Stage A Agent: OCR + YOLO → DB 저장 + face crop S3 업로드, episode.phase1a.complete 발행.

컷 처리 시 인접 컷(cut[N+1])을 미리 읽어 자연 구분선 기반 분할을 수행한다.
분할 후 각 세그먼트에 OCR + YOLO를 실행하고 bbox를 원본 컷 좌표로 변환해 저장한다.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from io import BytesIO

import faust
import httpx
from PIL import Image

from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.s3 import delete_face_crop, fetch_cut_image, upload_face_crop
from src.operators.cut_merger import CutSegment, adjust_bbox_to_cut, split_cut_pair
from src.operators.ocr_yolo_client import run_ocr_yolo
from src.worker import app

FACE_PAD_RATIO = 0.15
FACE_CROP_SIZE = (112, 112)
_IOU_DEDUP_THRESHOLD = 0.5


def _iou(a: list[float], b: list[float]) -> float:
    xi1, yi1 = max(a[0], b[0]), max(a[1], b[1])
    xi2, yi2 = min(a[2], b[2]), min(a[3], b[3])
    inter = max(0.0, xi2 - xi1) * max(0.0, yi2 - yi1)
    if inter == 0.0:
        return 0.0
    union = (a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - inter
    return inter / union if union > 0.0 else 0.0


# ── Kafka 메시지 스키마 ────────────────────────────────────────────────────────

class EpisodeStartMsg(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    retry_count: int = 0


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
    saved_face_bboxes: dict[int, list],
) -> tuple[int, int]:
    """단일 세그먼트에 OCR + YOLO를 실행하고 결과를 DB에 저장.

    httpx.HTTPError는 그대로 전파 → 에이전트 레벨에서 Kafka 재큐 처리.
    """
    ocr_blocks, faces = run_ocr_yolo(segment.image_bytes)

    saved_ocr = 0
    saved_faces = 0
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        # OCR 블록 저장
        for block in ocr_blocks:
            raw_bbox = block.get("bbox_2d") or [0, 0, 0, 0]
            adjusted_bbox, _ = adjust_bbox_to_cut(
                [raw_bbox[0], raw_bbox[1], raw_bbox[2], raw_bbox[3]], segment
            )
            abs_y_center = segment.y_offset + (raw_bbox[1] + raw_bbox[3]) / 2
            if abs_y_center >= segment.cut_n_height:
                continue
            actual_cut = cut_number_n

            cur.execute(
                """
                SELECT id FROM webtoon_cut
                WHERE episode_id = %s AND cut_number = %s
                """,
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

        # 얼굴 저장
        for face in faces:
            abs_y1 = segment.y_offset + face["bbox"][1]
            if abs_y1 >= segment.cut_n_height:
                continue

            adjusted_bbox, cut_offset = adjust_bbox_to_cut(face["bbox"], segment)
            actual_cut = cut_number_n + cut_offset

            cur.execute(
                """
                SELECT id FROM webtoon_cut
                WHERE episode_id = %s AND cut_number = %s
                """,
                (webtoon_episode_id, actual_cut),
            )
            row = cur.fetchone()
            if not row:
                continue
            cut_id = row[0]

            norm = [adjusted_bbox[0], max(0.0, adjusted_bbox[1]),
                    adjusted_bbox[2], max(0.0, adjusted_bbox[3])]
            if any(_iou(norm, s) >= _IOU_DEDUP_THRESHOLD for s in saved_face_bboxes.get(cut_id, [])):
                continue
            saved_face_bboxes.setdefault(cut_id, []).append(norm)

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
    """기존 컷의 face_record + S3 크롭 + Chroma + FaceEmbedding을 모두 제거한다."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id,
                   fe.embedding_model,
                   fe.chroma_doc_id
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

    return cut_id


def _cleanup_episode_faces(webtoon_episode_id: int, source: str, title_id: str) -> None:
    """에피소드 시작 시 전체 face 데이터 일괄 정리 (재처리 지원)."""
    with db_cursor() as cur:
        cur.execute("SELECT id FROM webtoon_cut WHERE episode_id = %s", (webtoon_episode_id,))
        cut_ids = [row[0] for row in cur.fetchall()]
    for cut_id in cut_ids:
        _cleanup_cut_faces(cut_id, source, title_id)


def _get_last_processed_cut(webtoon_episode_id: int) -> int:
    """가장 마지막으로 처리 완료된 컷 번호를 반환한다. 없으면 0."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(cut_number), 0)
            FROM webtoon_cut
            WHERE episode_id = %s AND processed_at IS NOT NULL
            """,
            (webtoon_episode_id,),
        )
        return cur.fetchone()[0]


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


def _process_episode(source: str, title_id: str, episode_no: int, webtoon_episode_id: int) -> tuple[int, str | None]:
    """에피소드 전체 컷을 처리. 반환: (total_cuts, error_msg or None).

    httpx.HTTPError는 잡지 않고 전파 → 에이전트가 Kafka 재큐 처리.
    """
    # 체크포인트: OOM 재시작 시 마지막 처리 컷 다음부터 재개
    last_processed = _get_last_processed_cut(webtoon_episode_id)
    resume_from = last_processed + 1 if last_processed > 0 else 1

    cut = resume_from
    total = 0
    total_ocr = 0
    total_faces = 0
    error_msg = None
    next_cut_bytes: bytes | None = None

    print(f"[ocr_yolo] {source}/{title_id} ep={episode_no} — 처리 시작 (cut={resume_from}부터)")

    # 처음부터 시작할 때만 기존 face 데이터 정리
    if resume_from == 1:
        _cleanup_episode_faces(webtoon_episode_id, source, title_id)

    face_index: dict[int, int] = {}
    saved_face_bboxes: dict[int, list] = {}

    try:
        next_cut_bytes = fetch_cut_image(source, title_id, episode_no, resume_from)
    except Exception as e:
        return 0, str(e)

    while True:
        cur_bytes = next_cut_bytes
        if cur_bytes is None:
            break

        try:
            next_cut_bytes = fetch_cut_image(source, title_id, episode_no, cut + 1)
        except Exception as e:
            error_msg = str(e)
            break

        now = datetime.now(timezone.utc)
        try:
            _upsert_cut(webtoon_episode_id, cut, now, source, title_id)
            if next_cut_bytes is not None:
                _upsert_cut(webtoon_episode_id, cut + 1, now, source, title_id)

            segments = split_cut_pair(cur_bytes, next_cut_bytes)

            region_index: dict[int, int] = {}

            cut_ocr = 0
            cut_faces = 0
            for segment in segments:
                ocr_n, face_n = _process_segment(
                    segment, webtoon_episode_id, cut,
                    source, title_id, region_index, face_index, saved_face_bboxes,
                )
                cut_ocr += ocr_n
                cut_faces += face_n

            total_ocr += cut_ocr
            total_faces += cut_faces
            total += 1

            print(
                f"[ocr_yolo] {source}/{title_id} ep={episode_no} cut={cut} "
                f"— 세그먼트 {len(segments)}개 | 텍스트 영역 {cut_ocr}개 | 얼굴 {cut_faces}개"
            )
        except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError):
            raise  # 에이전트 레벨에서 Kafka 재큐 처리
        except Exception as e:
            print(f"[ocr_yolo] {source}/{title_id} ep={episode_no} cut={cut} 오류: {e}")

        cut += 1

    print(
        f"[ocr_yolo] {source}/{title_id} ep={episode_no} — "
        f"완료: {total}컷 처리 | 텍스트 영역 총 {total_ocr}개 | 얼굴 총 {total_faces}개"
    )
    return total, error_msg


# ── Faust Agent ───────────────────────────────────────────────────────────────

@app.agent(cut_phase1_start, concurrency=1)
async def ocr_yolo_agent(stream):
    loop = asyncio.get_running_loop()

    async for msg in stream:
        print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} — 메시지 수신 (retry={msg.retry_count})")

        if msg.retry_count >= 5:
            print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} — retry 상한 초과, 에러 처리")
            await episode_phase1a_error.send(
                key=f"{msg.source}_{msg.title_id}",
                value=EpisodePhase1aError(
                    source=msg.source,
                    title_id=msg.title_id,
                    episode_no=msg.episode_no,
                    webtoon_episode_id=msg.webtoon_episode_id,
                    failed_cut=0,
                    error="model-api retry limit exceeded",
                ),
            )
            _update_phase1_status(msg.webtoon_episode_id, "error")
            continue

        _update_phase1_status(msg.webtoon_episode_id, "running")

        try:
            total, error_msg = await loop.run_in_executor(
                None,
                _process_episode,
                msg.source,
                msg.title_id,
                msg.episode_no,
                msg.webtoon_episode_id,
            )
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            # 인프라 일시 장애 (model-api 기동 중 등) → retry_count 소진 없이 재큐
            print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} model-api 미응답: {e}, 재큐")
            await cut_phase1_start.send(
                key=f"{msg.source}_{msg.title_id}",
                value=EpisodeStartMsg(
                    source=msg.source,
                    title_id=msg.title_id,
                    episode_no=msg.episode_no,
                    webtoon_episode_id=msg.webtoon_episode_id,
                    retry_count=msg.retry_count,  # 카운트 유지
                ),
            )
            continue
        except httpx.HTTPStatusError as e:
            # model-api 가 5xx 반환 → retry_count 증가
            print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} model-api 오류 {e.response.status_code}: {e}, 재큐")
            await cut_phase1_start.send(
                key=f"{msg.source}_{msg.title_id}",
                value=EpisodeStartMsg(
                    source=msg.source,
                    title_id=msg.title_id,
                    episode_no=msg.episode_no,
                    webtoon_episode_id=msg.webtoon_episode_id,
                    retry_count=msg.retry_count + 1,
                ),
            )
            continue
        except Exception as e:
            print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} 치명적 오류: {e}")
            _update_phase1_status(msg.webtoon_episode_id, "error")
            continue

        if error_msg:
            print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} 에러 종료: {error_msg}")
            await episode_phase1a_error.send(
                key=f"{msg.source}_{msg.title_id}",
                value=EpisodePhase1aError(
                    source=msg.source,
                    title_id=msg.title_id,
                    episode_no=msg.episode_no,
                    webtoon_episode_id=msg.webtoon_episode_id,
                    failed_cut=0,
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
