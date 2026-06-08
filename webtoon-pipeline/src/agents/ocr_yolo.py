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
_IOU_DEDUP_THRESHOLD = 0.5


def _iou(a: list[float], b: list[float]) -> float:
    xi1, yi1 = max(a[0], b[0]), max(a[1], b[1])
    xi2, yi2 = min(a[2], b[2]), min(a[3], b[3])
    inter = max(0.0, xi2 - xi1) * max(0.0, yi2 - yi1)
    if inter == 0.0:
        return 0.0
    union = (a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - inter
    return inter / union if union > 0.0 else 0.0

# 에피소드 30개 처리 후 worker 교체 → paddle C++ 힙 메모리 전체 반납
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
    saved_face_bboxes: dict[int, list],
) -> tuple[int, int]:
    """단일 세그먼트에 OCR + YOLO를 실행하고 결과를 DB에 저장.

    region_index, face_index, saved_face_bboxes: episode 레벨 공유 상태
    반환: (저장된 OCR 영역 수, 저장된 얼굴 수)
    """
    ocr_blocks = run_ocr(segment.image_bytes)
    faces = detect_faces(segment.image_bytes)

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

            # 세그먼트 간 공유 카운터로 같은 cut_id 내 index 중복 방지
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
            # 얼굴 상단이 split 아래(cut[N+1] 내부)로 완전히 내려가면 skip →
            # 다음 라운드에서 경계를 걸친(또는 cut[N] 내부) 온전한 형태로 잡혀 저장된다.
            # split 위 또는 경계를 걸친 얼굴만 이 라운드에서 처리해 병합 뷰의 full crop을 확보한다.
            abs_y1 = segment.y_offset + face["bbox"][1]
            if abs_y1 >= segment.cut_n_height:
                continue

            # center 기준 단일 귀속 (bbox 좌표계도 center가 정한 컷에 맞춰 변환됨)
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

            # cross-boundary 얼굴이 다음 라운드에서 잘린 형태로 재검출될 때 제거.
            # 경계 걸친 얼굴(이전 라운드, full)과 cut[N] 상단 재검출(다음 라운드, 잘림)이
            # 같은 cut_id로 비교되어, 먼저 저장된 full crop만 남는다.
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
        # face_record별 S3 크롭 삭제 및 Chroma/FaceEmbedding 정리
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

    # (face_id → set of (model, doc_id)) 집계
    face_ids: set[int] = set()
    chroma_entries: dict[str, list[str]] = {}  # model → [doc_id, ...]
    for face_id, model, doc_id in rows:
        face_ids.add(face_id)
        if model and doc_id:
            chroma_entries.setdefault(model, []).append(doc_id)

    # S3 크롭 삭제
    for face_id in face_ids:
        try:
            delete_face_crop(face_id, source, title_id)
        except Exception as e:
            print(f"[ocr_yolo] S3 crop 삭제 실패 face_id={face_id}: {e}")

    # Chroma 항목 삭제
    for model, doc_ids in chroma_entries.items():
        try:
            collection = get_face_collection(source, title_id, model)
            collection.delete(ids=doc_ids)
        except Exception as e:
            print(f"[ocr_yolo] Chroma 삭제 실패 model={model}: {e}")

    # DB: FaceEmbedding → face_record 순서로 삭제
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

        # 재처리 시 기존 텍스트 데이터 초기화
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
    """에피소드 전체 컷을 처리. 반환: (total_cuts, error_msg or None)."""
    cut = 1
    total = 0
    total_ocr = 0
    total_faces = 0
    error_msg = None
    next_cut_bytes: bytes | None = None

    print(f"[ocr_yolo] {source}/{title_id} ep={episode_no} — 처리 시작")

    # 재처리 시 기존 face 데이터 일괄 정리 (OCR text는 _upsert_cut에서 per-cut 정리)
    _cleanup_episode_faces(webtoon_episode_id, source, title_id)

    # episode 레벨 공유 상태: round 간 face_idx 충돌 방지 + IoU 중복 체크
    face_index: dict[int, int] = {}
    saved_face_bboxes: dict[int, list] = {}

    # cut[1]을 미리 읽어둠
    try:
        next_cut_bytes = fetch_cut_image(source, title_id, episode_no, 1)
    except Exception as e:
        return 0, str(e)

    while True:
        cur_bytes = next_cut_bytes
        if cur_bytes is None:
            break

        # 다음 컷 미리 읽기 (병합용)
        try:
            next_cut_bytes = fetch_cut_image(source, title_id, episode_no, cut + 1)
        except Exception as e:
            error_msg = str(e)
            break

        now = datetime.now(timezone.utc)
        try:
            # cut[N] upsert + cut[N+1] row 생성 (cross-boundary face 저장 대상)
            _upsert_cut(webtoon_episode_id, cut, now, source, title_id)
            if next_cut_bytes is not None:
                _upsert_cut(webtoon_episode_id, cut + 1, now, source, title_id)

            segments = split_cut_pair(cur_bytes, next_cut_bytes)

            # OCR index는 per-round (text는 _upsert_cut에서 매 round 초기화)
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
        print(f"[ocr_yolo] {msg.source}/{msg.title_id} ep={msg.episode_no} — 메시지 수신")
        _update_phase1_status(msg.webtoon_episode_id, "running")

        try:
            total, error_msg = await loop.run_in_executor(
                _process_pool,
                _process_episode,
                msg.source,
                msg.title_id,
                msg.episode_no,
                msg.webtoon_episode_id,
            )
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
