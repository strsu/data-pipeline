"""Step 1 Agent: OCR + YOLO → DB 저장 + face crop S3 업로드, episode.phase1.complete 발행."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from io import BytesIO

import faust
from PIL import Image

from src.config.db import db_cursor
from src.config.s3 import fetch_cut_image, upload_face_crop
from src.operators.ocr import run_ocr
from src.operators.yolo import detect_faces
from src.worker import app

FACE_PAD_RATIO = 0.15
FACE_CROP_SIZE = (112, 112)


# ── Kafka 메시지 스키마 ────────────────────────────────────────────────────────

class EpisodeStartMsg(faust.Record):
    source: str              # 'kakao' | 'naver'
    title_id: str            # 플랫폼 title ID
    episode_no: int          # 회차 번호
    webtoon_episode_id: int  # WebtoonEpisode DB PK


class EpisodePhase1Complete(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    total_cuts: int


class EpisodePhase1Error(faust.Record):
    source: str
    title_id: str
    episode_no: int
    webtoon_episode_id: int
    failed_cut: int
    error: str


# ── Kafka 토픽 ───────────────────────────────────────────────────────────────

cut_phase1_start = app.topic("cut.phase1.start", value_type=EpisodeStartMsg)
episode_phase1_complete = app.topic("episode.phase1.complete", value_type=EpisodePhase1Complete)
episode_phase1_error = app.topic("episode.phase1.error", value_type=EpisodePhase1Error)


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




def _save_to_db(
    webtoon_episode_id: int,
    cut_number: int,
    ocr_blocks: list[dict],
    faces: list[dict],
    image_bytes: bytes,
    source: str,
    title_id: str,
) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        # WebtoonCut upsert (재처리 멱등성)
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

        # 재처리 시 기존 데이터 초기화
        cur.execute("DELETE FROM text_annotation WHERE region_id IN (SELECT id FROM text_region WHERE cut_id = %s)", (cut_id,))
        cur.execute("DELETE FROM text_region WHERE cut_id = %s", (cut_id,))
        cur.execute("DELETE FROM face_record WHERE cut_id = %s", (cut_id,))

        # TextRegion + TextAnnotation(paddle)
        for idx, block in enumerate(ocr_blocks):
            bbox = block.get("bbox_2d") or [0, 0, 0, 0]
            cur.execute(
                """
                INSERT INTO text_region
                    (cut_id, index, bbox_x1, bbox_y1, bbox_x2, bbox_y2, is_excluded, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, false, %s, %s)
                RETURNING id
                """,
                (cut_id, idx, bbox[0], bbox[1], bbox[2], bbox[3], now, now),
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

        # FaceRecord + crop S3 업로드
        for idx, face in enumerate(faces):
            b = face["bbox"]
            cur.execute(
                """
                INSERT INTO face_record
                    (cut_id, face_idx, bbox_x1, bbox_y1, bbox_x2, bbox_y2, conf, chroma_doc_id, is_confirmed, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, '', false, %s, %s)
                RETURNING id
                """,
                (cut_id, idx, b[0], b[1], b[2], b[3], face["conf"], now, now),
            )
            face_record_id = cur.fetchone()[0]

            try:
                crop_bytes = _crop_face(image_bytes, b)
                upload_face_crop(face_record_id, source, title_id, crop_bytes)
            except Exception as e:
                print(f"[local_extract] face crop upload 실패 face_id={face_record_id}: {e}")


# ── Faust Agent ───────────────────────────────────────────────────────────────

@app.agent(cut_phase1_start)
async def local_extract_agent(stream):
    loop = asyncio.get_running_loop()

    async for msg in stream:
        cut = 1
        total = 0

        error_msg = None
        while True:
            try:
                image_bytes = await loop.run_in_executor(
                    None, fetch_cut_image, msg.source, msg.title_id, msg.episode_no, cut
                )
            except Exception as e:
                # 404는 fetch_cut_image 내부에서 None 반환. 여기에 도달하면 재시도 소진 후 S3 장애.
                error_msg = str(e)
                break

            if image_bytes is None:
                break

            try:
                ocr_blocks, faces = await asyncio.gather(
                    loop.run_in_executor(None, run_ocr, image_bytes),
                    loop.run_in_executor(None, detect_faces, image_bytes),
                )
                await loop.run_in_executor(
                    None, _save_to_db,
                    msg.webtoon_episode_id, cut, ocr_blocks, faces,
                    image_bytes, msg.source, msg.title_id,
                )
                total += 1
            except Exception as e:
                print(f"[local_extract] {msg.source}/{msg.title_id} ep={msg.episode_no} cut={cut} error: {e}")

            cut += 1

        if error_msg:
            await episode_phase1_error.send(
                key=f"{msg.source}_{msg.title_id}",
                value=EpisodePhase1Error(
                    source=msg.source,
                    title_id=msg.title_id,
                    episode_no=msg.episode_no,
                    webtoon_episode_id=msg.webtoon_episode_id,
                    failed_cut=cut,
                    error=error_msg,
                ),
            )
            continue

        await episode_phase1_complete.send(
            key=f"{msg.source}_{msg.title_id}",
            value=EpisodePhase1Complete(
                source=msg.source,
                title_id=msg.title_id,
                episode_no=msg.episode_no,
                webtoon_episode_id=msg.webtoon_episode_id,
                total_cuts=total,
            ),
        )
