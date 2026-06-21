"""Step 3 코어 — LLM 멀티모달 장면/화자 분석 (faust-free, 컷 단위 슬라이딩 윈도우).

활성 웹툰만(phase3) 실행. 컷 N에 대해:
  1) 컷 N의 paddle 텍스트 + 식별된 얼굴 로드
  2) 이미지 N-2/N-1/N 다운로드 + 현재 컷(N)에 얼굴 bbox/ID 오버레이
  3) LLM 1회 호출 → blocks(type/speaker/corrected_text) + scene_meta + name_discoveries
  4) 저장: TextAnnotation(source='llm'), cut_scene_meta, name_discoveries→Character, cut.llm_*
반환: 다음 컷에 넘길 prev_context(직전 컷 요약/마지막 대사).
LLM 모델/endpoint는 DB(LLMModel)에서 해석 — 코드에 모델명 하드코딩 없음(default=GLM).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2.extras import Json

from src.config.db import db_cursor
from src.config.s3 import fetch_cut_image
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import resolve_llm_model
from src.operators.overlay import overlay_faces

logger = logging.getLogger(__name__)

_NAME_AUTO_CONFIDENCE = 0.85

_SYSTEM_PROMPT = (
    "당신은 웹툰 컷을 분석하는 도우미입니다. 이미지(N-2, N-1, 현재 컷 N, 그리고 얼굴 bbox에 "
    "F0/F1 라벨을 오버레이한 현재 컷)와 OCR 텍스트, 식별된 얼굴 정보를 받습니다. "
    "현재 컷 N에 대해서만 결과를 JSON으로 출력하세요. 말풍선 꼬리와 오버레이된 얼굴 라벨을 매칭해 화자를 정하고, "
    "OCR 오타를 문맥에 맞게 교정하세요. 반드시 아래 JSON 스키마만 출력하세요:\n"
    '{"blocks":[{"index":<int>,"type":"narration|speech|sfx|caption|other",'
    '"speaker":"<이름 또는 null>","corrected_text":"<교정문>"}],'
    '"scene_meta":{"action_summary":"<현재 컷 줄거리>","key_objects":["..."]},'
    '"name_discoveries":[{"face_id":"F0","name":"<대사/나레이션에서 드러난 실제 이름>",'
    '"confidence":<0~1>,"evidence":"<근거>"}]}'
)


# ── 입력 로드 ─────────────────────────────────────────────────────────────────

def _get_webtoon_id(webtoon_episode_id: int) -> int:
    with db_cursor() as cur:
        cur.execute("SELECT webtoon_id FROM webtoon_episode WHERE id = %s", (webtoon_episode_id,))
        return cur.fetchone()[0]


def _cut_id(webtoon_episode_id: int, cut_number: int) -> Optional[int]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT id FROM webtoon_cut WHERE episode_id = %s AND cut_number = %s",
            (webtoon_episode_id, cut_number),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _load_regions(cut_id: int) -> list[dict]:
    """컷의 text_region + paddle 텍스트(index 순)."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT tr.id, tr.index, tr.bbox_x1, tr.bbox_y1, tr.bbox_x2, tr.bbox_y2,
                   ta.text
            FROM text_region tr
            LEFT JOIN text_annotation ta ON ta.region_id = tr.id AND ta.source = 'paddle'
            WHERE tr.cut_id = %s AND tr.is_excluded = false
            ORDER BY tr.index
            """,
            (cut_id,),
        )
        return [
            {"region_id": r[0], "index": r[1], "bbox": [r[2], r[3], r[4], r[5]], "text": r[6] or ""}
            for r in cur.fetchall()
        ]


def _load_faces(cut_id: int) -> list[dict]:
    """컷의 식별된 얼굴: id=F{face_idx}, bbox, 현재 캐릭터명, appearance_id."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.face_idx, fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2,
                   fr.appearance_id, c.name
            FROM face_record fr
            LEFT JOIN character_appearance ca ON fr.appearance_id = ca.id
            LEFT JOIN character c ON ca.character_id = c.id
            WHERE fr.cut_id = %s
            ORDER BY fr.face_idx
            """,
            (cut_id,),
        )
        return [
            {"id": f"F{r[0]}", "bbox": [r[1], r[2], r[3], r[4]], "appearance_id": r[5], "name": r[6]}
            for r in cur.fetchall()
        ]


# ── 저장 ──────────────────────────────────────────────────────────────────────

def _upsert_llm_annotation(region_id: int, block: dict, model_name: str) -> None:
    now = datetime.now(timezone.utc)
    btype = block.get("type")
    if btype not in ("narration", "speech", "sfx", "caption", "other"):
        btype = None
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO text_annotation
                (region_id, source, text, type, speaker, model_version, created_at, updated_at)
            VALUES (%s, 'llm', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (region_id, source)
            DO UPDATE SET text = EXCLUDED.text, type = EXCLUDED.type,
                          speaker = EXCLUDED.speaker, model_version = EXCLUDED.model_version,
                          updated_at = EXCLUDED.updated_at
            """,
            (region_id, block.get("corrected_text", ""), btype,
             block.get("speaker"), model_name, now, now),
        )


def _upsert_scene_meta(cut_id: int, scene_meta: dict) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO cut_scene_meta
                (cut_id, action_summary, key_objects, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cut_id)
            DO UPDATE SET action_summary = EXCLUDED.action_summary,
                          key_objects = EXCLUDED.key_objects, updated_at = EXCLUDED.updated_at
            """,
            (cut_id, scene_meta.get("action_summary", ""),
             Json(scene_meta.get("key_objects", [])), now, now),
        )


def _apply_name_discoveries(faces: list[dict], discoveries: list[dict]) -> None:
    """confidence>=0.85 → Character 이름 자동 지정(is_name_auto_assigned). 미만 → extra에 제안 적재."""
    face_by_id = {f["id"]: f for f in faces}
    now = datetime.now(timezone.utc)
    for d in discoveries or []:
        face = face_by_id.get(d.get("face_id"))
        if not face or not face.get("appearance_id"):
            continue
        name = (d.get("name") or "").strip()
        if not name:
            continue
        conf = float(d.get("confidence", 0) or 0)
        with db_cursor() as cur:
            cur.execute(
                """
                SELECT c.id, c.name, c.is_confirmed, c.extra
                FROM character c JOIN character_appearance ca ON ca.character_id = c.id
                WHERE ca.id = %s
                """,
                (face["appearance_id"],),
            )
            row = cur.fetchone()
            if not row:
                continue
            char_id, cur_name, is_confirmed, extra = row
            extra = extra if isinstance(extra, dict) else {}
            if conf >= _NAME_AUTO_CONFIDENCE and not is_confirmed and (cur_name or "").startswith("NEW_CHAR_"):
                # 자동 지정 (human 미확정 상태로 — UI에서 검토)
                cur.execute(
                    "UPDATE character SET name=%s, is_name_auto_assigned=true, updated_at=%s WHERE id=%s",
                    (name[:64], now, char_id),
                )
            else:
                # 제안 큐(전용 테이블 없음 → extra.name_suggestions에 적재)
                sugg = extra.get("name_suggestions") or []
                sugg.append({"name": name, "confidence": conf, "evidence": d.get("evidence", "")})
                extra["name_suggestions"] = sugg[-20:]
                cur.execute(
                    "UPDATE character SET extra=%s, updated_at=%s WHERE id=%s",
                    (Json(extra), now, char_id),
                )


def _mark_cut_analyzed(cut_id: int, llm_model_id: Optional[int]) -> None:
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            "UPDATE webtoon_cut SET llm_analyzed_at=%s, llm_model_id=%s, is_stale=false, updated_at=%s WHERE id=%s",
            (now, llm_model_id, now, cut_id),
        )


# ── 진입점 ────────────────────────────────────────────────────────────────────

def analyze_cut_scene(
    source: str, title_id: str, episode_no: int, webtoon_episode_id: int,
    cut_number: int, prev_context: str,
) -> str:
    """컷 N을 LLM으로 분석·저장하고 다음 컷용 prev_context를 반환."""
    cut_id = _cut_id(webtoon_episode_id, cut_number)
    if cut_id is None:
        return prev_context

    webtoon_id = _get_webtoon_id(webtoon_episode_id)
    ctx = resolve_llm_model(webtoon_id)

    regions = _load_regions(cut_id)
    faces = _load_faces(cut_id)

    cur_img = fetch_cut_image(source, title_id, episode_no, cut_number)
    if cur_img is None:
        return prev_context
    images: list[bytes] = []
    for n in (cut_number - 2, cut_number - 1):
        if n >= 1:
            img = fetch_cut_image(source, title_id, episode_no, n)
            if img is not None:
                images.append(img)
    images.append(cur_img)
    images.append(overlay_faces(cur_img, faces))  # 오버레이된 현재 컷

    user_text = json.dumps({
        "prev_context": prev_context,
        "identified_faces": [{"id": f["id"], "name": f["name"], "bbox": f["bbox"]} for f in faces],
        "ocr_blocks": [{"index": r["index"], "text": r["text"], "bbox_2d": r["bbox"]} for r in regions],
    }, ensure_ascii=False)

    result = call_llm_json(ctx, _SYSTEM_PROMPT, user_text, images)

    # 저장
    region_by_index = {r["index"]: r["region_id"] for r in regions}
    for block in result.get("blocks", []):
        rid = region_by_index.get(block.get("index"))
        if rid is not None:
            _upsert_llm_annotation(rid, block, ctx["name"])
    scene_meta = result.get("scene_meta") or {}
    _upsert_scene_meta(cut_id, scene_meta)
    _apply_name_discoveries(faces, result.get("name_discoveries"))
    _mark_cut_analyzed(cut_id, ctx.get("id"))

    logger.info("[step3] %s/%s ep=%s cut=%s — blocks=%s discoveries=%s",
                source, title_id, episode_no, cut_number,
                len(result.get("blocks", [])), len(result.get("name_discoveries", [])))

    return scene_meta.get("action_summary", "") or prev_context
