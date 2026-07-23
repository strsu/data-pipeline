"""세그먼트-단위 Stage V (Phase B2/B3) — 컷 대신 세그먼트(콘텐츠 밴드)를 비전 분석단위로.

`segment_unit_enabled` 웹툰에서 `extract_episode`를 대체한다(activities에서 게이트, Phase D).
비전 입력만 세그로 바뀌고, 출력은 기존과 동일한 계약:
- 블록 → provisional annotation(region_id 키, 무변경)
- 장면 → `analysis_segment_scene_meta`(세그 키, cut_scene_meta의 세그 대응)
- belief/ExtractResult → 레코드의 `cut_number`가 **세그 index**, 블록 index가 **세그-로컬 읽기순**.
  Pass-2a/apply는 이를 (segment_index, index)로 해석(Phase C 세그-키잉).

step3의 헬퍼(build_pass1_input/_sanitize_pass1/belief/usage/teacher)를 그대로 재사용 —
입력 포맷·다운스트림 drift 방지.
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from psycopg2.extras import Json

from src.config.db import db_cursor
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import resolve_llm_model, VISION
from src.core.segment_loader import load_segment_units, SegUnit
from src.core import step3
from src.core.step3 import (
    Pass1Record, ExtractResult,
    build_pass1_input, _sanitize_pass1, _pass1_ctx, _PASS1_SYSTEM_PROMPT,
    _upsert_provisional_annotation, _provisional_speaker_id, _record_llm_sample,
    _insert_llm_usage, _accumulate_belief, _init_belief, prepare_episode_scene,
    _get_webtoon_id, _flow_first_enabled, _SPEAKER_TYPES, _PASS1_RETRIES, _PASS1_STAGE,
    _BLOCK_TYPES,
)

logger = logging.getLogger(__name__)


def segment_unit_enabled(webtoon_id: int) -> bool:
    """세그먼트-단위 분석 게이트 — config_webtoon_pipeline_state.segment_unit_enabled."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT COALESCE(segment_unit_enabled, false) FROM config_webtoon_pipeline_state "
            "WHERE webtoon_id = %s",
            (webtoon_id,),
        )
        row = cur.fetchone()
    return bool(row[0]) if row else False


# ── Phase C: 다운스트림 세그-키잉 (resolve/apply가 (segment_index, seg읽기순)로 블록 식별) ──

def episode_region_map_and_ranges(webtoon_episode_id: int):
    """apply/reresolve용 — (region_map, seg_cut_ranges).

    region_map: {(segment_index, seg읽기순_index): region_id}. resolve LLM이 낸 speaker_resolution
      ref (cut=segment_index, block_index=seg읽기순)를 region으로 되매핑(_commit_speaker_resolution).
    seg_cut_ranges: {segment_index: (cut_start, cut_end)}. beats(세그index 공간)를 컷범위로 환산용.
    로더(load_segment_units)를 재사용 — fresh extract와 **동일 순서** 보장(키 재현성).
    """
    units = load_segment_units(webtoon_episode_id)
    region_map = {(u.index, r["index"]): r["region_id"] for u in units for r in u.regions}
    seg_cut_ranges = {u.index: (min(u.cuts), max(u.cuts)) for u in units if u.cuts}
    return region_map, seg_cut_ranges


def remap_beats_to_cuts(beats: list[dict], seg_cut_ranges: dict) -> list[dict]:
    """resolve가 낸 beats의 cut_start/cut_end(세그 index 공간) → 실제 컷 범위로 환산(C4).

    세그 index를 구성 컷 범위로 편다: cut_start=해당 세그 첫 컷, cut_end=해당 세그 마지막 컷.
    범위 밖/미매핑 세그는 원값 유지(안전). 리포트는 컷 범위 유지(정석 결정).
    """
    if not seg_cut_ranges:
        return beats
    out = []
    for b in beats or []:
        nb = dict(b)
        cs, ce = b.get("cut_start"), b.get("cut_end")
        if cs in seg_cut_ranges:
            nb["cut_start"] = seg_cut_ranges[cs][0]
        if ce in seg_cut_ranges:
            nb["cut_end"] = seg_cut_ranges[ce][1]
        out.append(nb)
    return out


def load_pass1_records_segment(webtoon_episode_id: int) -> list[Pass1Record]:
    """reresolve(rerun_extract=False)용 — 영속 주석/segment_scene_meta에서 세그-키잉 레코드 재구성.

    _load_pass1_records_from_db의 세그 대응. 로더로 유닛 구조(리전 읽기순·얼굴)를 얻고, 각 리전의
    'llm' 주석으로 블록을 복원(index=seg읽기순, 재현성). scene=segment_scene_meta.
    """
    units = load_segment_units(webtoon_episode_id)
    all_rids = [r["region_id"] for u in units for r in u.regions]
    ann: dict[int, dict] = {}
    if all_rids:
        with db_cursor() as cur:
            cur.execute(
                "SELECT region_id, type, text, speaker_id FROM analysis_text_annotation "
                "WHERE region_id = ANY(%s) AND source = 'llm'",
                (all_rids,),
            )
            for rid, btype, text, sid in cur.fetchall():
                ann[rid] = {"type": btype, "text": text, "speaker_id": sid}
    seg_ids = [u.segment_id for u in units]
    scenes: dict[int, dict] = {}
    if seg_ids:
        with db_cursor() as cur:
            cur.execute(
                "SELECT segment_id, action_summary, key_objects FROM analysis_segment_scene_meta "
                "WHERE segment_id = ANY(%s)",
                (seg_ids,),
            )
            for sid, summ, ko in cur.fetchall():
                scenes[sid] = {"action_summary": summ or "", "key_objects": ko or []}

    records: list[Pass1Record] = []
    for u in units:
        blocks = []
        for r in u.regions:
            a = ann.get(r["region_id"])
            btype = a["type"] if a else None
            if btype not in _BLOCK_TYPES:
                btype = None
            blocks.append({
                "index": r["index"], "type": btype, "type_confidence": 0.0,
                "corrected_text": (a["text"] if a and a["text"] else r["text"]),
                "speaker": {"face_label": None, "name": None,
                            "character_id": (a["speaker_id"] if a else None),
                            "confidence": 0.0, "basis": "none", "tail_hint": "none"},
            })
        if not blocks and not u.faces:
            records.append(Pass1Record(cut_number=u.index, cut_id=None, faces=u.faces, skipped="empty"))
            continue
        scene = scenes.get(u.segment_id, {"action_summary": "", "key_objects": []})
        result = {
            "cut_summary": scene["action_summary"], "key_objects": scene["key_objects"],
            "characters": [{"face_label": f["id"], "prominence": None, "emotion": ""} for f in u.faces],
            "blocks": blocks, "name_evidence": [],
        }
        records.append(Pass1Record(cut_number=u.index, cut_id=None, result=result, faces=u.faces))
    return records


def _upsert_segment_scene_meta(segment_id: int, scene_meta: dict, run_id: Optional[int] = None) -> None:
    """segment_scene_meta upsert(OneToOne) — 세그-단위 Stage V 산출 + run 귀속."""
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO analysis_segment_scene_meta
                (segment_id, action_summary, key_objects, run_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (segment_id)
            DO UPDATE SET action_summary = EXCLUDED.action_summary,
                          key_objects = EXCLUDED.key_objects, run_id = EXCLUDED.run_id,
                          updated_at = EXCLUDED.updated_at
            """,
            (segment_id, scene_meta.get("action_summary", ""),
             Json(scene_meta.get("key_objects", [])), run_id, now, now),
        )


def _clear_episode_segment_scene(webtoon_episode_id: int) -> None:
    """재실행 멱등 — 이 회차의 segment_scene_meta 제거(prepare가 llm 주석·cut_scene_meta 처리)."""
    with db_cursor() as cur:
        cur.execute(
            """
            DELETE FROM analysis_segment_scene_meta
            WHERE segment_id IN (SELECT id FROM analysis_episode_segment WHERE episode_id = %s)
            """,
            (webtoon_episode_id,),
        )


def extract_segment(
    seg: SegUnit, webtoon_episode_id: int, *,
    webtoon_id: int, ctx: dict, run_id: Optional[int] = None,
    anonymize: bool = False, persist: bool = True,
) -> Pass1Record:
    """세그먼트 1개를 비전 LLM 1콜로 분석 → Pass-1 레코드(cut_number=세그 index).

    extract_cut의 세그 대응. 이미지=세그 크롭, 오버레이/ocr_blocks=세그-로컬. 블록 index는
    세그-로컬 읽기순(로더가 부여) — region_id로 매핑해 provisional annotation 적재.
    """
    seg_ix = seg.index
    if not seg.regions and not seg.faces:
        return Pass1Record(cut_number=seg_ix, cut_id=None, faces=seg.faces, skipped="empty")
    if not seg.image_bytes:
        return Pass1Record(cut_number=seg_ix, cut_id=None, faces=seg.faces, skipped="no_image")

    call_ctx = _pass1_ctx(ctx)
    overlay_img, user_text = build_pass1_input(
        seg.image_bytes, seg.faces, seg.regions, anonymize=anonymize
    )

    raw_result: dict = {}
    usage: dict = {}
    err: Optional[str] = None
    call = None
    for _attempt in range(_PASS1_RETRIES):
        try:
            call = call_llm_json(call_ctx, _PASS1_SYSTEM_PROMPT, user_text, [overlay_img])
            raw_result = call.result if isinstance(call.result, dict) else {}
            usage = call.usage or {}
            err = None
            break
        except Exception as e:  # noqa: BLE001 — 세그 단위 격리(run 중단 금지)
            err = str(e)
            raw_result = {}
    if err is not None:
        logger.warning("[step3.seg] ep=%s seg=%s — 비전 콜 실패(스킵): %s",
                       webtoon_episode_id, seg_ix, err)
        return Pass1Record(cut_number=seg_ix, cut_id=None, faces=seg.faces, usage=usage, error=err)

    result = _sanitize_pass1(raw_result, seg.regions)

    # teacher 입출력 수집 — 이미지 ref=세그(구성 컷+strip 범위로 재구성 가능).
    _record_llm_sample(
        call_ctx, stage=_PASS1_STAGE, system_prompt=_PASS1_SYSTEM_PROMPT, user_text=user_text,
        image_refs=[{"segment_id": seg.segment_id, "cuts": seg.cuts,
                     "strip_y1": seg.strip_y1, "strip_y2": seg.strip_y2, "kind": "pass1_seg_overlay"}],
        raw_output=getattr(call, "raw_text", "") or "", repaired=getattr(call, "repaired", False),
        finish_reason=(usage or {}).get("finish_reason"),
        webtoon_id=webtoon_id, episode_id=webtoon_episode_id, cut_id=None, run_id=run_id,
    )

    if persist:
        rid_by_index = {r["index"]: r["region_id"] for r in seg.regions}
        for block in result["blocks"]:
            rid = rid_by_index.get(block["index"])
            if rid is not None:
                _upsert_provisional_annotation(
                    rid, block, call_ctx["name"],
                    speaker_id=_provisional_speaker_id(block, seg.faces), run_id=run_id,
                )
        _upsert_segment_scene_meta(seg.segment_id, {
            "action_summary": result["cut_summary"],
            "key_objects": result["key_objects"],
        }, run_id=run_id)

    provisional_speakers = [
        {"cut": seg_ix, "block_index": b["index"],
         "face_label": b["speaker"]["face_label"], "name": b["speaker"]["name"],
         "confidence": b["speaker"]["confidence"], "basis": b["speaker"]["basis"]}
        for b in result["blocks"] if b["type"] in _SPEAKER_TYPES
    ]
    return Pass1Record(
        cut_number=seg_ix, cut_id=None, result=result, faces=seg.faces,
        name_evidence=result.get("name_evidence", []),
        provisional_speakers=provisional_speakers, usage=usage,
    )


def extract_episode_segment(
    webtoon_episode_id: int, heartbeat_cb=None, *,
    prepare: bool = True, run_id: Optional[int] = None,
) -> ExtractResult:
    """에피소드를 세그먼트 단위로 Pass-1 순회 추출(step3a 세그 대응). extract_episode의 대체.

    레코드는 세그 읽기순(episode_segment.index). `cut_number`=세그 index, `cut_id`=None.
    belief 누적/usage 적재는 extract_episode와 동일 패턴(usage cut_id=None).
    """
    webtoon_id = _get_webtoon_id(webtoon_episode_id)
    ctx = resolve_llm_model(webtoon_id, VISION)
    llm_model_id = ctx.get("id")
    anonymize = _flow_first_enabled(webtoon_id)

    if prepare:
        prepare_episode_scene(webtoon_episode_id)      # llm 주석 + cut_scene_meta 정리
        _clear_episode_segment_scene(webtoon_episode_id)  # segment_scene_meta 정리

    units = load_segment_units(webtoon_episode_id)

    belief = _init_belief()
    analyzed = skipped = 0
    agg = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "calls": 0}

    records_by_ix: dict[int, Pass1Record] = {}
    processed = 0
    workers = min(step3._PASS1_WORKERS, len(units)) or 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(extract_segment, u, webtoon_episode_id,
                        webtoon_id=webtoon_id, ctx=ctx, run_id=run_id, anonymize=anonymize): u.index
            for u in units
        }
        for future in as_completed(futures):
            ix = futures[future]
            records_by_ix[ix] = future.result()
            processed += 1
            if heartbeat_cb:
                heartbeat_cb(processed)

    records = [records_by_ix[u.index] for u in units]
    for rec in records:
        if rec.skipped is not None:
            skipped += 1
        else:
            _accumulate_belief(belief, rec)
        if rec.skipped is None and rec.usage:
            analyzed += 1
            _insert_llm_usage(webtoon_id, webtoon_episode_id, None, llm_model_id, rec.usage,
                              stage=_PASS1_STAGE, image_count=1, run_id=run_id)
            for k in ("prompt_tokens", "completion_tokens", "total_tokens"):
                agg[k] += int(rec.usage.get(k, 0) or 0)
            agg["calls"] += 1

    logger.info(
        "[step3.seg] episode %s — 세그 %s개 중 분석=%s 스킵=%s, roster=%s pending=%s name_evidence=%s tokens=%s",
        webtoon_episode_id, len(units), analyzed, skipped,
        len(belief["character_roster"]), len(belief["pending_speakers"]),
        len(belief["name_evidence"]), agg["total_tokens"],
    )
    return ExtractResult(
        webtoon_episode_id=webtoon_episode_id, records=records, belief=belief,
        cuts_total=len(units), cuts_analyzed=analyzed, cuts_skipped=skipped, usage_total=agg,
    )
