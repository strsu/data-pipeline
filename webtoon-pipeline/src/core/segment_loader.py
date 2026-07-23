"""세그먼트-단위 Stage V용 로더 (Phase B1) — 읽기전용.

에피소드의 저장된 `analysis_episode_segment`(콘텐츠 밴드)를 분석단위로 삼아 각 세그먼트의
{이미지, 세그-로컬 리전, 세그-로컬 얼굴}을 조립한다. 이미지는 컷 스트립을 재조립해 세그 y범위로
크롭(세그 이미지는 R2에 저장 안 함 — 재조립 결정). 리전/얼굴은 `segment_id`로 로드(step1이 이미
귀속). 세그-로컬 bbox는 저장된 strip_y가 있으면 사용, 없으면(재분석 전 회차) 재조립한 컷 오프셋으로
계산 — 배포 전/후 회차 모두에서 동작.

블록 index/얼굴 F라벨은 세그 내 **읽기순(strip_y, x)**으로 재부여 — apply의 region_map(DB 재구성)이
동일 순서로 재현 가능해야 함(Phase C 계약).
"""
from __future__ import annotations
import bisect
from io import BytesIO
from dataclasses import dataclass, field
import numpy as np
from PIL import Image

from src.config.s3 import fetch_cut_image
from src.config.db import db_cursor
from src.core.step1 import _resize_cut_to_width, _scan_common_width


@dataclass
class SegUnit:
    """세그먼트 1개 = Stage V 분석단위. bbox는 전부 세그-로컬 좌표."""
    segment_id: int
    index: int                 # episode_segment.index (에피소드 내 순번)
    strip_y1: int
    strip_y2: int
    image_bytes: bytes = b""
    regions: list = field(default_factory=list)   # {region_id, index, bbox(세그-로컬), text}
    faces: list = field(default_factory=list)      # {id(F라벨), bbox(세그-로컬), name, character_id, appearance_id, confirmed}
    cuts: list = field(default_factory=list)        # 구성 컷 번호(참조용)

    @property
    def height(self) -> int:
        return self.strip_y2 - self.strip_y1


def _episode_meta(cur, webtoon_episode_id: int):
    cur.execute(
        """
        SELECT w.source, w.title_id, e.no
        FROM webtoon_episode e JOIN webtoon w ON w.id = e.webtoon_id
        WHERE e.id = %s
        """,
        (webtoon_episode_id,),
    )
    return cur.fetchone()


def _build_strip(source: str, title_id: str, episode_no: int):
    """회차 컷을 W(min폭)로 리사이즈해 vstack한 스트립 + 컷별 (번호, 전역offset, scale)을 반환.

    step1의 스트립 구성과 동일(W=min폭, LANCZOS). 컷 오프셋은 세그-로컬 bbox 폴백 계산용.
    """
    W, total = _scan_common_width(source, title_id, episode_no)
    parts, cuts, off = [], [], 0
    for cn in range(1, total + 1):
        b = fetch_cut_image(source, title_id, episode_no, cn)
        if b is None:
            continue
        pil = Image.open(BytesIO(b)).convert("RGB")
        ow = pil.size[0]
        im = _resize_cut_to_width(pil, W)
        cuts.append({"cn": cn, "off": off, "scale": W / ow, "h": im.height})
        parts.append(np.asarray(im))
        off += im.height
    strip = np.vstack(parts) if parts else np.zeros((0, W, 3), dtype=np.uint8)
    return strip, W, cuts


def _cut_offset_at(cut_offsets: list[tuple[int, int]], cn: int) -> int | None:
    """cut_number → 전역 strip offset(재조립 폴백용)."""
    for c_cn, off in cut_offsets:
        if c_cn == cn:
            return off
    return None


def _cuts_overlapping(y1: int, y2: int, cuts: list[dict]) -> list[int]:
    """세그 strip 범위 [y1,y2)와 겹치는 컷 번호(빈 세그도 컷범위 확보 — beats remap 재현성)."""
    return sorted(c["cn"] for c in cuts if c["off"] < y2 and c["off"] + c["h"] > y1)


def load_segment_units(webtoon_episode_id: int) -> list[SegUnit]:
    """회차의 세그먼트 분석단위 목록을 조립(읽기전용). 세그 없으면 빈 리스트."""
    with db_cursor() as cur:
        meta = _episode_meta(cur, webtoon_episode_id)
        if not meta:
            return []
        source, title_id, episode_no = meta
        cur.execute(
            "SELECT id, index, strip_y1, strip_y2 FROM analysis_episode_segment "
            "WHERE episode_id = %s ORDER BY index",
            (webtoon_episode_id,),
        )
        seg_rows = cur.fetchall()
        if not seg_rows:
            return []
        # segment_id → 리전/얼굴(컷-로컬 bbox + 저장 strip_y가 있으면 사용)
        cur.execute(
            """
            SELECT tr.segment_id, tr.id, tr.cut_id, wc.cut_number,
                   tr.bbox_x1, tr.bbox_y1, tr.bbox_x2, tr.bbox_y2, tr.strip_y1, tr.strip_y2,
                   ta.text
            FROM analysis_text_region tr
            JOIN webtoon_cut wc ON wc.id = tr.cut_id
            LEFT JOIN analysis_text_annotation ta ON ta.region_id = tr.id AND ta.source = 'paddle'
            WHERE wc.episode_id = %s AND tr.is_excluded = false AND tr.segment_id IS NOT NULL
            """,
            (webtoon_episode_id,),
        )
        reg_rows = cur.fetchall()
        cur.execute(
            """
            SELECT fd.segment_id, fd.face_idx, wc.cut_number,
                   fd.bbox_x1, fd.bbox_y1, fd.bbox_x2, fd.bbox_y2, fd.strip_y1, fd.strip_y2,
                   fi.appearance_id, c.name, c.id, (fi.source = 'human') AS confirmed
            FROM analysis_face_detection fd
            JOIN webtoon_cut wc ON wc.id = fd.cut_id
            LEFT JOIN LATERAL (
                SELECT source, appearance_id FROM analysis_face_identity
                WHERE detection_id = fd.id AND deleted_at IS NULL
                ORDER BY source ASC LIMIT 1
            ) fi ON true
            LEFT JOIN analysis_character_appearance ca ON fi.appearance_id = ca.id
            LEFT JOIN analysis_character c ON ca.character_id = c.id
            WHERE wc.episode_id = %s AND fd.is_used = true AND fd.segment_id IS NOT NULL
            """,
            (webtoon_episode_id,),
        )
        face_rows = cur.fetchall()

    strip, W, cuts = _build_strip(source, title_id, episode_no)
    cut_off = {c["cn"]: c for c in cuts}

    def _global_y(cn, cut_local_y, stored_strip_y):
        """전역 strip y — 저장값 우선, 없으면 컷 오프셋으로 재조립 계산."""
        if stored_strip_y is not None:
            return float(stored_strip_y)
        c = cut_off.get(cn)
        if c is None:
            return None
        return c["off"] + cut_local_y * c["scale"]

    def _local_x(cn, x):
        c = cut_off.get(cn)
        return x * c["scale"] if c is not None else x

    units: dict[int, SegUnit] = {}
    for sid, idx, y1, y2 in seg_rows:
        units[sid] = SegUnit(segment_id=sid, index=idx, strip_y1=y1, strip_y2=y2)

    # 리전 배정
    for (sid, rid, cut_id, cn, bx1, by1, bx2, by2, sy1, sy2, text) in reg_rows:
        u = units.get(sid)
        if u is None:
            continue
        gy1 = _global_y(cn, by1, sy1)
        gy2 = _global_y(cn, by2, sy2)
        if gy1 is None:
            continue
        u.regions.append({
            "region_id": rid, "text": text or "",
            "bbox": [round(_local_x(cn, bx1)), round(gy1 - u.strip_y1),
                     round(_local_x(cn, bx2)), round(gy2 - u.strip_y1)],
            "_sort": (gy1, _local_x(cn, bx1)),
        })
        if cn not in u.cuts:
            u.cuts.append(cn)

    # 얼굴 배정
    for (sid, fidx, cn, bx1, by1, bx2, by2, sy1, sy2, appr, name, cid, confirmed) in face_rows:
        u = units.get(sid)
        if u is None:
            continue
        gy1 = _global_y(cn, by1, sy1)
        gy2 = _global_y(cn, by2, sy2)
        if gy1 is None:
            continue
        u.faces.append({
            "appearance_id": appr, "name": (name or None), "character_id": cid,
            "confirmed": bool(confirmed),
            "bbox": [_local_x(cn, bx1), gy1 - u.strip_y1, _local_x(cn, bx2), gy2 - u.strip_y1],
            "_sort": (gy1, _local_x(cn, bx1)),
        })

    # 읽기순 재인덱싱/재라벨링(strip_y, x) + 이미지 크롭 + 정렬키 제거
    out = []
    for u in sorted(units.values(), key=lambda z: z.index):
        u.regions.sort(key=lambda r: r["_sort"])
        for i, r in enumerate(u.regions):
            r["index"] = i
            r.pop("_sort", None)
        u.faces.sort(key=lambda f: f["_sort"])
        for i, f in enumerate(u.faces):
            f["id"] = f"F{i}"
            f.pop("_sort", None)
        # 컷범위는 strip 겹침으로(리전 유무 무관 — 빈 세그도 매핑돼 beats remap 재현성 보장)
        u.cuts = _cuts_overlapping(u.strip_y1, u.strip_y2, cuts)
        if strip.shape[0] >= u.strip_y2 > u.strip_y1 >= 0:
            bio = BytesIO()
            Image.fromarray(strip[u.strip_y1:u.strip_y2]).save(bio, "JPEG", quality=92)
            u.image_bytes = bio.getvalue()
        out.append(u)
    return out
