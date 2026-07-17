"""Step 1 코어 — OCR / YOLO 로컬 추출 (faust-free, 순수 함수).

Temporal 액티비티가 호출한다. OCR과 YOLO는 **통합 단일 다운로드 스트리밍 경로**로 처리된다:
- process_episode_step1 : 에피소드 컷을 한 번만 점진 다운로드 → 스트리밍 슬라이딩 윈도우
  (`_iter_episode_segments`)로 콘텐츠 세그먼트를 방출 → 세그먼트마다 OCR과 YOLO를 모두
  실행하여 text_region/annotation + face_detection(+crop S3 업로드)를 저장.

핵심 컴포넌트:
- `_scan_common_width` : 컷 바이트 헤더만 선스캔하여 Common_Width(W = min(폭))와 총 컷 수를
  확정한다(픽셀 디코드/보관 없음).
- `_iter_episode_segments`(Strip_Streamer) : 컷을 오름차순 점진 다운로드하여 경계가 정해진
  `Window_Buffer`에만 보유하고, 변경 없는 `cut_merger._content_intervals`로 버퍼를 분할해
  **공백으로 종료된 세그먼트만** 방출한다. 종료되지 않은 후행 블록은 Carry_Over_Block으로
  이월하여 컷 경계에 걸친 콘텐츠가 분할되지 않게 하고, 공백 없는 풀 블리드는 Hard_Cap에서
  강제 방출한다. 소비된 행은 폐기하므로 최대 상주 이미지 메모리가 W × MAX_BUFFER_PX로 묶인다.
- `process_episode_step1`(Step1_Processor) : 제너레이터를 1회 소비하며 세그먼트마다 OCR →
  YOLO를 차례로 실행하고, OCR/YOLO가 공유하는 단일 `episode_segment` 행을 기록한다.

같은 컷 행(webtoon_cut)은 컷이 버퍼에 추가될 때 `ensure_cut`로 멱등 보장하고, 에피소드
재처리용 정리는 `prepare_episode_*`가 에피소드 단위로 1회 수행한다. 에피소드당 다운로드와
스트립 분할은 1회로 통합된다(재다운 비용 무시 가능 — 일 ~1000컷).
"""
from __future__ import annotations

import logging
import time
from bisect import bisect_right
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO

import numpy as np
from PIL import Image

from src.config.chroma import get_face_collection
from src.config.db import db_cursor
from src.config.r2 import delete_face_crop, upload_face_crop
from src.config.s3 import fetch_cut_image
from src.operators.cut_merger import _content_intervals, split_tall_interval
from src.operators.ocr_yolo_client import run_ocr, run_yolo

logger = logging.getLogger(__name__)

FACE_PAD_RATIO = 0.15
FACE_CROP_SIZE = (112, 112)
_IOU_DEDUP_THRESHOLD = 0.5

# OCR 후처리: 저확신도 제거 + 같은 줄(세로 겹침 + 가로 근접) 병합
OCR_MIN_SCORE = 0.4       # 이 미만 검출은 버림(검출 노이즈 제거)
OCR_LINE_VOVERLAP = 0.4   # 두 박스의 세로 겹침이 작은 높이의 이 비율 이상이면 같은 줄 후보
OCR_XGAP_RATIO = 0.8      # 가로 간격이 (작은 글자높이 × 이 값) 이하일 때만 병합(멀면 별개 텍스트)
OCR_HEIGHT_RATIO_MAX = 1.5  # 같은 줄이라도 글자높이 비율(큰÷작은)이 이 값 초과면 다른 크기 텍스트로 보고 병합 안 함

# 스트리밍 윈도우 구성(슬라이딩 윈도우). cut_merger의 MARGIN_PX / MIN_BAND_PX 와 동일하게
# 모듈 수준 상수로 둔다.
# 분할·방출 전 Window_Buffer가 담고자 하는 목표 최대 높이(Height_Budget).
WINDOW_BUDGET_PX = 10_000
# 미소비 높이가 이 값 아래로 떨어지면 다음 컷을 추가(리필)한다(Refill_Threshold).
REFILL_THRESHOLD_PX = 2_000
# 단일 미종료 콘텐츠 블록이 도달할 수 있는 절대 최대 높이(Hard_Cap). 이 값에서 강제 방출.
MAX_BUFFER_PX = 16_000
# 불변식: REFILL_THRESHOLD_PX < WINDOW_BUDGET_PX <= MAX_BUFFER_PX


# ── 스트리밍 값 객체 / 에피소드 전역 상태 ──────────────────────────────────────

@dataclass(frozen=True)
class Segment:
    """방출된 Content_Segment 하나.

    image_bytes : 세그먼트 JPEG 바이트(연속 스트립에서 잘림 → 얼굴/텍스트 전체 포함).
    g_y0, g_y1  : 전역 스트립 y 범위 [g_y0, g_y1) (버퍼 로컬 y가 아니라 전역 y).
    width       : Common_Width (W).
    index       : 에피소드 내 세그먼트 순번(0부터, episode_segment.index).
    forced      : Hard_Cap 강제 방출 여부(공백 밴드 없이 캡 경계에서 잘린 세그먼트).
    """
    image_bytes: bytes
    g_y0: int
    g_y1: int
    width: int
    index: int
    forced: bool


@dataclass
class EpisodeState:
    """모든 윈도우 반복에 걸쳐 영속하는 에피소드 전역 상태.

    제너레이터와 처리기가 공유하는 단일 인스턴스. bounds/cut_numbers/cut_id_map은
    제너레이터가 컷을 추가하며 갱신하고, used_bboxes/face_index/region_index는 처리기가
    세그먼트를 처리하며 갱신한다. 둘 다 같은 인스턴스를 가리키므로 컷이 점진적으로 늘어나도
    전역 y 귀속과 인덱싱이 "에피소드 = 단일 스트립"과 동일하게 유지된다.
    """
    width: int                       # Common_Width (W)
    bounds: list[int]                # 컷 누적 y-오프셋([0, h0, h0+h1, ...])
    cut_numbers: list[int]           # bounds[k]~bounds[k+1] = cut_numbers[k] 영역
    cut_id_map: dict[int, int]       # cut_number -> webtoon_cut.id
    # 처리기 측 dedup/인덱싱 상태(컷 귀속 단위로 누적)
    used_bboxes: dict[int, list]     # cut_id -> 승인된 얼굴 bbox(IOU 기준)
    face_index: dict[int, int]       # cut_id -> 다음 face_idx
    region_index: dict[int, int]     # cut_id -> 다음 text_region.index


def _merge_group(items: list[dict]) -> dict:
    text = " ".join((i.get("text") or "") for i in items).strip()
    xs = [i["bbox_2d"][0] for i in items] + [i["bbox_2d"][2] for i in items]
    ys = [i["bbox_2d"][1] for i in items] + [i["bbox_2d"][3] for i in items]
    score = min(float(i.get("score") or 0) for i in items)
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
                (episode_id, cut_number, processed_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uniq_webtoon_cut_episode_no DO UPDATE
                SET processed_at = EXCLUDED.processed_at,
                    updated_at   = EXCLUDED.updated_at
            RETURNING id
            """,
            (webtoon_episode_id, cut_number, now, now, now),
        )
        return cur.fetchone()[0]


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
                "DELETE FROM analysis_text_annotation WHERE region_id IN "
                "(SELECT id FROM analysis_text_region WHERE cut_id = %s)",
                (cut_id,),
            )
            cur.execute("DELETE FROM analysis_text_region WHERE cut_id = %s", (cut_id,))


def _cleanup_cut_faces(cut_id: int, source: str, title_id: str) -> None:
    """컷의 face_detection + S3 crop + Chroma + face_embedding 제거."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT fr.id, fe.embedding_model, fe.chroma_doc_id
            FROM analysis_face_detection fr
            LEFT JOIN analysis_face_embedding fe ON fe.detection_id = fr.id
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
        except Exception:
            logger.warning(
                "[step1] S3 crop 삭제 실패 face_id=%s source=%s title_id=%s",
                face_id, source, title_id, exc_info=True,
            )

    for model, doc_ids in chroma_entries.items():
        try:
            get_face_collection(source, title_id, model).delete(ids=doc_ids)
        except Exception:
            logger.warning(
                "[step1] Chroma 삭제 실패 model=%s source=%s title_id=%s doc_ids=%d개",
                model, source, title_id, len(doc_ids), exc_info=True,
            )

    if face_ids:
        with db_cursor() as cur:
            cur.execute("DELETE FROM analysis_face_embedding WHERE detection_id = ANY(%s)", (list(face_ids),))
            cur.execute("DELETE FROM analysis_face_detection WHERE id = ANY(%s)", (list(face_ids),))


def prepare_episode_yolo(webtoon_episode_id: int, source: str, title_id: str) -> None:
    """에피소드의 모든 컷에서 기존 face 데이터 일괄 정리(재처리 멱등)."""
    with db_cursor() as cur:
        cur.execute("SELECT id FROM webtoon_cut WHERE episode_id = %s", (webtoon_episode_id,))
        cut_ids = [r[0] for r in cur.fetchall()]
    for cut_id in cut_ids:
        _cleanup_cut_faces(cut_id, source, title_id)


# ── 에피소드 단위 처리 (스트리밍 슬라이딩 윈도우 → 콘텐츠 세그먼트, 컷 페어링 없음) ──
#
# 에피소드 컷을 오름차순으로 점진 다운로드하여 경계가 정해진 Window_Buffer에만 보유하고,
# 콘텐츠 구간(여백/단색 제거) 단위로 OCR/YOLO를 1회씩 돌린다. 컷 페어링(2컷 결합)이 없으므로
# 세그먼트 중복이 없고, 컷 경계에 걸친 얼굴/텍스트도 (Carry_Over로) 온전히 처리된다. 검출은
# 전역 y의 중심이 속한 컷에 귀속하고 그 컷의 로컬 좌표로 변환해 저장한다.
#
# 메모리: 전체 스트립이 아니라 슬라이딩 윈도우 버퍼(최대 W × MAX_BUFFER_PX)만 상주한다.


def _scan_common_width(source: str, title_id: str, episode_no: int) -> tuple[int, int]:
    """Common_Width 헤더 선스캔 — (W = min(width), 총 컷 수) 반환.

    컷 바이트를 오름차순(컷 1부터)으로 받아 PIL `Image.open(BytesIO(b)).size`로 **너비만**
    수집한다. `.size`는 전체 픽셀 디코드 없이 헤더만 읽으므로 디코드된 컷 이미지를 동시에
    메모리에 보유하지 않는다(Req 1.6). `fetch_cut_image`가 컷 1에서 즉시 `None`이면 컷 0개로
    보고 `(0, 0)`을 반환한다(빈 에피소드). `None`은 에피소드 경계이므로 거기서 멈춘다.
    """
    W: int | None = None
    cut = 1
    while True:
        b = fetch_cut_image(source, title_id, episode_no, cut)
        if b is None:
            break
        width = Image.open(BytesIO(b)).size[0]
        W = width if W is None else min(W, width)
        cut += 1
    total = cut - 1
    if total == 0:
        return 0, 0
    return W, total


def _cut_index_at(bounds: list[int], y: float) -> int:
    """전역 y가 속한 컷 인덱스 k (bounds[k] <= y < bounds[k+1])."""
    k = bisect_right(bounds, y) - 1
    return max(0, min(k, len(bounds) - 2))


# ── 스트리밍 윈도우 버퍼 (Window_Buffer) ──────────────────────────────────────
#
# 아직 방출되지 않은(미소비) 스트립 행만 보유하는 경계가 정해진 RGB 픽셀 영역. 내부적으로는
# 작은 행 청크(컷 단위 또는 캐리오버 단위)의 리스트로 유지하고, 분할 직전에만 `np.vstack`으로
# 연속 배열을 얻는다(불필요한 결합/복사 회피). `buf_global_y0`로 버퍼 로컬 y ↔ 전역 y를 환산.

class _WindowBuffer:
    """미소비 스트립 행을 행 청크(numpy 배열) 리스트로 보유하는 슬라이딩 윈도우 버퍼.

    width         : Common_Width (W). 빈 버퍼의 `to_array`가 올바른 폭을 갖도록 보관.
    buf_global_y0 : 버퍼 0번 행이 전역 스트립에서 차지하는 절대 y(소비된 행 누계).
    height        : 미소비 높이(버퍼 로컬 행 수).

    전역 y는 항상 `buf_global_y0 + (버퍼 로컬 y)`로 환산한다.
    """

    def __init__(self, width: int) -> None:
        self.width = width
        self._chunks: list[np.ndarray] = []
        self._height = 0
        self.buf_global_y0 = 0

    @property
    def height(self) -> int:
        """버퍼에 남은 미소비 높이(행 수)."""
        return self._height

    def append(self, chunk: np.ndarray) -> None:
        """행 청크(컷/캐리오버 단위, shape (h, W, C))를 버퍼 뒤에 덧붙인다."""
        if chunk.shape[0] == 0:
            return
        self._chunks.append(chunk)
        self._height += chunk.shape[0]

    def prepend(self, chunk: np.ndarray) -> None:
        """행 청크를 버퍼 앞에 덧붙인다(Carry_Over_Block 이월용 — Task 4.2 사용).

        앞에 붙는 만큼 전역 시작 y가 당겨지므로 `buf_global_y0`를 그만큼 되돌린다.
        """
        if chunk.shape[0] == 0:
            return
        self._chunks.insert(0, chunk)
        self._height += chunk.shape[0]
        self.buf_global_y0 -= chunk.shape[0]

    def to_array(self, max_rows: int | None = None) -> np.ndarray:
        """분할 직전 연속 배열을 얻는다(행 청크 vstack). 빈 버퍼는 (0, W, C).

        max_rows : 지정하면 버퍼 앞쪽 최대 max_rows 행까지만 결합한다(Hard_Cap 상한).
                   이 경우 초과 행은 결합하지 않아 vstack이 max_rows를 넘는 거대 배열을
                   할당하지 않는다 — 상주/분할 배열 높이가 MAX_BUFFER_PX를 넘지 않게 한다
                   (Req 1.5, 11.1). 캡 경계 이후 행은 버퍼 청크에 그대로 남아 다음 반복에서
                   계속 분할된다.
        """
        if not self._chunks:
            return np.empty((0, self.width, 3), dtype=np.uint8)
        if max_rows is None or self._height <= max_rows:
            if len(self._chunks) == 1:
                return self._chunks[0]
            return np.vstack(self._chunks)
        # 상한 적용: 앞쪽 청크부터 max_rows 행까지만 모으고, 경계에 걸친 청크는 슬라이스한다.
        collected: list[np.ndarray] = []
        remaining = max_rows
        for ch in self._chunks:
            h = ch.shape[0]
            if h <= remaining:
                collected.append(ch)
                remaining -= h
                if remaining == 0:
                    break
            else:
                collected.append(ch[:remaining])
                break
        if len(collected) == 1:
            return collected[0]
        return np.vstack(collected)

    def local_to_global(self, y_local: int) -> int:
        """버퍼 로컬 y → 전역 y."""
        return self.buf_global_y0 + y_local

    def global_to_local(self, y_global: int) -> int:
        """전역 y → 버퍼 로컬 y."""
        return y_global - self.buf_global_y0

    def discard_before(self, y_local: int) -> None:
        """버퍼 로컬 `y_local` 이전 행을 폐기(메모리 회수)하고 `buf_global_y0`를 전진한다.

        청크 경계에 걸치면 해당 청크를 슬라이스한다. 소비된 버퍼 앞부분을 잘라내는
        Discard 단계(Task 4.2)에서 사용한다.
        """
        if y_local <= 0:
            return
        y_local = min(y_local, self._height)
        remaining = y_local
        new_chunks: list[np.ndarray] = []
        for ch in self._chunks:
            h = ch.shape[0]
            if remaining >= h:
                remaining -= h
                continue
            if remaining > 0:
                new_chunks.append(ch[remaining:])
                remaining = 0
            else:
                new_chunks.append(ch)
        self._chunks = new_chunks
        self._height -= y_local
        self.buf_global_y0 += y_local


def _resize_cut_to_width(img: Image.Image, width: int) -> Image.Image:
    """컷 이미지를 Common_Width로 LANCZOS 리사이즈(필요 시 업스케일 포함).

    정상 경로는 항상 다운스케일(`W = min(폭)`)이지만, 선스캔–처리 사이 소스 변경 등으로
    W보다 좁은 컷이 관측되면 W로 업스케일하여 스트립 폭 불변식을 유지한다(Req 1.2).
    """
    if img.width == width:
        return img
    return img.resize((width, round(img.height * width / img.width)), Image.LANCZOS)


def _iter_episode_segments(
    source: str, title_id: str, episode_no: int, webtoon_episode_id: int,
    state: EpisodeState,
    *,
    budget_px: int = WINDOW_BUDGET_PX,
    refill_px: int = REFILL_THRESHOLD_PX,
    cap_px: int = MAX_BUFFER_PX,
) -> Iterator[Segment]:
    """에피소드 컷을 점진 다운로드하며 Content_Segment를 순서대로 방출하는 제너레이터.

    Task 4.1 — 점진 다운로드 · 리필 · 버퍼 적재(이 함수가 구현하는 범위):
      - 헤더 선스캔(`_scan_common_width`)으로 W·총 컷 수를 먼저 확정한다.
      - 컷을 오름차순 `fetch_cut_image` → W로 LANCZOS 리사이즈(필요 시 업스케일) →
        `Window_Buffer`(행 청크 리스트)에 append.
      - 컷 추가 시 `EpisodeState`의 bounds/cut_numbers/cut_id_map을 갱신하고 `ensure_cut`으로
        `webtoon_cut`을 멱등 upsert.
      - 미소비 높이 < refill_px 이고 미처리 컷이 남으면 budget_px 도달 또는 컷 소진까지 리필.
      - buf_global_y0로 버퍼 로컬 y ↔ 전역 y 환산을 유지.

    Task 4.2 — 분할 · 방출 · 이월(이 함수가 구현하는 범위):
      - 버퍼를 `to_array()`로 vstack 후 변경 없는 `_content_intervals`로 콘텐츠 구간 식별.
      - 마지막 구간을 제외한 모든 구간은 Terminated_Segment로 방출(버퍼 로컬 y → 전역 y,
        JPEG q=92, seg_index 증가).
      - 마지막 구간은 미처리 컷이 남으면 Carry_Over_Block으로 보존하고, 소비된 버퍼 앞부분은
        `discard_before`로 폐기. 모든 컷 소비 후 남은 구간은 최종 세그먼트로 flush.

    Task 4.3 — 하드 캡 강제 방출(이 함수가 구현하는 범위):
      - 버퍼 전체가 단일 미종료 블록(종료 구간 없음 + 블록이 버퍼 맨 앞에서 시작)이고 미소비
        높이가 cap_px(MAX_BUFFER_PX)에 도달하면, 공백 밴드를 기다리지 않고 캡 경계에서
        forced=True 세그먼트를 강제 방출한다. 정확히 cap_px 행만 폐기(겹침 0)하고 캡 경계
        이후부터 분할을 연속한다 — cap보다 큰 블록은 연속된 forced 세그먼트 여러 개로 나뉜다.
      - 단일 미종료 블록은 budget_px가 아니라 cap_px까지 성장하도록 추가 컷을 계속 적재한다.

    state(bounds/cut_numbers/cut_id_map)는 방출 전에 항상 최신이라, 소비측이 전역 y로 컷
    귀속이 가능하다.
    """
    ep = f"{source}/{title_id} ep={episode_no}"

    # [0] Common_Width 헤더 선스캔으로 W·총 컷 수 확정(픽셀 디코드/보관 없음).
    W, total_cuts = _scan_common_width(source, title_id, episode_no)
    if total_cuts == 0:
        logger.info("[step1.stream] %s — 컷 없음", ep)
        return  # 빈 에피소드 → 방출 없음

    # EpisodeState 초기화(전역 스트립 누적 오프셋은 0에서 시작).
    state.width = W
    if not state.bounds:
        state.bounds = [0]

    buf = _WindowBuffer(W)
    next_cut = 1          # 다음에 다운로드할 컷 번호(오름차순)
    seg_index = 0         # 방출 세그먼트 순번(episode_segment.index) — Task 4.2/4.3에서 사용
    t0 = time.perf_counter()
    logger.info("[step1.stream] %s — 스트리밍 시작 (W=%d, 총 컷 %d)", ep, W, total_cuts)

    def _cuts_remaining() -> bool:
        """아직 버퍼에 적재하지 않은 컷이 남아 있는가."""
        return next_cut <= total_cuts

    def _add_next_cut() -> None:
        """다음 컷을 다운로드 → W로 리사이즈 → 버퍼에 append, state·webtoon_cut 갱신."""
        nonlocal next_cut
        cut = next_cut
        b = fetch_cut_image(source, title_id, episode_no, cut)
        if b is None:
            # 선스캔–처리 사이 에피소드 경계가 줄어든 경우(이론적): 더 이상 컷이 없다고 본다.
            next_cut = total_cuts + 1
            return
        img = _resize_cut_to_width(Image.open(BytesIO(b)).convert("RGB"), W)
        chunk = np.asarray(img)
        buf.append(chunk)
        # 전역 y 매핑 갱신: bounds[k]~bounds[k+1] = cut_numbers[k] 영역.
        state.bounds.append(state.bounds[-1] + img.height)
        state.cut_numbers.append(cut)
        state.cut_id_map[cut] = ensure_cut(webtoon_episode_id, cut)
        next_cut += 1

    def _refill() -> None:
        """미소비 높이가 refill_px 미만이고 미처리 컷이 남으면 budget_px까지 컷을 채운다."""
        if buf.height >= refill_px:
            return
        while _cuts_remaining() and buf.height < budget_px:
            _add_next_cut()

    def _make_segment(arr: np.ndarray, y0: int, y1: int, index: int, *, forced: bool) -> Segment:
        """버퍼 로컬 구간 [y0, y1) 을 Content_Segment로 인코딩한다.

        - 연속 버퍼(arr)에서 [y0, y1) 행을 잘라 PIL RGB → JPEG quality=92로 인코딩한다
          (레거시 process_episode_* 와 동일: 연속 스트립에서 잘라 얼굴/텍스트 전체 포함).
        - 버퍼 로컬 y → 전역 y(`buf_global_y0 + y`)로 환산해 g_y0/g_y1에 담는다.
        """
        bio = BytesIO()
        Image.fromarray(arr[y0:y1]).save(bio, format="JPEG", quality=92)
        return Segment(
            image_bytes=bio.getvalue(),
            g_y0=buf.local_to_global(y0),
            g_y1=buf.local_to_global(y1),
            width=W,
            index=index,
            forced=forced,
        )

    def _emit(arr: np.ndarray, y0: int, y1: int, *, forced: bool):
        """구간 [y0,y1)을 방출한다 — **OCR이 못 읽을 만큼 크면 먼저 쪼갠다**(2026-07-17).

        여백 밴드가 없어 `_content_intervals`가 못 끊은 구간은 수천 px로 자라고, 통짜로 OCR에
        넣으면 리사이즈에서 글자가 뭉개져 거의 못 읽는다(실측 690x7965 → 4개 vs 컷단위 33개;
        `segment-oversize-2026-07-17.md`). 방출 경로가 3곳(hard cap/final flush/terminated)이라
        **여기 한 군데에서** 분할해 셋 다 덮는다. seg_index는 호출부에서 증가시킨다.
        """
        nonlocal seg_index
        for (a, b) in split_tall_interval(arr, y0, y1):
            yield _make_segment(arr, a, b, seg_index, forced=forced)
            seg_index += 1

    # ── 윈도우 메인 루프 ──────────────────────────────────────────────────────
    # 한 번의 반복: 리필 → 분할 → (하드 캡 강제 방출: Task 4.3) → (최종 flush / 방출·이월:
    # Task 4.2) → 폐기. 하드 캡 검사는 최종 flush보다 **먼저** 수행한다(Req 4.1) — 마지막 컷이
    # 단일 미종료 블록을 cap_px 이상으로 만들면서 동시에 컷을 소진해도, 그 블록은 최종 flush로
    # 통째로 방출되지 않고 캡 경계에서 forced 분할되어야 하기 때문이다.
    while _cuts_remaining() or buf.height > 0:
        _refill()

        # ── 분할(Segment): 버퍼를 연속 배열로 vstack 후 변경 없는 _content_intervals로
        #    콘텐츠 구간을 식별한다(Req 2.1, 13.2 — 분기 구현 없이 동일 함수 사용).
        #    materialize·분할하는 배열은 최대 cap_px 행으로 묶어, 단일 미종료 블록 성장 경로에서
        #    버퍼가 일시적으로 cap_px를 넘겨도 상주/분할 배열 높이가 MAX_BUFFER_PX를 넘지 않게
        #    한다(Req 1.5, 11.1). 캡 경계 이후 행은 버퍼에 남아 다음 반복에서 계속 분할된다.
        arr = buf.to_array(max_rows=cap_px)
        intervals = _content_intervals(arr)

        # 구간 분해: 마지막 구간을 제외한 모든 구간은 종료된 구간(Terminated)이고, 마지막
        # 구간은 미확정(공백 밴드 종료인지 버퍼 끝 절단인지 모름). 버퍼 전체가 단일 미종료
        # 블록(종료 구간 없음 + 블록이 버퍼 맨 앞 0에서 시작)인지 판정한다.
        if intervals:
            *terminated, last = intervals
            carry_start = last[0]  # 이월(Carry_Over_Block) 블록의 버퍼 로컬 시작
        else:
            terminated, last, carry_start = [], None, None
        single_unterminated = bool(intervals) and not terminated and carry_start == 0

        # ── 하드 캡 강제 방출(Hard_Cap) — Task 4.3 (최종 flush보다 먼저) ─────────
        #   버퍼 전체가 단일 미종료 블록이고 미소비 높이가 cap_px에 도달하면, 컷 잔여 여부와
        #   무관하게 공백 밴드를 기다리지 않고 캡 경계 [buf_global_y0, buf_global_y0 + cap_px)
        #   에서 forced=True 세그먼트를 강제 방출한다(Req 4.1). 정확히 cap_px 행만 폐기하므로
        #   겹침이 없고(Req 4.3), 캡 경계 이후부터 분할이 연속된다(Req 4.4) — cap보다 큰 블록은
        #   연속된 forced 세그먼트 여러 개로 나뉘며 각 g_y0가 직전 forced 세그먼트의 g_y1과 같다.
        #   마지막 컷이 컷을 소진하면서 동시에 cap을 넘겨도, 여기서 forced 분할 후 잔여가 아래
        #   최종 flush로 forced=False 방출된다.
        if single_unterminated and buf.height >= cap_px:
            yield from _emit(arr, 0, cap_px, forced=True)
            buf.discard_before(cap_px)
            continue

        # ── 최종 flush: 더 붙을 컷이 없고 버퍼 전체가 배열로 materialize된 경우(buf.height
        #    <= cap_px), 남은 모든 구간(이월 블록 포함)을 최종 Content_Segment로 방출한다
        #    (Req 2.5). 버퍼가 cap_px를 넘어 절단된 상태(buf.height > arr 높이)라면 배열 밖의
        #    행이 남아 있으므로 통째 flush하지 않고 아래 방출·이월 경로로 진행해 다음 반복에서
        #    이어 처리한다.
        if not _cuts_remaining() and buf.height <= arr.shape[0]:
            for (y0, y1) in intervals:
                yield from _emit(arr, y0, y1, forced=False)
            buf.discard_before(buf.height)  # 버퍼 비움 → 루프 종료
            continue

        # ── 콘텐츠 없음(버퍼 앞부분이 여백/사실상 단색) → materialize한 영역을 폐기해 전진. ──
        if not intervals:
            buf.discard_before(arr.shape[0])
            continue

        # 방출(Emit): 마지막 구간을 제외한 모든 구간은 그 아래에 다음 콘텐츠 구간이 존재하므로
        # 사이에 완전한 공백 밴드가 버퍼 안에 들어 있음이 보장된다 → Terminated_Segment로
        # 안전하게 방출한다(Req 2.2). 마지막 구간은 종료가 진짜 공백 밴드 때문인지 버퍼가
        # 거기서 잘렸기 때문인지 알 수 없으므로 항상 미확정으로 보고 이월한다(Req 2.3).
        for (y0, y1) in terminated:
            yield from _emit(arr, y0, y1, forced=False)

        # ── 단일 미종료 블록이 아직 cap_px 미만이면 다음 컷을 적재해 cap_px를 향해 성장한다
        #    (단일 미종료 블록은 budget_px에 묶이지 않고 MAX_BUFFER_PX까지 성장해야 강제 방출
        #    된다 — Req 4.1). 새 컷이 종료 공백 밴드를 들여오면 다음 반복에서 일반 방출 경로로
        #    넘어간다. (여기 도달 시 컷이 남아 있다 — 컷이 없으면 위 최종 flush로 빠진다.)
        if single_unterminated:
            if _cuts_remaining():
                _add_next_cut()
            continue

        # 폐기(Discard): 이월 블록 앞(방출된 구간 + 구분 밴드)을 잘라 메모리를 회수하고
        # buf_global_y0를 전진한다. 이월 블록은 버퍼 앞에 남아 다음 반복에서 새로 적재된 컷과
        # 함께 분할된다(Req 2.4).
        buf.discard_before(carry_start)

    logger.info(
        "[step1.stream] %s — 컷 %d개 적재 완료 (세그먼트 %d개, %.1fs)",
        ep, total_cuts, seg_index, time.perf_counter() - t0,
    )
    return


def _ensure_segment(cur, episode_id: int, index: int, y0: int, y1: int, width: int) -> int:
    """episode_segment 멱등 upsert(OCR/YOLO가 같은 분할을 공유). id 반환."""
    now = datetime.now(timezone.utc)
    cur.execute(
        """
        INSERT INTO analysis_episode_segment
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
    blocks: list[dict], voverlap: float = OCR_LINE_VOVERLAP, xgap_ratio: float = OCR_XGAP_RATIO,
    hratio: float = OCR_HEIGHT_RATIO_MAX,
) -> list[tuple[dict, int]]:
    """raw OCR 블록(전량)에 같은 줄 그룹 id 부여. drop/merge 없이 (block, group_id) 반환.

    같은 줄 = 세로 겹침 + 가로 근접(글자높이×xgap_ratio) + **글자 높이 비율 유사**(큰÷작은 ≤ hratio).
    가로로 멀거나 높이 차가 크면(효과음/제목 vs 대사 등 다른 크기 텍스트) 다른 그룹.
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
                ph = prev["bbox_2d"][3] - prev["bbox_2d"][1]
                ch = cur_b["bbox_2d"][3] - cur_b["bbox_2d"][1]
                hmin, hmax = min(ph, ch), max(ph, ch)
                gap = cur_b["bbox_2d"][0] - prev["bbox_2d"][2]
                # 가로로 멀거나(다른 텍스트) 글자 높이 차가 크면(다른 크기 텍스트:
                # 효과음/제목 vs 대사 등) 같은 그룹으로 묶지 않는다 → 별개 region.
                if gap > xgap_ratio * hmin or hmax > hratio * max(1, hmin):
                    gid += 1  # 멀거나 높이 비율 초과 → 새 그룹
            out.append((cur_b, gid))
            prev = cur_b
        gid += 1  # 줄이 바뀌면 새 그룹
    for b in without_box:
        out.append((b, -1))
    return out


def _load_resume_state(webtoon_episode_id: int) -> tuple[dict[int, int], dict[int, int], dict[int, list]]:
    """이미 커밋된 text_region/face_detection으로부터 region_index/face_index/used_bboxes 복원.

    재시도(attempt) 시 이전 attempt에서 이미 커밋된 세그먼트를 건너뛰더라도, 그 세그먼트가
    속한 컷의 다음 index/face_idx 카운터와 YOLO 전역 dedup용 승인 bbox를 DB 상태로부터
    다시 계산해 이어받는다. 정상 신규 실행(기존 행 없음)에서는 모두 빈 dict를 반환하므로
    process_episode_step1 초기화와 동일하게 동작한다.
    """
    region_index: dict[int, int] = {}
    face_index: dict[int, int] = {}
    used_bboxes: dict[int, list] = {}
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT tr.cut_id, MAX(tr.index)
            FROM analysis_text_region tr JOIN webtoon_cut wc ON wc.id = tr.cut_id
            WHERE wc.episode_id = %s
            GROUP BY tr.cut_id
            """,
            (webtoon_episode_id,),
        )
        for cut_id, max_index in cur.fetchall():
            region_index[cut_id] = max_index + 1

        cur.execute(
            """
            SELECT fr.cut_id, MAX(fr.face_idx)
            FROM analysis_face_detection fr JOIN webtoon_cut wc ON wc.id = fr.cut_id
            WHERE wc.episode_id = %s
            GROUP BY fr.cut_id
            """,
            (webtoon_episode_id,),
        )
        for cut_id, max_idx in cur.fetchall():
            face_index[cut_id] = max_idx + 1

        cur.execute(
            """
            SELECT fr.cut_id, fr.bbox_x1, fr.bbox_y1, fr.bbox_x2, fr.bbox_y2
            FROM analysis_face_detection fr JOIN webtoon_cut wc ON wc.id = fr.cut_id
            WHERE wc.episode_id = %s AND fr.is_used = true
            """,
            (webtoon_episode_id,),
        )
        for cut_id, x1, y1, x2, y2 in cur.fetchall():
            used_bboxes.setdefault(cut_id, []).append([x1, y1, x2, y2])

    return region_index, face_index, used_bboxes


def prepare_episode_segments(webtoon_episode_id: int) -> None:
    """에피소드의 episode_segment 행 제거(재처리 멱등). 검출은 prepare_episode_ocr/yolo가 먼저 정리."""
    with db_cursor() as cur:
        cur.execute("DELETE FROM analysis_episode_segment WHERE episode_id = %s", (webtoon_episode_id,))


# ── 통합 처리기 (Step1_Processor) ─────────────────────────────────────────────
#
# `_iter_episode_segments`(Strip_Streamer)가 방출한 각 Content_Segment에 대해 OCR과 YOLO를
# 모두 실행하는 단일 다운로드 통합 처리기. 분리된 process_episode_ocr / process_episode_yolo
# 두 패스(각자 전체 스트립 재구성)를 대체한다 — 에피소드당 다운로드·분할 1회.
#
# 점진적 단계 구성:
#   - Task 5.1: 세그먼트별 OCR 경로(text_region/text_annotation).
#   - Task 5.2: 세그먼트별 YOLO 경로 + 에피소드 전역 IOU dedup + 크롭(face_record).  ← 이 변경
#   - Task 5.3: 단일 episode_segment 공유(segment_id) + heartbeat + 반환값 집계.


def _process_segment_ocr(
    cur, seg: Segment, state: EpisodeState, segment_id: int,
    source: str, title_id: str, episode_no: int, now: datetime,
) -> int:
    """방출된 세그먼트 하나에 대한 OCR 경로(Task 5.1). 기록한 text_region 수를 반환.

    process_episode_ocr 의 인라인 OCR 로직(동일 스키마/필드 의미)을 그대로 옮기되, 입력이
    전역 y 세그먼트이므로 좌표 환산을 버퍼 로컬(y0)이 아니라 전역 y(seg.g_y0) 기준으로 한다.

    - rep_cut: 세그먼트 시작 전역 y가 속한 컷(_cut_index_at(state.bounds, seg.g_y0)).
    - 같은 줄 그룹(_assign_line_groups, OCR_HEIGHT_RATIO_MAX 포함) → gid별 _merge_group.
    - 귀속: Global_Y_Center = (gy1+gy2)/2 의 _cut_index_at → cut_id.
    - Cut_Local_Coords: 귀속 컷 상단(bounds[k]) 기준, 클램핑 없음(컷 경계 교차 시 음수/초과 보존).
    - 컷별 region_index(state.region_index) 유지. is_used = score >= OCR_MIN_SCORE,
      is_excluded=false. text_annotation source='paddle', confidence=score.
    """
    bounds = state.bounds
    cut_numbers = state.cut_numbers
    cut_id_map = state.cut_id_map

    rep_cut = cut_numbers[_cut_index_at(bounds, seg.g_y0)]
    raw_blocks = run_ocr(
        seg.image_bytes, source=source, title_id=title_id, episode_no=episode_no, cut=rep_cut
    )
    grouped = _assign_line_groups(raw_blocks)

    # 같은 줄·가로 근접 그룹(gid)을 하나의 bbox+텍스트로 병합해 region 1개로 저장.
    # bbox 없는 블록(gid<0)은 위치 불명이라 제외(기존과 동일). 병합 라인의 score는 그룹 내
    # 최소값 → is_used도 최소 score 기준(전량 저장, 드롭 아님).
    line_groups: dict[int, list[dict]] = {}
    for blk, gid in grouped:
        if gid < 0 or not blk.get("bbox_2d"):
            continue
        line_groups.setdefault(gid, []).append(blk)

    count = 0
    for gid in sorted(line_groups):
        blk = _merge_group(line_groups[gid])
        bb = blk["bbox_2d"]
        # 세그먼트 로컬 bbox → 전역 y(seg.g_y0 기준). 버퍼 로컬(y0)이 아닌 전역 y로 컷 귀속.
        gy1, gy2 = seg.g_y0 + bb[1], seg.g_y0 + bb[3]
        k = _cut_index_at(bounds, (gy1 + gy2) / 2)
        cut_id = cut_id_map[cut_numbers[k]]
        ystart = bounds[k]
        # Cut_Local_Coords: 귀속 컷 상단 기준. 컷 경계를 걸친 텍스트는 클램핑하지 않으므로
        # 위로 넘친 경우 음수, 아래로 넘친 경우 컷 높이를 초과할 수 있다(충실한 좌표 보존).
        lb = [bb[0], gy1 - ystart, bb[2], gy2 - ystart]
        score = blk.get("score")
        is_used = score is not None and float(score) >= OCR_MIN_SCORE
        idx = state.region_index.get(cut_id, 0)
        state.region_index[cut_id] = idx + 1
        # ON CONFLICT DO NOTHING: (1) resume_from 이어받기가 커버 못하는 경합/재시도 상황(예:
        # heartbeat_timeout 좀비 실행)에 대한 안전망. text_region/text_annotation은 같은
        # 트랜잭션(segment 단위)에서 함께 커밋되므로, region이 이미 있으면 annotation도 이미
        # 있는 것이 보장된다 — 조회 없이 region 쪽만 확인해 스킵해도 안전하다. is_excluded 등
        # human 리뷰 필드를 덮어쓰지 않도록 DO UPDATE가 아니라 DO NOTHING을 쓴다.
        cur.execute(
            """
            INSERT INTO analysis_text_region
                (cut_id, segment_id, index, bbox_x1, bbox_y1, bbox_x2, bbox_y2,
                 score, line_group, is_used, is_excluded, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, false, %s, %s)
            ON CONFLICT ON CONSTRAINT uniq_text_region_cut_index DO NOTHING
            RETURNING id
            """,
            (cut_id, segment_id, idx, lb[0], lb[1], lb[2], lb[3],
             score, gid, is_used, now, now),
        )
        res = cur.fetchone()
        if not res:
            # 이미 커밋된 (cut_id, index) — 재시도/경합 안전망으로 스킵.
            logger.warning(
                "[step1] text_region 중복 스킵(안전망) cut_id=%s index=%s segment_id=%s",
                cut_id, idx, segment_id,
            )
            continue
        region_id = res[0]
        cur.execute(
            """
            INSERT INTO analysis_text_annotation
                (region_id, source, text, confidence, resolution_status, created_at, updated_at)
            VALUES (%s, 'paddle', %s, %s, 'unresolved', %s, %s)
            ON CONFLICT ON CONSTRAINT uniq_text_annotation_region_source DO NOTHING
            """,
            (region_id, blk["text"], score, now, now),
        )
        count += 1
    return count


def _process_segment_yolo(
    cur, seg: Segment, state: EpisodeState, segment_id: int,
    source: str, title_id: str, episode_no: int, now: datetime,
) -> int:
    """방출된 세그먼트 하나에 대한 YOLO 경로(Task 5.2). 기록한 face_record 수를 반환.

    process_episode_yolo 의 인라인 YOLO 로직(동일 스키마/필드 의미)을 그대로 옮기되, 입력이
    전역 y 세그먼트이므로 좌표 환산을 버퍼 로컬(y0)이 아니라 전역 y(seg.g_y0) 기준으로 한다.
    OCR이 쓴 것과 동일한 seg.image_bytes / segment_id 를 공유한다(Req 6.6, 8.4).

    - rep_cut: 세그먼트 시작 전역 y가 속한 컷(_cut_index_at(state.bounds, seg.g_y0)) — OCR과 동일.
    - 귀속: Global_Y_Center = (gy1+gy2)/2 의 _cut_index_at → cut_id.
    - Cut_Local_Coords: 귀속 컷 상단(bounds[k]) 기준, 클램핑 없음(컷 경계 교차 시 음수/초과 보존).
    - 에피소드 전역 dedup: state.used_bboxes(cut_id -> 승인 bbox 목록)와의 IOU가
      _IOU_DEDUP_THRESHOLD 이상이면 is_duplicate=true/is_used=false로 표시(삭제 아님). 승인된
      얼굴만 used_bboxes에 누적해 이후 세그먼트/윈도우 반복에 걸쳐 에피소드 전역으로 dedup.
    - 컷별 face_index(state.face_index) 유지. is_confirmed=false. ON CONFLICT DO NOTHING.
    - is_used 얼굴만 _crop_face(연속 세그먼트 이미지 seg.image_bytes에서 fb 로컬 좌표로 크롭) →
      upload_face_crop. 업로드 실패는 try/except로 흡수(레거시와 동일).
    """
    bounds = state.bounds
    cut_numbers = state.cut_numbers
    cut_id_map = state.cut_id_map
    used_bboxes = state.used_bboxes
    face_index = state.face_index

    rep_cut = cut_numbers[_cut_index_at(bounds, seg.g_y0)]
    faces = run_yolo(
        seg.image_bytes, source=source, title_id=title_id, episode_no=episode_no, cut=rep_cut
    )

    count = 0
    for face in faces:
        fb = face["bbox"]  # 세그먼트 로컬 좌표
        # 세그먼트 로컬 bbox → 전역 y(seg.g_y0 기준). 버퍼 로컬(y0)이 아닌 전역 y로 컷 귀속.
        gy1, gy2 = seg.g_y0 + fb[1], seg.g_y0 + fb[3]
        k = _cut_index_at(bounds, (gy1 + gy2) / 2)
        cut_id = cut_id_map[cut_numbers[k]]
        ystart = bounds[k]
        # 컷 경계를 걸친 얼굴(스트립을 컷으로 자른 경계가 얼굴 한가운데를 지나는 경우)도
        # 충실히 보존한다. y는 클램프하지 않으므로 위 컷으로 넘친 얼굴은 음수, 아래 컷으로
        # 넘친 얼굴은 컷 높이를 초과할 수 있다. crop은 연속 세그먼트(seg.image_bytes)에서
        # 잘려 항상 얼굴 전체를 담으므로, bbox도 같은 실제 영역을 가리키도록 맞춘다.
        lb = [fb[0], gy1 - ystart, fb[2], gy2 - ystart]
        # 에피소드 전역 IOU dedup(여러 세그먼트·윈도우 반복에 걸쳐 누적되는 used_bboxes 기준).
        is_dup = any(_iou(lb, s) >= _IOU_DEDUP_THRESHOLD for s in used_bboxes.get(cut_id, []))
        is_used = not is_dup
        if is_used:
            used_bboxes.setdefault(cut_id, []).append(lb)
        fidx = face_index.get(cut_id, 0)
        face_index[cut_id] = fidx + 1
        cur.execute(
            """
            INSERT INTO analysis_face_detection
                (cut_id, segment_id, face_idx, bbox_x1, bbox_y1, bbox_x2, bbox_y2,
                 conf, is_used, is_duplicate, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT uniq_face_record_cut_idx DO NOTHING
            RETURNING id
            """,
            (cut_id, segment_id, fidx, lb[0], lb[1], lb[2], lb[3],
             face["conf"], is_used, is_dup, now, now),
        )
        res = cur.fetchone()
        if not res:
            # 이미 커밋된 (cut_id, face_idx) — 재시도/경합 안전망으로 스킵.
            logger.warning(
                "[step1] face_detection 중복 스킵(안전망) cut_id=%s face_idx=%s segment_id=%s",
                cut_id, fidx, segment_id,
            )
            continue
        face_record_id = res[0]
        count += 1
        if is_used:
            try:
                crop_bytes = _crop_face(seg.image_bytes, fb)
                upload_face_crop(face_record_id, source, title_id, crop_bytes)
            except Exception:
                logger.warning(
                    "[step1] face crop upload 실패 face_id=%s source=%s title_id=%s",
                    face_record_id, source, title_id, exc_info=True,
                )
    return count


def process_episode_step1(
    source: str, title_id: str, episode_no: int, webtoon_episode_id: int,
    heartbeat_cb=None, resume_from: int = 0,
) -> dict:
    """에피소드 단위 통합 Step1 처리기 — 단일 다운로드/분할로 세그먼트마다 OCR(+YOLO) 실행.

    `_iter_episode_segments`(Strip_Streamer)를 **1회** 소비하며, 제너레이터와 공유하는 단일
    `EpisodeState`로 전역 y 귀속과 컷별 인덱싱을 "에피소드 = 단일 스트립"과 동일하게 유지한다.

    방출된 세그먼트마다:
      [OCR — Task 5.1, 이 변경]  run_ocr → _assign_line_groups(+OCR_HEIGHT_RATIO_MAX) → gid
                        그룹 → _merge_group → text_region/text_annotation 기록(전역 y로 컷
                        귀속, Cut_Local_Coords 클램핑 없음, 컷별 region_index 유지).
      [YOLO — Task 5.2]   같은 seg.image_bytes로 run_yolo → 에피소드 전역 used_bboxes
                        IOU dedup(is_duplicate/is_used 표시) → face_detection + is_used 얼굴 크롭
                        업로드. 컷별 face_index 유지.
      [공유/하트비트 — Task 5.3]  세그먼트당 _ensure_segment 1회 호출로 얻은 segment_id를
                        OCR/YOLO가 공유(방출 Content_Segment당 episode_segment 1행 — Req 6.6),
                        세그먼트 완료마다 heartbeat_cb(seg.index + 1) 호출(Req 12.1, 12.2),
                        반환값 최종 집계. 제너레이터를 1회만 소비하므로 다운로드/분할 패스도
                        에피소드당 1회다(단일 다운로드 보장 — Req 6.1, 11.2).

    resume_from: 이 인덱스 미만의 세그먼트는 이전 attempt에서 이미 커밋된 것으로 보고
                 OCR/YOLO 호출과 DB 쓰기를 건너뛴다(세그먼트 다운로드/분할 자체는 계속 소비 —
                 전역 y 귀속에 필요). Temporal이 활동을 재시도할 때(네트워크 순단 등으로 도중
                 실패) 이미 끝난 세그먼트를 다시 처리하다 text_region 등의 unique 제약을
                 위반하지 않도록 한다. region_index/face_index/used_bboxes는 건너뛴 세그먼트의
                 것이라도 DB에 이미 반영된 값을 `_load_resume_state`로 복원해 이어받는다.

    반환: {"segments": n, "texts": t, "faces": f}. resume_from>0이면 이번 attempt에서
    새로 처리한 세그먼트/텍스트/얼굴 수만 집계한다(이미 끝난 세그먼트는 세지 않음).
    빈 에피소드(컷 0)는 모든 값 0.
    """
    ep = f"{source}/{title_id} ep={episode_no}"

    # 제너레이터와 처리기가 공유하는 단일 EpisodeState. bounds/cut_numbers/cut_id_map은
    # 제너레이터가 컷을 적재하며 갱신하고, region_index(및 Task 5.2의 used_bboxes/face_index)는
    # 처리기가 세그먼트를 처리하며 갱신한다 — 같은 인스턴스이므로 컷이 점진적으로 늘어나도
    # 전역 y 귀속·인덱싱이 일관되게 유지된다(Req 9.3, 9.4).
    # region_index/face_index/used_bboxes는 재시도 이어받기를 위해 DB의 기존 커밋 상태로
    # 초기화한다 — 신규 실행(빈 에피소드)에서는 조회 결과가 없어 기존과 동일하게 빈 dict.
    region_index, face_index, used_bboxes = _load_resume_state(webtoon_episode_id)
    state = EpisodeState(
        width=0, bounds=[], cut_numbers=[], cut_id_map={},
        used_bboxes=used_bboxes, face_index=face_index, region_index=region_index,
    )

    seg_count = 0
    skipped = 0
    texts = 0
    faces = 0  # Task 5.2(YOLO 경로)에서 채워짐
    now = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    logger.info(
        "[step1] %s — 통합 처리 시작 (resume_from=%d, region_index 복원 %d개 컷, "
        "face_index 복원 %d개 컷, used_bboxes 복원 %d개 컷)",
        ep, resume_from, len(region_index), len(face_index), len(used_bboxes),
    )

    for seg in _iter_episode_segments(source, title_id, episode_no, webtoon_episode_id, state):
        if seg.index < resume_from:
            # 이전 attempt에서 이미 커밋된 세그먼트 — OCR/YOLO 재호출·재삽입 없이 건너뛴다.
            # 제너레이터는 계속 소비해 bounds/cut_id_map(전역 y 귀속)만 갱신시킨다.
            skipped += 1
            if heartbeat_cb:
                heartbeat_cb(seg.index + 1)
            continue

        try:
            with db_cursor() as cur:
                # OCR/YOLO가 공유할 단일 episode_segment 행(방출 Content_Segment당 1개 — Req 6.6).
                # 세그먼트 처리 시작에 _ensure_segment 를 한 번만 호출하고, 이 segment_id 를 OCR과
                # YOLO 양쪽에 그대로 넘겨 두 결과가 동일한 episode_segment 행을 참조하게 한다(Task 5.3).
                segment_id = _ensure_segment(
                    cur, webtoon_episode_id, seg.index, seg.g_y0, seg.g_y1, seg.width
                )

                # ── OCR 경로 (Task 5.1) ──────────────────────────────────────────
                texts += _process_segment_ocr(
                    cur, seg, state, segment_id, source, title_id, episode_no, now
                )

                # ── YOLO 경로 (Task 5.2) ─────────────────────────────────────────
                # OCR과 동일한 seg.image_bytes / segment_id 를 공유한다(Req 6.6, 8.4).
                faces += _process_segment_yolo(
                    cur, seg, state, segment_id, source, title_id, episode_no, now
                )
        except Exception:
            # 어느 세그먼트에서, 몇 번째 처리 중, 얼마나 진행된 상태에서 실패했는지 남긴다.
            # 이 세그먼트의 DB 쓰기는 트랜잭션 전체가 롤백되므로(Task 5.3), 다음 attempt는
            # heartbeat_details(마지막으로 완료된 seg.index+1)부터 안전하게 재개된다.
            logger.error(
                "[step1] %s — 세그먼트 처리 실패 seg.index=%s (g_y0=%s, g_y1=%s), "
                "이번 attempt 진행: 완료 %d개/스킵 %d개(resume_from=%d), "
                "누적 텍스트 %d개, 누적 얼굴 %d개, %.1fs 경과",
                ep, seg.index, seg.g_y0, seg.g_y1, seg_count, skipped, resume_from,
                texts, faces, time.perf_counter() - t0, exc_info=True,
            )
            raise

        # ── 하트비트/진행 (Task 5.3) ─────────────────────────────────────────
        # 세그먼트 하나(OCR+YOLO+episode_segment 영속화)가 끝날 때마다 진행 정보(처리된
        # 세그먼트 수 = seg.index + 1)와 함께 Temporal 하트비트 콜백을 호출한다(Req 12.1).
        # step1_episode 액티비티가 activity.heartbeat 를 그대로 전달한다(Req 12.2).
        if heartbeat_cb:
            heartbeat_cb(seg.index + 1)
        seg_count += 1
        logger.info(
            "[step1] %s — 세그먼트 %d 처리(누적 텍스트 %d개, 누적 얼굴 %d개, %.1fs)",
            ep, seg_count, texts, faces, time.perf_counter() - t0,
        )

    logger.info(
        "[step1] %s — 통합 처리 완료 (세그먼트 %d개 처리/%d개 스킵, 텍스트 %d개, 얼굴 %d개, %.1fs)",
        ep, seg_count, skipped, texts, faces, time.perf_counter() - t0,
    )
    return {"segments": seg_count, "texts": texts, "faces": faces}
