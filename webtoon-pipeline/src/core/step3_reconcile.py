"""VLM 윈도우 정체·화자 reconcile (remain-trouble D7) — 정체 척추를 CCIP→대사흐름 슬롯으로.

Stage V(병렬 지각) 뒤, R 앞에서 회차를 슬라이딩 윈도우로 훑는다. 각 윈도우에서 비전 모델
(glm-4.6v)에 [연속 컷 F라벨 오버레이 이미지 + 읽기순 대사 + 직전까지의 슬롯 로스터]를 주고:
  ① 대사 흐름(교대·화법·호칭·존대·POV)으로 익명 슬롯(A/B/…)을 세우고
  ② 각 대사를 슬롯에 배정, ③ 말풍선 꼬리가 가리키는 얼굴을 슬롯에 부착.
로스터를 윈도우 간 carry-forward → 교차창 슬롯 안정 + granularity 앵커. 얼굴 외형매칭 안 함
(각도·표정 변이에 안 무너짐, c100 F0·F1 같은 청명도 대사로 이어짐). CCIP는 prior만.

검증(3-테스트, exp_window_flow/carry): 대사역할로 슬롯 안정·명명인물 격리·장르불문·carry-forward가
교차창 링킹+과분할 교정. 정본 remain-trouble.md D7.

반환(reconcile_episode): {slots:{A:persona}, speaker_map:{(cut,block):slot}, face_map:{(cut,face):slot}}.
커밋은 commit_reconcile(별도) — 슬롯→cluster 인물, 화자·얼굴을 슬롯에 바인딩.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from src.core import step3
from src.core.step3 import (
    _load_faces, _load_regions, _pass1_ctx, _episode_info, _cut_id,
    overlay_faces, _downscale, _PASS1_MAX_DIM, _get_webtoon_id,
)
from src.operators.llm_client import call_llm_json
from src.operators.llm_resolver import resolve_llm_model, VISION

logger = logging.getLogger(__name__)

RECONCILE_WINDOW = 6      # 윈도우 컷 수
_RECON_RETRIES = 2

# ⚠️ 로스터 정체 안정성(과앵커 붕괴 vs 과분할)이 미해결 — D7 크럭스. 아래는 페르소나기반 잠정판.
_SYSTEM_BASE = (
    "너는 웹툰 연속 컷들의 화자를 **대사 흐름**으로 해소하고 얼굴을 그 화자에 붙인다.\n"
    "입력: N장 컷 이미지(얼굴 bbox에 F0/F1 라벨 — **라벨은 컷마다 독립, 컷 넘으면 무의미**) + 읽기순 대사.\n"
    "⚠️ 얼굴 생김새로 맞추지 마라(각도·표정 변이). 슬롯 = **구별되는 인물(사람)** 이다(역할이 아니라).\n"
    "1) 대사 흐름으로 **서로 다른 인물**을 슬롯(A,B,…)으로 가른다. 각 슬롯 설명은 **그 인물의 고유 특징**"
    "(말버릇·화법·호칭관계·언급된 이름)으로 — '질문자/설명자' 같은 역할·순간행동어 금지. 예: '청명을"
    " 사형이라 부르는 소년', '~더냐 쓰는 노인 스승'.\n"
    "2) 각 speech/monologue/narration 대사를 인물 슬롯에 배정(억지 금지, 불명이면 slot=null).\n"
    "3) 얼굴→슬롯: **말풍선 꼬리가 명확히 가리키는 얼굴**만 그 풍선 슬롯에 부착(그 컷 face_label→slot)."
    " 꼬리 없거나 오프패널이면 생략. 발화 없이 서 있기만 한 얼굴도 생략.\n"
)
_SYSTEM_CARRY = (
    "⚠️ 아래는 지금까지 등장한 **인물 로스터**다. 같은 인물 재등장이면 같은 슬롯 재사용,"
    " **새 인물(다른 말버릇·관계)이면 반드시 새 슬롯 추가** — 역할 비슷하다고 다른 사람을 기존 슬롯에"
    " 욱여넣지 마라(과앵커 금지). 로스터:\n{roster}\n"
)
_SYSTEM_TAIL = (
    "반드시 JSON만: {\"slots\":{\"A\":\"화법/역할\"},"
    "\"speakers\":[{\"cut\":<int>,\"block\":<int>,\"slot\":\"A\"}],"
    "\"face_slot\":[{\"cut\":<int>,\"face\":\"F0\",\"slot\":\"A\"}]}"
)


def _windows(cut_numbers: list[int], size: int) -> list[list[int]]:
    return [cut_numbers[i:i + size] for i in range(0, len(cut_numbers), size)]


def reconcile_episode(
    webtoon_episode_id: int,
    *,
    webtoon_id: Optional[int] = None,
    ctx: Optional[dict] = None,
    window: int = RECONCILE_WINDOW,
    run_id: Optional[int] = None,
    heartbeat_cb=None,
    cut_numbers: Optional[list] = None,
) -> dict:
    """회차를 윈도우로 훑어 대사흐름 슬롯·화자·얼굴부착을 해소. carry-forward 로스터.

    반환: {slots, speaker_map:{"cut:block":slot}, face_map:{"cut:F":slot}, usage, error}.
    실패한 윈도우는 스킵(격리) — 부분 결과라도 반환.
    """
    if webtoon_id is None:
        webtoon_id = _get_webtoon_id(webtoon_episode_id)
    if ctx is None:
        ctx = _pass1_ctx(dict(resolve_llm_model(webtoon_id, VISION)))
    info = _episode_info(webtoon_episode_id)

    if cut_numbers is None:
        from src.config.db import db_cursor
        with db_cursor() as cur:
            cur.execute("SELECT cut_number FROM webtoon_cut WHERE episode_id=%s ORDER BY cut_number",
                        (webtoon_episode_id,))
            cut_numbers = [r[0] for r in cur.fetchall()]

    roster: dict = {}
    speaker_map: dict = {}
    face_map: dict = {}
    agg_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    n_win = 0
    for wi, win in enumerate(_windows(cut_numbers, window)):
        images, tlines, has_dlg = [], [], False
        for cn in win:
            cid = _cut_id(webtoon_episode_id, cn)
            faces = _load_faces(cid) if cid else []
            regions = _load_regions(cid) if cid else []
            img = step3.fetch_cut_image(info["source"], info["title_id"], info["episode_no"], cn)
            if img is None:
                continue
            images.append(_downscale(overlay_faces(img, faces), _PASS1_MAX_DIM))
            for r in regions:
                tlines.append(f"[컷{cn} idx{r['index']}] {r['text']}")
                has_dlg = True
        if not images or not has_dlg:
            continue
        sysprompt = _SYSTEM_BASE + (
            _SYSTEM_CARRY.format(roster=json.dumps(roster, ensure_ascii=False)) if roster else ""
        ) + _SYSTEM_TAIL
        user = f"이미지 순서 = 컷 {win}.\n대사(읽기순):\n" + "\n".join(tlines)
        res = None
        for _ in range(_RECON_RETRIES):
            try:
                call = call_llm_json(ctx, sysprompt, user, images)
                res = call.result if isinstance(call.result, dict) else {}
                for k in agg_usage:
                    agg_usage[k] += int((call.usage or {}).get(k, 0) or 0)
                break
            except Exception as e:  # noqa: BLE001 — 윈도우 격리
                logger.warning("[reconcile] ep=%s win=%s 실패(재시도): %s", webtoon_episode_id, win, e)
                res = None
        if not res:
            continue
        n_win += 1
        for s, d in (res.get("slots") or {}).items():
            if isinstance(s, str) and s not in roster:
                roster[s] = d if isinstance(d, str) else ""
        for sp in (res.get("speakers") or []):
            if not isinstance(sp, dict):
                continue
            slot = sp.get("slot")
            if slot in roster and isinstance(sp.get("cut"), int) and isinstance(sp.get("block"), int):
                speaker_map[f"{sp['cut']}:{sp['block']}"] = slot
        for fs in (res.get("face_slot") or []):
            if not isinstance(fs, dict):
                continue
            slot = fs.get("slot")
            face = fs.get("face")
            if slot in roster and isinstance(fs.get("cut"), int) and isinstance(face, str):
                face_map[f"{fs['cut']}:{face}"] = slot
        if heartbeat_cb:
            heartbeat_cb(wi + 1)
        logger.info("[reconcile] ep=%s win%s 컷%s~%s — 슬롯누적=%s 화자=%s 얼굴부착=%s",
                    webtoon_episode_id, wi + 1, win[0], win[-1], len(roster),
                    len(speaker_map), len(face_map))

    logger.info("[reconcile] ep=%s 완료 — 윈도우 %s개, 슬롯 %s개, 화자배정 %s, 얼굴부착 %s, tokens=%s",
                webtoon_episode_id, n_win, len(roster), len(speaker_map), len(face_map),
                agg_usage["total_tokens"])
    return {"slots": roster, "speaker_map": speaker_map, "face_map": face_map,
            "usage": agg_usage, "error": None if n_win else "no_windows"}
