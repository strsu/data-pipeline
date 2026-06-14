"""Reembed Agent: webtoon.reembed.start → 첫 에피소드부터 임베딩+매칭 체인 재시작.

A2(이중 임베딩 제거) 이후 임베딩은 face_identify가 phase1a.complete를 직접 구독해
1패스로 수행한다. 따라서 reembed는 별도 임베딩을 하지 않고, 첫 에피소드에 대해
episode.phase1a.complete를 재발행해 face_identify 체인을 처음부터 다시 굴린다.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import faust

from src.agents.ocr_yolo import EpisodePhase1aComplete, episode_phase1a_complete
from src.config.db import db_cursor
from src.worker import app


# ── Kafka 메시지 스키마 ───────────────────────────────────────────────────────

class WebtoonReembedStart(faust.Record):
    source: str
    title_id: str
    webtoon_id: int


webtoon_reembed_start = app.topic("webtoon.reembed.start", value_type=WebtoonReembedStart)


# ── DB 헬퍼 ───────────────────────────────────────────────────────────────────

def _get_first_episode(webtoon_id: int) -> Optional[dict]:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.id, we.no, w.source, w.title_id
            FROM webtoon_episode we
            JOIN webtoon w ON we.webtoon_id = w.id
            WHERE we.webtoon_id = %s
              AND we.deleted_at IS NULL
            ORDER BY we.no ASC
            LIMIT 1
            """,
            (webtoon_id,),
        )
        row = cur.fetchone()
        return {"id": row[0], "no": row[1], "source": row[2], "title_id": row[3]} if row else None


def _reset_phase2_watermark(webtoon_id: int) -> None:
    """phase2 워터마크 리셋 — reembed가 첫 에피소드부터 실제로 재실행되도록 보장.

    face_identify는 `phase2_last_completed_episode`(= last_completed_no) 멱등 가드로
    이미 처리된 에피소드를 조용히 skip한다. 리셋하지 않으면 reembed가 no-op이 된다.
    (기존 자동 생성 캐릭터/Chroma 문서의 전면 정리는 범위 밖 — 배치/Phase4에서 처리.)
    """
    now = datetime.now(timezone.utc)
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE webtoon_pipeline_state
            SET phase2_last_completed_episode_id = NULL,
                phase2_processed_count = 0,
                phase2_status = 'idle',
                updated_at = %s
            WHERE webtoon_id = %s
            """,
            (now, webtoon_id),
        )


# ── Faust Agent ───────────────────────────────────────────────────────────────

@app.agent(webtoon_reembed_start, concurrency=1)
async def reembed_agent(stream):
    """webtoon.reembed.start → 첫 에피소드부터 face_identify 체인 재시작."""
    loop = asyncio.get_running_loop()
    async for msg in stream:
        try:
            first_ep = await loop.run_in_executor(
                None, _get_first_episode, msg.webtoon_id
            )
            if not first_ep:
                print(f"[reembed_agent] no episodes for webtoon_id={msg.webtoon_id}")
                continue

            # 멱등 가드로 인한 no-op 방지: phase2 워터마크 리셋 후 첫 ep부터 재실행
            await loop.run_in_executor(None, _reset_phase2_watermark, msg.webtoon_id)

            await episode_phase1a_complete.send(
                key=f"{msg.source}_{msg.title_id}",
                value=EpisodePhase1aComplete(
                    source=msg.source,
                    title_id=msg.title_id,
                    episode_no=first_ep["no"],
                    webtoon_episode_id=first_ep["id"],
                    total_cuts=0,
                ),
            )
            print(f"[reembed_agent] triggered reembed from ep={first_ep['no']} webtoon_id={msg.webtoon_id}")
        except Exception as e:
            print(f"[reembed_agent] webtoon_id={msg.webtoon_id} error: {e}")
