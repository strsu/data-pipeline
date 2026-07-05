"""stale 에피소드 재해소 CLI — human 수정(얼굴 확정/텍스트 수정) 후 Step3 재실행(v4.0).

    python -m src.tools.reresolve <source> <title_id> <episode_no|stale> [--rerun-extract]

    episode_no      : 해당 회차 1개를 재해소.
    stale           : 재해소가 필요한 회차 전부를 순서대로 재해소. "필요"는 저장 플래그가
                      아니라 도출이다(§17.1): webtoon_cut.human_modified_at >
                      최신 succeeded resolve run.finished_at.
    --rerun-extract : 비전(Stage V)부터 재실행. **human이 얼굴↔캐릭터 매칭을 고친 경우 필수** —
                      identified_faces(F라벨→character_id/confirmed) 입력 자체가 바뀌기 때문.
                      텍스트/이름만 바뀐 경우는 생략 가능(영속 provisional 레코드로 R부터).

재해소는 새 resolve run(성공 시 succeeded)을 만들고, 그 finished_at이 human_modified_at을
넘어서면서 stale 도출이 자연 해소된다 — 클리어할 플래그가 없다. Temporal 없이 직접 코어를
호출하므로 DB/LLM 접속 env(POSTGRES_*, LLM 엔드포인트)가 설정된 셸에서 실행한다
(예: `set -a && source ../prod.env && set +a`).
"""
from __future__ import annotations

import logging
import sys

from src.config.db import db_cursor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("reresolve")


def _webtoon_id(source: str, title_id: str) -> int:
    with db_cursor() as cur:
        cur.execute(
            "SELECT id FROM webtoon WHERE source=%s AND title_id=%s AND deleted_at IS NULL",
            (source, title_id),
        )
        row = cur.fetchone()
    if not row:
        raise SystemExit(f"webtoon not found: {source}/{title_id}")
    return row[0]


def _stale_episodes(webtoon_id: int) -> list[tuple[int, int]]:
    """재해소 필요 (episode_id, no) 목록 — human 수정 > 최신 succeeded resolve run(§17.1 도출)."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT we.id, we.no
            FROM webtoon_episode we
            JOIN LATERAL (
                SELECT MAX(ar.finished_at) AS last_resolved
                FROM analysis_run ar
                WHERE ar.episode_id = we.id AND ar.kind = 'resolve'
                  AND ar.status = 'succeeded' AND ar.deleted_at IS NULL
            ) r ON true
            WHERE we.webtoon_id = %s AND we.deleted_at IS NULL
              AND r.last_resolved IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM webtoon_cut wc
                WHERE wc.episode_id = we.id
                  AND wc.human_modified_at IS NOT NULL
                  AND wc.human_modified_at > r.last_resolved
              )
            ORDER BY we.no
            """,
            (webtoon_id,),
        )
        return list(cur.fetchall())


def _episode_id(webtoon_id: int, episode_no: int) -> int:
    with db_cursor() as cur:
        cur.execute(
            "SELECT id FROM webtoon_episode WHERE webtoon_id=%s AND no=%s AND deleted_at IS NULL",
            (webtoon_id, episode_no),
        )
        row = cur.fetchone()
    if not row:
        raise SystemExit(f"episode not found: no={episode_no}")
    return row[0]


def main(argv: list[str]) -> None:
    if len(argv) < 3:
        print(__doc__)
        raise SystemExit(2)
    source, title_id, target = argv[0], argv[1], argv[2]
    rerun_extract = "--rerun-extract" in argv[3:]

    from src.core import step3  # DB env 확인 후 지연 import

    wid = _webtoon_id(source, title_id)
    if target == "stale":
        episodes = _stale_episodes(wid)
        if not episodes:
            print("재해소 필요 에피소드 없음(human 수정 < 최신 resolve run).")
            return
    else:
        no = int(target)
        episodes = [(_episode_id(wid, no), no)]

    print(f"재해소 대상 {len(episodes)}개 회차 (rerun_extract={rerun_extract}): "
          f"{[no for _, no in episodes]}")
    for ep_id, no in episodes:
        logger.info("=== ep%s (id=%s) 재해소 시작 ===", no, ep_id)
        out = step3.reresolve_episode(ep_id, rerun_extract=rerun_extract, webtoon_id=wid)
        stats = (out.get("episode_meta") or {}).get("stats") or {}
        logger.info(
            "=== ep%s 완료 — run=%s records=%s resolve_error=%s speakers=%s ===",
            no, out.get("run_id"), out.get("records"), out.get("resolve_error"),
            stats.get("speakers_resolved"),
        )


if __name__ == "__main__":
    main(sys.argv[1:])
