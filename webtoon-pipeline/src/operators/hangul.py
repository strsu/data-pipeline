"""Hangul 자모 정규화 — 이름 표면형 변종 병합(redesign §8②·Phase 4).

오독 변종(에르웬/에르완/에르렌/에르윈)은 음절 편집거리론 안 붙는데(각 1음절 차) 자모(초/중/종성)
편집거리론 가깝다. name_edge canonical 선택·교차회차 dedup의 유사도 키. 외부 라이브러리 없이
Unicode 산술로 분해. `anon-roster/exp_jamo_norm.py`(실측 thresh 0.65~0.70)의 프로덕션 이식.

⚠️ 자모는 **문지기가 아니다**: "이름이냐" 판정은 발화행위 게이트가 하고, 자모는 이미 이름으로 확정된
표면형끼리의 정규화만 한다(redesign §8 순서). 죽은 이름 방향은 merge 아니라 상위 로직의 veto.
"""
from __future__ import annotations

CHO = list("ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ")
JUNG = list("ㅏㅐㅑㅒㅓㅔㅕㅖㅗㅘㅙㅚㅛㅜㅝㅞㅟㅠㅡㅢㅣ")
JONG = [""] + list("ㄱㄲㄳㄴㄵㄶㄷㄹㄺㄻㄼㄽㄾㄿㅀㅁㅂㅄㅅㅆㅇㅈㅊㅋㅌㅍㅎ")


def to_jamo(s: str) -> list[str]:
    out: list[str] = []
    for ch in s:
        o = ord(ch)
        if 0xAC00 <= o <= 0xD7A3:
            off = o - 0xAC00
            cho, jung, jong = off // 588, (off % 588) // 28, off % 28
            out.extend([CHO[cho], JUNG[jung]])
            if JONG[jong]:
                out.append(JONG[jong])
        elif ch.strip():
            out.append(ch)
    return out


def _edit(a: list, b: list) -> int:
    m, n = len(a), len(b)
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, n + 1):
            cur = dp[j]
            dp[j] = min(dp[j] + 1, dp[j - 1] + 1, prev + (a[i - 1] != b[j - 1]))
            prev = cur
    return dp[n]


def jamo_ratio(a: str, b: str) -> float:
    """자모 유사도 0~1(1=동일). 표면형 변종 판정용."""
    ja, jb = to_jamo(a), to_jamo(b)
    denom = max(len(ja), len(jb)) or 1
    return 1 - _edit(ja, jb) / denom


def name_match(a: str, b: str, thr: float = 0.70) -> bool:
    """두 이름이 같은 인물의 표면형인가 — 자모 유사(변종) 또는 포함(이름↔이름+성).

    자모: 에르웬↔에르완(오독). 포함(substring): '브라운 로트밀러'⊇'로트밀러', '엘리사'⊆'엘리사 베헨크'
    (redesign §17 부분이름 중복). 둘 다 아니면 다른 인물.
    """
    a, b = a.strip(), b.strip()
    if not a or not b:
        return False
    if a == b:
        return True
    # 포함(부분문자열) — 이름+성/전체이름 관계. 공백 토큰 경계 존중(레→레이 오검출 방지).
    ta, tb = set(a.split()), set(b.split())
    if a in b or b in a or (ta and tb and (ta <= tb or tb <= ta)):
        return True
    return jamo_ratio(a, b) >= thr


def canonical_of(a: str, b: str) -> str:
    """같은 인물의 두 표면형 중 정본 — 더 완전한 것(긴 쪽, 이름+성 우선)."""
    return a if len(a.strip()) >= len(b.strip()) else b
