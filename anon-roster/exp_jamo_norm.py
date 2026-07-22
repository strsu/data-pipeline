"""E5 — Hangul 자모 정규화 프로토타입.

redesign §8 ②: 슬롯별 이름 후보를 자모 편집거리로 병합해 오독 변종을 하나로.
가설: 에르웬/에르완/에르렌/에르윈은 음절 편집거리론 안 붙는데(각 1음절 다름), **자모** 편집거리론
가깝게 붙는다(종성/중성 1개 차이). 반대로 진짜 다른 이름(시아·소금·소론)은 안 붙어야 한다.

외부 라이브러리 없이 Unicode 산술로 자모 분해(초/중/종성). 데이터: exp8 실제 오독 + 정본.
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


def edit(a: list, b: list) -> int:
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


def syl_dist(a: str, b: str) -> int:
    return edit(list(a), list(b))


def jamo_dist(a: str, b: str) -> int:
    return edit(to_jamo(a), to_jamo(b))


def jamo_ratio(a: str, b: str) -> float:
    ja, jb = to_jamo(a), to_jamo(b)
    denom = max(len(ja), len(jb)) or 1
    return 1 - jamo_dist(a, b) / denom


def cluster(names: list[str], thresh: float = 0.6) -> list[list[str]]:
    """자모 유사도 >= thresh 면 같은 클러스터(단순 union-find)."""
    parent = {n: n for n in names}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for i, a in enumerate(names):
        for b in names[i + 1:]:
            if jamo_ratio(a, b) >= thresh:
                parent[find(a)] = find(b)
    groups: dict[str, list[str]] = {}
    for n in names:
        groups.setdefault(find(n), []).append(n)
    return list(groups.values())


if __name__ == "__main__":
    # exp8 실제 오독(에르웬) + 정본 + 진짜 다른 이름들(유령 포함)
    erwen = ["에르웬", "에르완", "에르렌", "에르윈"]
    print("=== 에르웬 변종: 음절 vs 자모 편집거리(정본 대비) ===")
    for v in erwen[1:]:
        print(f"  에르웬~{v}: 음절거리={syl_dist('에르웬', v)}  자모거리={jamo_dist('에르웬', v)}  자모유사={jamo_ratio('에르웬', v):.2f}")
    print("  레이븐~레이본: 음절거리=%d 자모거리=%d 자모유사=%.2f" % (
        syl_dist("레이븐", "레이본"), jamo_dist("레이븐", "레이본"), jamo_ratio("레이븐", "레이본")))

    print("\n=== 클러스터링 (에르웬 변종 + 유령/진짜 이름 섞어서) ===")
    pool = erwen + ["시아", "소금", "소론", "제베", "투리엔", "비요른", "비요문서",
                    "레이븐", "레이본", "미샤", "미샤의 아버지"]
    for th in (0.55, 0.65, 0.75):
        groups = cluster(pool, th)
        merged = [g for g in groups if len(g) > 1]
        print(f"  thresh={th}: 병합된 그룹 → {merged}")
