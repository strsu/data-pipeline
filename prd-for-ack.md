# Stage A — 아크(Arc) 추출 설계 (draft)

> **상태**: draft · 작성일: 2026-07-07 · 브랜치: `claude/webtoon-episodes-arcs-6fgwd1`
> **정본 관계**: 본 문서는 `prd.md` §13 A1(로드맵 미착수)·§17.4 Stage A·§17.7-3의 **설계 구체화 draft**다. 확정되면 요지는 `prd.md`로 흡수하고 본 문서는 아카이브한다. 스키마/테이블 정본은 여전히 `service .../models.py`와 `prd.md` §7/§17/§18.
> **범위**: `data-pipeline`(Stage A 생산자 신설) + 소량 `service`(load_prior 역주입은 pipeline, 조회 API·확정 UI는 service/webtoonmoa). **아직 코드 수정 없음 — 논의용.**

---

## 0. 한 줄 요약

에피소드 요약(`episode_report`)이 쌓이면, **별도 액티비티**가 회차를 하나씩 이어 붙이며 "여기서 하나의 서사 단위가 닫혔나"를 판정해 **아크 경계**를 찾고, 구간을 종합해 `story_arc`로 커밋한다. 아크 요약은 다시 다음 회차의 prior로 역주입되어 "1~30화 나열식 요약 늘어짐"을 근본에서 해소한다.

---

## 1. 배경 / 문제

- **에피간 단기 연결은 이미 됨**: `narrative_context.load_prior`가 매 회차 R/N에 `confirmed_roster`(정체성) + `open_threads`(떡밥) + 최근 10화 `episode_report.summary`를 prior로 주입한다(`step3.py:3232`).
- **장기 연결이 없음**: raw 회차 요약을 무한히 이어 붙이는 방식은 화수가 쌓일수록 열화한다 — `prd.md` §17.4가 지목한 "1~30 fold 나열식 늘어짐"의 원인. 요약 위에 **아크 추상화 계층**이 없다.
- **`story_arc`는 껍데기만 있음**: 모델(`StoryArc`, `service .../models.py:993` — arc/part 계층·parent self-FK·episode_start/end·summary·appeal_point·is_confirmed), run 상수(`KIND_ARC`, `runs.py:27`), usage enum(`stage='arc'`) 전부 존재하나 **생산자 코드가 0**이다(주석 언급뿐, `load_prior`도 실제로는 안 읽음).

## 2. 목표 / 비목표

**목표**
1. 에피소드 경계를 가로지르는 **아크 단위 서사 종합**을 자동 산출(`story_arc`).
2. 아크 요약을 **prior로 역주입**해 장기 연결을 요약 체이닝에서 아크 추상화로 대체.
3. 경계 판정은 **반자동 제안 + human 확정**(`is_confirmed`) — 어려운 부분(경계)에 HITL.

**비목표**
- Step1~3 로직 변경 없음(아크는 `episode_report`만 소비, raw 컷 안 봄).
- 실시간성 요구 없음 — 회차 처리 뒤 지연 실행 OK.
- part(부) 다계층 자동 분절은 후속. **flat arc부터.**

## 3. 핵심 설계 결정 (사용자 방향)

| # | 결정 | 이유 |
|---|---|---|
| D1 | **Step 체인이 아닌 별도 액티비티** | 아크는 웹툰 단위·주기가 다르고 실패 격리·usage 귀속이 별개. `KIND_ARC` run이 이걸 위해 이미 있음 |
| D2 | **스트리밍 증분 경계 탐지**(회차 하나씩 붙여가며 판정) | 배치보다 파이프라인 철학에 맞고, 재처리 시 해당 아크만 재생성 |
| D3 | **판정 질문 = "서사 단위가 닫혔나"** (❌"연관 있나") | 회차는 거의 항상 "연관 있음" → 이진 연관 질문은 경계가 영영 안 닫혀 통짜 아크 1개가 됨 |
| D4 | **`narrative_thread` 해소를 객관 앵커로** | 아크가 닫힐 땐 open thread 뭉치가 몰려 resolved됨 — 이미 DB에 있는 신호. LLM 감(感)에만 의존 안 함 |
| D5 | **greedy + 1화 히스테리시스** | 종료 신호가 떠도 1화 더 보고 확정 → 숨고르기(막간) 1화를 새 아크로 오분할하는 false split 방지 |
| D6 | **최대 길이 가드**(예: 20화) | 종료 신호가 안 뜨는 긴 웹툰이 아크 하나로 50화 가는 것 방지 → part 소프트 경계 제안 |
| D7 | **반자동 제안 + human 확정** | 경계는 hindsight로만 명확 → `is_confirmed=false`로 제안, webtoonmoa에서 확정/경계 보정 |
| D8 | **초기 백필 = 배치 1패스 재분절, 신규 = 온라인** | 이미 30화 분석된 웹툰을 greedy 재생하지 않고 전체 요약을 한 번에 분절 |

---

## 4. 알고리즘

### 4.1 상태

웹툰당: `current_arc_start`(열린 아크의 시작 회차 no), 잠정 경계 `tentative_boundary`.

### 4.2 온라인 경로 (신규 회차 E의 resolve/apply 성공 후)

```
1. 창 로드: episode_report[current_arc_start .. E] (summary/teaser/appeal/cliffhanger
   + beats hook_type + threads planted/resolved + roster 변화).
2. thread 신호 계산: 이 창에서 resolved된 open_thread 수 / 새로 planted된 고비중 thread 수.
3. Stage A LLM 호출 → { arc_closed, boundary_episode, confidence,
                        arc_title, arc_summary, arc_appeal_point,
                        closed_thread_ids, next_arc_hook }.
4. arc_closed=false → E를 창에 편입, 종료(다음 회차 대기).
5. arc_closed=true (경계 B, 보통 B=E-1):
   5a. confidence < τ  또는  resolved thread=0  → 약한 신호로 보고 편입(4번처럼)유지.
   5b. 강한 신호 → tentative_boundary=B 로 마킹하되 즉시 확정 안 함(히스테리시스).
       다음 회차(E+1)에서 "B+1..E+1이 진짜 새 아크로 굴러가나" 재확인:
         - 새 아크가 momentum 있음(신규 hook 유지/≥2화) → arc[current_arc_start..B] 커밋(is_confirmed=false),
           current_arc_start = B+1.
         - B+1이 옛 아크로 회귀(막간이었음) → tentative_boundary 취소, 계속 누적.
6. 길이 가드: E - current_arc_start + 1 >= MAX_ARC_LEN(기본 20)이고 미종료 →
   part 레벨 소프트 경계 제안(is_confirmed=false, level=part 후보) + 계속.
```

### 4.3 배치 경로 (초기 백필 — 이미 요약이 다 있는 웹툰)

전체 `episode_report`를 회차순으로 한 콜(또는 토큰 초과 시 슬라이딩 윈도우)에 넣고 **한 번에 분절** → `[{start,end,title,summary,appeal}]` 리스트를 받아 story_arc 일괄 커밋(is_confirmed=false). 이후는 온라인 경로로 이어감. 온라인 greedy를 과거 회차에 재생하지 않는다.

### 4.4 재처리

회차 재해소로 `episode_report`가 바뀌면 해당 회차가 속한 아크를 stale로 보고 **그 아크만** 재산출(Step3 쓰고-버리기 철학 계승). human 확정된 경계(`is_confirmed=true`)는 동결 — 요약만 갱신.

---

## 5. 입력 데이터 — 무엇을 넣나

아크는 **raw 컷/이미지를 안 본다.** `episode_report` 계층의 압축 산출만 소비한다(§17.4 계약). 창 `[start..E]`의 각 회차에서:

| 필드 | 출처 | 역할 |
|---|---|---|
| `episode_no` | webtoon_episode.no | 경계 좌표 |
| `summary` | episode_report.summary | 정보성 줄거리(아크 종합의 본문 재료) |
| `appeal_point` | episode_report.appeal_point | 아크 소구 포인트 종합 |
| `cliffhanger` | episode_report.cliffhanger | 회차 간 텐션 연결 신호 |
| `beats[].hook_type` | episode_beat.hook_type | 회차 내 서사 리듬(예: "결전 — 천마와 청명의 최후", "반전·코미디 — 거지 소년으로 환생") → 국면 전환 감지 |
| `beats[].intensity` | episode_beat.intensity | 클라이맥스/해소 위치 추정 |
| **thread 활동** | narrative_thread (planted/resolved_episode) | **경계 앵커** — 이 창에서 열린/닫힌 떡밥 |
| **roster 델타** | analysis_character(first_seen) + character_claim(사망 등) | 주요 인물 등장/퇴장/사망 = 국면 전환 보조 신호 |

> **thread를 왜 앵커로 쓰나(D4)**: 1화 떡밥 "천마의 유언 — 마는 다시 돌아올 것이다"는 초반 아크에선 안 닫히는 장기 떡밥(아크를 안 끊음). 반면 "구파일방에 화산파 부재"(1화 클리프행어)→2화에서 "직접 확인하러 화산으로"로 **행동 목표로 전환·부분 해소**되는 흐름은 아크 진행 신호다. thread 해소 밀도가 LLM 판정에 객관 근거를 준다.

### 입력 JSON 예시 (창 = 1~2화)

```json
{
  "webtoon_title": "화산귀환",
  "current_arc_start": 1,
  "window": [
    {"episode": 1,
     "summary": "구파일방 결사대가 천마와의 결전 끝에 공멸하고, 청명이 천마의 목을 치고 전사한다. 백 년 뒤 거지 소년의 몸에 환생한 청명은 …",
     "appeal_point": "전설의 검객이 백 년 뒤 거지 소년으로 환생하며 겪는 비극과 코미디의 극단적 톤 전환 …",
     "cliffhanger": "구파일방에 화산파라는 문파가 존재하지 않는다는 사실이 밝혀진다",
     "beat_hooks": ["비극적 전개 — 전장의 참상", "결전 — 천마와 청명의 최후",
                    "정체 공개 — 매화검존의 죽음", "회상 — 화산의 주마등",
                    "반전·코미디 — 거지 소년으로 환생", "충격적 진실 — 변해버린 세상"],
     "threads_planted": [{"id": 11, "desc": "천마의 유언 — 마는 다시 돌아올 것"},
                         {"id": 12, "desc": "화산파의 소멸"}],
     "threads_resolved": [],
     "roster_delta": {"introduced": ["청명/초삼", "왕초", "개방 소년"], "died": ["천마(회상)"]}},
    {"episode": 2,
     "summary": "구칠에게 화산파가 망했다는 말을 들은 청명은 직접 확인하기 위해 섬서 화산으로 향하기로 결심한다. …",
     "appeal_point": "천하제일 검객이 어린 거지의 몸으로 기초부터 다시 쌓으며 …",
     "cliffhanger": "청명이 화산 장문인 앞에서 '본도 청명, 화산에 입문하고 싶습니다!'라고 선언하나 반응 미공개",
     "beat_hooks": ["충격 가중 — 화산 망했다는 소식", "결의와 작별 — 직접 확인하러 화산으로",
                    "수련 — 육합공 선택과 성취", "액션·갈등 — 시장 거지 무리와 충돌",
                    "클리프행어 — 화산 도착, 입문 청원"],
     "threads_planted": [{"id": 21, "desc": "장문인이 청명의 입문을 받아들일지"}],
     "threads_resolved": [{"id": 12, "desc": "화산파의 소멸 — 망했으나 장문인·제자 존재 확인"}],
     "roster_delta": {"introduced": ["구칠", "운암", "장문인"], "died": []}}
  ]
}
```

---

## 6. 프롬프트 설계

### 6.1 시스템 프롬프트 (Stage A)

핵심은 D3(질문 재구성) + D4(thread 앵커) + 근거 강제. 텍스트 모델(`role='text'`, glm-5.2)로 라우팅.

```
너는 웹툰 서사 구조 분석가다. 회차 요약들을 보고 "하나의 아크(완결된 서사 단위)"의
경계를 판정한다.

[아크의 정의]
- 아크 = 하나의 목표/갈등이 제시되고(setup) → 전개·상승하며 → 해소되거나 새 국면으로
  전환되는(payoff/pivot) 완결된 서사 단위. 회차 수는 무관(3화짜리도, 15화짜리도 가능).
- "인물이 겹친다 / 세계관이 같다 / 이야기가 이어진다"는 아크 경계와 무관하다 — 웹툰은
  거의 항상 연결돼 있다. 판정 기준은 "연결됐나"가 아니라 "하나의 단위가 닫혔나"다.

[경계 신호 — 아래가 겹칠수록 아크가 닫힌 것]
1. 이 구간을 이끌던 핵심 목표/갈등이 달성·해소됨(threads_resolved가 그 근거).
2. 다음 회차에서 명백히 새로운 목표/무대/국면이 시작됨(장소 이동, 새 적대자, 시간 도약).
3. 주요 인물의 결정적 상태 변화(사망/합류/이탈 — roster_delta).
[비-신호 — 아크를 끊지 않음]
- 장기 떡밥(여러 아크를 관통, 예: "천마의 귀환")은 열려 있어도 아크를 안 끊는다.
- 회차 내 톤 전환(비극→코미디)이나 단발 액션은 아크 경계가 아니다.
- 정보/휴식 성격의 1화짜리 막간은 독립 아크로 쪼개지 말 것.

[근거 강제]
- arc_closed=true면 반드시 closed_thread_ids(해소된 떡밥)나 명시적 국면 전환 근거를
  evidence에 회차 번호와 함께 적는다. 근거 없이 닫지 말 것.
- 확신 없으면 arc_closed=false(과분할보다 미분할이 안전 — 히스테리시스로 뒤에서 교정).
- 자연어 출력은 한국어. 엄격 JSON.
```

### 6.2 유저 프롬프트

`§5`의 입력 JSON + 지시:

```
현재 열린 아크는 {current_arc_start}화부터다. window의 마지막 회차까지 봤을 때
이 아크가 어느 회차에서 닫혔는지 판정하라.
```

### 6.3 출력 계약 (엄격 JSON)

```json
{
  "arc_closed": true,
  "boundary_episode": 4,          // 아크가 닫힌 마지막 회차(닫힘=true일 때). null이면 미종료
  "confidence": 0.0,              // 0~1
  "evidence": "3~4화에서 '화산 입문' 목표가 장문인 승낙(thread 21 해소)으로 달성되고, 5화는 새 무대(수련 라이벌)로 전환",
  "closed_thread_ids": [12, 21],
  "arc_title": "환생과 귀환 — 화산으로",
  "arc_summary": "천마와의 공멸로 전사한 매화검존 청명이 백 년 뒤 거지 소년의 몸으로 환생, 쇠락한 화산파의 실상을 확인하러 찾아가 입문을 청하기까지.",
  "arc_appeal_point": "천하제일 검객이 최약체 몸으로 밑바닥부터 다시 시작하는 격차의 쾌감과 비극·코미디의 톤 전환",
  "next_arc_hook": "화산 입문 이후 제자로서의 재기"   // 다음 아크 씨앗(있으면)
}
```

- `arc_closed=false`면 boundary/title/summary는 생략 또는 null, `next_arc_hook`만 관찰용.
- **teaser는 아크 레벨에 안 만든다**(스포 차단 복잡도 회피 — 회차 teaser로 충분). 필요 시 후속.

### 6.4 프롬프트 규칙이 실측 함정을 어떻게 막나 (§18.6 오라클 대응)

- **환생 동일인(청명=초삼)**: roster_delta는 정본 `analysis_character`(aliases 병합)를 쓰므로 프롬프트가 두 이름을 별개 등장으로 오인하지 않는다.
- **회상/현재 분리**: beat_hook에 "회상 — 화산의 주마등"이 태깅돼 있어 회상 구간을 국면 전환으로 오판하지 않게 힌트.
- **천마 생사**: 장기 떡밥(귀환 유언)은 "비-신호"로 명시 → 초반 아크를 억지로 안 끊음.

---

## 7. 출력 / 커밋

- `analysis_run(kind='arc', webtoon FK, episode NULL)` 1건 생성 → 성공 시 succeeded.
- `story_arc(level='arc', webtoon, ordinal, title, episode_start, episode_end, summary, appeal_point, is_confirmed=false)` upsert. parent(part)는 미사용(후속).
- `llm_usage(stage='arc', run FK)` 콜당 1행.
- 커밋은 결정론(LLM 판정 결과를 그대로 기록) — human 확정 행(`is_confirmed=true`)은 동결(경계·제목 덮어쓰기 금지, 요약만 갱신 허용은 옵션).

## 8. prior 역주입 (장기 연결의 완성)

`narrative_context.load_prior`를 확장:
- 현행: 최근 10화 raw `episode_report.summary`.
- 변경: **확정/제안된 아크 요약 + 현재 열린 아크의 최근 회차 요약 2~3개**로 대체.
  - 즉 "완결된 과거 = 아크 요약 N개", "진행 중 = 최근 회차 raw 요약 소수"의 2계층.
- 효과: 30화 시점 prior가 "아크 3~4개 요약 + 최근 2~3화"로 압축 → §17.4 "나열식 늘어짐" 해소. `_compress`의 "오래된 요약부터 제거"도 "오래된 아크 요약은 유지, 회차 raw만 제거"로 자연스러워짐.
- ⚠️ 아크가 아직 없는 초반부는 현행(회차 요약)로 폴백 — 아크 생산 전에도 회귀 없음.

## 9. Temporal / 트리거

- 온라인: `EpisodeChainWorkflow`의 step3c(apply) 성공 후 **아크 액티비티를 조건부 kick**(별도 activity, `ARC_QUEUE` 또는 기존 텍스트 큐 재사용). 실패해도 회차 파이프라인엔 영향 없음(격리).
- 배치: `python -m src.tools.build_arcs <source> <title_id>` CLI(백필·수동 재분절). 재해소 자동 트리거는 §11 오픈.

## 10. 오픈 이슈

1. **히스테리시스 확정 시점** — 1화 지연이 충분한가, 2화까지 볼 것인가(막간이 2화짜리인 경우).
2. **thread 신호 임계** — "resolved ≥ K개 && confidence ≥ τ"의 K·τ 초기값(데이터 없음 → 1~2화 실측 후 보정, 비트 경계 불안정 §9.12-5과 동류 리스크).
3. **경계 안정성** — 재해소로 요약이 흔들리면 경계도 흔들림. human 확정 경계 동결 + `ordinal` 안정 키 필요.
4. **part(부) 다계층** — 아크 위 상위 계층 자동 분절은 아크가 쌓인 뒤 별도 설계.
5. **길이 가드 값** — MAX_ARC_LEN 기본 20이 장르별로 적정한지(옴니버스 vs 장편).
6. **아크 레벨 teaser/스포 차단** 필요 여부(현재 비목표).

## 11. 구현 순서 (제안)

1. Stage A 코어(`src/core/arc.py`: 창 로드 + 프롬프트 + JSON 파싱) + `story_arc` 커밋.
2. 배치 CLI로 화산귀환 1~N화 오프라인 검증(경계가 상식과 맞나 — 1~2화가 "환생·귀환" 아크로 묶이나).
3. `load_prior` 역주입(아크 요약 계층).
4. 온라인 트리거(Temporal) + 재해소 stale 처리.
5. webtoonmoa 확정 UI(경계 조정 + is_confirmed).
6. 실측 후 K·τ·MAX_ARC_LEN·히스테리시스 튜닝.
