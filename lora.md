# Webtoon Analyzer LoRA — 설계안 (초안 리뷰 반영판)

작성: 2026-07-15. 초안(AI 생성)을 실제 코드·프로덕션 DB 실측으로 검증하고 수정한 문서.
정본 파이프라인 설계는 `prd.md` 참조 — 이 문서는 LoRA 학습 계획만 다룬다.

---

## 0. 결론 요약

- **방향(방법론 내재화 LoRA)은 유효하나, 초안의 핵심 전제 2개가 실측과 어긋남**: ① 시스템 프롬프트는 5k~10k토큰이 아니라 900~1,200토큰, ② 텍스트 스테이지 콜의 비용은 프롬프트가 아니라 payload(에피소드 메타데이터)가 지배 → "프롬프트 절약" 프레이밍은 성립 안 함.
- **LoRA의 진짜 가치**: 큰 API급 모델(glm-5.2/glm-4.6v) 수준의 방법론 수행을 **작은 로컬 모델(9B급)에 이식**하는 것. 속도·자립성 이득은 여기서 나온다.
- **1차 타깃은 초안이 제외했던 vision Pass1** — 유일하게 데이터 볼륨이 있고(4,466콜), qwen 9B의 구조적 실패 모드(블록 병합 위반)가 정확히 fine-tuning으로 고치는 유형임이 실측됨(2026-07-10).
- **텍스트 스테이지(roster/R/N)는 보류** — 에피소드 33개(콜 46~48건)로 학습 불가능 + 방법론이 주 단위로 변경 중.
- **모든 것의 선결 조건 = 학습쌍 캡처 인프라.** LiteLLM DB는 요청 프롬프트를 저장하지 않고 응답도 잘림 → 과거 콜의 소급 데이터셋화 불가. 호출 시점 캡처를 지금 넣어야 데이터가 쌓이기 시작한다.

---

## 1. 실측 근거 (2026-07-15, prod DB)

### 1.1 시스템 프롬프트 크기 (step3.py 실측, chars/2 휴리스틱)

| 스테이지 | 크기 |
|---|---|
| Pass1 (`_PASS1_SYSTEM_PROMPT`) | ~900 tok |
| Roster (`_ROSTER_SYSTEM_PROMPT` + guidance) | ~600 tok |
| Resolve (`_RESOLVE_SYSTEM_PROMPT`, guidance 포함) | ~1,100 tok |
| Narrative (`_NARRATIVE_SYSTEM_PROMPT`, guidance 포함) | ~1,050 tok |

### 1.2 콜 볼륨·크기 (`analysis_llm_usage`)

| stage | 콜 수 | 평균 total_tokens | 프롬프트 비중 |
|---|---|---|---|
| vision (Pass1) | 4,466 | ~4,100 | ~20% |
| resolve | 48 | ~95,500 | **~1%** |
| roster | 46 | ~56,000 | ~2% |
| narrative | 47 | ~43,000 | ~3% |
| judge | 103 | ~31,000 | — |
| profile | 10 | ~34,000 | — |

- succeeded resolve 에피소드: **33개**. 텍스트 스테이지 학습 예제 = 스테이지당 50건 미만.
- human 라벨: suggestion accepted 20 / rejected 85 — 학습용이 아니라 **평가셋 시드** 수준.

### 1.3 함의

- 텍스트 스테이지에서 프롬프트를 0으로 만들어도 콜 크기 1~3% 감소 — 무의미. 메타데이터 payload는 LoRA가 제거할 수 없는 본질 입력.
- resolve 예제 1건 = 시퀀스 ~95k tok → 이 길이의 (Q)LoRA 학습은 하드웨어 부담이 크다. 컷 단위(~4k)인 Pass1과 대비됨.
- 속도 향상의 유일한 경로 = **베이스 모델 다운사이징** (glm 계열 → qwen 9B급). LoRA는 그것을 가능하게 하는 수단.

---

## 2. 목표 재정의

초안의 "Huge System Prompt 제거"가 아니라:

> **큰 teacher 모델의 웹툰 분석 방법론을 작은 open-weight student 모델에 증류하여,
> 품질 저하 없이 로컬 서빙 가능한 스테이지부터 단계적으로 전환한다.**

기대 효과 (수정):

1. ~~System Prompt 감소~~ → 부수 효과일 뿐, 목표 아님
2. 추론 속도 향상 — **베이스 다운사이징에서** (qwen 9B는 glm-4.6v 대비 3~6배, 2026-07-10 실측)
3. 분석 일관성 향상 — 단, 방법론 경직화와 트레이드오프 (§6)
4. 방법론 내재화 — 특히 프롬프트로 못 고치는 **구조적 출력 습관** 교정
5. ~~JSON Output 안정화~~ → **LoRA 불필요, vLLM guided decoding으로 즉시 해결** (§7.3)
6. 외부 API 의존도 축소, self-hosted 자립
7. Self-improvement 루프 — 단, human 게이트 필수 (§8)

---

## 3. 선결 조건: 학습쌍 캡처 인프라 (Phase 0 — LoRA 여부와 무관하게 지금)

### 3.1 왜 소급이 불가능한가

- `litellm` DB의 `LiteLLM_SpendLogs.messages`(요청 프롬프트)는 **빈 값 `{}`으로 저장됨** (2026-07-07 실측, UI에서만 보임).
- `response`도 길면 중간 절단 (`MAX_STRING_LENGTH_PROMPT_IN_DB`).
- 즉 지금까지의 vision 4,466콜은 **input 복구 불가 → 데이터셋화 불가.** 캡처를 넣는 시점부터만 쌓인다.

### 3.2 캡처 설계

`call_llm_json`(공통 LLM 호출 경로) 레벨에서 호출 시점에 영속화:

```json
{
  "stage": "vision | roster | resolve | narrative | judge | profile",
  "system_prompt_version": "<프롬프트 텍스트의 해시 or 명시 버전>",
  "model": "glm-4.6v",
  "run_id": 123,
  "webtoon_id": 1, "episode_id": 45, "cut_id": 678,
  "input": { "system": "...", "user": "...", "images": ["<r2 key>"] },
  "output_raw": "<모델 원문 응답 — repair 전>",
  "output_parsed": { ... },
  "finish_reason": "stop",
  "created_at": "..."
}
```

- 저장처: JSONL 파일(로컬/R2) 또는 전용 테이블. 조회·필터 편의상 테이블 + 대용량 필드 R2 오프로드 권장. **최종 목표가 지식베이스(§17.1 "재생성가능≠휘발")인 만큼 캡 없이 전량 영속.**
- `system_prompt_version` 해시가 핵심 — 방법론이 바뀌면 구버전 프롬프트로 생성된 쌍을 필터로 걸러낼 수 있어야 한다 (§9).
- 이미지 input은 원본 재전송 대신 R2 키 참조로 (Pass1 오버레이 이미지는 재생성 가능하나 결정론 보장 위해 키 고정 권장).

---

## 4. 스테이지별 타당성 판정

| 스테이지 | 데이터 | 시퀀스 길이 | 방법론 안정성 | 판정 |
|---|---|---|---|---|
| **vision (Pass1)** | 4,466 (증가 중) | ~4k | 상대적 안정 | ✅ **1차 타깃** |
| roster | 46 | ~56k | 변경 중 | ⏸ 보류 |
| resolve | 48 | ~95k | 변경 중 (3주간 step3.py 16커밋) | ⏸ 보류 |
| narrative | 47 | ~43k | 변경 중 | ⏸ 보류 |
| profile | 10 | ~34k | regen 방금 배포 | ⏸ 보류 |
| judge (adjudicate) | 103 | ~31k | 드라이런 단계 | ⏸ 보류 |

### 4.1 왜 Pass1인가

1. **볼륨**: 유일하게 수천 건. 컷 단위라 계속 빠르게 쌓임.
2. **시퀀스**: ~4k로 QLoRA 학습이 로컬 하드웨어에서 현실적.
3. **명확한 실패 모드**: qwen-base 9B 실측(2026-07-10, `model-eval-qwen-base-vision.md`) — 이해력은 glm-4.6v 대등·3~6배 빠름이나, **블록 1:1 병합 위반이 greedy에서도 모달 = 샘플링/프롬프트로 교정 불가.** 구조적 출력 습관 교정은 fine-tuning이 가장 잘 하는 일.
4. **구도**: glm-4.6v(teacher) → qwen-vl 9B급(student) 증류. "이해력 대등한 빠른 모델의 유일한 결함을 학습으로 제거".
5. **다운스트림 내성 확인(2026-07-16)**: qwen 비전 출력을 그대로 step3b(glm-5.2 로스터/R/N)에 흘려도 최종 요약/티저/비트/떡밥이 **운영(glm-4.6v) 산출과 동등**했다(바바리안 1화, `pass1-bench-w23-2026-07-16.md` §다운스트림 검증). qwen의 1:1 병합/누락은 Stage-V 구조 지표에 갇히고 서사로 전파되지 않음 → 비전 슬롯 전환(Phase 2)의 실패 반경이 작다. 재현: `tools/pass2_run.py`(오프라인·읽기전용) + `tools/pass_compare_view.py`(비교 HTML).

### 4.2 텍스트 스테이지 재개 조건 (모두 충족 시)

- succeeded resolve 에피소드 수백 건 이상 축적
- 해당 스테이지 프롬프트/스키마가 4주 이상 무변경 (방법론 수렴 신호)
- Phase 0 캡처로 현행 프롬프트 버전의 쌍이 충분히 확보됨

---

## 5. 데이터셋 설계

### 5.1 예제 스키마 (멀티태스크 단일 포맷)

```json
{
  "id": "vision-run123-cut678",
  "stage": "vision",
  "system_prompt_version": "sha256:abcd...",
  "teacher_model": "glm-4.6v",
  "quality": "teacher_raw | human_verified | human_corrected",
  "instruction": "<축약된 태스크 지시 — 학습 후 시스템 프롬프트를 대체할 최소 지시>",
  "input": { "...스테이지별 payload 그대로..." },
  "output": { "...스키마 준수 JSON..." },
  "meta": { "webtoon_id": 1, "episode_id": 45, "cut_id": 678, "run_id": 123 }
}
```

- `instruction`은 현행 풀 프롬프트가 아니라 **학습 후 쓸 짧은 태스크 태그** (예: "웹툰 컷을 분석해 blocks/characters/summary JSON을 산출하라"). 방법론 본문은 output 패턴으로 내재화시키는 게 목적이므로 instruction에 규칙을 다시 쓰지 않는다.
- `quality` 등급이 학습 포함 여부를 결정: `human_corrected` > `human_verified` > `teacher_raw`. teacher_raw는 아래 필터 통과분만.

### 5.2 teacher_raw 필터 (오류 증류 방지)

teacher 산출을 무비판적으로 학습하면 teacher의 오류 모드까지 내재화된다(청명 19명 사건, 심판 named 통째병합 등 전례). 편입 조건:

- `finish_reason = stop` (절단/캡 도달 제외)
- json-repair 개입 없이 파싱된 응답 우선
- 해당 컷/에피소드에 **human 수정이 없거나** (human_modified_at, human face_identity, rejected suggestion), 있다면 human 값으로 output을 교정한 뒤 `human_corrected`로 편입
- rejected suggestion과 연관된 산출은 학습 제외 → **회귀 평가셋으로 이동** (§7.2)

### 5.3 규모 목표 (Pass1 v1 기준)

- 학습: 3,000~5,000쌍 (현행 프롬프트 버전으로 캡처된 것만)
- 검증: 300~500쌍 (웹툰 단위로 분리 — 같은 웹툰이 train/val에 걸치지 않게. 웹툰 암기가 아니라 방법론 일반화를 측정)
- 회귀 평가: rejected suggestions 85건 + 알려진 실패 사례(블록 병합 위반 케이스) 수동 큐레이션

---

## 6. 학습 전략

### 6.1 단일 멀티태스크 LoRA vs 스테이지별 분리

**단일 멀티태스크 LoRA로 시작한다** (`stage` 필드가 태스크 태그).

- 초안의 3분할(Core/Narrative/Profile)은 현 데이터 규모에서 조각당 데이터만 줄이는 손해.
- 서빙은 vLLM multi-LoRA로 분리 비용이 낮지만, 문제는 서빙이 아니라 데이터.
- 분리 전환 조건: 스테이지당 수천 쌍 + 태스크 간 간섭(한 스테이지 성능이 다른 스테이지 학습으로 하락)이 평가에서 실측될 때.
- 단, v1은 어차피 Pass1 단독이므로 이 논점은 텍스트 스테이지 합류 시점의 결정.

### 6.2 방식

- **QLoRA** (4-bit base + LoRA rank 16~64) — 9B급이면 단일 24GB GPU로 가능.
- 베이스: **qwen-vl 9B급** (vision). 텍스트 스테이지 합류 시 qwen3.5 계열 별도 검토 (vision과 텍스트는 베이스가 달라 어차피 어댑터가 분리됨 — "단일 LoRA"는 모달리티 내에서의 얘기).
- glm-5.2/glm-4.6v 자체를 fine-tune하는 선택지는 서빙 가능한 open-weight 확보가 전제 — 현행 구도(작은 student로 증류)가 더 현실적.
- 하이퍼파라미터 탐색보다 **데이터 품질·필터가 성능을 지배**한다는 전제로, 학습 레시피는 표준값에서 시작.

### 6.3 방법론 churn 리스크 (초안 미언급, 최대 리스크)

- step3.py는 최근 3주 16커밋 — 규칙이 주 단위로 변경 중.
- LoRA는 규칙 변경 = 데이터셋 재구축 + 재학습 + 재평가. 시스템 프롬프트의 즉시 수정 가능성을 포기하는 것.
- 대응: ① 방법론이 수렴한 스테이지만 학습 (§4.2), ② `system_prompt_version` 태깅으로 구버전 쌍 필터링 (§3.2), ③ LoRA 배포 후에도 **프롬프트 오버라이드 경로를 유지** — 긴급 규칙 수정은 짧은 추가 지시로 패치하고 다음 버전 학습에 반영.

---

## 7. 추론 파이프라인 · 평가

### 7.1 서빙

- vLLM `--enable-lora` 로 base + adapter 서빙. `config_llm_model`에 LoRA 모델 row 추가 (예: `qwen-vl-webtoon-v1`), modality 슬롯 교체로 전환 — 기존 2-슬롯 해석 구조 그대로 활용.
- fallback 체인 유지: LoRA 모델 실패 시 기존 glm 경로로 (기존 fallback self-FK 메커니즘).

### 7.2 평가 게이트 (배포 전 필수)

- **구조 평가**: 블록 1:1 병합 준수율, 스키마 준수율, index 보존율 — qwen 실패 모드 직격 지표.
- **내용 평가**: teacher(glm-4.6v) 산출과의 필드별 일치율 + 검증셋 human 라벨 정합.
- **회귀셋**: rejected suggestions 85건 + 큐레이션 실패 사례 — "teacher가 틀렸던 것을 따라 틀리는가".
- 게이트: 구조 지표가 glm-4.6v 동등 이상 && 내용 지표 열화 5% 이내일 때만 전환.

### 7.3 JSON 안정화는 학습 없이 즉시

vLLM guided decoding(structured output)으로 스키마 위반 자체를 차단 가능 — 기존 json-repair와 병행. LoRA의 근거 항목에서 제외하고 독립 개선으로 진행.

---

## 8. Self-Improvement 루프 (수정판)

```text
Webtoon → Vision Metadata → Teacher/Student 모델 → JSON
    ↓
호출 시점 캡처 (Phase 0, 전량)
    ↓
Human Review (webtoonmoa 라벨링·suggestion 수락/거절)  ← optional 아님: 학습 편입 게이트
    ↓
품질 필터 (§5.2) → 데이터셋 스냅샷 vN
    ↓
QLoRA 학습 → 평가 게이트 (§7.2) → 통과 시에만 배포
    ↓
프로덕션 사용 → 캡처 계속 → ...
```

- **human 게이트 없는 자기 산출 재학습은 model collapse 경로** — 초안의 "Human Review (Optional)"를 필수로 격상.
- student 배포 후의 캡처는 student 산출이므로, 다음 버전 학습의 teacher_raw로 쓰지 않는다 (자기 증류 금지). human_verified/corrected만 편입하거나, 주기적으로 teacher 재산출로 갱신.

---

## 9. 버저닝

- **데이터셋 스냅샷이 1급 시민**: `dataset-v1 = {prompt_version 집합, 필터 기준, 예제 id 목록}` 을 명시 태깅. 모델 버전은 데이터셋 버전에서 파생 (`lora-v1 ← dataset-v1`).
- 방법론 변경 시: 새 prompt_version으로 캡처 재개 → 구버전 쌍은 규칙 충돌 여부에 따라 유지/폐기 결정 → dataset-v2.
- 모델 row는 `config_llm_model`에 버전별로 추가하고 is_active 전환 — 롤백은 row 스위치.

---

## 10. 로드맵

| 단계 | 내용 | 조건/시점 |
|---|---|---|
| **Phase 0** | 학습쌍 캡처 인프라 (§3) + guided decoding 도입 (§7.3) | ✅ 파일럿 완료(2026-07-16) — 인라인 캡처 대신 **소급 재구축 생성기**(`tools/pass1_bench.py`, `build_pass1_input` 공유)로 구현. 바바리안 1~10화 클린 쌍 1,110건 + qwen-base 베이스라인 실측: `pass1-bench-w23-2026-07-16.md`. guided decoding은 미착수 |
| **Phase 1** | Pass1 vision LoRA v1: glm-4.6v teacher → qwen-vl 9B student, 블록 병합 교정 목표 | 현행 프롬프트 버전 캡처 3k쌍 축적 후 |
| **Phase 2** | 평가 게이트 통과 시 vision 슬롯 전환 (fallback=glm-4.6v 유지) | Phase 1 게이트 통과 |
| **Phase 3** | 텍스트 스테이지 멀티태스크 LoRA 검토 | 에피소드 수백 건 + 방법론 4주 무변경 (§4.2) |
| 상시 | human 라벨 축적 → 평가셋 확충, rejected를 회귀셋으로 | webtoonmoa 라벨링 운영 |

---

## 부록: 초안 대비 주요 수정 사항

1. "System Prompt 5k~10k tok" → 실측 0.9k~1.2k. 비용 지배 요인은 payload.
2. "프롬프트 감소로 속도 향상" → 속도는 베이스 다운사이징에서. LoRA는 그 수단.
3. 텍스트 3-LoRA 분할안 → 데이터 부족(스테이지당 <50건)으로 전면 보류, 재개 조건 명시.
4. 초안이 제외한 vision Pass1을 1차 타깃으로 (데이터 4.5k건 + 실측된 교정 대상 실패 모드).
5. Human Review Optional → 필수 게이트. rejected suggestions는 학습 제외·회귀 평가셋행.
6. JSON 안정화는 LoRA 근거에서 제외 (guided decoding으로 즉시 해결).
7. 선결 조건 추가: 호출 시점 캡처 인프라 (LiteLLM 소급 복구 불가 실측).
8. 버저닝을 모델 중심 → 데이터셋 스냅샷 중심으로.
