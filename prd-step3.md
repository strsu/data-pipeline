# Step 3/4 재설계 PRD — 에피소드 단위 연속 이해 + 소구포인트

> **상태**: 초안(working draft) · 2026-06-29 · **2026-07-01 기준 `prd.md` v3.3 §9로 전면 흡수됨 — 이 문서는 실험 로그/인용 근거를 보존하는 이력 문서**. 최신 스펙·구현 상태는 `prd.md` §9를 참조.
> **성격**: 논의 정리용 working PRD였음. 여기서 합의된 설계가 정식 스펙(`.kiro/specs/episode-scene-resolution/`)으로 내려갔고, 그 구현이 `prd.md` §9에 통합 반영됐다.
> **관계**: `prd.md`(마스터) §9(Step3/4)에 **흡수됨**. Step1·2(로컬 추출/얼굴식별)와 Temporal 오케스트레이션 골격은 마스터를 그대로 따른다.
> **한 줄 요약**: "컷 단위 즉시 확정"을 버리고 **"에피소드 단위 2-pass(추출 → 전역 해소)"**로 간다. 그 위에 **컷→비트→에피소드 계층의 소구포인트**를 얹는다.

---

## 1. 문제 정의 — 왜 갈아엎나

현재 `step3.analyze_cut_scene`는 **전진(forward-only) 슬라이딩 윈도우 + 즉시 확정** 구조다.

- **맥락이 한 방향뿐**: 이미지는 N-2/N-1/N만 본다. N+1/N+2는 못 본다. 컷 간 전달은 `prev_context`(직전 컷 `action_summary`) **문자열 한 줄**이라 정보 손실이 크다.
- **컷마다 즉시 DB 확정**: 각 컷에서 바로 `speaker_id` 확정, `name_discoveries` confidence≥0.85면 NEW_CHAR 즉시 rename. "그 컷만 보고 내린 판단"을 영구 기록한다.
- **소급 경로 없음**: 컷 10에서 화자 미상 → `speaker=null`로 적으면, 컷 50에서 이름이 밝혀져도 컷 10을 고칠 길이 없다.

→ "다음/다다음 컷 때문에 비로소 누구 대사인지 알 수 있는" 상황을 구조적으로 못 잡는다. **파라미터가 아니라 아키텍처 문제.**

---

## 2. 목표 / 비목표

### 목표
1. **양방향 연속 이해**: 에피소드 전체를 보고 화자·이름을 해소(나중 컷의 단서를 앞 컷에 소급 반영).
2. **추정 → 확정 분리**: 컷별 결과는 provisional(추정)으로 들고, 정해진 시점에 confirmed(확정)로 커밋.
3. **소구포인트 추출**: 에피소드 / 비트(연속 컷 묶음) 단위의 핵심 훅을 구조화 산출.
4. **모델 가용성에 강건**: GLM(대용량 컨텍스트) 정상 경로 + 로컬 LLM(`max_token=16384`) fallback 모두에서 동작.
5. Step3(장면/화자) + Step4(회차 요약)를 **하나의 에피소드 추론 단계로 통합**.

### 비목표 (이번 범위 밖)
- Step1·2 로직 변경(OCR/YOLO/얼굴식별은 그대로 입력으로 사용).
- 70만 배치 백필(마스터 §5대로 보류).
- 풀 롱컨텍스트 단일 호출(Pegasus형 옵션 B)의 즉시 채택 — **북극성으로만** 남김(§6).

---

## 3. 근거 (요약)

- **TwelveLabs Pegasus 1.5**: "클립 단위 QA → 영상 전체에 걸친 시간 기반 구조화 메타데이터"로 전환. 의미는 한 순간이 아니라 시간적 연속성·인과로 드러난다는 전제. → **에피소드=영상, 컷=프레임** 매핑. ([ref](https://www.twelvelabs.io/blog/introducing-pegasus-1-5)) *(표현은 라이선스 준수 위해 바꿔 옮김)*
- **Tails Tell Tales (ACCV 2024)**: 페이지가 아니라 **챕터 전체**에서 대사를 화자에 귀속 + **인물 이름을 챕터 내내 일관 유지**. 우리 "나중 컷 이름 소급" 문제와 동일. ([ref](https://openaccess.thecvf.com/content/ACCV2024/html/Sachdeva_Tails_Tell_Tales_Chapter-wide_Manga_Transcriptions_with_Character_Names_ACCV_2024_paper.html))
- **Zero-Shot Character ID & Speaker Prediction (2024)**: 컷별 분류 대신 **반복적 융합**으로 점진 수렴. ([ref](https://arxiv.org/html/2404.13993v4))
- **문학 화자 귀속**: **coreference(상호참조) 해소** 문제로 — 문장 단위가 아니라 작품 전역에서 엔티티를 묶어 푼다. ([ref](https://arxiv.org/html/2307.03734v1))

정석: **컷 즉시 확정이 아니라 에피소드(챕터) 단위 전역 해소.**

---

## 4. 핵심 개념 구체화

### 4.1 추정(provisional) → 확정(confirmed)
- Pass 1(컷별)은 결과를 **provisional**로 적재(확정 아님).
- Pass 2(에피소드)가 전역 판단으로 **confirmed 커밋 + resolved 마킹**.
- `name_discoveries`는 "한 컷 0.85 즉시 rename"이 아니라 **여러 컷의 증거 누적 투표**로 확정.

### 4.2 신념 상태(belief state) — 구조화 캐리오버
`prev_context`(문자열) 대신 컷을 지나며 누적되는 구조화 상태:
- `character_roster`: 등장 얼굴/캐릭터 + 알려진 이름(또는 미상)
- `pending_speakers`: 미확정 화자 가설(후보 + confidence + 근거 컷)
- `name_evidence`: face_id별 이름 단서 누적 투표함

### 4.3 비트(beat) 계층
"n~n+m 컷의 소구포인트" = **비트**(같은 서사 목적을 공유하는 연속 컷 묶음 = 한 장면/감정 단위).
```
에피소드 ── 소구포인트(전체 훅) + 줄거리 요약 + 클리프행어
  └─ 비트(n~n+m 컷) ── 비트별 소구포인트 + 강도
       └─ 컷 ── scene_meta(상황 서술)   ← 마스터 §7 기존 레이어
```
비트 분절은 Pass 2 resolver가 scene_meta + 대사 흐름으로 수행(화자 해소용 scene segmentation과 동일 작업).

### 4.4 소구포인트 스키마 (초안)
각 비트 / 에피소드에 대해:
- `hook_type`: **enum 고정 안 함 → free-form 텍스트**(장르 공통/예외가 섞이고, 데이터가 쌓인 뒤 군집화로 어휘 도출). 예: "사이다/카타르시스", "떡밥 투척", "각성"…
- `appeal_point`(한 줄): 독자가 왜 끌리는가 / 무엇이 다음을 보게 만드는가
- `intensity`(0~1): 훅 강도
- `cut_range`: [n, n+m] 근거 컷
- (에피소드) `cliffhanger`: 다음 화로 끄는 마지막 훅
- **단위 유연성**: 소구포인트는 컷/비트/에피소드뿐 아니라 **교차 에피소드(아크)** 단위일 수 있음(예: 여러 화 걸친 복선·성장). 비트는 **1~N 유연**(에피소드 전체가 1비트일 수도; 최소/최대 개수 제약 없음).
소구포인트는 비트에서 모아 에피소드로 **bottom-up** 종합. → webtoonmoa의 "명장면/볼거리", 훅 검색, 회차 추천 표면(마스터 §12).

### 4.5 효과음(SFX) → 장면 해석으로 흡수
- "쿠루룽", "철썩" 같은 의성어/효과음은 **블록 단위로 교정·화자귀속하지 않는다**(낭비 + RAG 노이즈).
- Pass 1 비전이 **의미를 `scene_meta.action_summary`에 융합**: "쿠루룽 철썩" → "천둥과 파도가 치는 배 위에서 인물들이 다툰다".
- 해당 OCR 블록은 `type=other` + `is_used=false`(마스터 v3.1 일치). 효과음은 장면을 *설명*하되 대사 레코드를 만들지 않음.
- **판정은 LLM이**: 큰 스타일 텍스트라도 외침/대사면 `speech` 유지, 주변음/의성어면 흡수. (step1 기하 휴리스틱보다 VLM이 잘함)

### 4.6 캐릭터 중요도(significance) 티어링 + soft-exclude
- **문제**: Step2가 미매칭 얼굴마다 NEW_CHAR 양산 → 행인/엑스트라가 캐릭터 DB 오염. (마스터 §15 보류 항목 "엑스트라/효과음 soft-exclude, Human/VL 판정"을 Step3 LLM으로 구체화)
- LLM이 **중요도 티어**를 매긴다:
  - `main/supporting`(재등장·이름·서사역할) → 풀 처리(이름해소·roster)
  - `minor_functional`("화산파 중급 제자 A") → 기능 라벨로 보존, 실명 추적 안 함, roster 오염 안 함
  - `extra`("행인 1"/배경) → soft-exclude(분석 제외)
- **기존 기계 재사용**: extra → `character.is_match_excluded=true`(step2 `_get_excluded_appearance_ids`가 이미 매칭 후보 제외) + 필요시 `face_record.is_used=false`. **하드 삭제 금지(가역·human 검토)**.
- 신호원: Pass 1(시각 — 얼굴 크기/전경·배경/디테일/대사 인접) = 힌트, Pass 2(서사 — 재등장·대사량·호명·플롯 관여) = 최종 티어. **2스테이지 안에서 처리(새 LLM 콜 X)**.
- ⚠️ 주의: ① 티어는 **가역**(cut5 엑스트라가 ep200 주연 가능 → 에피소드/교차에피소드 재평가) ② `is_confirmed`/human은 **절대 강등 금지**(동결 규칙).

### 4.7 정답 데이터(human / is_confirmed) 취급 — 동결 + 선택 주입
human 텍스트·`is_confirmed` 얼굴/이름은 100% 정답. 세 역할로 분리:
1. **동결(freeze) — 필수**: LLM이 절대 덮어쓰지 않음(레이어링 `human>llm>paddle` + Pass 2b 결정론 적용이 보장). 재-OCR/재명명 시키지 않음 → 일감·토큰 절약.
2. **고정 앵커 주입 — 관련 있을 때**: 확정 얼굴/대사를 *잠금(non-overridable)* 단서로 넣어 주변 모호 항목 해소 품질↑("F0=확정 철수 → 옆 미상 대사는 철수일 가능성↑").
3. **제외 — 무관할 때**: 멀리 있는 확정/human은 뺌. **correctness 리스크 0**(최종값은 동결로 보존) — 비용은 주변 해소 품질 트레이드오프뿐.
> 토큰 무제한이어도 노이즈 앵커는 attention을 흐림("lost in the middle") → **입력 큐레이션은 품질을 위해 여전히 필요**. 관련성 휴리스틱(초안): 현재 윈도우/비트에 등장하는 캐릭터·같은 장면 범위만 주입(구체화는 스펙에서).

---

## 5. 모델 제약 & 컨텍스트 적응형 설계 (핵심)

### 5.1 가용 예산 — 텍스트 vs 비전이 완전히 다르다
| 모델 | 모드 | 토큰 예산 | 비고 |
|---|---|---|---|
| GLM (glm-5.x) | **텍스트 전용** | ~131,072 | 이미지 없으면 큰 예산 — 에피소드 전역 해소(Pass 2a)에 충분 |
| GLM-4.6v / zai vision | **멀티모달(이미지)** | **32,768** | 비전은 텍스트의 1/4. 컷당 소수 이미지만 가능 |
| Qwen3-VL-32B-fp8 (로컬) | 멀티모달(이미지) | **16,384** | 비전 fallback. GLM 비전의 절반 |
| 로컬 LLM | 텍스트 | 16,384 | 텍스트 fallback |

> ⚠️ **검증 항목**: GLM 텍스트 `max_tokens`(131072)가 입력 컨텍스트인지 출력 상한인지 실측 확정. 단, 아래 설계는 이 숫자에 의존하지 않게 만든다.

### 5.1b 이미지 토큰 비용 — 컷당은 싸다, 에피소드 통째가 불가능하다
- 컷 크기 ~**700×1600px**(웹툰별 상이) 기준 비전 토큰(Qwen-VL 계열, 28×28=1토큰): 1.12M px ÷ 784 ≈ **컷당 ~1,300토큰**(기본 `max_pixels` 캡 시 ~1,280).
- 따라서 **단일 콜 멀티이미지는 토큰상 여유**: 16k 로컬 ~8컷, 32k GLM-v ~20컷(프롬프트/출력 여유 제외 전 상한). 3~5장 동봉도 가능.
- **그러나 에피소드 통째 이미지는 불가능**: 100~300컷 × 1.3k = **130k~390k 비전 토큰 ≫ 32k/16k**. → "이미지로 에피소드 전역 연속성"은 어떤 모델로도 한 콜 불가.
- 결론: **이미지 장수는 Pass 1의 국소 시각 맥락 튜닝일 뿐, 연속성 수단이 아니다.** 연속성은 이미지 없는 텍스트 Pass 2(§6)에서. (OCR/얼굴은 Paddle·Step2가 이미 보유 → 비전 역할은 말풍선↔얼굴·행동·표정.)

### 5.1c 100만 컷 throughput — 진짜 병목 (비용 아님)
- 컷당 비전 1콜 = **~100만 콜**(1회성 백필). GLM 무제한 플랜이라 **비용은 비제약**(§5.1d) → 병목은 **rate-limit(RPM/동시성) + 호출 지연**. **실측 후 확정**(마스터 §14.5 승격).
- 완화: ① **활성 웹툰만 + 증분 ~1000컷/일**(백필 보류, 마스터 §1·§5) → 일상 운영은 사소. ② **빈 컷 스킵**(텍스트·얼굴 둘 다 없는 전환/배경컷). ③ **컷 배칭**: 1.3k/컷이라 한 비전 콜에 K컷(예 4~6) 묶어 콜 수 1/K(컷별 출력 분리 + OCR 1:1 유지 조건). ④ GLM rate-limit 도달 시 **Qwen 로컬로 흘림**(병렬 처리량 확보).

### 5.1d 모델 역할 (주력 / 폴백)
- **주력 = GLM**(무제한 플랜, ~2027-03-31; 5시간당 토큰 예산 거대 → 비용 비제약). rate-limit/지연 시 **폴백 = Qwen3-VL-32B 로컬**.
- 사용자 체감상 Qwen3-VL ≳ GLM-4.6v(해석 품질) → 격차 작을 수 있음. **A/B 실측 항목**.
- 전환은 `LLMModel`/`WebtoonLLMSetting`(마스터 §7) **config로 — 코드 변경 없이 모델 교체**.

### 5.2 설계 원칙: 윈도우 크기 = 모델 토큰 예산의 함수
에피소드 전역 해소를 **고정 "에피소드 통째 1콜"로 가정하지 않는다.** 대신:
- **해소 윈도우 크기 = f(모델 컨텍스트 예산)**. GLM 텍스트 모드면 윈도우 = 에피소드 전체(사실상 1콜). 로컬 16K면 윈도우 = 여러 개로 자동 분할.
- 윈도우 경계에서 정보 손실이 없도록 **belief state를 캐리오버**(§4.2)한다.
- 같은 알고리즘이 16K든 131K든 그대로 돈다(컨텍스트 적응형). → **로컬 fallback이 무시되지 않고 1급 경로로 들어온다.**

### 5.3 양방향 전파를 16K에서도 보장하는 트릭
윈도우는 전진(forward)인데 "컷 50 이름을 컷 10에 소급"하려면? → **이름 확정과 적용을 분리**한다.
- **Pass 2a (LLM, 윈도우)**: 전진 해소로 **에피소드 최종 캐릭터/이름 테이블** + 컷별 화자 가설(face_id/라벨 기준)을 만든다.
- **Pass 2b (LLM 없음, 결정론적)**: 최종 이름 테이블이 확정되면, 에피소드 전체 provisional 화자 참조(face_id → 확정 캐릭터)를 **단순 조인으로 일괄 재기록**. → 소급(backward) 전파가 **공짜 결정론적 연산**이 되어 컨텍스트 한계와 무관.
- 얼굴 없는 모호 화자 등 LLM 판단이 더 필요한 건 belief state의 `open_questions`로 들고 다니며 윈도우 내에서 처리.

---

## 6. 아키텍처 — 2-pass 하이브리드 (채택안)

### 옵션 비교 (요약)
| 옵션 | 내용 | 판정 |
|---|---|---|
| A. 룩어헤드만 추가(N+1,N+2 이미지) | 현 구조 유지 | ❌ 여전히 즉시 확정 + 비용↑ |
| B. 풀 롱컨텍스트 단일 멀티모달 호출(Pegasus형) | 에피소드 전체 컷 한 번에 | ⏸ 북극성. 이미지 토큰/비용/OCR 1:1 깨짐 |
| **C. 2-pass 하이브리드** | 컷별 추출(멀티모달) + 에피소드 해소(텍스트) | ✅ **채택** |

### 핵심 원칙: 비전과 연속성을 분리한다
- **연속성은 멀티이미지가 아니라 텍스트 패스에서 얻는다.** 에피소드 통째 이미지는 토큰상 불가(§5.1b)이므로, 이미지 장수와 무관하게 연속성은 텍스트 Pass 2가 담당.
- **비전 = 컷 단위(기본 오버레이 1장).** 압축 텍스트 레코드를 산출. 컷당 ~1.3k 토큰이라 단일 콜에 여러 이미지가 들어가지만(§5.1b), 이는 **throughput·국소 시각맥락 레버**이지 연속성 수단이 아니다:
  - **throughput**: 한 콜에 K컷 배칭(콜 수 ↓).
  - **국소 단서**: 필요 시 직전 컷 1장 동봉(말풍선 꼬리가 화면 밖 인물 가리킬 때) — 선택. 기본은 오버레이 1장.

### 단계 구성
> **LLM 스테이지는 2개**(Pass 1 비전 / Pass 2a 텍스트). Pass 2b는 LLM 아님(결정론적), Step4·비트·소구포인트는 Pass 2a에 흡수 → **과분해 회피(지연 통제)**.
```
Pass 1 (컷별, 멀티모달, provisional) ──┐  컷당 이미지 1장(현재 컷 오버레이)만
   OCR 1:1 교정 · type 분류 ·          │  → 컷의 "압축 텍스트 레코드" 산출
   얼굴↔대사 후보 · 꼬리방향 힌트 ·     │     (누가 보임/화자후보/행동/표정)
   scene_meta · SFX→scene 흡수(§4.5) ·  │  belief state 캐리오버
   prominence 힌트(엑스트라, §4.6)      │
                                       ▼
Pass 2a (에피소드, 텍스트, 윈도우)  ── 레코드만으로 전역 화자/이름 해소 + 비트 분절 + 소구포인트
                                       │  (이미지 없음 → 130k 텍스트 마음껏)
                                       │  → 최종 캐릭터/이름 테이블
                                       ▼
Pass 2b (LLM 없음, 결정론적)       ── provisional → confirmed 일괄 커밋(양방향 전파)
                                       ▼
Step4 흡수                          ── 회차 요약/타임라인/떡밥 = Pass 2a 산출에 포함
```

- **Pass 1**: 비전은 컷당 1회·1장으로 묶고(비용/throughput 통제), 순수 시각 단서(말풍선 꼬리 방향 등)는 *텍스트 힌트*로 적어 Pass 2가 이웃 레코드로 해소. 결과는 확정 아닌 provisional. 빈 컷(텍스트·얼굴 X)은 스킵/경량.
- **Pass 2a**: 텍스트 위주라 GLM 텍스트 예산(또는 16K 윈도우)에서 값싸게. Step4(요약/떡밥)를 여기 흡수.
- **Pass 2b**: LLM 없이 결정론적 적용 → 소급 전파 + 멱등 재실행 용이.
- B(Pegasus형)는 GLM 멀티모달 컨텍스트가 충분해지거나 전용 비디오 이해 API가 가능해지면 이행하는 north star로 보존.

---

## 7. 데이터 모델 + Pass-1/Pass-2 계약 (service)

> 결정(2026-06-29): provisional 저장 = **①TextAnnotation에 status 컬럼**(별도 staging 아님), name 증거 = **신규 NameDiscoverySuggestion 테이블**, 토큰 사용량 = **신규 LLMUsage fact 테이블**.

### 7.1 Pass-1 LLM 출력 계약 (프롬프트 스키마 — 확정 초안)
```json
{
  "cut_summary": "<현재 컷 상황서술 1~2문장 (효과음 의미 흡수)>",
  "key_objects": ["..."],
  "characters": [
    {"face_label": "F0", "appearance": "갈색머리 근육질", "emotion": "당황",
     "prominence": "main|minor|extra", "visible": true}
  ],
  "blocks": [
    {"index": 0,
     "type": "speech|monologue|narration|system|other", "type_confidence": 0.0,
     "corrected_text": "도대체 이게 무슨...",
     "speaker": {"face_label": "F0", "name": null, "confidence": 0.0,
                 "basis": "tail|face|context|none", "tail_hint": "F0|offpanel_left|offpanel_right|ambiguous|none"}}
  ],
  "name_evidence": [
    {"face_label": "F0", "name": "철수", "confidence": 0.8, "evidence": "옆 인물이 호칭"}
  ]
}
```
- 규칙: **엄격 JSON**(자유 텍스트 금지), `blocks`는 입력 OCR index와 **1:1**(병합·생략 금지), 모르면 `null`(지어내지 마), 자연어는 한국어, low temp(0.0~0.2). 효과음은 block이 아니라 `cut_summary`로 흡수(§4.5).
- **분류 먼저 → 화자 나중 (순서 강제)**: ① 모든 블록 `type`(+`type_confidence`)을 먼저 확정 → ② speech/monologue에만 `speaker`(+`confidence`, `basis`=무엇 근거인지: 꼬리/얼굴/문맥) 귀속. 인물 정체성을 라벨+상황 다요인으로 확정하듯, **글자도 분류+근거+confidence**로 단계화. 확신 없으면 confidence를 낮게(과확신 금지).

### 7.2 영속화 매핑 (재사용 / 신규 / 비영속)
| Pass-1 필드 | 저장처 | 구분 |
|---|---|---|
| `blocks[].corrected_text` | `TextAnnotation.text`(source='llm') | 재사용 |
| `blocks[].type` | `TextAnnotation.type` | **enum 정리(7.3-1)** |
| `blocks[].speaker.{face_label,name}` | `TextAnnotation.speaker_id`(Pass-2b 확정) | 재사용 |
| `blocks[].speaker.{confidence,tail_hint}`, `characters[].{prominence,emotion}` | belief state로 전이 | **비영속(테이블 X)** |
| `cut_summary`, `key_objects` | `CutSceneMeta` | 재사용 |
| `name_evidence[]` | `NameDiscoverySuggestion`(신규) | **신규(7.3-3)** |
| (Pass-2 산출) significance 티어 | `Character.significance`(신규) + `is_match_excluded` | **신규(7.3-4)** |
| provisional↔confirmed | `TextAnnotation.resolution_status`(신규) | **신규(7.3-2)** |
| 토큰 사용량 | `LLMUsage`(신규) | **신규(7.4)** |

### 7.3 신규/변경 스키마 (확정)
1. **`TextBlockType` enum 정리(마이그레이션 + 코드 수정)**: 최종 = `speech|monologue|narration|system|other`. ⚠️ 현재 `step3.py._upsert_llm_annotation`은 `narration/speech/sfx/caption/other`만 허용(=v3.1 미반영) → **monologue 추가, sfx/caption 제거**(sfx 의미는 §4.5로 흡수). 테스트에서 독백 분류가 핵심으로 확인됨(§11).
2. **`TextAnnotation.resolution_status`(신규 enum 컬럼)**: `unresolved`(Pass-1 적재) → `resolved`(Pass-2b 커밋). 2-pass "추정→확정"의 저장 표식. human 수정은 항상 우선(레이어링).
3. **`NameDiscoverySuggestion`(신규 테이블)**: (webtoon FK, character/appearance FK, name, confidence, evidence, source_episode FK, source_cut, status `pending|accepted|rejected`, created_at). **다중 컷 증거 누적 투표** → 에피소드 종료 시 확정. 기존 `Character.extra.name_suggestions` json 적재 폐기.
4. **`Character.significance`(신규 enum 필드)**: `main|supporting|minor_functional|extra`. `extra`는 동시에 `is_match_excluded=true`로 soft-exclude(step2 후보 제외 재사용). **가역 + is_confirmed/human 동결 존중**(§4.6).
5. (Pass-2) `EpisodeBeat`(신규: episode FK, cut_start, cut_end, hook_type, appeal_point, intensity) + `EpisodeReport.{appeal_point,cliffhanger}`(필드 추가). **`hook_type`은 enum 미고정 = free-form 텍스트**(§4.4), 비트 개수 제약 없음, 소구포인트는 교차에피소드(아크) 단위도 허용.

### 7.4 토큰 사용량 로깅 — `LLMUsage`(신규 per-call fact 테이블)
목적: **웹툰/에피소드/컷별 입력·응답 토큰 집계**(GLM 무제한 → 과금 아닌 **사용량 가시성** + 로컬 Qwen 용량계획).

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `id` | bigserial PK | |
| `webtoon_id` | FK→webtoon | 항상 |
| `episode_id` | FK→webtoon_episode, NULL | 웹툰레벨 콜이면 NULL |
| `cut_id` | FK→webtoon_cut, NULL | 에피소드/웹툰레벨 콜이면 NULL |
| `stage` | varchar(enum) | `pass1` / `pass2_resolve` / `pass2_appeal` / `step4` |
| `llm_model_id` | FK→llm_model | 사용 모델 |
| `prompt_tokens` | int | 입력(이미지 토큰 포함) |
| `completion_tokens` | int | 응답 |
| `total_tokens` | int | 합 |
| `image_count` | int, NULL | 콜에 포함된 이미지 수(비전) |
| `finish_reason` | varchar, NULL | stop/length 등 |
| `extra` | jsonb, NULL | provider별 분해(cached_tokens 등) |
| `created_at` | timestamptz | |

- **per-call 1행**: 컷당 다중 콜·재시도·배치·Pass-2 윈도우 다중콜도 정확 집계. (컷콜=cut_id 채움 / 에피소드해소=cut_id NULL / 웹툰레벨=episode_id NULL).
- 인덱스: `(webtoon_id)`, `(episode_id)`, `(cut_id)`, `(stage)`, `(created_at)` — 축별 SUM 롤업.
- 소스: `llm_client`가 이미 파싱하는 `usage{prompt,completion,total}` + `finish` 를 **반환·적재**(현재 로그만 → 반환값 확장 필요).
- 집계 예: `SELECT webtoon_id, stage, SUM(total_tokens) FROM llm_usage GROUP BY 1,2`. 필요시 `WebtoonCut`/`Episode`에 롤업 캐시(선택적 비정규화).

> 모든 신규/변경은 service 레포 `apps/api/toon/models.py` + 마이그레이션. 마스터 §7과 정합.

---



### 7.5 Pass-2a 출력 계약 & 추출 방식 (비트 / 소구포인트 / cliffhanger)
> §11.3에서 "너무 잘 뽑힌" 비트·에피소드 소구포인트·cliffhanger가 **어떻게** 나왔는지의 방식 기록. 프로토타입 프롬프트 = `qwen-vl/_pass2.py`의 SYS.

**입력 (이미지 0장, 텍스트만)**: 에피소드 **전체 Pass-1 레코드를 읽기순으로 한 번에** + character_roster.
- 컷별: `cut_summary`(상황서술, 효과음 흡수), `blocks[type, corrected_text, speaker(face_label/name/conf/tail_hint)]`, `faces[F라벨 → character_id, known_name]`, `name_evidence`.
- roster: character_id별 known_name + 등장 컷 수.
- → GLM 텍스트(130k)에 **단일 콜**(클린 ep1 ≈ 11k tok). 긴 에피소드/16k fallback은 §5.2 윈도우 + §4.2 belief state.

**출력 계약 (JSON)**: `characters`(이름·significance·**label_conflict**·merge_suggestion), `speaker_resolution`(cut·block_index→character_id), `beats`, `episode`.

**⚠️ Step2 얼굴ID는 정답이 아니다 (mis-ID distrust 규칙 — 필수)**: `identified_faces`의 character_id/이름은 Step2 임베딩 **추정값**(is_confirmed 아니면 신뢰 금지). **대사·호칭·맥락 증거가 얼굴 라벨보다 우선**. 서사 모순(예: ep1에서 죽은 천마가 ep2 화산파에 살아 등장)은 **오인식으로 의심**하고, name은 대사 근거로 정한 뒤 `characters[].label_conflict`에 사유 기록. → merge_suggestion(같은 인물 합치기)의 **반대 방향(이 얼굴은 라벨과 다른 인물)** 신호.

**📜 텍스트 종류의 "진실성 등급"으로 추론 (전개·거짓 탐지 — 필수)**: 같은 텍스트라도 신뢰 등급이 다르다.
- `narration`/`system`(시나리오·캡션) = 작가의 **객관적 진실** → 전개(plot) 골격을 여기서 잡는다.
- `monologue`(독백) = 인물의 **진짜 속마음/의도**(보통 정직).
- `speech`(대사) = 인물이 남에게 한 말 → **거짓·과장·책략일 수 있는 주장(claim)**.
- **거짓/책략 탐지 = speech(주장)가 monologue·narration·확정정체성(진실)과 충돌**할 때. (예시 ep3: 청명이 *"청진 제자의 후손"*이라 **speech로 거짓 주장**(배분 획득 목적) — 독백·확정정체성(회귀한 청명 본인)과 모순 → 책략. ⚠️ 현재 두 엔진 모두 이 책략을 못 잡음 = §11.6.)
- Pass-2는 ① 인물별 **speech/monologue 타임라인**(누가 언제 무엇을)을 먼저 만들고 → ② narration으로 전개 정리 → ③ **speech 주장 vs 진실 대조로 거짓/오해/책략을 명시 플래그**(`episode.deceptions` 등). **명시적으로 "주장 vs 진실 괴리를 찾으라"고 지시해야** surfacing됨(토대만으론 자동 안 됨).
- **교차 에피소드 확정 로스터 prior 필수**(§11.5): "청명의 진짜 정체"가 있어야 "청진 후손" 주장이 거짓임을 안다.
- **confidence 게이팅**: type·speaker confidence가 낮으면 그 위에 강한 서사 결론을 내리지 않음(provisional 유지).

**그 3개의 추출 방식 — 핵심은 "에피소드 전체 텍스트를 한 콜에 보고 아크 수준 추론"**:
- **beats**: 연속 컷의 `cut_summary` + 대사 흐름을 **서사 단위로 묶어** `[{cut_start, cut_end, hook_type(free-form), appeal_point, intensity}]`. 개수 제약 없음(전체=1비트 가능). → 예시(ep1): "전장 비극(2-46) → 죽음(47-63) → 회상(64-79) → 환생(80-104) → 화산파 소멸(105-146)".
- **episode.appeal_point**: 비트/요약을 **bottom-up 종합**한 에피소드 핵심 훅. → 예시: "최고 검객이 100년 뒤 거지로 환생한 시대착오 + 사라진 고향에 대한 안타까움".
- **episode.cliffhanger**: 마지막 비트/컷의 **미해결 훅**(다음 화 견인). → 예시: "화산파가 구파일방 명단에 없음을 확인하고 충격".

**왜 잘 됐나(설계 함의)**: 컷별로 보면 안 보이는 **인과·반전·복선이 에피소드 전체 텍스트를 동시에 볼 때 드러난다**(연속성=텍스트, §6). 회귀물 설정도 cut64 회상 + cut75 독백 + cut125 "백년?!"을 **묶어** 추론. → Pass-2 입력은 반드시 **에피소드 전 컷 레코드(윈도우 시 belief state 캐리오버)**여야 하며, 컷 단위로 쪼개면 이 품질이 사라진다.

> ⚠️ 안정성: 비트 경계는 재처리 시 흔들릴 수 있음(§10-7) → `EpisodeBeat`에 안정적 키 필요. 머지/이름은 confidence 게이팅 + human 확정.

---

## 8. Temporal 오케스트레이션 함의

`EpisodeChainWorkflow.steps`(현행)에 단계만 추가/치환:
- `step3a_extract`(컷 루프, STEP3_QUEUE, 멀티모달, heartbeat) → `step3b_resolve`(에피소드 텍스트 해소, 윈도우) → `step3c_apply`(결정론적 커밋).
- 에피소드가 이미 자연 배치 단위라 궁합 좋음. 직전 컷 요약 문자열 캐리오버 대신 **belief state를 activity 반환값/입력으로 전달**(워크플로 변수, 마스터 §9.7과 동일 취지).
- `phase3_enabled` 게이트, 동시성 1(STEP3_QUEUE) 그대로.

---

## 9. 재처리 / staleness 재설계

- 전역 해소라 **재해소 단위 = 컷이 아니라 에피소드**. human 수정 1건이 에피소드 전체 화자/소구포인트 해소를 바꿀 수 있음.
- 마스터 §11.2 short-circuit("이 컷부터 재분석 + 동일하면 중단")을 **에피소드 단위 재해소**로 다시 설계.
- Pass 2b가 결정론적이라 **이름 테이블만 바뀌면 LLM 없이 일괄 재적용** 가능(부분 재처리 비용 절감).

---

## 10. 리스크 / 오픈 퀘스천

1. **GLM 토큰 의미 확정**(§5.1 검증 항목): 131072가 입력 컨텍스트인지 출력 상한인지 실측.
2. **로컬 LLM(16K) 품질**: 윈도우 분할 시 해소 정확도 저하 폭? GLM 대비 품질 격차 측정 필요.
3. **belief state 직렬화 크기**: 긴 에피소드에서 roster/pending이 윈도우 예산을 먹지 않도록 압축 규칙 필요.
4. **소구포인트 주관성**: 정답 없는 추출 → enum 고정 + 초기 human 검토 큐로 품질 감 잡기. 장르별 `hook_type` 보정.
5. **비용/지연**: 최종 결과가 "에피소드 종료 후" 확정(실시간성 포기 — 이미 배치라 수용). Pass 1 멀티모달 호출 수가 주 비용.
6. **OCR 1:1 바인딩 유지**: Pass 1에서 region index ↔ block 1:1 불변식(현 프롬프트 규칙) 계속 보장.
7. **비트 경계 안정성**: 재처리 시 비트 분절이 흔들리면 소구포인트 ID가 불안정 → 안정적 키 설계 필요.

---

## 11. 모델 비교 실험 (2026-06-29, glm-4.6v vs qwen-vl)

> 하니스: `qwen-vl/_vltest.py` (LiteLLM 게이트웨이 경유, prod S3 컷, **프롬프트×temperature×모델** 매트릭스, `--ocr-inject`로 Paddle 텍스트 주입). DB 쓰기 없음.
> 대상 컷: `naver/808482 ep1 cut50`(시스템텍스트·얼굴X), `cut77`(얼굴2명+충격대사·독백).
> 프롬프트: `free`("인물간 대화/상황/인물 구별" 자유) vs `struct`(JSON 강제 + 꼬리방향 화자귀속 + 효과음 scene 흡수 + "지어내지마/모르면 null").

### 11.1 관측 결과
- **화자귀속**: cut77에서 양 모델 모두 대사를 올바른 인물(갈색머리)에 귀속. **얼굴 라벨 없이 위치만으로도 정확** → YOLO 오버레이 라벨 추가 시 더 결정론적.
- **GLM free 환각**: cut50(얼굴 없음)에서 GLM이 **없는 캐릭터 "피똥/피크"를 지어냄** + "튜토리얼→뉴토리얼" 오독. `struct` + "지어내지마"로 **환각 제거**(`characters:[]`).
- **Qwen 해석 깊이 우위**: 말풍선 모양=충격, 금발=냉소적 관찰자(반전 암시) 등 **scene/복선 신호**를 더 풍부히. 단 free는 매우 장황.
- **구조화 출력 = Qwen throughput 4~5배**: free(에세이 ~1000토큰)=**140~172s** vs struct(JSON ~200토큰)=**24~37s**. 품질 손실 없음.
- **temperature**: 0.0이 규율 있음. 0.7에서 Qwen이 `sfx_absorbed` 필드 오용, GLM은 대사 개수 흔들림. → **추출은 0.0~0.2**.
- **OCR 약점 공통**: 스타일/시스템 텍스트는 Paddle("완료→환료", 단편화)·VLM("튜토리얼→뉴토리얼") **둘 다 약함**.
- **속도 실측**: glm-4.6v ~13~34s/콜, qwen-vl struct ~24~37s / free ~140~172s.

### 11.2 설계 반영
1. **블록별 `type`(narration/speech/monologue) 필수**: 테스트 스키마에 type이 없어 독백을 놓침. 모델 능력은 충분(Qwen free가 "내면 독백"으로 정확 인지) → 마스터 §7 type 레이어를 Pass-1 스키마에 반드시 포함.
2. **구조화 JSON 강제**: 환각 제거 + Qwen 4~5배 가속 → Pass-1 출력은 **항상 엄격 JSON**(§6).
3. **low temperature(0.0~0.2)** + "지어내지마/unknown 허용"(§4.7) + grounding(OCR·얼굴 라벨).
4. **Qwen-VL 주력 후보**(품질·struct속도). GLM은 안정적 대안. 최종 선택은 **정량 평가셋으로 A/B**(§10-2).

### 11.3 Pass-1→Pass-2 end-to-end 프로토타입 (2026-06-30, 769209 ep1)
> 하니스: `qwen-vl/_pass1.py`(§7.1 struct 계약으로 컷별 provisional 레코드 → JSONL, DB 쓰기 없음) + `qwen-vl/_pass2.py`(레코드 전체 → GLM 텍스트 1콜 전역 해소). 입력: Step1/2 완료된 769209 ep1(146컷, character_id 보유). Pass-1=glm-4.6v, Pass-2=glm-4.6(텍스트, ~11k tok 입력).

**결과(부분 데이터로도 검증 — Pass-1 41컷 성공/29 에러)**:
- **이름 해소(컷 가로질러)**: 418→"매화검존 청명", 421→"천마", 424→"초삼" — 흩어진 대사/OCR에서 확정.
- **머지(#3) 성공** ⭐: Step2가 쪼갠 `426`을 `418`(청명)과 **동일 인물 merge_suggestion**으로 정확 포착(자동 아닌 제안).
- **significance 티어링**: main/supporting/minor_functional/minor/extra 구분.
- **화자 해소**: 독백 체인(cut23→24→29→32)을 맥락으로 연결.
- **비트 6개**(free-form hook_type) + **에피소드 소구포인트**("피로스의 승리의 비극적 아름다움") + **복선**(회귀물 설정을 텍스트만으로 추론).

**발견 → 반영**:
1. **아키텍처 검증**: 비전 per-cut → 텍스트 전역 해소(이미지 0장)로 화자·이름·머지·비트·소구포인트 전부 산출. **연속성=텍스트 가설 확정.**
2. **회복력**: Pass-1 41% 깨진 입력에도 Pass-2가 에피소드 서사 복원.
3. ⚠️ **Pass-1 `max_tokens` 절단**: 1536은 블록 많은 컷에서 JSON 절단(~41% 파싱 에러) → **4096+ 상향 + 강건 파싱/재시도**(GLM-4.6v 추론모델 quirk). `_sanitize_result` 패턴 필요.
4. **품질 caveat**: 가끔 오라벨(402→"화산"=종파명을 인물로) → **confidence 게이팅 + human 검토**(§9.5와 정합).

**클린 재실행(max_tokens 4096 + robust parse, 2026-06-30)**: 동일 ep1 재처리 → **JSON 에러 41%→2%**(146컷 중 ok=135/err=3/skip=8). 완전한 입력 위에서 Pass-2가 **에피소드 전체 서사를 텍스트만으로 정확 복원**:
- 회귀/환생 플롯 이해("청명이 100년 뒤 거지 아이 몸으로 깨어남"), 5개 비트로 전체 아크(전장 비극→죽음→회상→환생→화산파 소멸 미스터리) 분절.
- 머지 제안: 418↔426(청명, 정확). 단 418↔419는 **오탐 가능**(병사/제자) → confidence 게이팅·human 확정 필요(자동 병합 금지 재확인).
- episode.appeal_point="최고 검객이 100년 뒤 거지로 환생한 시대착오 + 사라진 고향", cliffhanger="화산파가 구파일방 명단에 없음 확인" — 정확.
- Step2 얼굴 라벨 노이즈(423=죽은 천마 vs 행위자)를 Pass-2가 **스스로 플래그** → 노이즈 흡수력 확인.
→ **max_tokens 4096 + raw_decode 파싱 + 1회 재시도**를 Pass-1 구현 기본값으로 확정.

**A/B: Pass-1 엔진 GLM-4.6v vs Qwen-VL (동일 ep1, 동일 Pass-2 리졸버, 2026-06-30)**:
| 항목 | GLM-4.6v | Qwen-VL |
|---|---|---|
| Pass-1 시간 / JSON에러 | 69분 / 3 | 97분(+40%) / **0** |
| **1:1 블록 바인딩** | ✅ 엄수 | ❌ 병합(cut23: 4→1) |
| cut_summary | 간결 | 장황(효과음 더 포착) |
| Pass-2 입력 토큰 | ~11k | ~45k(4배) |
| name_evidence | 깔끔 | 제공 이름을 "발견"으로 에코 |
- **Pass-2 다운스트림 품질은 동급**(이름·아크·appeal·cliffhanger 둘 다 정확). 머지: GLM은 418↔426(정확)+418↔419(오탐), Qwen은 418↔426+423↔421(천마시신, 합리적)로 Qwen이 약간 깔끔.
- **판정**: **Pass-1 엔진은 GLM-4.6v 우선**(1:1 region 바인딩 엄수=구조화 계약 핵심, Pass-2 입력 4배 작음, 40% 빠름). raw-image 인상(§11.1 Qwen 우위)과 달리, **OCR grounded + 1:1 요구 상황에선 GLM이 더 적합**. Qwen은 견고한 폴백.
- ⚠️ Qwen은 **1:1 위반** → region별 annotation 바인딩 깨짐. 프롬프트 강화로 교정 시도 필요(미검증).

### 11.4 Step2 얼굴 오인식의 Pass-2 전파 & distrust 규칙 (2026-06-30, ep2)
> ep2(769209)에서 **현 화산파 장문인 주변 얼굴을 Step2가 ep1의 죽은 천마(421)로 오매칭**(CCIP/CLIP false-match). cut81/89/112/115 face → character_id 421 "천마".

- **전파(수정 전)**: Qwen-records Pass-2가 "천마" 라벨을 정답으로 믿어 *"청명이 화산파에서 천마와 마주, 문도 운암 거쳐 입문"*이라는 **틀린 서사** 생성(name_conf 0.9, 근거가 "identified_faces에 천마로 명시"=순환).
- **엔진 차이**: 같은 Pass-2 모델인데 **GLM-records 버전은 대사("운암입니다")로 운암이라 명명하고 천마를 복선으로 헷지** → 오류 약함. Qwen-records는 verbose·1:1위반으로 대사근거 약해 라벨에 끌려감. → GLM Pass-1이 다운스트림 오류에도 더 강함(§11.3 재확인).
- **수정 = mis-ID distrust 규칙 + `label_conflict` 필드 추가**(§7.5) 후 재실행:
  - Qwen: 421을 significance=extra/conf 0.5 "화산파 제자 추정, Step2가 천마로 태깅"으로 강등, 서사에서 천마 제거(→ 장문인 면담 입문). ✅
  - GLM: 421="운암", `label_conflict`="Step2는 천마로 인식했으나 대사상 운암"으로 명시 플래그. ✅
- **교훈**: ① Step2 자동 라벨은 provisional(§4.7) — Pass-2가 의심·override 가능해야 함 ② merge뿐 아니라 **mis-ID/relabel 신호(label_conflict)** 필요 ③ Step2 파생 이름은 낮은 confidence로 ④ human 확정 필수.

### 11.5 교차 에피소드 정체성 불안정 + distrust 양날 (2026-06-30, ep3)
> ep3에서 두 엔진이 **주인공 정체(character_id 418)를 두고 분기**: GLM-records=418→"청명"(주인공 유지), Qwen-records=418→"청진(장문인)"으로 **재판정**(label_conflict로 "Step2는 청명이라 했으나 맥락상 청진" 기록), 426을 진짜 청명으로 봄.
- **원인 ①(구조)**: 프로토타입 `_pass2.py`는 **에피소드별 독립 실행** — ep1/ep2에서 확정된 "418=청명" **로스터를 prior로 승계 안 함** → 매 화 정체성 재추론 → 흔들림. (설계 §의 "기존 확정 로스터 prior"가 프로토타입 미구현.)
- **원인 ②(distrust 양날)**: §7.5 mis-ID 규칙이 ep2 천마 오류는 잡았으나 ep3에선 **맞는 라벨(418)까지 의심**해 뒤집었을 가능성(over-fire).
- **반영**: Pass-2 입력에 **교차 에피소드 확정 로스터(character_id→확정이름) prior 필수**(미구현 프로토타입 한계). distrust는 **is_confirmed/human 라벨엔 적용 금지**, 비확정에만. 정체성은 webtoon 글로벌(에피소드 독립 아님) → §9 재처리 단위와 함께 재설계.

### 11.6 책략(거짓 주장) 미탐지 → 텍스트 진실성 등급 추론 도입 (2026-06-30, ep3)
> **정정(도메인)**: 청진은 장문인이 아님. **청명이 "과거 고위 인물 청진의 제자의 후손"이라 거짓말**을 쳐서 장문인과 동급 배분(직위)을 얻으려는 **책략**이 실제 플롯. GLM·Qwen Pass-2 **둘 다 이 책략을 못 잡음**(GLM은 청진을 별 인물로, Qwen은 정체성 혼란으로 흘림).
- **단서는 있었다**: Qwen이 cut80에서 *"'나 청명이 그 나무꾼의 후손이다'는 거짓 설정"*이라 적었으나, **화자 귀속이 부실해 '책략(플롯)'이 아니라 '정체성 혼란(418 vs 426)'으로 오라우팅**됨. → 분류·화자 토대가 약하면 서사를 못 잇는다는 사용자 진단을 데이터가 뒷받침.
- **도입(§7.1·§7.5)**: ① Pass-1 **분류 먼저→화자 나중 + type/speaker confidence + basis**. ② Pass-2 **텍스트 진실성 등급**(narration/monologue=진실, speech=주장) + **speech vs 진실 대조로 거짓/책략 명시 플래그** + **교차에피소드 prior**. ③ "주장 vs 진실 괴리를 찾으라"를 명시 지시.
- 다음: 위 개선을 `_pass1.py`/`_pass2.py`에 반영 후 ep3 재실행 → 청진 책략 포착 여부 검증.

**검증 결과(2026-06-30, 개선 프롬프트 + prior 재실행)** ✅ — **청진 책략 포착 성공**:
- `deceptions` 신규 출력에 cut80: claim="청진의 제자 나무꾼의 후손이다", contradicts="cut81 monologue '이 각본의 좋은 점은 배분 조절'", conf=1 → **speech(거짓) vs monologue(진짜 의도) 대조로 책략 정확 surfacing**. 비트에도 "가짜 이력서"로 반영.
- **천마 mis-ID 차단**: prior("천마=100년 전 사망")로 421을 "현 장문인"으로 재판정(label_conflict 기록).
- **418 정체성 안정**: confirmed_roster_prior로 418=청명 고정 + 426을 다른 장로로 올바르게 **분리**(§11.5 흔들림 해소).
- **효율 발견**: 이 모든 게 **기존 Pass-1 레코드 + 개선 Pass-2 프롬프트 + prior**만으로 나옴 → 책략·정체성 해결은 **Pass-2 쪽 + prior로 충분**(Pass-1 재실행 불필요). Pass-1 개선(type_confidence/basis)은 보강용.

---

## 12. 결정 로그

| 날짜 | 결정 | 비고 |
|---|---|---|
| 2026-06-29 | **실험**: struct 프롬프트가 환각 제거 + Qwen 4~5배 가속 / 블록별 `type` 필수 / low temp / Qwen 주력후보 | §11 |
| 2026-06-29 | **Pass-1 계약 확정** + provisional=①`TextAnnotation.resolution_status` / name증거=신규 `NameDiscoverySuggestion` 테이블 | §7.1~7.3 |
| 2026-06-29 | **`TextBlockType` enum 정리 확정**: speech/monologue/narration/system/other (현 코드 sfx·caption 잔존·monologue 누락 → 수정 필요) | §7.3-1 |
| 2026-06-29 | **토큰 사용량 로깅** = 신규 `LLMUsage` per-call fact 테이블(웹툰/에피소드/컷 SUM 롤업) | §7.4 |
| 2026-06-29 | `hook_type` **enum 미고정 → free-form**(데이터 후 군집화), 비트 개수 제약 없음(전체=1비트 가능), 소구포인트 교차에피소드(아크) 허용 | §4.4 |
| 2026-06-29 | character **병합은 제안만**(자동 X), ep1 end-to-end 테스트로 검증하며 정함 | §7.1, 테스트 |
| 2026-06-30 | **Pass-1→Pass-2 프로토타입 검증**(769209 ep1): 텍스트 전역해소로 이름·머지(#3)·화자·비트·소구포인트·복선 산출. 연속성=텍스트 가설 확정 | §11.3 |
| 2026-06-30 | Pass-1 `max_tokens` 1536 절단(~41% JSON에러) → **4096+ 상향 + 강건파싱/재시도** 필요 | §11.3 |
| 2026-06-30 | **클린 재실행 확정**: max_tokens 4096 + raw_decode + 1회 재시도 → 에러 41%→2%. Pass-2가 전체 서사(회귀플롯)·소구포인트·cliffhanger 정확 복원 | §11.3 |
| 2026-06-30 | **Pass-2a 계약·추출방식 정식화**(§7.5): 입력=에피소드 전 컷 레코드 1콜(텍스트), beats/appeal_point/cliffhanger 추출 방식 + 예시 기록. "전체를 한 콜에" 가 품질 핵심 | §7.5 |
| 2026-06-30 | **Pass-1 엔진 A/B**: GLM-4.6v 우선(1:1 바인딩 엄수·간결·40%빠름), Qwen은 1:1 깨고 4배 verbose지만 JSON 0에러 폴백. Pass-2 품질은 엔진 무관 동급 | §11.3 |
| 2026-06-30 | **Step2 얼굴 오인식이 Pass-2로 전파**(ep2: 화산 장문→죽은 천마 오매칭→틀린 서사). → **mis-ID distrust 규칙 + `label_conflict` 필드**로 차단(양 엔진 검증). Step2 라벨=provisional, 대사 우선 | §7.5, §11.4 |
| 2026-06-30 | **교차 에피소드 정체성 불안정 발견**(ep3: 418=청명 vs 청진 분기). → Pass-2에 **확정 로스터 prior 필수**(프로토타입 미구현), distrust는 is_confirmed엔 미적용, 정체성=webtoon 글로벌 | §11.5 |
| 2026-06-30 | **책략 미탐지 → 텍스트 진실성 등급 추론 도입**: Pass-1 분류먼저→화자(+conf/basis), Pass-2 narration/monologue=진실 vs speech=주장 대조로 거짓/책략 플래그 + 교차ep prior | §7.1, §7.5, §11.6 |
| 2026-06-30 | **검증 성공**: 개선 Pass-2(deceptions + prior)로 청진 책략 포착(speech vs monologue 대조) + 천마 mis-ID 차단 + 418 정체성 안정. **Pass-2+prior로 충분(Pass-1 재실행 불필요)** | §11.6 |
| 2026-06-29 | Step3/4를 **에피소드 단위 2-pass 하이브리드(옵션 C)**로 재설계 결정. 컷 즉시 확정 폐기 | §6 |
| 2026-06-29 | 해소 윈도우 = **모델 토큰 예산의 함수**(GLM 텍스트 ~131K / 로컬 16K fallback 둘 다 1급) | §5 |
| 2026-06-29 | 양방향 전파 = **Pass 2a(LLM 이름확정) + Pass 2b(결정론적 소급 적용)** 분리 | §5.3 |
| 2026-06-29 | 소구포인트 = **컷→비트→에피소드 계층 + hook_type 스키마**로 구체화 | §4 |
| 2026-06-29 | **비전 ≠ 연속성**: 비전은 컷당 1회·오버레이 1장으로 한정(현 4장→1장), 연속성은 텍스트 Pass 2가 담당. 멀티이미지 동봉 폐기 | §5.1b, §6 |
| 2026-06-29 | 비전 모델 예산 확정: GLM-4.6v/zai vision **32k**, Qwen3-VL-32B 로컬 **16k**, GLM 텍스트 ~131k | §5.1 |
| 2026-06-29 | **정정**: 컷 ~700×1600 → 컷당 ~1.3k 비전토큰. 멀티이미지는 토큰상 가능하나 에피소드 통째는 불가(130k~390k). 이미지 장수=throughput/국소레버, 연속성은 텍스트 | §5.1b, §6 |
| 2026-06-29 | 규모 확정: 에피소드당 100~300컷, 전체 ~100만컷. 대량 비전=로컬 Qwen / 텍스트추론=GLM. 컷 배칭·빈컷 스킵으로 콜 수 통제 | §5.1c |
| 2026-06-29 | **GLM 무제한 플랜** → 비용 비제약, GLM 주력 / Qwen3-VL-32B 폴백(체감 동급, A/B 추후). 병목은 rate-limit·지연(실측) | §5.1c, §5.1d |
| 2026-06-29 | **과분해 금지**: LLM 스테이지 2개 유지(Pass1 비전 / Pass2a 텍스트). 비트·소구포인트·Step4는 Pass2a 흡수 | §6 |
| 2026-06-29 | **효과음→장면 해석 흡수**(블록 교정/귀속 안 함, 의미만 scene_meta로). 판정은 LLM | §4.5 |
| 2026-06-29 | **캐릭터 중요도 티어링**(main/minor_functional/extra) + extra soft-exclude(is_match_excluded 재사용). 가역·동결 존중 | §4.6 |
