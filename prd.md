# 웹툰 분석 파이프라인 — 통합 PRD (마스터)

> **상태**: v4.0-설계확정 · 갱신일: 2026-07-05
> **⚠️ 진행 중 인수인계**: v4.0 구현이 §17.7의 1~2단계(+service API)까지 완료된 **미커밋** 상태다 — 작업 현황·남은 단계·배포 순서 주의사항은 **§17.9**를 먼저 읽을 것.
> **⚠️ v4.0 재설계 확정(§17)**: 분석 도메인을 AnalysisRun 단위 쓰고-버리기 + character.kind 판별자 + 얼굴 레이어링(face_detection/face_identity) + character_profile(source 레이어링) + suggestion 통합 큐 + LLM 스테이지 V/R/N/apply(+주기 A)로 재설계. §7/§9의 현행 스키마·스테이지 서술은 **v4.0 구현 완료 시점까지의 현행(as-is)** 기록이다 — 신규 작업은 §17을 정본으로 본다.
> **통합 출처**: 기존 `prd.md`(v2.0 마스터), `prd-renew.md`(v1.4), `prd_embedding.md` 3개 문서를 하나로 합치고 최신 결정을 반영. 원본은 `docs/archive/`에 보존.
> **범위**: `data-pipeline`(파이프라인·모델서빙) + `service`(Django 백엔드) + `webtoonmoa`(조회·라벨링 프론트) + `proxmox-configuration`(k3s GitOps) — **4개 레포**
> **이번 v3.0 핵심 변경**:
> 1. 스트림 처리 레이어 **Faust/Kafka → Temporal 워크플로**로 피벗 결정 (§4)
> 2. model-api **OCR/YOLO 엔드포인트 분리** 적용 (`/ocr`, `/yolo`, 모드 `ocr`/`yolo`) (§6.1)
> 3. **LLM(Step 3/4)를 1급 섹션으로 승격** — 현재 미구현, GLM-4.6v로 구현 예정 (§9)
> 4. 70만 배치 백필은 **이번 범위에서 보류**(증분 경로 집중) (§5)
>
> **v3.1 변경 (2026-06-28)**:
> - **`TextBlockType` 개편(A)**: 독백(Monologue) 신규 추가, 효과음(SFX) 타입 제거(→ OTHER + soft-exclude), "상황 서술"은 `TextBlockType`이 아니라 `CutSceneMeta`(장면 서술) 레이어로 분리 (§7, §9.4).
> - (보류) 엑스트라/효과음 soft-exclude 정리 정책(Human/VL 판정)은 별도 개정에서 확정.
>
> **v3.2 변경 (2026-07-01) — 실제 코드/배포 상태 반영**:
> - **Faust→Temporal 전환 완료**: `service`는 이미 `config/temporal.py`만 사용(`config/kafka.py` 삭제됨), `proxmox-configuration`의 `pipeline_repo` configmap도 `TEMPORAL_ADDRESS`만 있고 Kafka 관련 설정 없음. 본 문서 곳곳의 "미구현/POC/목표 아키텍처" 표현은 과거 스냅샷이었음 — §2·§4·§13 갱신.
> - **Step 3 구현 완료**: `webtoon-pipeline/src/core/step3.py`가 extract(pass1)→resolve(pass2a)→apply(pass2b, LLM 없는 결정론적 커밋) 2-pass로 동작 중. 상세 설계는 `prd-step3.md`(§9를 대체)로 이관.
> - **레포 4개로 분리 명시**: 이전 버전은 `webtoonmoa`를 `service`(Django)에 묶어 표기했으나, 실제로는 별도 SvelteKit 프론트엔드 레포(`/Users/jj/github/webtoonmoa`)다. §2에 4번째 레포로 분리.
>
> **v3.3 변경 (2026-07-01) — `prd-step3.md` 전면 흡수 + Step4 판단 정정**:
> - **§9를 `prd-step3.md`의 최종 설계로 전면 교체**: 문제정의·목표·모델 토큰예산/윈도우 적응 설계·2-pass 아키텍처·Pass-1/2a/2b 계약·belief state·소구포인트(비트) 계층·캐릭터 중요도 티어링·mis-ID distrust/책략 탐지/교차에피소드 prior 신뢰성 규칙·정답 데이터 취급·Temporal 배선까지 전부 흡수. `prd-step3.md`는 실험 로그·인용 근거를 보존하는 이력 문서로 유지.
> - **v3.2의 "Step4 미착수" 판단을 정정**: Step3+Step4는 하나의 에피소드 추론 단계로 통합됐고, Pass-2b(`apply_resolution`)가 매 에피소드 처리마다 `EpisodeReport`(summary/appeal_point/cliffhanger/foreshadowing/character_timeline)를 자동 커밋한다 — 즉 Step4는 이미 구현·운영 중이다. `episode-summary/main.py`는 이 통합 이전 시점의 요약 품질 비교용 레거시 실험 스크립트로 재확인.
> - **DB 스키마 8종 추가 반영**(§7): `TextAnnotation.resolution_status`, `Character.significance`, `EpisodeReport`, `EpisodeBeat`, `NameDiscoverySuggestion`, `StoryArc`, `NarrativeThread`, `CharacterClaim`, `LLMUsage`, `WebtoonNarrativeState` — 전부 마이그레이션·코드 반영 완료.
> - **골든 회귀 테스트 3종 작성·통과**: mis-ID distrust(ep2 천마→운암), 책략 탐지(ep3 청진, Property 10), 교차에피소드 정체성 prior(ep3 418=청명) — `webtoon-pipeline/tests/`. 부수적으로 `test_workflow_orchestration.py`의 stale 스텁(`step3_episode` 단일 액티비티 잔존) 버그를 step3a/b/c 3-스텁으로 갱신해 수정.
> - **재처리(§11.2)를 에피소드 단위로 재설계**: 컷 단위 short-circuit → 에피소드 단위 `reresolve_episode`/`reapply_episode`.
>
> **v3.4 변경 (2026-07-03) — 홈랩 배포 환경 신뢰성 장애 대응 세션**:
> - **신규 §16**: 사용자의 실제 배포 환경(홈랩 k3s + Cloudflare Tunnel + 불안정 홈 네트워크)을 문서화하고, 이 환경에서 실제로 터진 3개 버그(Step1 `resolution_status` NOT NULL / Step1 재시도 비멱등성 / Step2·Chroma·DB 정합성 드리프트)의 원인·수정 내역·검증 상태를 기록. `data-pipeline`(`step1.py`/`step2.py`/`step3.py`/`activities.py`/`ocr_yolo_client.py`) + `service`(`tasks.py`/`chroma_client.py`/`views.py`) 양쪽 레포 수정.
> - **model-api 구조적 리스크 발견(미수정)**: 전 라우터가 `async def` 안에서 동기 CPU-bound 추론을 직접 호출 — 부하 시 이벤트루프 블로킹으로 gunicorn WORKER TIMEOUT/SIGKILL 유발 가능(§16.2). 다음 세션 논의 필요.
>
> **v3.6 변경 (2026-07-05) — 화자 매칭 구조 결함 수정 + 인물도감 profile + HITL stale 배선 + 캐시 슬림화**:
> - **화자 매칭률 1~2% 구조 결함 발견·수정**: naver/820097 전 30회차 실측 — llm speech/monologue 블록의 speaker_id 부착률이 회차당 0~7%. 3중 원인: ① Pass-1이 얼굴 기반으로 확신한 화자 후보를 **DB에 저장 안 함**(speaker_id=NULL 강제), ② Pass-2a 프롬프트가 "provisional speaker가 null/불확실한 블록만" speaker_resolution으로 내라고 지시 → 확신 블록은 재출력 안 됨, ③ Pass-2b는 speaker_resolution만 커밋 → 확신 화자가 전부 유실. 수정: Pass-1 화자 후보(conf≥0.5)를 provisional speaker_id로 영속(`resolution_status='unresolved'` 유지), Pass-2a는 **전수 화자 테이블**(모든 speech/monologue, confirm-or-override) 출력으로 변경, Pass-2b에 provisional 화자 resolved 승격 안전망 추가(§9.4~9.6).
> - **is_confirmed가 모델 입력에 실리지 않던 버그 수정**: 프롬프트는 "is_confirmed는 진실로 동결"을 지시하는데 `_load_faces`/페이로드에 그 플래그가 아예 없었음 — Pass-1 `identified_faces`와 Pass-2a `faces`에 `confirmed` 추가(human 얼굴 확정이 실제로 화자/정체 판단에 반영되는 경로 확보).
> - **인물도감(범용 profile)**: Pass-2a characters에 `profile{gender, age_group, affiliation, role, personality[], traits{}}`(장르 특이값은 free-form traits) 추가 — `character.extra['llm_profile']`에 병합 커밋(스키마 변경 없음, is_confirmed 캐릭터 동결), `episode_report.character_timeline`에도 스냅샷 포함.
> - **HITL stale 배선**: service 얼굴 확정/일괄 재배정/텍스트 어노테이션 API가 `webtoon_cut.is_stale`/`human_modified_at`을 마킹하도록 수정(종전엔 human이 고쳐도 파이프라인이 알 수 없었음). 2-pass가 `llm_analyzed_at`/`is_stale=false`를 전혀 안 찍던 것도 수정(apply_resolution에서 에피소드 컷 일괄 마킹). 재해소 실행용 CLI `python -m src.tools.reresolve <source> <title_id> <ep|stale> [--rerun-extract]` 추가(Temporal 자동 트리거는 후속 과제).
> - **WebtoonNarrativeState 캐시 슬림화**: fold(순수·단조)는 유지하고 `persist_state` 쓰기 시점에 roster를 유의미 인물(실명 확정 또는 main/supporting)로 한정 + key_facts 인물당 12개 캡 + running_summary 최근 30화 줄로 캡(실측 ep30에 roster 69명 대부분 NEW_CHAR 엑스트라 — 무한 증가 구조였음). 정본은 row 단위 테이블(character/episode_report/narrative_thread)에 그대로.
> - **max_tokens 재상향**: `_PASS1_MIN_MAX_TOKENS`/`_PASS2_MIN_MAX_TOKENS` 8192→16384 (8192에서도 pass1 finish_reason='length' 절단 7건 잔존 + Pass-2a 출력이 전수 화자 테이블로 커짐).
> - **요약/책략 프롬프트 보정**: deception은 "다른 인물을 속이려는 의도가 있는 speech"만(monologue/자조/한탄 제외 — ep2에서 독백이 deception으로 오판된 실사례), summary/appeal_point는 narration·실제 사건 근거만(근거 없는 낙인·평가어 금지).
>
> **v3.5 변경 (2026-07-04) — `naver/820097` end-to-end 검증 + Step2/Step3 회귀 버그 3건 추가 발견·수정 + Step3 신뢰성/품질 개선**:
> - **§16.5 R2 종결**: `naver/820097` ep2를 실제로 재실행해 step1→step2→step3 전 구간 완료 확인(§16.6). 이후 ep10/ep11 등 추가 회차도 정상 진행 중.
> - **Step2 자기-런 스냅샷 버그 발견·수정**(§16.6): 지난 세션에 추가한 드리프트 방어 로직(`valid_appearance_ids`)이 루프 시작 전 스냅샷이라, 같은 에피소드 처리 중 새로 생긴 캐릭터를 "유령"으로 오판해 42/42 얼굴이 전부 신규로 쪼개지는 회귀가 있었음 — 즉시 반영하도록 수정.
> - **Step3 Temporal 워커 액티비티 미등록 버그 발견·수정**(§16.7): `worker.py`가 옛 단일 패스 `step3_episode`만 등록하고 있어 실제 워크플로가 호출하는 `step3a_extract`/`step3b_resolve`/`step3c_apply`가 `NotFoundError`로 전부 실패 — 2-pass 액티비티로 교체. `step3_episode`(+`step3.py`의 legacy `analyze_cut_scene`/`analyze_episode_scenes`)는 호출자 없는 죽은 코드로 확인(미삭제, 필요시 별도 정리).
> - **`narrative_context.fold` 캐시 불일치 버그 발견·수정**(§16.8): `_commit_threads`가 DB에 실제로 커밋하는 `planted_episode`와, `apply_resolution`이 fold에 넘기는 캐시용 값이 서로 다른 계산 경로라 어긋날 수 있음(실사례: DB엔 정확히 ep2로 기록됐는데 캐시 JSON엔 LLM이 반환한 원본값 1로 남음) — 정규화 헬퍼로 일치시킴.
> - **Step4(회차 요약) 별도 프로덕션 연결 불필요로 확정**: 사용자가 원래 계획했던 별도 Step4는 이미 §9.6(Pass-2b 흡수)로 대체돼 있음을 재확인, 이번 세션 논의로 계획 철회.
> - **vllm(`vllm.prup.xyz`) 502/530 대응 + Pass-1 병렬화 + max_tokens 절단 버그 수정 + 프롬프트 한국어 강제**(§16.9): `llm_client.py`에 OCR/YOLO와 동일한 10회 지수 백오프 재시도 추가, `extract_episode`의 컷별 LLM 호출을 `ThreadPoolExecutor`로 병렬화(belief 누적 등 순서 의존 후처리는 완료 후 재정렬), `_PASS1_MIN_MAX_TOKENS` 4096→8192 상향(추론형 모델 glm-4.6v가 reasoning_content로 토큰 예산을 먼저 소모해 본문이 잘리는 문제), Pass-1/2a 시스템 프롬프트에 "반드시 한국어" 지시 강조, `LLM_MAX_CONCURRENCY`/`PASS1_WORKERS`를 `proxmox-configuration` configmap에 노출(이전엔 코드 defaults에 고정돼 배포 없이 못 바꿨음).

---

## 1. 개요 & 목적

웹툰 컷 이미지에서 **텍스트(OCR)**, **얼굴/캐릭터(YOLO+임베딩)**, **장면·화자(멀티모달 LLM)** 를 추출하고 회차 단위로 서사를 요약하는 파이프라인. 결과는 PostgreSQL + S3 + Chroma에 저장되어 `webtoonmoa` 서비스가 소비한다.

| 구분 | 내용 |
|------|------|
| 입력 | S3 컷 이미지 (`{S3_LOCATION}/{source_dir}/{title_id}/{ep}/{title_id}_{ep}_{cut}.jpg`) — boto3 직접 다운로드 |
| 로컬 모델 | PaddleOCR(korean), [deepghs/anime_face_detection](https://huggingface.co/deepghs/anime_face_detection)(YOLO), CLIP ViT-L/14, CCIP(deepghs) |
| 원격 모델 | GLM-4.6v (z.ai API, 1차) / Qwen3-VL-32B(로컬, 폴백) — Step 3+4 통합 구현 완료(§9) |
| 저장 | PostgreSQL(텍스트·얼굴·캐릭터 메타) + S3(face crop) + Chroma(얼굴 임베딩) |
| 규모 | 웹툰 30+종, 누적 에피소드 ~6,000, 누적 컷 ~60만, 일 신규 ~10 ep(~1,000 컷) |
| 처리 | **증분 스트리밍**(일 신규분) 중심. 배치 백필(70만)은 보류 |

### 파이프라인 4단계 (Step 3·4는 하나의 에피소드 추론 단계로 통합됨, §9)

```
Step 1 ── 로컬 추출 (OCR + YOLO, 분리 병렬)      ← 모든 웹툰   ✅ 구현 (Temporal)
Step 2 ── 인물 식별 (임베딩 + Chroma 매칭)        ← 모든 웹툰   ✅ 구현 (Temporal)
Step 3 ── LLM 2-pass 장면/화자 분석(extract→resolve→apply) ← 활성 웹툰만 ✅ 구현 (GLM, §9)
Step 4 ── 회차 종합 요약(EpisodeReport)          ← 활성 웹툰만 ✅ Step3 Pass-2b(apply)에 흡수되어 자동 산출(§9.6)
```

> **Step 1·2 전체 / Step 3·4 활성 웹툰만**: Step 1·2는 누적 백로그+신규 전체 처리해 face DB를 완성. Step 3·4는 비용·시간이 크므로 `phase3_enabled=True`인 활성 웹툰만 실행. Step 4는 별도 실행 단계가 아니라 Step3의 Pass-2b 커밋에서 매 에피소드 처리마다 자동으로 나온다 — `episode-summary/main.py`는 이 통합 이전 시점의 요약 품질 비교용 레거시 실험 스크립트로, 현재 프로덕션 경로가 아니다.

---

## 2. 시스템 구성 — 4개 레포 책임

전체 시스템은 4개 Git 레포로 나뉜다. "어디서 무엇을 하는가"를 한 곳에 정리한다. (이전 버전은 webtoonmoa를 `service`에 묶어 3개 레포로 표기했으나, 실제로는 별도 프론트엔드 레포다.)

| 레포 | 로컬 경로 |
|---|---|
| `data-pipeline` | `/Users/jj/github/data-pipeline` (본 문서) |
| `service` | `/Users/jj/github/service` |
| `webtoonmoa` | `/Users/jj/github/webtoonmoa` |
| `proxmox-configuration` | `/Users/jj/github/proxmox-configuration` |

### 2.1 `data-pipeline` — ML 파이프라인 + 모델 서빙

| 하위 디렉터리 | 역할 | 상태 |
|---|---|---|
| `webtoon-pipeline/` | **Temporal 워커**(메인). `core/`(faust-free Step1·Step2·Step3 로직) + `temporal/`(workflows/activities/worker/starter). Faust/Kafka 제거됨 | ✅ 배포·운영 중(`proxmox-configuration/pipeline_repo`) |
| `model-api/` | FastAPI 모델 서빙. `MODEL_API_MODE`로 OCR/YOLO/CLIP/CCIP 분리 로드. 단일 이미지 + 모드 플래그 | ✅ 운영 중 |
| `episode-summary/` | Step4(회차 요약) **실험 러너**. `core/step3.py`가 쌓아둔 산출물을 모아 요약 LLM 품질을 비교(DB 미저장) | 🔬 실험, 프로덕션 미연결 |
| `face-embed-lab/` | CLIP/DINOv2/CCIP/DeepDanbooru **비교 실험 도구**(정식 검증 하니스 아님) | 🔬 lab |
| `local_analysis/` | 로컬 분석 실험(자체 PRD 보유). 배치 백필 맥락 | 🔬 |
| `face_crop/`, `chromadb` | 실험용 crop 샘플 / 로컬 chroma 산출물 | 데이터 |

### 2.2 `service` — Django 백엔드

DB 모델의 **source of truth**이자 파이프라인 **트리거 주체**.

| 위치 | 역할 |
|---|---|
| `backend/apps/api/toon/` | 웹툰 도메인. `models.py`(전 스키마), `views.py`(API, 얼굴 재배정/확정), `tasks.py`(웹툰 다운로드 + Temporal 트리거), `admin.py`, `management/commands/` |
| `backend/config/temporal.py` | **파이프라인 트리거** — `send_phase1_trigger`(웹툰당 1회 kick) 등. 과거 `config/kafka.py`는 삭제되고 이걸로 완전히 교체됨 |
| `backend/config/celery.py` + `celery_configs/` | Celery 앱. beat reconciler + hipri/lopri 워커(파이프라인 트리거와는 별개 — 다운로드/알림 등 일반 비동기 작업) |
| `backend/apps/api/{account,domain,naver,notification,openapi}` | 인증·도메인·네이버 연동·알림·OpenAPI |
| Postgres / Redis | 운영 DB / Celery 브로커·캐시 |

### 2.3 `webtoonmoa` — 조회·라벨링 프론트엔드

SvelteKit 앱. `service`의 Django API를 소비해 분석된 웹툰을 사람이 보고 검증(라벨링)할 수 있게 한다 — 캐릭터 이름 확정, 텍스트 제외, 재분석 트리거 등 §11 Human-in-the-loop·§12 기능 요구사항의 실제 UI.

### 2.4 `proxmox-configuration` — k3s GitOps (ArgoCD)

ArgoCD app-of-apps(`apps/`)로 전 워크로드를 선언적 배포.

| repo 디렉터리 | 배포 대상 |
|---|---|
| `pipeline_repo/` | `webtoon-pipeline`(Temporal 워커, replicas=1, k3s-super-worker-01) + **model-api**(`clip`/`ccip`, OCR/YOLO는 별도 GPU 서버 호출) + configmap(`TEMPORAL_ADDRESS`, *_API_URL) |
| `temporal_repo/` | Temporal 서버(frontend/ui) 배포 — Faust/Kafka를 대체 |
| `service_repo/` | Django backend(+HPA) / celery(beat·hipri·lopri·KEDA) / redis / nginx / flower |
| `envoy_repo/` | Envoy Gateway, 라우트·정책, cloudflared 터널(public/private) |
| `monitoring_repo/` | prometheus / alloy / kube-state-metrics |
| `ollama_repo/` | ollama + open-webui (**로컬 LLM 인프라** — §9 LLM 선택지) |
| `system/` | cert-manager / keda / infisical-operator / metrics-server / nfs |

> **외부 인프라(클러스터 밖)**: Chroma(`oci-croma.prup.xyz`, OCI), S3, GPU 서버(OCR/YOLO 추론). Kafka는 Temporal 전환 완료로 더 이상 사용하지 않는다(§4).

---

## 3. 도메인 데이터 흐름 (4단계 상세)

```
컷 N ─┬─ PaddleOCR ──→ TextRegion + TextAnnotation(paddle)        [Step1]
      └─ YOLO ───────→ FaceRecord (+ face crop S3)                [Step1]
                          │
                          ▼ 임베딩(CLIP/CCIP) + Chroma 매칭         [Step2]
                   CharacterAppearance 매칭 or NEW_CHAR 발급
                          │
                          ▼ step3a_extract(Pass-1, 컷별 비전)       [Step3]
                   provisional TextAnnotation(llm) + CutSceneMeta
                          │
                          ▼ step3b_resolve(Pass-2a, 에피소드 텍스트 전역해소, 이미지 없음)
                   characters/speaker_resolution/beats/episode/deceptions/threads
                          │
                          ▼ step3c_apply(Pass-2b, LLM 없음, 결정론 커밋+소급전파) [Step3/4 통합]
회차 전체 ─────────────────┴─→ TextAnnotation(resolved) + EpisodeBeat + EpisodeReport +
                                NarrativeThread + CharacterClaim + WebtoonNarrativeState(fold)
```

### Step 1 — 로컬 추출 (모든 웹툰, ✅)
- PaddleOCR → 텍스트+bbox, YOLO → face bbox. **GLM 호출 없음.**
- 에피소드 내 컷 순차, 에피소드 간 병렬. 404(이미지 없음)로 에피소드 경계 감지.
- **OCR/YOLO 분리**(v3.0): 별도 model-api 서비스/엔드포인트 호출, 독립 재시도 (§6.1).

### Step 2 — 인물 식별 (모든 웹툰, ✅)
- face crop → 임베딩 추출 → Chroma 유사도/CCIP metric 매칭 → threshold 이상이면 캐릭터 귀속, 아니면 `NEW_CHAR_*`(웹툰 글로벌 스코프) 발급.
- 웹툰별 에피소드 순차(ep1 확정이 ep2 매칭에 반영). 임베딩+매칭 **1패스 통합**(이중 임베딩 제거 완료).
- doc_id `{webtoon_id}_{episode}_{cut}_F{idx}` 고정 + `upsert` 멱등성.

### Step 3 + 4 — LLM 2-pass 장면/화자/서사 분석 (활성 웹툰만, ✅ 구현) — 상세는 §9
- `step3a_extract`(컷별 비전 추출, Pass-1) → `step3b_resolve`(에피소드 전역 화자/이름/서사 해소, Pass-2a, 이미지 없음) → `step3c_apply`(결정론적 커밋+소급전파, Pass-2b, **LLM 미사용**).
- Step4(회차 요약)는 별도 단계가 아니라 Pass-2b 커밋에 흡수되어 `EpisodeReport`로 매 에피소드마다 자동 산출된다(§9.6).
- 골든 회귀 테스트 3종으로 mis-ID distrust/책략 탐지/교차에피소드 정체성 prior 핵심 동작이 고정됨(§9.10).

---

## 4. 아키텍처 결정 — 스트림 처리 Faust → Temporal 피벗 (완료)

> 이 절은 결정 당시(v3.0) 기록을 그대로 두되, **현재는 마이그레이션이 완료된 상태**다(§13, v3.2 변경 참조). "Faust=현행/Temporal=목표"로 쓰인 표현은 결정 시점 기준이다.

### 4.1 배경 (결정 당시)
당시 현행은 Faust+Kafka. 이 워크로드의 본질은 "**엔티티(에피소드) 단위 다단계 durable workflow** + 웹툰당 순차/웹툰 간 병렬 + 중간 재개 + 단계별 재시도"이며, 일 ~1000컷으로 스트리밍 처리량 요구는 없다. Faust의 한계:
- faust-streaming 생태계 유지보수 불안.
- 컷/에피소드 진행을 **메시지 자가 재발행**으로 구현 → 암묵적 상태머신, 추적 난해.
- OOM 재시작 시 **Kafka 오프셋 미커밋 → 컷1부터 재처리**(체크포인트 부재).

### 4.2 결정: Temporal durable workflow
| 관심사 | Faust(현행) | Temporal(목표) |
|---|---|---|
| 웹툰당 순차 / 웹툰 간 병렬 | Kafka 파티션 키 | 웹툰=워크플로 인스턴스 1개(`workflow_id={source}_{title_id}`) |
| 중간 재개 | 오프셋+자가 재발행 | 워크플로 history 영속(activity 완료 단위 재개) |
| 단계별 재시도 | 수기 `retry_count` | activity `RetryPolicy` |
| 트리거/스케줄 | Celery beat | Temporal Schedule / 멱등 start |
| history 무한 증가 | 해당 없음 | `continue_as_new` (컷·에피소드 단위) |

워크플로 계층(POC `temporal-pipeline/` 구현·검증 완료):
```
WebtoonWorkflow (id="{source}_{title_id}")        # 웹툰당 1개 → 순차/병렬
  └─ EpisodeWorkflow (child)                       # 에피소드 순차
       └─ 컷 루프: ocr ∥ yolo (각자 추론+저장 독립)  # OCR/YOLO 분리 병렬
       └─ (컷 완료 후) face_identify → chroma_sync   # 얼굴식별 = 에피소드 단위
```
- Kafka + Faust + Celery beat **3개를 Temporal 하나로 통합** 가능.
- 배치 백필이 필요해지면 별도 재개형 스크립트(오케스트레이션 불필요)로 분리.

### 4.3 service 트리거 전환 (완료)
`config/kafka.py` 프로듀서 → Temporal 클라이언트로 **교체 완료**:
- `send_phase1_trigger(...)`가 `client.start_workflow(WebtoonWorkflow.run, ..., id="{source}_{title_id}")` 호출(멱등 kick)로 동작 중. `backend/config/kafka.py`는 삭제되고 `config/temporal.py`만 남음.
- 에피소드 체이닝은 워크플로 내부로 내려가 **service는 웹툰당 1회 kick**으로 단순화됨.

> **상태**: Kafka/Faust는 완전히 제거됐고 Temporal이 유일한 오케스트레이션 경로다. `proxmox-configuration`에도 `temporal_repo`(Temporal 서버)가 배포돼 있고 `pipeline_repo` configmap에는 `TEMPORAL_ADDRESS`만 있다(Kafka 설정 없음).

---

## 5. 실행 컨텍스트

```
┌─ 증분 스트리밍 (일 ~1000컷) — 본 PRD 집중 ──────────────┐
│  위치: k3s (Temporal 워커 + model-api)                   │
│  처리: 신규 다운로드분, 웹툰당 에피소드 순차             │
│  매칭: 기존 앵커 대비 greedy 1-NN                        │
└────────────────────────────────────────────────────────┘

┌─ 배치 백필 (70만 장, 1회성) — ⏸ 이번 범위 보류 ─────────┐
│  위치: RTX3060 데스크톱(WSL2), Kafka/Temporal 없이 스크립트│
│  처리: OCR+YOLO+CCIP feature + 오프라인 클러스터링        │
│  ※ operator 라이브러리화 후 별도 진행. 현 시점 비활성     │
└────────────────────────────────────────────────────────┘
```

> 사용자 방침: 배치 백필은 보류하고 증분 경로(Temporal 전환 + LLM 구현)에 집중한다.

---

## 6. 컴포넌트 상세

### 6.1 model-api (모델 서빙)

단일 이미지 + `MODEL_API_MODE` 플래그로 로드 모델·라우터 선택. Deployment를 모드별로 나눠 **서버별 로드 분리**(독립 스케일/재시작).

| MODE | 로드 | 엔드포인트 | k3s 배포 |
|---|---|---|---|
| `ocr` | PaddleOCR | `/ocr` | (분리 시) ocr-api |
| `yolo` | YOLO | `/yolo` | (분리 시) yolo-api |
| `ocr-yolo` | OCR+YOLO | `/ocr` `/yolo` `/ocr-yolo`(결합, 하위호환) | `ocr-yolo-api` |
| `embed-clip` | CLIP ViT-L/14 | `/embed` | `embed-clip-api` |
| `embed-ccip` | CCIP | `/embed-ccip` `/ccip-compare` | `embed-ccip-api` |
| `all` | 전부 | 위 전체 | (개발용) |

- **v3.0 변경**: OCR과 YOLO를 별도 모드(`ocr`/`yolo`) + 엔드포인트(`/ocr`,`/yolo`)로 분리. PaddleOCR(무거운 CPU ~1.5GB)와 YOLO(가벼움 ~100MB)의 로드 프로파일이 달라 독립 스케일이 유리. 기존 `/ocr-yolo` 결합 엔드포인트는 Faust 호환 위해 유지.
- 스레드 과구독 방지: `OMP_NUM_THREADS`/`torch.set_num_threads`/paddle cpu_threads env 설정화.
- paddle C++ 누수: gunicorn `--max-requests`로 주기적 프로세스 교체.
- configmap URL: `OCR_YOLO_API_URL`/`EMBED_CLIP_API_URL`/`EMBED_CCIP_API_URL`(+ OCR/YOLO 분리 시 `OCR_API_URL`/`YOLO_API_URL`).

### 6.2 임베딩·매칭 (Step 2 코어)
- feature 추출(무거움)은 model-api, **metric 비교(가벼움)는 파이프라인**에서. CCIP metric 비교는 model-api `/ccip-compare`에 둬 파이프라인에 모델 재유입 방지.
- 증분 = greedy 1-NN, (배치 재개 시) = 오프라인 클러스터링(avg linkage).

---

## 7. DB 스키마 (PostgreSQL, service 관리)

Region(위치)과 Annotation(텍스트 해석) 분리 원칙. `apps/api/toon/models.py`.

### 핵심 테이블
- **Webtoon**(source, title_id) + **WebtoonEpisode**(webtoon FK, no) — Kakao/Naver 통합 모델.
- **WebtoonCut**(episode FK, cut_number, processed_at, is_stale, `llm_analyzed_at`, human_modified_at, **`llm_model` FK**). `image_url` 없음 — S3 경로 재현.
- **TextRegion**(cut FK, index, bbox, is_excluded) — bbox 불변. `is_excluded`=human이 분석 제외(간판/UI 등).
- **TextAnnotation**(region FK, source `paddle|llm|human`, text, type, speaker, confidence, model_version, **`resolution_status`** `unresolved|resolved`) — 레이어 적재. 최종 우선순위 `human > llm > paddle`. `resolution_status`는 Step3 2-pass의 provisional(Pass-1 적재)→confirmed(Pass-2b 커밋) 표식(§9.6).
  - **`type`(TextBlockType) — OCR 텍스트 영역 분류 (레이어 A, region 귀속)**:

    | 값 | 의미 | speaker |
    |---|---|---|
    | `speech` | 대사 — 입 밖으로 낸 말(일반 말풍선) | 있음 |
    | `monologue` | **독백 — 속마음(구름/각진 말풍선). v3.1 신규** | 있음 |
    | `narration` | 나레이션 — 세계관/상황 해설(사각 박스). 특정 화자 아님 | 없음(null) |
    | `system` | 시스템/캡션 — 상태창·서적 글귀·편지·연도 표시 등 | 없음 |
    | `other` | 그 외(효과음 OCR 원문 등). RAG 제외 후보(`is_used=False`) | - |

    - **독백 분리 이유**: 독백(캐릭터 속마음)과 나레이션(화자 없는 해설)은 화자 유무가 본질적으로 달라 RAG의 의도/감정 추론에서 분리 필수. 독백은 `speaker` FK로 화자 귀속.
    - **효과음(SFX) 제거**: 별도 type 없이 OCR 원문은 `other`로 두고 `is_used=False`로 RAG 제외. 효과음의 *의미*는 아래 "상황 서술"로 흡수.
- **CutSceneMeta**(cut OneToOne, action_summary, key_objects) — **상황 서술 (레이어 B, OCR 텍스트 아님)**. 효과음·배경 묘사를 통합한 컷 단위 시각적 사건 요약("폭발이 일어남", "눈물을 흘림"). region에 귀속되지 않으므로 `TextBlockType`이 아니라 Step3 `scene_meta` 산출물로 저장.
- **Character**(webtoon_id, name, aliases, age, skills, first_seen_*, is_confirmed, **is_name_auto_assigned**, **`significance`** `main|supporting|minor_functional|extra`, notes) — 논리 인물. `significance=extra`는 `is_match_excluded=true`를 동반해 Step2 매칭 후보에서 soft-exclude(하드 삭제 아님, 가역, human 동결 존중 — §9.5). **(v3.6 과도기)** 인물도감 메타(profile)는 현재 `extra['llm_profile']` jsonb에 병합 저장·`CharacterSerializer.profile`로 노출 중이나, **출처 구분(llm/human/human-edited)이 안 되는 구조라 별도 `CharacterProfile` 모델로 이행 예정**(§14-9, 설계 논의 중 — 마이그레이션 전까지만 임시).
- **CharacterAppearance**(character FK, label, description, first_seen_*, is_canonical) — 시각 외형 단위(변장/이세계/성장 대응). Chroma는 character_id+appearance_id 함께 저장.
- **FaceRecord**(cut FK, face_idx, appearance_id FK/NULL, bbox, conf, chroma_doc_id, match_score, is_confirmed). crop은 `crop_s3_key` property로 재현.
- **WebtoonPipelineState**(webtoon OneToOne, phaseN_status, phase2_last_completed_episode, phase2_processable_max_episode, phase3_enabled, ...) — **웹툰 단위 오케스트레이션/설정**.
- **EpisodePipelineProgress**(episode FK, phase 값1~4, status, completed_at, unique(episode,phase)) — per-episode 완료 추적(phase는 컬럼 아닌 값). 현재 phase1 채택, phase2~ 점진 흡수.

### Step 3/4 스키마 (2-pass 서사 해소, `episode-scene-resolution` 스펙 — 전부 구현·마이그레이션 완료)
- **EpisodeReport**(episode OneToOne, summary, appeal_point, cliffhanger, foreshadowing jsonb, character_timeline jsonb) — 옛 "Step4" 산출물. Pass-2b가 매 에피소드 처리마다 자동 upsert(§9.6).
- **EpisodeBeat**(episode FK, cut_start, cut_end, hook_type **free-form 텍스트**(enum 아님), appeal_point, intensity, stable_key) — 비트 개수 제약 없음(에피소드 전체가 1비트일 수도). `stable_key`로 재처리 시 비트 식별 안정화.
- **NameDiscoverySuggestion**(webtoon FK, character/appearance FK, name, confidence, evidence, source_episode FK, source_cut, status `pending|accepted|rejected`) — 다중 컷 이름 증거 누적(§9.5 name_evidence). 옛 `Character.extra.name_suggestions` json 적재 방식 폐기.
- **StoryArc**(webtoon FK, level `arc|part`, parent FK NULL, ordinal, title, episode_start, episode_end, summary, appeal_point, is_confirmed) — 교차 에피소드(아크) 단위 소구포인트(§9.5 "단위 유연성").
- **NarrativeThread**(webtoon FK, description, type, status `open|resolved`, planted_episode FK, planted_cut, resolved_episode FK NULL, resolved_cut NULL, confidence) — 떡밥 심음/회수 추적. `EpisodeReport.foreshadowing`(jsonb 요약)보다 구조화된 정식 테이블.
- **CharacterClaim**(cut FK, character FK NULL, claim, contradicts, is_deception, confidence) — Pass-2a `deceptions` 산출물의 영속화(§9.7 텍스트 진실성 등급/책략 탐지).
- **LLMUsage**(webtoon FK, episode FK NULL, cut FK NULL, stage, llm_model FK, prompt_tokens, completion_tokens, total_tokens, image_count NULL, finish_reason NULL, extra jsonb NULL) — LLM call당 1행, 웹툰/에피소드/컷 축 SUM 집계용.
- **WebtoonNarrativeState**(webtoon OneToOne, last_resolved_episode FK, roster jsonb, open_threads jsonb, running_summary) — belief state의 웹툰 전역 영속(§9.5). `narrative_context.load_prior`/`fold`가 매 에피소드 처리 전후로 조회/갱신. **(v3.6) 캐시 슬림화**: 무한 누적 방지를 위해 `persist_state` 쓰기 시점에 roster는 실명 확정 또는 main/supporting 인물만(+key_facts 인물당 12개 캡), running_summary는 최근 30화 줄만 유지(실측: ep30에 roster 69명 — 대부분 NEW_CHAR 엑스트라). 정본은 row 단위 테이블(character/episode_report/narrative_thread)이므로 캐시 절삭에 정보 손실 없음. fold 자체의 단조성(Property 9)은 인메모리 체인에서 유지.

### 설정 테이블 (모델 일반화)
- **EmbeddingModel**(name unique, display_name, metric_type `cosine|ccip`, default_threshold, params json, is_default, is_active) — 시드 `clip`(cosine,0.25), `ccip`(ccip,0.16,is_active).
- **WebtoonEmbeddingSetting**(webtoon FK, embedding_model FK, threshold null, is_enabled, unique(webtoon,model)) — 웹툰별 모델/threshold override.
- **LLMModel**(name unique, provider, model_id, params json, supports_vision, is_default, is_active) — 시드 `glm-4.6v`(provider=zai, is_default).
- **WebtoonLLMSetting**(webtoon FK, llm_model FK, is_enabled, unique(webtoon)) — 웹툰 단위 LLM 선택.

> `glm→llm` 일반화 적용 완료: `TextAnnotationSource.LLM`, `WebtoonCut.llm_analyzed_at`, `PipelinePhase.SCENE_LLM`(값3).

---

## 8. 임베딩 모델 / Threshold 설정화 (CLIP·CCIP)

### 8.1 실험 근거 (face-embed-lab, 1292 crop)
- CLIP: 식별력 약함(여러 인물 한 덩어리). CCIP(deepghs, 애니 동일인 판별 전용) 채택.
- CCIP avg linkage 스윕 → **threshold 0.16** 기준값.

### 8.2 해석 규칙
```
모델  = WebtoonEmbeddingSetting(webtoon, is_enabled).model  또는  EmbeddingModel(is_default)
임계값 = WebtoonEmbeddingSetting.threshold ?? EmbeddingModel.default_threshold
```
하드코딩(`MATCH_THRESHOLD`, `EMBEDDING_MODEL_NAME`) 제거, 웹툰 처리 시작 시 1회 조회·캐시.

### 8.3 metric_type 분기 (핵심 제약)
> **feature 추출은 동일**: CLIP·CCIP 모두 **얼굴 1개당 model-api 호출 1회**(`embed_for` → `/embed` 또는 `/embed-ccip`). 추출 패턴에는 차이가 없다. 차이는 **매칭(비교) 단계에서만** 발생한다.

- `cosine`(CLIP): feature 1콜 → Chroma 코사인 query + threshold. **추가 모델 호출 없음.**
- `ccip`: feature 1콜 → 매칭 시 컬렉션 앵커 feature를 모아 **`/ccip-compare` 1회 추가 호출**(학습된 metric onnx 모델 실행, min diff ≤ 0.16). CCIP 차이값은 코사인으로 대체 불가하므로 비교 콜이 별도로 붙는다. 대규모는 코사인 top-K 후보 → CCIP re-rank 2단계(prefilter recall 검증 필요).
- 즉 face당 호출 수: **CLIP = 추출 1**, **CCIP = 추출 1 + 비교 1**.
- 컬렉션 분리: `character_faces_{source}_{title_id}_{model}` (모델 혼합 금지).

### 8.4 전환 시퀀싱
- 현재 default = `clip`(cosine) 유지 → 운영 동작 불변. CCIP는 `WebtoonEmbeddingSetting` **opt-in**으로만.
- 전역 default를 ccip로 승격하는 것은 **(배치 백필 재개 시) 해당 웹툰 CCIP 앵커 시딩 이후**. 빈 CCIP 컬렉션 위 NEW_CHAR 양산 방지.

---

## 9. Step 3/4 — 에피소드 단위 2-pass 장면·화자·서사 분석 (구현 완료, Step4 흡수)

> **v3.3 갱신 (2026-07-01)**: 이 섹션은 `prd-step3.md`(재설계 working draft, 2026-06-29~30, 프로토타입 실험 포함)의 최종 합의 내용을 **전면 흡수**한 것이다 — 원 문서는 실험 로그·모델 A/B·인용 근거를 그대로 보존하는 이력 문서로 남기고, 여기서부터가 최신 스펙이다. **Step3(장면/화자)와 Step4(회차 요약)는 별도 4번째 파이프라인 단계가 아니라 하나의 에피소드 추론 단계로 통합**됐고, 아래 설계 그대로 `webtoon-pipeline/src/core/step3.py`에 구현되어 Temporal에 배선·운영 중이며, 골든 회귀 테스트로 핵심 동작이 고정됐다(§9.10).

### 9.0 왜 다시 설계했나
최초 설계(§9 구버전, 컷 단위 즉시 확정 + N-2/N-1/N forward-only 슬라이딩 윈도우)는 구조적 한계가 있었다: 이미지가 한 방향만 보고, 컷마다 즉시 DB 확정(`speaker_id`, `name_discoveries` confidence≥0.85 즉시 rename)해 "그 컷만 보고 내린 판단"이 영구 기록되며, 소급 수정 경로가 없었다("컷 50에서 이름이 밝혀져도 컷 10을 고칠 길이 없다"). "다음/다다음 컷 때문에 비로소 누구 대사인지 알 수 있는" 상황을 구조적으로 못 잡는 **아키텍처 문제**였다. 근거: manga/영상 이해 선행연구(Tails Tell Tales, Zero-Shot Character ID 등)는 공통적으로 **챕터/영상 전체 단위 전역 해소**를 쓴다 — 컷 즉시 확정이 아니라 **에피소드 단위 전역 해소**가 정석이라는 결론.

### 9.1 목표
1. **양방향 연속 이해** — 에피소드 전체를 보고 화자·이름을 해소(나중 컷의 단서를 앞 컷에 소급 반영).
2. **추정(provisional) → 확정(confirmed) 분리** — 컷별 결과는 provisional로 적재, 정해진 시점에 confirmed로 커밋.
3. **소구포인트 추출** — 에피소드/비트(연속 컷 묶음) 단위 핵심 훅을 구조화 산출.
4. **모델 가용성에 강건** — GLM(대용량 컨텍스트) 정상 경로 + 로컬 Qwen3-VL(`max_token=16384`) 폴백 모두에서 동작.
5. **Step3+Step4 통합** — 장면/화자 분석과 회차 요약을 하나의 에피소드 추론 단계로 합쳐 과분해(LLM 스테이지 난립)를 피한다.
- 비목표: Step1·2 로직 변경 없음. 70만 배치 백필 보류(§5). 풀 롱컨텍스트 단일 멀티모달 호출(에피소드 전체를 한 번에 보는 Pegasus형 방식)은 **북극성**으로만 보존 — GLM 비전 컨텍스트가 커지거나 전용 비디오 이해 API가 나오면 이행 후보.

### 9.2 모델 제약 & 컨텍스트 적응형 설계 (핵심 제약)
| 모델 | 모드 | 토큰 예산 | 비고 |
|---|---|---|---|
| GLM (텍스트 전용) | 텍스트 | ~131,072 | 이미지 없으면 큰 예산 — 에피소드 전역 해소(Pass-2a)에 충분 |
| GLM-4.6v / zai vision | 멀티모달 | 32,768 | 비전은 텍스트의 1/4 |
| Qwen3-VL-32B-fp8 (로컬) | 멀티모달 | 16,384 | 비전 폴백. GLM 비전의 절반 |
| 로컬 LLM | 텍스트 | 16,384 | 텍스트 폴백 |

- **이미지 토큰**: 컷 ~700×1600px 기준 컷당 ~1,300 비전 토큰. 단일 콜 멀티이미지는 여유(16K 로컬 ~8컷, 32K GLM-v ~20컷)지만, **에피소드 통째 이미지는 불가**(100~300컷×1.3k = 130k~390k ≫ 32k/16k) — 이미지 장수는 throughput/국소 시각맥락 레버일 뿐, **연속성 수단이 아니다**. 연속성은 이미지 없는 텍스트 Pass-2a가 담당(§9.3).
- **설계 원칙 — 윈도우 크기 = 모델 토큰 예산의 함수**: 에피소드 전역 해소를 "에피소드 통째 1콜"로 고정 가정하지 않는다. GLM 텍스트 모드면 윈도우=에피소드 전체(사실상 1콜), 로컬 16K면 여러 윈도우로 자동 분할하고 윈도우 경계에서 belief state를 캐리오버(§9.5)한다 — 같은 로직이 16K든 131K든 1급 경로로 동작(`resolve_episode_windowed`, task 6.2 구현).
- **양방향 전파를 16K에서도 보장하는 트릭**: 이름 확정(Pass-2a, LLM, 윈도우 전진)과 적용(Pass-2b, LLM 없음, 결정론)을 분리한다. 최종 이름 테이블이 확정되면 에피소드 전체 provisional 화자 참조를 단순 조인으로 일괄 재기록 — **소급(backward) 전파가 컨텍스트 한계와 무관한 공짜 결정론적 연산**이 된다.
- **모델 역할**: 주력=**GLM**(무제한 플랜, 비용 비제약 → 병목은 rate-limit/지연), rate-limit·장애 시 폴백=**Qwen3-VL-32B 로컬**. 전환은 `LLMModel`/`WebtoonLLMSetting`(§7) config로 — 코드 변경 없이 모델 교체.

### 9.3 아키텍처 — 2-pass 하이브리드 (채택안)
비전과 연속성을 분리하는 것이 핵심 원칙이다: **비전=컷 단위**(기본 오버레이 1장, 압축 텍스트 레코드 산출), **연속성=텍스트 Pass-2a**(이미지 없이 에피소드 전체를 봄). **LLM 스테이지는 2개**(Pass-1 비전 / Pass-2a 텍스트)로 한정 — Pass-2b는 LLM이 아니며(결정론), Step4·비트·소구포인트는 Pass-2a에 흡수해 과분해(지연 통제 실패)를 피한다.
```
Pass-1 (컷별, 멀티모달, provisional) ── OCR 1:1 교정 · type 분류 · 얼굴↔대사 후보 ·
   컷당 이미지 1장(오버레이)만            꼬리방향 힌트 · scene_meta · SFX→scene 흡수 ·
                                         prominence 힌트(엑스트라 판정용) → belief state 캐리오버
                                       ▼
Pass-2a (에피소드, 텍스트, 윈도우) ── 레코드만으로 전역 화자/이름 해소 + 비트 분절 +
   이미지 없음 → 130k 텍스트 마음껏         소구포인트 + (Step4 흡수) 요약/타임라인/떡밥
                                       ▼
Pass-2b (LLM 없음, 결정론적)       ── provisional → confirmed 일괄 커밋(양방향 전파) +
                                        EpisodeReport/EpisodeBeat/NarrativeThread/CharacterClaim 커밋
```
구현: `step3a_extract`(Pass-1) → `step3b_resolve`(Pass-2a) → `step3c_apply`(Pass-2b, `apply_resolution`), 모두 `webtoon-pipeline/src/core/step3.py`.

### 9.4 Pass-1 계약 — 컷별 추출 (멀티모달)
- 입력: **현재 컷 N 1장**(face bbox+라벨 OpenCV 오버레이) + 해당 컷 OCR 블록(`identified_faces` 포함). 이웃 컷 동봉은 연속성 목적으로 하지 않음(연속성은 Pass-2a 담당) — 국소 시각 단서가 필요하면 직전 컷 1장을 선택적으로만.
- 순서 강제: **분류 먼저 → 화자 나중** — 모든 블록에 `type`(speech/monologue/narration/system/other)+`type_confidence`를 먼저 매기고, 그다음 speech/monologue 블록에만 `speaker`(face_label/confidence/basis[tail|face|context|none]/tail_hint)를 귀속. 확신 없으면 낮은 confidence(과확신 금지), 모르면 `null`(지어내지 않음).
- 효과음/의성어: 별도 대사 레코드를 만들지 않고 `type=other`+`is_used=false`로 두며, *의미*는 `cut_summary`(scene_meta)로 흡수.
- 각 등장 인물에 `prominence`(main/minor/extra) 힌트+`emotion` 기록, 이름이 드러나면 `name_evidence`(face_label/name/confidence/evidence)로 기록.
- 출력은 **엄격 JSON**, `blocks`는 입력 OCR index와 **1:1**(병합·분할·생략·재번호 금지), **provisional**로 적재. **(v3.6 변경)** 화자 후보도 함께 영속한다: 얼굴 라벨 기반 화자(confidence≥0.5, face_label→character_id 매핑 성공)는 `speaker_id`로 저장하되 `resolution_status='unresolved'` 유지 — resolved 전이(확정)는 여전히 Pass-2b만. 종전의 "speaker_id 항상 NULL(belief로만 캐리오버)" 설계는 Pass-2a가 재출력하지 않은 확신 화자를 전부 유실시켜 화자 매칭률 1~2%의 주원인이었다. 빈 컷(텍스트도 얼굴도 없음)은 스킵/경량 처리 가능.
- **(v3.6)** `identified_faces`에 `confirmed`(=`face_record.is_confirmed`) 플래그 포함 — 프롬프트의 "human 확정은 진실로 동결" 지시가 실제로 작동하려면 모델 입력에 실려야 한다(종전엔 프롬프트만 있고 데이터가 없었음).
- 강건성: `max_tokens>=16384`(1536→4096→8192→16384. 추론형 모델 glm-4.6v가 답 이전에 `reasoning_content`로 예산을 먼저 소모 — 8192에서도 naver/820097 전 회차 기준 'length' 절단 7건 잔존해 2026-07-05 재상향), `raw_decode` 기반 강건 파싱(여분 텍스트/코드펜스 방어) + 1회 재시도, temperature 0.0~0.2.

```json
{
  "cut_summary": "<현재 컷 상황서술 1~2문장 (효과음 의미 흡수)>",
  "characters": [{"face_label": "F0", "prominence": "main|minor|extra", "emotion": "당황"}],
  "blocks": [
    {"index": 0, "type": "speech", "type_confidence": 0.9, "corrected_text": "도대체 이게 무슨...",
     "speaker": {"face_label": "F0", "name": null, "confidence": 0.6, "basis": "context", "tail_hint": "F0"}}
  ],
  "name_evidence": [{"face_label": "F0", "name": "철수", "confidence": 0.8, "evidence": "옆 인물이 호칭"}]
}
```

### 9.5 Pass-2a 계약 — 에피소드 전역 해소 (텍스트, 이미지 없음)
- 입력: 에피소드 **전체 Pass-1 레코드를 읽기순으로 한 번에**(컷별 cut_summary/blocks/name_evidence) + `character_roster` + **교차에피소드 확정 로스터 prior**(`confirmed_roster_prior` — 이전 화까지 확정된 character_id→이름, `narrative_context.load_prior`가 조립) + belief state.
- **belief state(누적 서사 컨텍스트)** — `prev_context` 문자열 한 줄을 대체하는 구조화 캐리오버: `character_roster`(등장 얼굴/캐릭터+알려진 이름), `pending_speakers`(미확정 화자 가설), `name_evidence`(face_id별 이름 단서 누적 투표). 윈도우 분할 시(§9.2) 경계 정보 손실 없이 캐리오버(`narrative_context.py`의 `load_prior`/`fold`로 웹툰 전역 영속).
- **비트/소구포인트 계층**: 연속 컷을 비트(같은 서사 목적을 공유하는 묶음, 개수 제약 없음 — 에피소드 전체가 1비트일 수도)로 분절하고 각 비트에 `hook_type`(**enum 미고정, free-form 텍스트** — 데이터가 쌓인 뒤 군집화로 어휘 도출), `appeal_point`, `intensity`(0~1)를 부여. 에피소드 단위로 bottom-up 종합해 `episode.appeal_point`+`cliffhanger`+`foreshadowing`도 산출 — 단위는 컷/비트/에피소드뿐 아니라 교차 에피소드(아크)까지 유연.
- **캐릭터 중요도 티어링**: `main/supporting`(풀 처리, 이름해소·로스터 반영) / `minor_functional`(기능 라벨 보존, 실명 추적 안 함) / `extra`(soft-exclude — `is_match_excluded=true`, 하드 삭제 금지·가역·human 동결 존중). Step2가 미매칭 얼굴마다 NEW_CHAR를 양산해 캐릭터 DB가 오염되는 문제를 새 LLM 콜 없이 Pass-2a 안에서 해결.
- 출력 계약: `characters[character_id, name, significance, name_confidence, evidence, label_conflict, merge_suggestion, profile{gender, age_group, affiliation, role, personality[], traits{}}]`(**v3.6** — profile은 인물도감용 범용 메타, 장르 특이값은 free-form traits, 근거 있는 항목만), `speaker_resolution[cut, block_index→character_id, confidence, reason]`, `beats[cut_start, cut_end, hook_type, appeal_point, intensity]`, `episode{summary, appeal_point, cliffhanger, foreshadowing}`, `deceptions[cut, character_id, claim, contradicts, confidence]`, `threads[description, type, status, planted/resolved episode·cut, confidence]`.
- **(v3.6) speaker_resolution은 "불확실 블록만"이 아니라 모든 speech/monologue의 전수 화자 테이블**: 블록의 `spk_face`/`spk_cid`(Pass-1 후보)가 맥락과 맞으면 확인(confirm), 다르면 교체(override), 진짜 판단 불가만 null. 종전 "provisional speaker가 null/불확실한 블록만" 지시는 확신 블록을 재출력하지 않게 만들어 Pass-2b가 커밋할 화자를 잃는 구조였다. Pass-2a 입력 faces에도 `confirmed` 플래그 포함(confirmed=true + prior는 동결·화자 판정에 신뢰).
- **(v3.6) deception 판정 규칙 강화**: "다른 인물을 속이려는 의도가 있는 speech"만 — monologue/혼잣말/자조/한탄/수사적 표현은 deception 아님(속일 상대 부재. ep2에서 독백 "운명의 신이 날 조롱하는 기분"이 deception으로 오판된 실사례). episode.summary/appeal_point는 narration·실제 컷 사건에만 근거, 근거 없는 낙인·평가어("분탕", "배신자" 등) 금지.

### 9.6 Pass-2b — 결정론적 커밋 & 소급 전파 (LLM 없음, Step4 흡수 지점)
- Pass-2a의 최종 이름/화자 테이블을 **LLM 호출 없이** 결정론적으로 에피소드 전체에 투영: `TextAnnotation.speaker`+`resolution_status=resolved` 커밋, 이름 테이블을 character_id 키로 전 컷에 **소급(backward) 투영**.
- **(v3.6) provisional 화자 승격 안전망**: Pass-2a가 명시 해소하지 않은 speech/monologue 중 Pass-1이 영속한 provisional `speaker_id` 보유 블록은 그 화자로 resolved 승격 — Pass-2a가 전수 테이블을 일부 빠뜨려도 얼굴 근거 화자가 유실되지 않는다.
- **(v3.6) 컷 분석 상태 마킹**: apply 성공 시 에피소드 컷 전체에 `llm_analyzed_at` 기록 + `is_stale=false` 해제. 종전엔 레거시 단일-pass 경로(죽은 코드)만 이 컬럼을 만져 2-pass 운영에서 분석 완료/stale 추적이 전무했다.
- **(v3.6) profile 커밋(과도기)**: characters.profile을 `Character.extra['llm_profile']`에 병합 커밋(스칼라 최신 우선/personality 합집합/traits 병합, is_confirmed 동결). **이 저장 위치는 과도기** — 출처(provenance: llm/human/human-edited) 구분이 필요해 별도 `CharacterProfile` 모델로 이행 예정(§14 오픈 퀘스천, 설계 논의 중).
- 같은 커밋 트랜잭션에서 `Character.name/significance`, `NameDiscoverySuggestion`, `EpisodeBeat`, **`EpisodeReport`**(summary/appeal_point/cliffhanger/foreshadowing/character_timeline), `NarrativeThread`, `CharacterClaim`(deceptions)을 함께 확정한다 — **즉 회차 요약(옛 "Step4")은 별도 실행 단계가 아니라 Pass-2b가 매 에피소드 처리마다 자동으로 산출·커밋**하는 부산물이다(`_commit_episode_report` 등, `apply_resolution` 내부).
- 멱등: 동일 결과 재적용 시 DB 불변. `human`/`is_confirmed` 값은 절대 덮어쓰지 않음(레이어 우선순위 `human > llm > paddle`, 동결 규칙).
- 커밋 후 `narrative_context.fold`로 `WebtoonNarrativeState`(로스터/미해결 떡밥/running 요약) 갱신 — 다음 에피소드 Pass-2a의 prior가 됨.

### 9.7 신뢰성 규칙 — mis-ID distrust / 텍스트 진실성 등급(책략 탐지) / 교차에피소드 prior
Step2 얼굴인식·Pass-1 단독 판단이 서사 결론으로 그대로 전파되지 않도록 세 규칙을 둔다(모두 프로토타입 실험으로 검증되고 골든 회귀 테스트로 고정 — §9.10):
- **mis-ID distrust**: `identified_faces`의 character_id/이름은 Step2 **추정값**(is_confirmed 아니면 정답 아님). 대사·호칭·맥락·prior와 모순되면(예: 죽은 인물이 다른 시대에 재등장) 오인식으로 의심하고 name은 대사 근거로 정한 뒤 `label_conflict`에 사유 기록. Step2 파생 이름의 confidence는 보수적으로. `is_confirmed`/human 라벨에는 절대 적용 안 함(동결).
- **텍스트 진실성 등급 (책략/거짓 탐지)**: narration/system=객관적 진실, monologue=인물의 진짜 의도, speech=거짓일 수 있는 주장으로 취급. speech 주장이 monologue·narration·확정정체성과 모순되면 `deceptions`(cut, character_id, claim, contradicts, confidence)에 명시 기록 — "주장 vs 진실 괴리를 적극 탐색"하도록 프롬프트에 명시해야 surfacing된다(토대만으론 자동으로 드러나지 않음, §9.10 ep3 청진 사례).
- **교차에피소드 정체성 prior**: Pass-2a는 이전 화 확정 character_id→이름(+핵심 사실)을 진실 기준선으로 입력받는다. prior에 있는 character_id는 확정 이름을 우선 적용하고, 동일 에피소드 내 모순 단서가 강할 때만 `label_conflict`로 이의 제기. 정체성은 에피소드 독립이 아니라 **webtoon 글로벌**로 다룬다(`WebtoonNarrativeState`).

### 9.8 정답 데이터(human/is_confirmed) 취급
1. **동결(필수)** — LLM이 절대 덮어쓰지 않음(레이어링 + Pass-2b 결정론 적용이 보장). 재-OCR/재명명 시키지 않아 일감·토큰 절약.
2. **고정 앵커 주입(관련 있을 때)** — 확정 얼굴/대사를 잠금 단서로 넣어 주변 모호 항목 해소 품질 향상.
3. **제외(무관할 때)** — 멀리 있는 확정/human은 뺀다(정확성 리스크 0, 최종값은 동결로 보존 — 비용은 주변 해소 품질 트레이드오프뿐). 노이즈 앵커는 attention을 흐리므로("lost in the middle") 토큰 여유와 무관하게 입력 큐레이션은 필요.

### 9.9 Temporal 오케스트레이션 매핑
`EpisodeChainWorkflow`의 step3 단계가 `step3a_extract`(STEP3_QUEUE, 컷 루프, 멀티모달, heartbeat, `start_to_close=2h`) → `step3b_resolve`(에피소드 텍스트 해소, 윈도우, `1h`) → `step3c_apply`(결정론 커밋, `15m`) 순으로 실행된다. 단계 간 데이터는 activity 반환값/입력으로 스레딩(step3a의 `ExtractResult` dict → step3b 입력, step3b의 `ResolveResult` dict → step3c 입력). `phase3_enabled` 게이트, STEP3_QUEUE 동시성 1(두 에피소드의 step3가 동시에 돌지 않음, LLM 스테이지 전체에 걸쳐 유지) 그대로.

### 9.10 구현·검증 상태 (2026-07-01)
- **구현 완료**: service 스키마(§7 신규 테이블 전부), 코어(`extract_cut`/`extract_episode`/`resolve_episode`/`resolve_episode_windowed`/`apply_resolution`), `narrative_context.py`(`load_prior`/`fold`), Temporal 배선(§9.9), 토큰 로깅(`LLMUsage`), 재처리 경로(§11.2)까지 전부 코드화되어 운영 중.
- **골든 회귀 테스트 3종 통과**(오프라인, 실제 LLM 호출 없이 프로토타입 산출 픽스처로 고정):
  - `tests/test_step3_distrust_regression.py` — ep2 "천마→운암" mis-ID distrust(§9.7).
  - `tests/test_step3_deception_regression.py` — ep3 "청진 후손" 책략 탐지, Property 10(허구 아닌 실제 근거 참조).
  - `tests/test_step3_prior_identity_regression.py` — ep3 교차에피소드 정체성 prior(418=청명 유지 + 청진 분리).
- **모델 A/B 결론**(`qwen-vl/_vltest.py`·`_pass1.py`·`_pass2.py` 하니스, naver 769209 실측): Pass-1 엔진은 **GLM-4.6v 우선**(OCR region 1:1 바인딩 엄수, Pass-2 입력 토큰 1/4, 40% 빠름). Qwen3-VL-32B는 1:1 바인딩을 깨고(예: 4블록→1병합) 4배 verbose하지만 JSON 에러 0 — **견고한 폴백**으로 채택. Pass-2a 다운스트림 품질(이름/아크/appeal/cliffhanger)은 두 엔진 동급.
- 전체 실험 로그·인용 근거·결정 이력은 `prd-step3.md`에 그대로 보존(이 문서가 최신 스펙, `prd-step3.md`는 이력).

### 9.11 재처리 (에피소드 단위 재설계)
전역 해소라 **재해소 단위는 컷이 아니라 에피소드**다 — human 수정 1건이 에피소드 전체 화자/소구포인트 해소를 바꿀 수 있다. 상세는 §11.2.

### 9.12 오픈 리스크
1. GLM 텍스트 `max_tokens`(131072)가 입력 컨텍스트인지 출력 상한인지 미확정 — 실측 필요(설계는 이 숫자에 의존하지 않게 만들어짐).
2. 로컬 LLM(16K, 윈도우 분할) 해소 정확도가 GLM 대비 얼마나 낮은지 미측정.
3. belief state 직렬화 크기 — 긴 에피소드에서 roster/pending이 윈도우 예산을 먹지 않도록 압축 규칙 필요.
4. 소구포인트 주관성 — 정답 없는 추출이라 human 검토 큐로 품질 감 잡기, 장르별 `hook_type` 보정 필요.
5. 비트 경계 안정성 — 재처리 시 비트 분절이 흔들리면 소구포인트 ID가 불안정(안정적 키 `_beat_stable_key`로 완화했으나 근본 해결은 아님).
6. 100만 컷 백필 시 진짜 병목은 비용이 아니라 rate-limit/동시성(GLM 무제한 플랜) — 활성 웹툰만+증분 처리로 현재는 회피 중.

---

## 10. Chroma 벡터 DB

- **웹툰별 독립 컬렉션** `character_faces_{webtoon_id}[_{model}]`, `hnsw:space=cosine`. 60만 규모에서 metadata 필터 후처리 성능 저하 회피 + 웹툰별 drop/재생성 가능.
- doc_id `{webtoon_id}_{episode}_{cut}_F{face_idx}` 고정 + `upsert` 멱등.
- 배포: `oci-croma.prup.xyz:8000`(운영, OCI 호스팅 — k3s 클러스터 밖) / docker-compose(개발). 토큰 인증. env `CHROMA_HOST/PORT/AUTH_TOKEN`.
- metadata: webtoon_id, episode, cut, face_idx, character_id, appearance_id, appearance_label, character_name, is_confirmed, bbox, conf, created_at.
- **⚠️ v1 REST API는 서버에서 완전히 제거됨(2026-07 실측 확인, `HTTP 410 "The v1 API is deprecated. Please use /v2 apis"`)**. `data-pipeline`은 공식 `chromadb==1.5.9` 클라이언트로 이미 v2(tenant/database 경로, `default_tenant`/`default_database`)를 쓰지만, `service` 쪽 수기 REST 호출은 v1을 쓰고 있었다가 이번에 v2로 이관(§16.4). **앞으로 Chroma REST를 직접 호출하는 코드를 새로 짤 때는 반드시 v2(`/api/v2/tenants/{tenant}/databases/{database}/collections/...`)를 쓸 것** — v1은 404가 아니라 410을 반환하므로 "존재 안 함"과 오인하기 쉽다.

---

## 11. Human-in-the-loop

### 11.1 Human Checkpoint
`WebtoonPipelineState.phase2_processable_max_episode`로 Step2 처리 범위를 ep 번호로 제어(`null`=전체, `10`=10화까지 후 idle). 도달 시 다음 이벤트 미발행 → 자연 대기. 검토 후 값 올려 resume. (Temporal에선 Schedule/signal 또는 워크플로 가드로 동일 구현.)

### 11.2 재처리 (Human Correction → 일괄 재분석, **에피소드 단위로 재설계됨 — §9.11**)
- Step3가 컷 즉시확정에서 에피소드 전역 해소로 바뀌면서 **재해소 단위도 컷이 아니라 에피소드**다 — human 수정 1건이 에피소드 전체 화자/소구포인트 해소를 바꿀 수 있기 때문(구버전 §11.2의 "컷 단위 short-circuit"은 이 아키텍처와 맞지 않아 폐기).
- human 컷 수정 → 해당 에피소드 `is_stale=True` → `reresolve_episode`(step3b+3c 재실행, `resolve_episode`/`apply_resolution` 재호출)로 에피소드 전체 재해소.
- **(v3.6) stale 마킹 배선 완료**: service의 얼굴 단건 재배정(`FaceRecordReassignAPIView`)/일괄 재배정(`FaceRecordBulkReassignAPIView`)/텍스트 어노테이션(`TextRegionAnnotateAPIView`) API가 `webtoon_cut.is_stale=true`+`human_modified_at`을 마킹한다(종전엔 human이 고쳐도 파이프라인에 신호가 없었음). 실행은 `python -m src.tools.reresolve <source> <title_id> <ep_no|stale> [--rerun-extract]` CLI — **얼굴↔캐릭터 매칭을 고친 경우 `--rerun-extract` 필수**(identified_faces 입력 자체가 바뀜). Temporal 자동 트리거는 미구현(오픈).
- **부분 재처리**: 이름 테이블만 바뀐 경우(예: NameDiscoverySuggestion 수락)는 Pass-2a(LLM)를 다시 돌리지 않고 `reapply_episode`(step3c만 재실행, `apply_resolution`)로 **LLM 없이 결정론적으로 일괄 재적용** 가능 — Pass-2b가 결정론적이라 값싸다(§9.6).
- confidence 게이팅: 저신뢰 type/speaker/name은 provisional 유지. 자동 이름/중요도/병합은 제안만(자동 수행 금지), `human`/`is_confirmed`는 항상 동결.

### 11.3 이름 자동 확정
§9.5(name_evidence 누적) 참조. 주요 캐릭터는 여러 컷 증거가 쌓이며 `NameDiscoverySuggestion`으로 확정 제안(human은 confirm만), 조연은 제안 큐, 단역(`significance=extra`)은 soft-exclude 유지.

---

## 12. webtoonmoa 기능 요구사항

| 기능 | 우선순위 | 데이터 소스 |
|---|---|---|
| 캐릭터 관리(face 라벨링, web_manager 대체) | P1 | FaceRecord, Character, Chroma |
| 텍스트 제외 관리 | P1 | TextRegion.is_excluded |
| 수정 후 일괄 재분석 트리거 | P1 | WebtoonCut.is_stale |
| 파이프라인 관리(checkpoint/resume) | P1 | WebtoonPipelineState |
| 이름 확정 제안 | P1 | NameDiscoverySuggestion |
| 캐릭터 프로필 페이지 | P1 | Character + CharacterAppearance |
| 에피소드 요약 표시 | P1 | EpisodeReport(summary/appeal_point/cliffhanger/character_timeline) |
| 소구포인트/비트 하이라이트 | P1 | EpisodeBeat(hook_type/appeal_point/intensity) |
| mis-ID/책략 검토 큐(label_conflict, deceptions) | P1 | Character.label_conflict, CharacterClaim |
| 떡밥 추적(심음/회수) | P2 | NarrativeThread |
| 대사 검색 | P2 | TextAnnotation 전문검색(pg_trgm/to_tsvector) |
| 전체 줄거리 요약(아크 단위) | P2 | StoryArc |
| AI 채팅 도우미 | P2 | 구조화 DB 쿼리 + LLM(모델 미정) |

API base `/v1/toon/webtoon/`, source 필드(kakao/naver)로 통합. `imageBaseForSource(source)`.

---

## 13. 마이그레이션 / 롤맵

### 완료 (✅)
- Django 스키마, Step1(OCR/YOLO) + Step2(face_identify) 처리 로직, 에피소드 게이팅(EpisodePipelineProgress).
- 이중 임베딩 제거(임베딩+매칭 1패스).
- model-api 모드 분리(clip/ccip) + CCIP 엔드포인트 + OCR/YOLO 엔드포인트 분리(v3.0, 이후 별도 GPU 서버로 이전).
- EmbeddingModel/WebtoonEmbeddingSetting + model_resolver/metric 분기(기본 CLIP 유지).
- LLM 스키마 반영(rename + LLMModel/WebtoonLLMSetting/llm_model), CutSceneMeta 마이그레이션(0009/0010).
- **Faust→Temporal 전면 이관(v3.0, v3.2 시점 운영 확인)**: Faust/Kafka 완전 제거(`service`의 `config/kafka.py` 삭제, `proxmox-configuration` configmap에 Kafka 설정 없음). `webtoon-pipeline`은 k3s에 Temporal 워커로 배포·운영 중, `proxmox-configuration/temporal_repo`에 Temporal 서버 배포됨. `service`는 `config/temporal.py`의 `send_phase1_trigger`로 웹툰당 1회 kick.
- **Step3+4 재설계·구현 완료(v3.3, `episode-scene-resolution` 스펙 — 2026-07-01 확인)**: `prd-step3.md`의 에피소드 단위 2-pass(extract→resolve→apply)가 서비스 스키마(§7 신규 8개 테이블) + 코어(`core/step3.py`, `narrative_context.py`) + Temporal 배선(step3a/b/c)까지 전부 구현되어 운영 중. Step4(회차 요약)는 Pass-2b에 흡수돼 `EpisodeReport`로 자동 산출(§9.6). mis-ID distrust(12.1)/책략 탐지(12.2)/교차에피소드 prior(12.3) 골든 회귀 테스트 3종 작성·통과. 부수적으로 `tests/test_workflow_orchestration.py`가 옛 `step3_episode` 단일 액티비티 스텁에 멈춰 있어 깨져 있던 걸 step3a/b/c 3-스텁으로 갱신해 수정.

### 진행/예정
| 단계 | 작업 | 상태 |
|---|---|---|
| T3 | chroma_sync/rematch/reembed → Temporal 워크플로/signal 패리티 (Faust 에이전트 삭제분 재구현) | 🔲 미확인 |
| L3 | webtoonmoa 관리 UI(§12 신규 행: 소구포인트/mis-ID·책략 검토 큐/떡밥 추적 화면) | 🔲 |
| E1 | CCIP opt-in 웹툰 검증(precision/recall), threshold 보정 | 🔲 |
| S1 | Step3 오픈 리스크 실측(§9.12: GLM 토큰 예산 의미, 로컬 16K 품질 격차, belief state 압축, 비트 경계 안정성) | 🔲 |
| B1 | (보류) operator 라이브러리화 마무리 + 70만 배치 백필 | ⏸ |
| R1 | model-api 라우터(`ocr`/`yolo`/`ocr_yolo`/`embed`/`embed_ccip`) `async def` 안 동기 CPU-bound 추론 블로킹 이벤트루프 수정(`run_in_threadpool`/`asyncio.to_thread`) + `HF_HUB_OFFLINE=1` 추가(§16.2) | ✅ `run_in_threadpool` 수정 완료 / 🔲 `HF_HUB_OFFLINE=1` 미적용 |
| R2 | `naver/820097` ep2 재실행 end-to-end 검증(§16.5) — 이번 세션 수정 사항이 실전에서 통하는지 확인 | ✅ 완료(§16.6) — ep2 step1→2→3 전 구간 정상 완료 확인, 이후 ep10/ep11도 진행 중 |
| R3 | Step2 자기-런 스냅샷 버그(§16.6) / Step3 워커 액티비티 미등록 버그(§16.7) / narrative fold 캐시 불일치 버그(§16.8) | ✅ 완료 |
| R4 | vllm 502/530 재시도, Pass-1 병렬화, max_tokens 절단 수정, 프롬프트 한국어 강제, LLM 동시성 config화(§16.9) | ✅ 완료 — 단, `LLM_MAX_CONCURRENCY`를 1보다 올려도 되는지(vllm 서버가 실제로 동시 요청을 받아줄 수 있는지)는 미실측 |
| R5 | `step3_episode`(+`analyze_cut_scene`/`analyze_episode_scenes`/`analyze_episode_scenes_by`) 죽은 코드 정리 여부 결정(§16.7) | 🔲 발견만 됨, 삭제 여부 미결정 |

> Temporal 전환, Step3+4 통합 구현, 골든 회귀 테스트, `naver/820097` end-to-end 검증까지 핵심 라인은 이미 끝났다. 남은 T3/L3/E1/S1/R1(HF_HUB_OFFLINE)/R5는 모두 후순위 정리 작업.

---

## 14. 오픈 퀘스천 / 리스크

> Step3/4(LLM 2-pass) 고유 리스크는 §9.12에 모아뒀다(GLM 토큰 예산 실측, 로컬 16K 품질 격차, belief state 압축, 비트 경계 안정성 등). 아래는 그 외 인프라/매칭 리스크.

1. **Temporal 운영 부담**: 자가호스팅(k3s) Temporal 서버(+Postgres) vs Temporal Cloud. 운영 최소화 시 Postgres durable 큐 대안 검토.
   > **결론 (2026-06-21, 리소스 평가)**: 이 워크로드(일 ~1000컷 = 분당 이벤트 수개)에선 **안 무겁다**. "Temporal 무겁다" 평판은 대부분 Elasticsearch(고급 visibility) + 풀 HA 때문 → **둘 다 불필요**. 최소 구성:
   > - Temporal 서버(단일 바이너리) ~0.5~1.5Gi / CPU 거의 idle, **ES 미사용**(Postgres visibility), **기존 Postgres에 `temporal` DB 추가**, retention 3~7일(continue-as-new로 DB 증가량 미미).
   > - 배치 노드 `k3s-super-worker-01`(AMD 5825U 8C/16T, 60GB): 진짜 제약은 **CPU**(ollama 버스트 8 + model-api paddle/torch). **메모리는 60GB로 여유**. Temporal 추가분은 메모리 +1~1.5Gi(무시 가능)·CPU 거의 0이고, **Faust 런타임 제거분이 Temporal 워커로 상쇄**돼 순증가 미미. CPU 병목 주범은 여전히 model-api 추론.
   > - 운영 부담을 더 줄이려면 Postgres durable 큐 대안도 가능하나, ES 없는 self-host면 차이가 크지 않음.
2. **CCIP 코사인 prefilter 유효성**(대규모 2단계 매칭 recall@K) — 미검증.
3. **증분 CCIP on CPU 지연**: 일 1000컷 CCIP feature 추출 k3s CPU 감당 여부. 과하면 증분=CLIP/배치=CCIP.
4. **threshold 환경 차이**: 0.16은 오프라인 클러스터링 기준 → 증분 1-NN 재검증 필요.
5. **GLM rate limit**: z.ai 동시 호출 상한 실측 후 Step3 워커/activity 동시성 결정(§9.12-6, 100만 컷 백필 시 진짜 병목).
6. **CharacterAppearance 자동 분리 기준**: 자동 감지 vs 항상 human 병합.
7. **배치 vs 증분 threshold/방식 정합**(보류 항목, 백필 재개 시).
8. **AI 챗 LLM 선택**(GLM vs ollama vs 기타) — P2.
9. ~~(v3.6) `CharacterProfile` 모델 설계~~ → **§17.2로 종결**(source 레이어링 (a)안 채택). 원 논의: `CharacterProfile` 모델 설계 — 인물도감 메타의 정식 저장처. `extra['llm_profile']`은 출처 구분이 안 됨. 방향(사용자 결정): 신규 모델 + "llm이 넣었나/사람이 넣었나/llm 것을 사람이 수정했나" 구분. 후보안: (a) `TextAnnotation`과 동일한 **source 레이어링**(character FK + source `llm|human`, unique(character,source), 서빙 시 human 필드 우선 병합 — 기존 관용구와 일치, LLM 재실행이 human 행을 절대 안 건드림), (b) 단일 행 + status enum(`llm|human|human_edited`) — 필드 일부만 수정한 경우 표현 불가, (c) 단일 행 + 필드별 provenance jsonb — 유연하나 복잡. **구현 전 논의 필요.**
10. ~~(v3.6) 모델 인벤토리 정리~~ → **§17.1/§17.3으로 종결**(AnalysisRun 도출 + 폐기 목록). 원 논의: 모델 인벤토리 정리 — PRD 개정을 거듭하며 모델이 누적돼 정리가 안 된 상태(사용자 지적). 현황: `StoryArc`는 읽기(load_prior 압축)만 있고 **생산자가 없어 항상 빈 테이블**, 파이프라인 진행도는 `EpisodePipelineProgress`(phase1만 채택)/`WebtoonPipelineState` 카운터/`webtoon_cut.llm_analyzed_at`(v3.6에야 기록 시작)로 **3원화**, `character.extra['name_suggestions']`는 죽은 레거시 경로(R5)와 `NameDiscoverySuggestion` 병존. Temporal이 durable 상태머신 정본이므로 DB 진행도는 조회용 파생으로 최소화할지 등 **존폐/통합 논의 필요.**
11. ~~(v3.6) `WebtoonNarrativeState` 캐시 존폐~~ → **§17.3으로 종결**(폐기, prior는 정본 조인). 원 논의: 캐시 존폐 — roster/threads는 이미 정본 테이블에서 load_prior가 직접 읽고, 캐시 고유 가치는 running_summary와 key_facts뿐. running_summary를 "최근 N화 episode_report.summary 조인"으로 대체하면 **캐시 테이블 자체를 제거**할 수도 있음(row 단위 기록 선호 방향과 일치). 슬림화(v3.6)로 급한 불은 껐고, 제거는 별도 논의.

---

## 15. 결정 로그 (통합)

| 날짜 | 결정 | 비고 |
|------|------|------|
| 2026-07-05 | **v4.0 설계 확정(§17)** — "분석 데이터는 전량 폐기·재생성 가능, human 노동분만 불멸" 전제(사용자 결정)로 분석 도메인 재설계: ①제자리 멱등 갱신 → **AnalysisRun 단위 쓰고 버리기**(진행도/stale 플래그는 저장 않고 도출), ②`character.kind(cluster\|character)` 판별자(NEW_CHAR 관습 폐기), ③얼굴 레이어링 `face_detection`+`face_identity`(human>step2), ④`character_profile`(source `llm\|human` 레이어링, 필드 단위 human 우선 병합), ⑤`suggestion` 통합 검토 큐, ⑥LLM 스테이지 **V→R→N→apply**(+주기 A로 story_arc 부활, 에피소드당 LLM 3콜 상한), ⑦summary/teaser 분리+데이터 기반 스포 차단, ⑧`webtoon_narrative_state`/진행도 3원화/`name_discovery_suggestion` 폐기. 앙상블·Graph DB는 보류. §14-9~11은 이 결정으로 종결 | §17 |
| 2026-07-05 | **v3.6 — 화자 매칭 구조 결함 수정(Pass-1 화자 영속 + Pass-2a 전수 테이블 + Pass-2b 승격 안전망), confirmed 플래그 모델 입력 배선, 인물도감 profile(extra['llm_profile']), HITL stale 마킹(service 3개 API) + `src.tools.reresolve` CLI, narrative 캐시 슬림화, max_tokens 16384 재상향, deception/요약 프롬프트 보정** — naver/820097 전 30회차 화자 부착률 0~7% 실측이 계기 | §7,§9,§11,§16 헤더 v3.6 |
| 2026-07-04 | **v3.5 — `naver/820097` end-to-end 검증 + 회귀 버그 3건(Step2 자기-런 스냅샷/Step3 워커 미등록/narrative fold 캐시 불일치) 발견·수정 + Step3 신뢰성·품질 개선**: ep2 재실행으로 §16.3/16.4 수정이 실제로 통함을 확인(R2 종결). 그 과정에서 새 버그 3건 발견·수정(§16.6~16.8). Step4 별도 구현 계획은 철회(이미 Pass-2b에 흡수됨을 재확인). vllm 502/530 재시도, Pass-1 병렬화, max_tokens 절단 수정(4096→8192), 프롬프트 한국어 강제, `LLM_MAX_CONCURRENCY`/`PASS1_WORKERS` config화(§16.9) | §9.4,§13,§16 |
| 2026-07-03 | **v3.4 — 홈랩 배포 환경 신뢰성 장애 대응**: 신규 §16에 사용자 배포 환경(k3s 홈랩+Cloudflare Tunnel+불안정 홈 네트워크)과 실제 장애 3건(Step1 `resolution_status` NOT NULL, Step1 재시도 비멱등성, Step2/Chroma/DB 정합성 드리프트) 원인·수정·검증 상태 기록. `service`의 Chroma REST 호출이 서버에서 제거된 v1 API를 쓰고 있던 것 발견해 v2로 이관(§10, §16.4). model-api 라우터의 async/blocking 구조적 리스크는 **발견만 하고 미수정**(§16.2, R1) | §10,§13,§16 |
| 2026-07-01 | **v3.3 — `prd-step3.md` 전면 흡수 + v3.2 정정**: §9를 에피소드 단위 2-pass(Pass-1/2a/2b) 설계로 전면 교체. **v3.2에서 "Step4 미착수"라 적었던 것을 정정** — 실제로는 Step3+4가 하나로 통합돼 Pass-2b가 매 에피소드마다 `EpisodeReport`를 자동 산출하므로 Step4는 이미 구현·운영 중이다(`episode-summary/main.py`는 통합 이전 레거시 실험 스크립트). 신규 스키마 8종(§7) 반영, 골든 회귀 테스트 3종(mis-ID distrust/책략 탐지/교차에피소드 prior) 통과 확인, `test_workflow_orchestration.py`의 stale 스텁 버그 수정 | §7,§9,§11,§12,§13 |
| 2026-06-29~30 | `prd-step3.md` 자체 결정 로그(발췌, 전문은 원 문서): 2-pass 하이브리드 채택(옵션 C) · 해소 윈도우=모델 토큰 예산의 함수 · 양방향 전파=Pass-2a(이름확정)+Pass-2b(결정론 소급) 분리 · 비전≠연속성(오버레이 1장 한정, 연속성은 텍스트 Pass-2a) · `hook_type` free-form + 비트 개수 제약 없음 · 효과음→scene_meta 흡수 · 캐릭터 중요도 티어링+extra soft-exclude · Pass-1 엔진 A/B(GLM 우선, Qwen 폴백) · mis-ID distrust/책략 탐지/교차에피소드 prior 프로토타입 검증 성공 | §9, `prd-step3.md` §12 |
| 2026-07-01 | **v3.2 — 실제 코드/배포 상태로 정정**: 레포를 4개(webtoonmoa 분리)로 명시, Faust→Temporal 전환 완료 반영, Step3 구현 완료(설계는 `prd-step3.md` 우선) 반영 — **Step4 "미착수" 판단은 위 v3.3에서 정정됨** | §2,§4,§9,§13 |
| 2026-06-28 | **v3.1 — TextBlockType 개편(A)**: 독백(monologue) 추가, 효과음(sfx) type 제거(→other+soft-exclude), 상황 서술은 CutSceneMeta 레이어로 분리. 엑스트라/효과음 정리 정책(B, Human/VL 판정)은 보류 | §7,§9.4 |
| 2026-06-21 | **v3.0 통합** — 3개 PRD 병합. ①Faust→Temporal 피벗 ②model-api OCR/YOLO 분리 ③LLM 1급 섹션화(GLM, 미구현) ④배치 백필 보류 ⑤3-repo 책임 정리 | §2,§4,§6,§9 |
| 2026-06-15 | prd-renew v1.4 — model-api 모드 분리(D5a①) + CCIP 엔드포인트, EmbeddingModel/WebtoonEmbeddingSetting(clip default 유지), model_resolver/metric 분기 | (archive) |
| 2026-06-15 | prd-renew v1.3 — glm→llm 일반화 + LLMModel/WebtoonLLMSetting/llm_model | (archive) |
| 2026-06-15 | prd-renew v1.2 — EpisodePipelineProgress(phase 값) 도입 | (archive) |
| 2026-06-14~15 | prd-renew v1~1.1 — 실행 컨텍스트 2분할, 이중 임베딩 제거, 에피소드 게이팅, 3-writer 동시성 불변식 | (archive) |
| 2026-05-28 | prd v2.0 — MATCH_THRESHOLD 0.25, Step2 멱등성, overlay 스타일, is_name_auto_assigned, 404 vs 5xx | (archive) |
| ~2026-05-27 | prd v1.x — Faust/Kafka 아키텍처, Chroma 멀티테넌시, Human Checkpoint, name_discoveries | (archive) |

> 상세 이력·코드 스니펫은 `docs/archive/`의 원본 3개 문서 참조.

---

## 16. 인프라 환경 & 신뢰성(Reliability) — 홈랩 배포 특성과 2026-07-03 장애 대응

> 이 섹션은 세션 종료 후에도 새 세션/에이전트가 "왜 이런 재시도·멱등성 설계가 들어갔는지" 맥락을 잃지 않도록 남긴다. 코드 조각이 아니라 **배경(왜)과 현재 상태(뭐가 됐고 뭐가 안 됐는지)**에 집중.

### 16.1 배포 환경 — 홈랩 특성 (신뢰성 설계 전제)
- 전체 워크로드(§2.4 `proxmox-configuration`)가 **사용자의 홈랩 k3s 클러스터**에서 구동된다. 클라우드 매니지드 인프라가 아니다.
- 외부에 노출되는 도메인은 전부 **Cloudflare Tunnel**을 경유한다(직접 공인 IP 아님) — 그래서 일반적인 5xx 외에 **Cloudflare 고유 에러(520~526, 특히 522 Connection timed out)**도 발생한다. 숫자상 전부 5xx라 "상태코드 >= 500이면 재시도" 규칙 하나로 같이 걸러진다.
- **홈 네트워크 회선 자체도 가끔 불안정**(사용자 확인, 2026-07-03) — Cloudflare Tunnel을 거치는 것과 별개로 근본적인 원인. 순수 5xx 상태코드 응답이 아니라 **커넥션 자체가 끊기는 경우**(`httpx.ConnectError`/`ReadError`/`RemoteProtocolError`/타임아웃)도 정상적으로 발생한다고 가정해야 한다 — HTTP 상태코드 체크만으로는 부족하고 커넥션 레벨 예외도 재시도 대상에 포함해야 함.
- 외부 인프라 위치:
  - GPU 서버(OCR/YOLO 추론): `gpgpu.prup.xyz` — 홈랩 밖(§5 배치 백필 노드와는 별개).
  - Chroma: `oci-croma.prup.xyz:8000` — **OCI(Oracle Cloud Infra)에 별도 호스팅, k3s 클러스터 밖**(§10).
  - 나머지(Temporal 서버, model-api clip/ccip, Django/Celery, Postgres 등)는 k3s 안(`proxmox-configuration`).
- **PaddleOCR 주기적 재시작**: 사용자가 메모리 누수 완화를 위해 의도적으로 구성(model-api `--max-requests`/`--max-requests-jitter`, `model-api/Dockerfile:69-77`). 재시작 윈도우 동안 OCR 호출이 일시적으로 502를 반환하는 것은 **의도된 정상 동작**이지 버그가 아니다 — 다만 이게 Step1의 재시도 트리거가 되므로 재시도 경로 자체는 견고해야 한다(§16.3).
- **결론**: 이 프로젝트의 재시도/타임아웃/멱등성 설계는 전부 "가끔 502/522/커넥션에러가 나는 게 정상"이라는 전제 위에 있다. 새로 짜는 외부 호출 코드는 기본적으로 재시도+백오프를 갖춰야 한다(예외: 진짜 버그를 감추면 안 되는 4xx).

### 16.2 model-api 구조적 리스크 — 핵심 수정 완료, `HF_HUB_OFFLINE`만 남음 (§13 R1)
- `model-api/src/routers/{ocr,yolo,ocr_yolo,embed,embed_ccip}.py` **전부** 라우터 핸들러가 `async def`인데, 그 안에서 동기(blocking) CPU-bound 모델 추론(`extract_ccip_feature`, PaddleOCR, YOLO 등)을 **`await`/스레드 오프로드 없이 직접 호출**한다. 예:
  ```python
  # model-api/src/routers/embed_ccip.py
  @router.post("/embed-ccip")
  async def embed_ccip(file: UploadFile):
      image_bytes = await file.read()
      feature = extract_ccip_feature(image_bytes)   # 동기 ONNX 추론 — 이벤트루프 블로킹
      return {"feature": feature}
  ```
  `UvicornWorker`는 이벤트루프 기반이라 이 호출이 도는 동안 **그 워커 전체가 다른 요청도, gunicorn 마스터에 대한 생존 응답도 못 한다**.
- **실측 사고(2026-07-03 01:34)**: embed-ccip 워커 2개(pid 7, 8)가 거의 동시에 `WORKER TIMEOUT`(gunicorn `--timeout=120`, `Dockerfile:74`) → `SIGKILL`/`SIGABRT`로 죽고 재시작. 직전 로그에 `step2`가 짧은 시간에 요청을 몰아 보낸 정황(`임베딩 진행 32/42`) — 부하가 몰리면 이벤트루프가 120초 넘게 막혀 재현 가능한 구조.
- **의심되지만 미검증**: 이 세션 맨 처음 진단했던 OCR 502(`gpgpu.prup.xyz`)도 "PaddleOCR 주기 재시작"만이 아니라 이 구조적 블로킹 문제가 섞여 있을 가능성 — `ocr.py`/`yolo.py`/`ocr_yolo.py`도 동일 패턴이기 때문. 확인 안 됨.
- **`HF_HUB_OFFLINE` 미설정**: CCIP 모델 가중치 자체는 Docker 이미지에 baked-in(`Dockerfile:24,45,56`, `HF_HOME=/project/hf_cache`)이라 **재다운로드는 안 되지만**, 워커 재시작마다 huggingface_hub가 캐시 최신 여부를 huggingface.co에 확인하는 네트워크 왕복(`GET .../api/...`, `HEAD .../model_feat.onnx`)이 매번 발생 — 불필요한 외부 의존/지연. `HF_HUB_OFFLINE=1` 추가로 제거 가능(미적용).
- **수정 완료(2026-07-03)**: `model-api/src/routers/{ocr,yolo,ocr_yolo,embed,embed_ccip}.py` 전 라우터의 동기 CPU-bound 호출(`run_ocr`/`detect_faces`/`extract_embedding`/`extract_ccip_feature`/`compare_features`)을 `starlette.concurrency.run_in_threadpool`로 감싸 이벤트루프 블로킹 제거. 함께, 클라이언트 쪽 동시 요청 수도 서버 워커 수(gunicorn `--workers=2`)에 맞춰 제한 — `data-pipeline/webtoon-pipeline/src/core/step2.py::_EMBED_WORKERS`를 8→2로 축소(대기열이 쌓여 `--timeout=120`에 걸리는 상황 자체를 줄임). **미검증**: 실제 부하(step2 재실행)로 WORKER TIMEOUT 재발 여부 확인 안 됨 — §16.5/R2 재실행 시 같이 지켜볼 것.
- **남은 항목**: `HF_HUB_OFFLINE` 미설정(위 항목)은 이번엔 손대지 않음. 라우터 수정으로 이벤트루프 블로킹은 해소됐지만, 그 자체가 워커 재시작 빈도를 낮추므로 불필요한 huggingface.co 왕복 빈도도 간접적으로는 줄어들 것 — 그래도 근본 해결은 아니므로 별도 작업으로 남겨둠.

### 16.3 Step1(OCR/YOLO) 신뢰성 강화 — 이번 세션에서 완료
`webtoon-pipeline/src/core/step1.py` / `src/temporal/activities.py` / `src/operators/ocr_yolo_client.py`.

**발견된 버그 2건(둘 다 수정 완료)**:
1. **`text_annotation.resolution_status` NOT NULL 위반**: Step3 작업 때 `service` 마이그레이션 `0017_character_significance_and_more.py`로 이 컬럼이 추가됐는데(Django `default=`는 DB 레벨 기본값이 아님, `db_default` 아님 — 앱 레벨에서만 채워짐), Step1의 raw SQL INSERT(`_process_segment_ocr`)와 Step3의 레거시 단일-pass 경로(`_upsert_llm_annotation`, 현재 프로덕션 워크플로는 안 씀 — `step3a/b/c` 2-pass가 정식 경로, 이건 백필/단독실행용으로만 남아있는 액티비티)가 이 컬럼을 안 채워서 터짐. → 둘 다 명시적 값(`'unresolved'`/`'resolved'`) 지정하도록 수정.
2. **`step1_episode` Temporal 액티비티 재시도 비멱등성**: 세그먼트 단위로 즉시 커밋(`with db_cursor()`)하고 재시도 시 처음부터 다시 도는데, `prepare_episode`(기존 데이터 정리)는 워크플로 시작 시 딱 1번만 실행됨 → 어떤 이유로든(§16.1의 502/522/커넥션에러, 또는 §16.2의 gunicorn WORKER TIMEOUT) 도중 실패해 Temporal이 재시도하면, 이미 커밋된 `text_region`을 다시 INSERT 시도 → `uniq_text_region_cut_index` UniqueViolation. 실제로 이 순서로 재현됨: attempt N이 §16.1/16.2 원인으로 중간 실패 → attempt N+1이 세그먼트 0부터 재시작 → 충돌.

**수정 내역**:
1. **이어하기(resume)**: `_load_resume_state(webtoon_episode_id)`가 이미 커밋된 `text_region`/`face_record`에서 `region_index`/`face_index`/`used_bboxes`를 복원 → `process_episode_step1(resume_from=...)`가 이미 끝난 세그먼트를 OCR/YOLO 재호출·재삽입 없이 스킵. `resume_from`은 `activity.info().heartbeat_details[0]`로 전달(`step2.py::identify_episode_faces`의 기존 resume 패턴과 동일 구조로 맞춤, `activities.py::step1_episode`).
2. **HTTP 재시도+지수 백오프**: `ocr_yolo_client.py::_post_image` — `httpx.HTTPStatusError`(상태코드 `>=500`, Cloudflare 520~526 포함) + `httpx.TransportError`(커넥션/타임아웃, 애초에 응답을 못 받는 경우)만 최대 10회, 1s→8s 캡 지수 백오프. 4xx는 재시도 없이 즉시 전파(우리 쪽 요청 문제이므로 숨기면 안 됨). 백오프 총 소요시간(최악 ~55초/콜)은 `step1_episode`의 `heartbeat_timeout=5분`(`workflows.py:98`)보다 충분히 짧게 설계 — **이 5분 값은 그대로 유지하기로 결정**(사용자 판단: 정상 OCR/YOLO 호출은 10초 내 끝나므로 5분 초과는 진짜 이상 신호로 보는 게 맞음).
3. **UPSERT 안전망**: `text_region`/`text_annotation` INSERT에 `ON CONFLICT DO NOTHING` 추가(`face_record`가 이미 쓰던 패턴과 동일) — `is_excluded` 등 human 리뷰 필드를 덮어쓰면 안 되므로 `DO UPDATE`가 아니라 `DO NOTHING`. resume 로직에 놓친 경우가 있어도 크래시 대신 스킵되는 마지막 방어선.
4. **로깅 강화**: `step1.py` 전체를 `print()` → `logging` 모듈(`logger`)로 전환. 세그먼트 실패 시 `seg.index`/`g_y0,g_y1`/완료·스킵 개수/`resume_from`/누적 텍스트·얼굴 수·경과시간을 한 번에 남기고 재전파. 안전망(ON CONFLICT) 스킵 발생 시 경고 로그. `ocr_yolo_client.py`는 재시도/최종실패 로그에 `source/title_id/episode_no/cut` 컨텍스트 포함. `activities.py::step1_episode`는 시작 시 `attempt`/`resume_from`을 로그.

### 16.4 Step2(인물식별) / Chroma·Postgres 정합성 드리프트 — 이번 세션에서 발견·수정
`data-pipeline/webtoon-pipeline/src/core/step2.py` + `service/backend/apps/api/toon/{tasks.py,service/chroma_client.py,views.py}`.

**증상**: `face_identify_episode` 액티비티가 `face_record.appearance_id`의 FK 위반(`character_appearance` id가 Postgres에 없음)으로 실패.

**근본 원인 2건, 둘 다 curl로 실측 확인 후 수정 완료**:
1. **`service`의 관리자 액션 "분석데이터 초기화하기"(`admin.py::reset_analysis_action` → `tasks.py::reset_webtoon_analysis`)가 DB/S3/Chroma 3단계를 독립 try/except로 처리**하는데, `_reset_chroma_collections`가 `webtoon.embedding_settings`(**웹툰별 명시적 오버라이드** `WebtoonEmbeddingSetting`)만 순회해서 지울 컬렉션 이름을 만들었다. 근데 `data-pipeline`의 `resolve_embedding_model`(`model_resolver.py`)은 오버라이드가 없으면 **전역 기본 모델**(`EmbeddingModel.is_default`)로 조용히 폴백해서 계속 임베딩한다. **사용자 확인: 대부분 웹툰이 명시적 오버라이드 없이 전역 기본 모델만 쓴다** — 즉 대부분의 웹툰에서 "초기화하기"를 눌러도 Chroma 컬렉션 삭제가 **통째로 스킵**됐다(DB는 하드 삭제되고 Chroma엔 유령 벡터만 남음). 로그 증거: `{'chroma': {'status': 'success', 'collections': {}}}` — 시도한 컬렉션이 0개.
2. **Chroma 서버가 v1 REST API를 완전히 제거함**(2026-07 실측: `GET /api/v1/... → HTTP 410 "The v1 API is deprecated. Please use /v2 apis"`). `service`의 `chroma_client.py::delete_collection`과 `views.py::ChromaCollectionsAPIView`는 v1 경로(`/api/v1/collections/...`)를 썼고, `404`만 "이미 없음(정상)"으로 처리해서 `410`은 `resp.raise_for_status()`에서 예외로 터졌다 — **명시적 오버라이드가 있는 웹툰이라도 Chroma 삭제가 지금까지 실제로 성공한 적이 없었을 가능성이 높다.** 게다가 `reset_webtoon_analysis`의 상위 상태 집계가 컬렉션별 실패를 무시하고 항상 `chroma: {"status": "success"}`로 찍어서 이 문제가 안 보였다. (참고: `data-pipeline`은 공식 `chromadb==1.5.9` 클라이언트로 이미 v2를 쓰고 있었다 — §10.)
3. **실제 오염 확인**: webtoon_id=60(naver/820097)의 `character_faces_naver_820097_CCIP` 컬렉션에 82개 문서, 그중 20개가 2026-06-29(에피소드 107)에 만들어진 `appearance_id=491`(`NEW_CHAR_005`, 이미 Postgres에서 하드 삭제됨) 유령 벡터였고, ep2 재처리 재시도가 **그 순간에도 같은 유령 id에 새 벡터를 계속 추가하고 있었다**(`created_at: 2026-07-03T01:42:49`, episode=2, cut=4).

**수정 내역**:
- `service/backend/apps/api/toon/service/chroma_client.py::delete_collection`: v1 → v2(`/api/v2/tenants/default_tenant/databases/default_database/collections/{name}`, 이름 기반 삭제도 v2에서 동작 확인됨)로 전환.
- `service/backend/apps/api/toon/views.py::ChromaCollectionsAPIView`: 동일한 v1 버그를 발견해서 같이 v2로 전환(응답 포맷/집계 로직은 그대로 유지 — 별도 버그인 `suffix="_clip"` 대소문자 매칭 문제는 이번엔 안 건드림, 필요하면 별도로 다룰 것).
- `service/backend/apps/api/toon/tasks.py::_reset_chroma_collections`: 순회 대상을 `webtoon.embedding_settings.all()` → `EmbeddingModel.raw_objects.all()`(soft-delete 포함 전체 모델)로 확장 — 실제 존재 안 하는 조합은 `delete_collection`이 이미 404→"absent"로 처리하므로 안전.
- `reset_webtoon_analysis`: 컬렉션별 `"failed"`가 하나라도 있으면 chroma 단계 자체의 `status`도 `"failed"`로 집계하도록 수정(이전엔 컬렉션이 전부 실패해도 `overall: success`로 가려졌음).
- **일회성 데이터 정리(2026-07-03)**: webtoon_id=60의 `character_faces_naver_820097_CCIP` 컬렉션을 v2 DELETE로 완전 제거 확인(삭제 후 GET 404 확인). Postgres/S3/원본 이미지 등은 건드리지 않음.
- **재발 방지**(`data-pipeline/webtoon-pipeline/src/core/step2.py`): `_get_valid_appearance_ids(webtoon_id)`로 Postgres에 실제 존재하는 `character_appearance` id 집합을 조회해 (a) `ccip_anchors` 로드 직후 일괄 필터링(유령 anchor 제거 + 경고 로그), (b) 매칭 루프 안에서도 최종 방어선으로 재검증(cosine 경로는 사전 필터가 안 되므로 필요) — 유령 `appearance_id`에 매칭되면 크래시 대신 신규 캐릭터로 재할당하고, 같은 에피소드 내 재발을 막도록 제외 목록에 즉시 추가.
- **남은 구조적 한계**: DB 리셋과 Chroma 리셋은 여전히 원자적이지 않다(부분 실패 자체는 여전히 가능한 설계). 이번 수정은 (1) 정리가 실제로 되게(v2 API + 전체 모델 순회) 만들고 (2) 그래도 놓치는 경우에 대비한 방어 로직(step2.py)을 추가한 것 — "리셋이 완벽히 원자적"이라고 가정하면 안 된다.

### 16.5 검증 상태 (2026-07-03 기준)
- §16.3/16.4의 코드 수정은 전부 `py_compile` 통과, Chroma API 동작(v1 410/v2 200, 이름 기반 GET/DELETE)은 curl로 실측 확인함.
- §16.2(model-api async/blocking 구조, `HF_HUB_OFFLINE`)는 **발견만 하고 아직 미수정** — 우선순위/방향 논의 필요(§13 R1).
- (2026-07-04 갱신) `naver/820097` ep2 재실행 결과는 §16.6 이하 참조 — end-to-end 검증 완료, 단 그 과정에서 새 회귀 버그 3건이 발견됨.

### 16.6 `naver/820097` ep2 end-to-end 검증 + Step2 자기-런 스냅샷 회귀 버그 (2026-07-04)

**검증 결과**: ep2(`webtoon_episode_id=11757`)를 재실행해 `episode_pipeline_progress`에 phase 1/2/3 전부 `completed` 확인. `text_region`(356)/`text_annotation`(paddle 356 + llm 356) 1:1 정합, `cut_scene_meta`(93, pass1 컷 수와 일치), `llm_usage`(pass1 93콜/pass2_resolve 1콜), `webtoon_narrative_state.last_resolved_episode_id=11757`, `narrative_thread` 3건 모두 크래시 없이 커밋됨 — §13 R2 종결.

**회귀 버그 발견**: 재실행 로그가 "매칭 진행 42/42 (매칭=0, 신규=42)" — 얼굴 42개가 **전부** 신규 캐릭터로 쪼개졌다. 원인은 지난 세션(§16.4)에 추가한 드리프트 방어 로직 자체의 회귀:

```python
# step2.py::identify_episode_faces
valid_appearance_ids = _get_valid_appearance_ids(webtoon_id)   # 루프 시작 전 딱 1회 스냅샷
...
for i, face in enumerate(pending):
    ...
    if match is not None and match["meta"]["appearance_id"] not in valid_appearance_ids:
        ...  # "유령"으로 오판 → 신규 캐릭터로 강제 재할당
```

`valid_appearance_ids`는 루프 시작 **전** Postgres 스냅샷인데, 같은 에피소드 처리 중 `_allocate_character`가 만든 새 캐릭터는 즉시 `ccip_anchors`(매칭 후보)엔 추가되면서도 `valid_appearance_ids`엔 반영되지 않았다. 그래서 컷85에서 만든 캐릭터를 컷106의 얼굴이 정확히 재매칭했는데도 "스냅샷에 없는 유령"으로 오판돼 강제로 새 캐릭터로 쪼개짐 — 이게 전체 얼굴에 걸쳐 반복돼 42/42 전부 신규가 나왔다. 실제 Chroma/Postgres 드리프트는 전혀 없었음(Chroma 42개 문서의 appearance_id가 Postgres 42개 행과 정확히 1:1 일치 확인).

**수정**: `_allocate_character` 호출 직후 `valid_appearance_ids.add(appearance_id)` 한 줄 추가 — `ccip_anchors.append(...)`와 동일한 패턴으로 즉시 반영. 재실행 결과 "매칭=29, 신규=13"(최종 캐릭터 13명)으로 정상화 확인.

**데이터 정리**: 잘못 생성된 42개 캐릭터/appearance/face_embedding과 Chroma 컬렉션(`character_faces_naver_820097_CCIP`)을 삭제(Postgres는 `face_record.appearance_id`만 NULL로 리셋, `face_record` 행 자체는 Step1 산출물이라 보존)하고 `webtoon_pipeline_state`의 phase2 카운터도 리셋 — 수정된 코드로 재실행할 수 있게 원복.

### 16.7 Step3 Temporal 워커 액티비티 미등록 버그 (2026-07-04)

수정된 step2 코드로 재실행 후 step3에서 새 에러 발생:
```
temporalio.exceptions.ApplicationError: NotFoundError: Activity function step3a_extract for workflow
naver_820097_chain_step3_2_2 is not registered on this worker, available activities: step3_episode
```

**원인**: `src/temporal/worker.py`의 `step3_worker`가 `activities=[activities.step3_episode]`(옛 단일 패스 액티비티)만 등록하고 있었는데, `workflows.py::_run_step3`는 이미 2-pass 체인(`step3a_extract`→`step3b_resolve`→`step3c_apply`, 전부 STEP3_QUEUE)을 호출하도록 되어 있었다 — 즉 **워크플로/액티비티 구현은 2-pass로 전환됐는데 워커 프로세스의 등록 목록만 갱신이 안 된 상태**였다.

**수정**: `worker.py`의 `step3_worker` 등록 목록을 `[activities.step3a_extract, activities.step3b_resolve, activities.step3c_apply]`로 교체. `activities.py` 상단 docstring도 갱신.

**확인된 죽은 코드(미삭제)**: `activities.py::step3_episode`(→ `step3.py::analyze_episode_scenes`/`analyze_cut_scene`/`analyze_episode_scenes_by`)는 리포 전체에서 호출자가 전무함(`workflows.py`는 안 씀, `smoke_test.py`는 같은 이름의 독립적인 자체 stub일 뿐 이 함수를 안 씀). §13 R5로 남겨둠 — 삭제 여부는 별도 결정 필요(꽤 큰 블록이라 이번엔 안 건드림).

### 16.8 `narrative_context.fold` 캐시 불일치 버그 (2026-07-04)

ep2 재실행 결과를 검증하던 중 발견. `webtoon_narrative_state.open_threads`(JSON 캐시, 다음 화 프롬프트에 "이전 화까지의 미해결 떡밥"으로 그대로 들어감)에 저장된 떡밥 3건이 `planted_episode: 1`을 갖고 있었는데, 실제 `narrative_thread` 테이블(authoritative)은 같은 떡밥의 `planted_episode_id`를 정확히 ep2(no=2)로 기록하고 있었다 — 캐시와 DB가 어긋남.

**원인**: `_commit_threads`(DB 커밋 담당)는 status가 'resolved'이고 이번 화에 심긴 게 아닌 경우를 제외하면(대부분의 open 떡밥), LLM이 뭐라 답했든 `planted_episode_id`를 **항상 현재 화**로 강제 저장한다. 그런데 `apply_resolution`이 `narrative_context.fold`에 넘기는 `episode_meta["threads"]`는 이 보정 없이 `result.threads`(LLM 원본 응답)를 그대로 썼다 — 이번 케이스에서 LLM이 (ep1이 아예 처리된 적 없는 첫 실행이라) `planted_episode=1`을 추측성으로 반환한 값이 그대로 캐시에 박혔다.

**수정**: `_normalize_threads_for_fold(threads, this_ep_no)` 헬퍼를 추가해 `_commit_threads`와 동일한 보정 규칙을 `episode_meta` 생성 시점에도 적용 — DB와 캐시가 항상 같은 값을 갖도록 함. 이미 오염된 캐시(webtoon_id=60의 `narrative_thread` 3건 + `webtoon_narrative_state` 1건)는 삭제해 다음 실행 때 깨끗하게 재생성되도록 함(`load_prior`는 상태 없으면 빈 초기값으로 처리하므로 안전).

### 16.9 vllm 신뢰성 + Pass-1 병렬화 + max_tokens 절단 수정 + 프롬프트 한국어 강제 + LLM 동시성 config화 (2026-07-04)

**vllm 502/530 재시도**: `vllm.prup.xyz`(Step3 LLM 엔드포인트)도 Cloudflare Tunnel 경유라 §16.1과 같은 이유로 502/530(터널 재연결/일시 다운)이 간헐 발생. `llm_client.py::call_llm_json`엔 재시도가 전혀 없었고, `step3.py`의 `_PASS1_RETRIES=2`도 sleep 없이 즉시 2회뿐이라 지속 장애 구간에서 컷이 통째로 스킵됐다. `ocr_yolo_client.py`와 동일 패턴(최대 10회, 1s→8s 캡 지수 백오프, 5xx+`httpx.TransportError`만 재시도·4xx 즉시 실패)을 추가 — 스트리밍 호출 로직은 `_stream_llm_once`(1회 시도)로 분리하고 `call_llm_json`이 재시도 루프를 담당. 백오프 대기 중엔 `_LLM_SEMAPHORE`를 놓아줘서 다른 컷 시도를 막지 않음.

**Pass-1 병렬화**: `extract_cut`(컷별 비전 콜)은 컷 간 belief 의존이 없음을 코드로 확인(연속성은 Pass-2 담당, `belief` 파라미터는 현재 미사용 예약값) — `extract_episode`에서 `ThreadPoolExecutor`(기본 4워커, `PASS1_WORKERS` env)로 병렬 호출하도록 변경. `_accumulate_belief`의 `last_seen_cut` 갱신처럼 순서 의존적인 후처리는 완료 후 `cut_number`로 재정렬해 단일 스레드로 수행. 실제 동시 LLM 요청 수는 `_LLM_SEMAPHORE`(`LLM_MAX_CONCURRENCY`)가 최종 상한이므로, 워커 수를 올려도 이 값을 같이 올리지 않으면 실질 동시성은 안 늘어남. `db_cursor()`가 `ThreadedConnectionPool`(maxconn=10)이라 컷별 동시 DB 쓰기도 안전 확인.

**max_tokens 절단 버그**: `ep=11 cut=2`에서 `json.JSONDecodeError: Unterminated string...` 발생. DB의 유일한 `llm_model`(`glm-4.6v`) params엔 `max_tokens` 설정이 없어 `_pass1_ctx`의 하한선(4096)이 그대로 적용되고 있었는데, glm-4.6v는 추론형 모델이라 답 이전에 `reasoning_content`(사고과정)로 토큰 예산을 먼저 소모 — 컷이 복잡하면 본문이 다 끝나기 전에 잘린다. `_PASS1_MIN_MAX_TOKENS`를 8192로 상향(§9.4 갱신)하고, `llm_client.py`에서 JSON 파싱 실패 시 `finish_reason`/`completion_tokens`/응답 길이를 에러 메시지에 실어 향후 truncation 여부를 로그만으로 바로 판단 가능하게 함.

**프롬프트 한국어 강제**: beat/summary 등 자연어 출력이 종종 영어로 나오는 문제 — `_PASS1_SYSTEM_PROMPT`/`_PASS2_SYSTEM_PROMPT` 둘 다 앞부분에 "반드시 한국어로 작성" 지시를 눈에 띄게(⚠️) 추가하고, 기존에 문장 끝에 묻혀있던 지시도 강조 처리(`corrected_text`만 OCR 원문 언어 유지 예외).

**LLM 동시성 config화**: `LLM_MAX_CONCURRENCY`/`PASS1_WORKERS`는 코드엔 이미 env var로 있었지만 실제 배포 `proxmox-configuration/pipeline_repo/configmap.yaml`엔 값이 없어 코드 기본값(1/4)에 고정돼 있었다 — ConfigMap에 명시적으로 추가(기존 기본값과 동일 유지, 동작 변화 없음). 이제 이 파일만 고쳐서 배포하면 코드 변경 없이 동시성 조절 가능. `LLM_MAX_CONCURRENCY`를 1보다 올려도 되는지는 vllm 서버의 실제 동시호출 허용치를 실측해야 함(미실측).

**py_compile + 관련 pytest(24개 중 Temporal 테스트서버 포트충돌로 인한 무관한 1개 제외 전부) 통과 확인.**

---

## 17. v4.0 재설계 (확정 방향, 2026-07-05) — 분석 도메인 신규 스키마 + LLM 스테이지 재편

> **전제(사용자 결정)**: 분석 산출 데이터는 전량 폐기·재생성해도 된다. 불가침은 콘텐츠 도메인(`webtoon`/`webtoon_episode`/`webtoon_cut` 등)과 **human 노동분**뿐이다. 따라서 마이그레이션 호환이 아니라 "백지에서 다시 설계해도 같은 걸 만들 것인가"를 기준으로 분석 도메인을 재설계한다. **본 섹션은 설계 확정본이며 구현 전 상태** — 구현 순서는 §17.7.

### 17.1 핵심 전환: 제자리 멱등 갱신 → AnalysisRun 단위 쓰고 버리기

현행 복잡도의 큰 부분(멱등 upsert, scope delete-reinsert, `stable_key`, `resolution_status` 단방향 전이, 리셋 태스크의 부분 실패 — §16.4)은 전부 "기존 데이터를 보존하며 제자리 덮어쓰기"에서 온다. 잘못된 분석을 과감히 버리는 운영 철학에서는 **run 단위 교체**가 정답이다.

- **`AnalysisRun`**(webtoon FK, episode FK NULL허용, kind `vision|resolve|arc`, llm_model FK, status `running|succeeded|failed`, vision_run FK NULL, started/finished_at, stats jsonb).
  - `vision` run: 컷 비전(Stage V) 산출 귀속 — llm `text_annotation`, `cut_scene_meta`.
  - `resolve` run: 정체·화자(Stage R) + 서사(Stage N) 두 콜의 산출 귀속 — 화자 테이블, `episode_report`/`episode_beat`/`narrative_thread`/`character_claim`/profile delta/suggestion. `vision_run` FK로 어떤 비전 산출을 읽었는지 기록. (R/N을 한 run으로 묶는 이유: N은 R의 정정 결과에 의존하므로 재실행 단위가 같다.)
  - `arc` run: 웹툰 단위 주기적 아크 종합(Stage A) — `story_arc` 산출 귀속.
- **재분석 = 새 run 작성 후 현재 포인터 스왑**(에피소드의 최신 succeeded run이 서빙 대상). 반쯤 죽은 run은 그냥 버려짐 — 멱등성 장치 대부분 불필요해짐.
- **"잘못 분석된 데이터 날리기" = run 삭제**(cascade). 테이블 7개를 스코프 맞춰 지우는 리셋 태스크 소멸.
- **진행도/stale 플래그 저장 안 함(도출)**: "step3 됐나" = succeeded resolve run 존재. "stale인가" = human 수정 타임스탬프 > 최신 succeeded resolve run.finished_at. → `EpisodePipelineProgress`·`WebtoonPipelineState` 진행 카운터·`webtoon_cut.llm_analyzed_at/is_stale` 3원화가 run 하나로 수렴(Temporal=실행 정본, run=결과 정본). `WebtoonPipelineState`의 **설정성 필드**(phase3_enabled, processable_max_episode 등)만 분리 존치.
- **human 레이어는 run 밖(불멸)**: `text_annotation(source=human)`, `face_identity(source=human)`, `character_profile(source=human)`, `character`의 확정 이름/is_confirmed, `text_region.is_excluded`. 안정 엔티티(region/face/character)에 부착되어 재분석 몇 번을 돌아도 생존. 모델 A/B는 run 비교로 공짜.

### 17.2 인물 도메인: kind 판별자 + 얼굴 레이어링 + CharacterProfile

- **`character`에 `kind(cluster|character)` 판별자 추가** — 별도 테이블 분리 대신 채택(FK 단일성, 승격=kind 전환+명명으로 참조 재작성 불필요, entity-resolution 표준 3단[관측→기계군집→golden record]의 실용 구현). Step2 산출은 전부 `kind=cluster`(현행 NEW_CHAR placeholder 대체), **승격**(→`kind=character`)은 human 확정 또는 LLM 고신뢰 명명 시. 도감/UI/roster prior는 `kind=character`만. 얼굴 없는 고아 cluster는 주기 GC. `NEW_CHAR_` 이름 접두사 관습 폐기.
- **얼굴을 텍스트와 동일한 레이어링으로 분리**: `face_record`(탐지+매칭 혼재) → **`face_detection`**(cut FK, face_idx, bbox, conf — Step1 산출, 불변) + **`face_identity`**(detection FK, source `step2|human`, appearance FK, score, run FK NULL[human은 NULL]) — `human > step2` 우선. label_conflict → 얼굴 재배정 제안(problem.md ②)이 "llm 제안 레이어"로 들어갈 자리가 자연히 생긴다.
- **`character_profile`**(character FK, source `llm|human`, unique(character, source), gender, age_group, affiliation, role, personality jsonb, traits jsonb, key_facts jsonb) — TextAnnotation과 동일한 source 레이어링. "llm이 넣은 걸 사람이 수정했나" = human 행의 존재로 표현(LLM 원본 보존, diff 공짜). 서빙 병합은 **필드 단위 human 우선**. LLM은 llm 행만 upsert(human 행 불가침). 단위는 Character(appearance 단위 확장은 필요 시). **`webtoon_narrative_state`의 key_facts를 여기로 흡수.** v3.6의 `extra['llm_profile']` 과도기 저장은 이 모델 도입 시 제거.
- **`suggestion` 통합 검토 큐**(webtoon FK, type `name|merge|face_reassign|label_conflict`, 대상 ref[character/face/cut], payload jsonb, confidence, source_run FK, status `pending|accepted|rejected`) — 현행 `name_discovery_suggestion` + character_timeline jsonb 속 merge_suggestion/label_conflict + (미구현) 얼굴 재배정 제안 4곳을 하나로. webtoonmoa 검토 UI가 화면 하나로 수렴.

### 17.3 폐기 목록

| 대상 | 처리 | 대체 |
|---|---|---|
| `webtoon_narrative_state` | **폐기** | prior = `character(kind=character)`+`character_profile`+`narrative_thread(open)`+최근 N화 `episode_report.summary` 조인(웹툰당 에피소드 1회 처리라 조인 비용 무시 가능). running_summary 개념 소멸 |
| `name_discovery_suggestion` | 폐기 | `suggestion(type=name)` |
| `character.extra['llm_profile'/'name_suggestions']` | 폐기 | `character_profile` / `suggestion` |
| `story_arc`(생산자 없는 현행) | **Stage A 산출로 재정의**(부활) | arc run이 생산 |
| `EpisodePipelineProgress`, `WebtoonPipelineState` 진행 카운터 | 폐기 | `AnalysisRun` 도출(§17.1). 설정성 필드만 존치 |
| `webtoon_cut.llm_analyzed_at/is_stale` | 폐기 | run + human 타임스탬프 비교 도출 |
| `face_record` | 분해 | `face_detection` + `face_identity` |
| 레거시 죽은 코드(R5: `step3_episode`/`analyze_cut_scene` 계열) | 삭제 | — |

유지(형태 유지, run FK 추가): `text_region`, `text_annotation`, `cut_scene_meta`, `episode_report`(+`teaser` 필드 신설), `episode_beat`, `narrative_thread`, `character_claim`, `llm_usage`(+run FK), `character_appearance`, 설정 테이블(EmbeddingModel/WebtoonEmbeddingSetting/LLMModel/WebtoonLLMSetting).

### 17.4 LLM 스테이지 재편: V → R → N → apply (+주기적 A)

Pass-2a 한 콜(정체+화자+비트+요약+떡밥+책략)의 attention 분산과 출력 비대(v3.6 전수 화자 테이블화 이후)를 해소하기 위해 텍스트 해소를 둘로 분리한다. **LLM 스테이지 상한은 에피소드당 3콜** — 이 이상의 분해는 금지(§9.3 과분해 원칙 유지).

| Stage | 콜 | 입력 | 출력 |
|---|---|---|---|
| **V** 컷 비전 | 컷당 1 | 오버레이 1장 + OCR + identified_faces(confirmed 포함) | 현행 Pass-1 유지(v3.6 수정 승계: provisional 화자 영속, 1:1, 한국어) |
| **R** 정체·화자 | 에피소드당 1 | V 트랜스크립트 + 도감 prior(profile) + confirmed 앵커 | characters(승격 제안 포함), **전수 화자 테이블**, name/merge/face_reassign/label_conflict → suggestion |
| **N** 서사 | 에피소드당 1 | **R로 정정된 트랜스크립트**(화자 확정 상태) | beats / threads / deceptions / episode{summary, **teaser**, appeal_point, cliffhanger} / profile delta |
| apply | 0 (결정론) | R+N 산출 | 커밋(human 동결), suggestion 적재, profile llm 행 병합 |
| **A** 아크 종합 | 주기적(매 N화 또는 아크 경계) | 해당 구간 episode_report들 | `story_arc` — 웹툰 전체 줄거리는 아크 요약의 연결(1~30 fold 나열식 늘어짐의 근본 해법) |

- R/N 분리 이점: 서사 분석이 정체 정정 **후의** 텍스트를 읽음(현재는 한 콜에서 동시 수행), 출력 분산, 실패 격리(N이 죽어도 화자 데이터 착지). 비용은 에피소드당 텍스트 콜 1→2(무제한 플랜에서 무시 가능).
- **보류 결정**: 앙상블/교차검증(단일 모델 튜닝이 먼저 — "두 번째 의견"은 suggestion 큐+human이 담당), 도감용 Graph/Vector DB(규모상 Postgres 조인으로 충분, 신규 인프라 운영 부담 회피 — 도감 정본은 Postgres, 얼굴 벡터만 Chroma 유지). 민감물 로컬 라우팅은 기존 `WebtoonLLMSetting`으로 충족.

### 17.5 요약/티저 품질 규칙 (Stage N 프롬프트 계약)

실측 문제(820097 1~30 요약): "파문당한 귀족 에드 로스테일러" 식 수식어 반복으로 늘어짐 + 정보성 요약과 궁금증 유발 카피의 목적 혼재.

1. **기지 인물 수식어 금지**: roster/도감에 이미 있는 인물은 이름만. 소개 문구는 그 인물의 **첫 등장 회차 요약에서 단 1회**.
2. **summary(정보성, 스포 OK)와 teaser(궁금증 유발, 스포 금지) 필드 분리** — `episode_report.teaser` 신설.
3. **데이터 기반 스포 차단**: teaser는 이번 화에 resolved된 `narrative_thread`의 답과 `deceptions`의 진실 언급 금지, 미회수 떡밥은 암시만 — 스포일러 목록을 프롬프트 감이 아니라 구조 데이터로 강제.
4. summary 2~3문장 상한. v3.6 규칙(근거 기반, 평가어 금지, deception은 기만 의도 speech만) 승계.

### 17.6 v3.6 코드 diff 처리 (미커밋 상태)

- **v4.0에 그대로 승계**: Pass-1 화자 영속 + 전수 화자 테이블 + provisional 승격 안전망, confirmed 플래그 배선, max_tokens 16384, deception/요약 프롬프트 규칙, service human-수정 API의 수정 신호(단, is_stale 플래그 → human 타임스탬프 비교로 형태 변경), reresolve CLI(run 재실행으로 개념 승계).
- **v4.0에서 걷어냄**: `character.extra['llm_profile']` 커밋부 + `CharacterSerializer.profile`의 extra 참조(→ `character_profile` 모델로), `apply_resolution`의 `llm_analyzed_at/is_stale` 컷 마킹(→ run 도출로), `webtoon_narrative_state` 캐시 슬림화(테이블 자체 폐기로 무의미).

### 17.7 구현 순서 (다음 세션부터)

1. **service 마이그레이션**: `AnalysisRun`/`character.kind`/`face_detection`+`face_identity`/`character_profile`/`suggestion`/`episode_report.teaser` 신설 + §17.3 폐기 마이그레이션. human 노동분 보존 스크립트(is_confirmed character 이름, 확정 얼굴↔인물, human annotation, is_excluded).
2. **data-pipeline 스테이지 재편**: Step2가 kind=cluster 생성, Step3를 V/R/N/apply로 재구성(Temporal 액티비티 3분할 유지·R/N은 한 액티비티 내 2콜), run 라이프사이클 배선, 죽은 코드(R5) 삭제.
3. **Stage A(아크 종합)** 신설 + 트리거 주기 결정.
4. **webtoonmoa**: suggestion 통합 검토 큐 화면, 도감(profile) 화면.
5. **검증**: `naver/820097` 전량 재분석(신 스키마) — 화자 부착률/요약·teaser 품질/도감 확인. 골든 회귀 테스트를 신 계약으로 이식.

### 17.8 구현 노트 (2026-07-05, §17.7 1~2단계 구현하며 확정된 보강)

- **AnalysisRunKind에 `step1`/`step2` 추가**: §17.1은 LLM 도메인(vision/resolve/arc)만 정의했으나, 진행도 3원화를 run으로 수렴하려면 step1/2 완료도 run이어야 한다(구 `EpisodePipelineProgress` 대체). step1/2는 산출물 run FK 귀속 없이 **완료 원장 행만** 남긴다(`runs.record_completed_run`) — 탐지/매칭 레이어는 run 교체 대상이 아니기 때문. 체인 진행 판정(`next_chain_episode`)의 step→kind 매핑은 `shared.STEP_RUN_KIND`(step3→resolve).
- **R/N run 공유**: step3b가 resolve run을 시작(R+N 2콜 usage 귀속), step3c apply 성공이 succeeded 전이 — "에피소드 step3 완료"의 정본 시각. N 콜만 실패하면 화자 데이터는 유지된 채 서사 필드만 빈 값(실패 격리 확인).
- **face_reassign suggestion 생산 구현됨(2026-07-05 후속 세션)**: Stage R 출력에 `face_reassignments: [{cut, face(F라벨), to_character_id|null, evidence, confidence}]` 섹션 추가(얼굴 단위 판단 — 인물 전반의 의심은 기존 label_conflict 유지, 프롬프트가 구분 지시). apply(step3c)가 `(cut_number, face_idx)`→`face_detection.id`로 해석(`_episode_face_detection_map`)해 `suggestion(type=face_reassign, detection_id, payload={to_character_id, evidence})`로 적재 — 수락 시 service가 human FaceIdentity 생성(기구현). 커밋 규칙: human 확정(confirmed) 얼굴 동결, 실재하지 않는 (cut,face) 무시, 웹툰에 없는 to_character_id는 null 강등(오배정 신호만 유지), 현재 배정과 동일/미배정+대상미상 제안은 드롭. 윈도우 병합은 (cut,face) dedup(confidence 우선).
- **reapply는 suggestion 큐 불가침(2026-07-05 후속 세션, 유실 버그 수정)**: `apply_resolution(refresh_suggestions=False)` — reapply(이름만 변경 시 LLM 없는 재투영)가 pending 제안을 delete-reinsert하면 스냅샷에 비영속인 원료(name 후보 confidence, face_reassignments)가 재생성 불가라 직전 run의 pending name/face_reassign 제안이 통째로 유실되던 문제. 재투영은 큐를 건드리지 않는다(제안 재생성은 새 resolve run의 apply만).
- **마이그레이션 전략 전환: 이식 → 전량 wipe(사용자 결정, 2026-07-05 후속 세션)**: §17 전제의 "human 노동분 불가침"을 이번 전환에 한해 완화 — 분석 데이터 전량 폐기·재생성을 택했다(유실 실측: face 확정 1,696건, 이름 확정 캐릭터 84건, human 주석 36건, 제외 마킹 49건 — 재작업 감수). 불가침은 콘텐츠 도메인(webtoon/webtoon_episode/webtoon_cut/webtoon_author)·설정 테이블·S3 원본뿐. 이에 따라 **구 손작성 0022(RenameModel pk 보존+이식)는 폐기**하고 `0022_v4_wipe_analysis_data`(TRUNCATE, postgres 벤더 가드) + `0023`(전부 makemigrations 자동 생성, faceembedding.detection one-off default는 빈 테이블이라 무의미) 구성으로 재생성. sqlite 체인 검증을 위해 삭제 예정 모델(EpisodePipelineProgress/FaceRecord/NameDiscoverySuggestion)의 constraint/index를 RemoveField 앞에 명시 제거(Django가 자동 생성 안 함 — sqlite 테이블 재작성 크래시 + `uniq_face_record_cut_idx` 이름 충돌 예방).
- **테이블 prefix 도입(사용자 결정, 2026-07-05 후속 세션)**: 분석 산출 테이블 17개에 `analysis_`(예: `analysis_text_region`, `analysis_character`, `analysis_face_detection`, `analysis_suggestion`, `analysis_llm_usage`, `analysis_episode_segment` — `analysis_run`은 기존 이름 유지), 설정 테이블 5개에 `config_`(`config_llm_model`, `config_webtoon_llm_setting`, `config_embedding_model`, `config_webtoon_embedding_setting`, `config_webtoon_pipeline_state`). 콘텐츠(webtoon*)·추천(reco_*)은 불변. 파이프라인 raw SQL 124곳 일괄 치환(키워드 FROM/JOIN/INTO/UPDATE 앵커로 안전 치환). **constraint 이름은 불변**(파이프라인이 `ON CONFLICT ON CONSTRAINT`로 이름 참조 — `uniq_face_record_cut_idx`는 analysis_face_detection 테이블에 legacy 이름으로 유지됨을 문서화).
- **reapply(이름만 변경 시 LLM 없는 재투영)는 run을 만들지 않는다** — 기존 run 산출의 재투영이므로 최신 succeeded resolve run id를 그대로 스탬프.
- **마이그레이션 안전장치**: `face_record`는 Delete+Create가 아니라 **RenameModel**(0022)로 이행해 pk 보존(S3 crop 경로 `face_crop/{pk}.jpg` + human 확정 유지). `is_confirmed=True` 행은 human FaceIdentity로, appearance 배정 행은 step2 FaceIdentity로 이식. `character.kind` 백필: is_confirmed 또는 비-NEW_CHAR 이름 → character.
- **클러스터 이름**: `name=""`(빈 문자열) + 로그/Chroma 메타 표시용 라벨은 `cluster#{id}`. `_find_character_by_name`은 `kind='character'`만 후보로.
- **Stage N 윈도잉 없음**: N 입력(정정된 트랜스크립트, 텍스트만)은 컴팩트해서 단일콜. 로컬 16K 폴백에서 긴 에피소드가 초과하면 절단 위험 — 실측 후 필요 시 후속(§9.12 계열 리스크로 이월).
- **기존 깨진 테스트 7건(step1 resume SQL 픽스처 미모델링) 수리 완료**: conftest FakeCursor에 resume 복원 3쿼리(`SELECT tr.cut_id, MAX(tr.index)`/`fr.face_idx`/`is_used bbox`) 핸들러 추가 + face_record→face_detection 매칭 문자열 전환. 파이프라인 테스트 스위트 전체 그린(2026-07-05, 워크플로 테스트 1건은 Temporal 테스트서버 포트 플래키로 재실행 통과).

### 17.9 작업 현황 & 인수인계 (2026-07-05 세션 종료 시점 — 새 세션은 여기부터 읽을 것)

**현재 상태: §17.7의 1·2단계 + 3단계 일부(service API)가 구현 완료됐고, 두 레포 모두 미커밋 working tree로 남아 있다(사용자 결정: 유지). 프로덕션 DB에는 아직 아무것도 적용 안 됨.**

#### 완료된 변경 (미커밋)

- `data-pipeline` 레포: `prd.md`(본 문서 v4.0), `webtoon-pipeline/src/core/{step1,step2,step3,runs}.py`(runs.py 신규), `src/operators/narrative_context.py`(정본 조인 재작성), `src/temporal/{activities,workflows,shared}.py`, `src/tools/reresolve.py`, `tests/conftest.py`(신 스키마+resume SQL 픽스처), `tests/test_workflow_orchestration.py`(step3 mark 제거 계약), `tests/test_step3_face_reassign.py`(신규 — face_reassign 생산+reapply 큐 보존 계약), `smoke_test.py`(step3a/b/c 스텁).
- `service` 레포: `backend/apps/api/toon/models.py`(+테이블 prefix), `migrations/0022_v4_wipe_analysis_data.py`(**손작성 wipe** — 구 이식형 0022는 사용자 결정으로 폐기, §17.8), `migrations/0023_analysisrun_characterprofile_facedetection_and_more.py`(자동 생성 + 삭제 모델 constraint 선행 제거 3건 손보정), `views.py`/`serializers.py`/`admin.py`/`tasks.py`/`urls.py`/`service/face_crop.py`/`management/commands/sync_confirmed_face_embeddings.py`.
- 검증 상태: 파이프라인 pytest 스위트 **전체 그린**(기존 깨져 있던 픽스처 7건 포함 수리, §17.8). service는 `manage.py check` 통과 + 마이그레이션 체인(0001→0023) sqlite 실행 통과 + `makemigrations --check` 클린. **실 Postgres/실 LLM으로는 아무것도 안 돌려봄.**

#### 남은 작업 (순서 제안)

1. **커밋 정리**: 사용자 검토 후 두 레포 커밋(단위 분리 권장: service 스키마 / service API / pipeline 코어 / 테스트·문서).
2. **배포 & 검증(§17.7-5)** — ⚠️ 순서 중요 (wipe 전략 반영, §17.8):
   1. **prod DB 백업**(권장 — 0022가 분석 데이터 TRUNCATE. 콘텐츠/설정만 남기는 게 의도지만 롤백 보험).
   2. service 마이그레이션 적용(`migrate toon`: 0022 wipe + 0023 스키마) → **곧바로 파이프라인 워커도 함께 배포**(동시 교체 — 구 워커는 구 테이블명(`face_record` 등)·폐기 컬럼을 쓰고, 신 워커는 `analysis_*`/`config_*` 신 테이블 필요).
   3. **Chroma 컬렉션 리셋**: 기존 벡터가 TRUNCATE로 사라진 face pk를 참조하므로 전량 삭제 후 재시작.
   4. **step1부터 전량 재분석**(분석 데이터가 비었으므로 step3만이 아니라 전체 체인): 컷 이미지는 S3에 있어 재다운로드 없음. face crop은 새 pk로 S3 덮어쓰기(pk 시퀀스 RESTART).
   5. 확인 지표: llm speech/monologue의 speaker_id 부착률(v3.6 이전 1~2%가 기준선), `analysis_episode_report.teaser` 품질(스포 여부), `analysis_suggestion` 큐 적재(name/merge/label_conflict/face_reassign), `analysis_character_profile` 생성. (구 human 확정은 wipe로 소멸 — 페니아 confirmed 존중 검증은 webtoonmoa에서 재확정 후 재해소로 대체.)
3. **Stage A(아크 종합) 신설(§17.7-3)**: story_arc 생산자 — 입력=구간 episode_report들, kind='arc' run, 트리거 주기(매 N화 vs 떡밥 회수 등 경계 감지) **논의 후** 구현. 아직 미설계.
4. **webtoonmoa(§17.7-4)**: suggestion 통합 검토 큐 화면(`GET .../suggestions/`, `PATCH /suggestion/{id}/` 이미 서비스에 있음), 인물도감 화면(CharacterSerializer의 `kind`/`profile` 노출 완료).
5. **이월 항목**: litellm 요청 프롬프트(messages) 로깅 설정, Stage N 로컬 16K 절단 리스크 실측, `LLM_MAX_CONCURRENCY` 상향 실측(§13 R4), model-api `HF_HUB_OFFLINE`(§13 R1). ~~face_reassign suggestion 생산~~(2026-07-05 후속 세션에 구현 — §17.8, `tests/test_step3_face_reassign.py`).

#### 새 세션 주의사항

- 사용자 워크플로: **논의 먼저, 코드 수정은 명시 승인 후.** 스키마/설계 변경은 PRD에 결정 기록이 선행된다.
- 진행도/stale은 이제 컬럼이 아니라 도출이다(§17.1) — "분석 됐나"를 확인하려면 `analysis_run`을 조회할 것(구 `episode_pipeline_progress`/`llm_analyzed_at` 쿼리는 무효).
- CLAUDE.md의 "자주 쓰는 테이블" 목록은 v4 신 스키마(analysis_/config_ prefix 포함)로 갱신됨(2026-07-05) — 단 **prod 적용 전까지 실제 DB는 구 스키마**임에 유의.
