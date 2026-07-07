# 웹툰 분석 파이프라인 — 통합 PRD (마스터)

> **상태**: v4.1 · 갱신일: 2026-07-07
> **정본 규칙**: 현행 설계·계약·백로그는 **본 문서가 정본**이다. 지난 버전들의 변경 경위·장애 대응 서사·세션 인수인계 로그는 **`prd-history.md`**(아카이브)로 이관했다(v3.x 헤더 불릿 전문 포함). v2.0 이전 원본 3개 문서는 `docs/archive/`. `prd-step3.md`·`prd-identity-roster.md`는 본 문서로 흡수 후 삭제됨(2026-07-07, 원문은 git 이력).
> **범위**: `data-pipeline`(파이프라인·모델서빙) + `service`(Django 백엔드) + `webtoonmoa`(조회·라벨링 프론트) + `proxmox-configuration`(k3s GitOps) — **4개 레포**
>
> **v4.1 (2026-07-06~07) — 정체성·서사 로스터 + 신뢰성 (§18)**: 스테이지 role별 모델 배선(비전=glm-4.6v, 텍스트=glm-5.2, modality 2-슬롯), **에피소드 로스터 스테이지 신설**(V→roster→R→N→apply), max_tokens 기본 미전송 정책, 런타임 fallback(self-FK), json-repair 파싱 폴백, Temporal heartbeat, CCIP 매칭 앵커캡+마진. **커밋·prod 배포·가동 확인**(§18.7).
> **v4.0 (2026-07-05) — 분석 도메인 재설계 (§17)**: AnalysisRun 단위 쓰고-버리기, character.kind 판별자, 얼굴 레이어링(face_detection/face_identity), character_profile(source 레이어링), suggestion 통합 큐, `analysis_`/`config_` 테이블 prefix. **마이그레이션 0022(wipe)~0026 prod 적용 완료, 전량 재분석 진행 중.**
> 이전 변경(v3.0~v3.6)의 상세는 `prd-history.md` §H1, 한 줄 요약은 §15 결정 로그.

---

## 1. 개요 & 목적

웹툰 컷 이미지에서 **텍스트(OCR)**, **얼굴/캐릭터(YOLO+임베딩)**, **장면·화자(멀티모달 LLM)** 를 추출하고 회차 단위로 서사를 요약하는 파이프라인. 결과는 PostgreSQL + S3 + Chroma에 저장되어 `webtoonmoa` 서비스가 소비한다.

| 구분 | 내용 |
|------|------|
| 입력 | S3 컷 이미지 (`{S3_LOCATION}/{source_dir}/{title_id}/{ep}/{title_id}_{ep}_{cut}.jpg`) — boto3 직접 다운로드 |
| 로컬 모델 | PaddleOCR(korean), [deepghs/anime_face_detection](https://huggingface.co/deepghs/anime_face_detection)(YOLO), CLIP ViT-L/14, CCIP(deepghs) |
| 원격 모델 | 비전(Stage V)=glm-4.6v / 텍스트(roster·R·N)=glm-5.2 / fallback=qwen-vl — 전부 vllm 게이트웨이 경유, DB `config_llm_model`로 해석(§18.1) |
| 저장 | PostgreSQL(텍스트·얼굴·캐릭터 메타) + S3(원본 컷) + R2(face crop, 2026-07-06 S3→R2 이전) + Chroma(얼굴 임베딩) |
| 규모 | 웹툰 30+종, 누적 에피소드 ~6,000, 누적 컷 ~60만, 일 신규 ~10 ep(~1,000 컷) |
| 처리 | **증분 스트리밍**(일 신규분) 중심. 배치 백필(70만)은 보류 |

### 파이프라인 4단계 (Step 3·4는 하나의 에피소드 추론 단계로 통합됨, §9)

```
Step 1 ── 로컬 추출 (OCR + YOLO, 분리 병렬)      ← 모든 웹툰   ✅ 구현 (Temporal)
Step 2 ── 인물 식별 (임베딩 + Chroma 매칭)        ← 모든 웹툰   ✅ 구현 (Temporal)
Step 3 ── LLM 스테이지 V→roster→R→N→apply       ← 활성 웹툰만 ✅ 구현 (§17.4, §18.1)
Step 4 ── 회차 종합 요약(EpisodeReport)          ← 활성 웹툰만 ✅ apply(결정론 커밋)에 흡수되어 자동 산출(§9.6)
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
컷 N ─┬─ PaddleOCR ──→ analysis_text_region + analysis_text_annotation(paddle)   [Step1]
      └─ YOLO ───────→ analysis_face_detection (+ face crop R2)                  [Step1]
                          │
                          ▼ 임베딩(CLIP/CCIP) + Chroma 매칭                        [Step2]
                   analysis_face_identity(step2) — 기존 인물 매칭 or 신규 cluster
                          │
                          ▼ step3a: Stage V(컷별 비전, vision run)                 [Step3]
                   provisional text_annotation(llm) + cut_scene_meta
                          │
                          ▼ step3b: roster(에피소드 로스터 추출) → R(정체·화자) → N(서사)
                            — 텍스트 전용 3콜, resolve run 공유 (§17.4, §18.1~18.2)
                          │
                          ▼ step3c: apply(LLM 없음, 결정론 커밋+소급전파) [Step3/4 통합]
회차 전체 ─────────────────┴─→ text_annotation(resolved) + episode_report(+teaser) +
                                episode_beat + narrative_thread + character_claim +
                                suggestion(name/merge/face_reassign/label_conflict) +
                                character_profile(llm)
```

### Step 1 — 로컬 추출 (모든 웹툰, ✅)
- PaddleOCR → 텍스트+bbox, YOLO → face bbox. **GLM 호출 없음.**
- 에피소드 내 컷 순차, 에피소드 간 병렬. 404(이미지 없음)로 에피소드 경계 감지.
- **OCR/YOLO 분리**(v3.0): 별도 model-api 서비스/엔드포인트 호출, 독립 재시도 (§6.1).

### Step 2 — 인물 식별 (모든 웹툰, ✅)
- face crop → 임베딩 추출 → Chroma 유사도/CCIP metric 매칭 → threshold 이상이면 캐릭터 귀속, 아니면 `NEW_CHAR_*`(웹툰 글로벌 스코프) 발급.
- 웹툰별 에피소드 순차(ep1 확정이 ep2 매칭에 반영). 임베딩+매칭 **1패스 통합**(이중 임베딩 제거 완료).
- doc_id `{webtoon_id}_{episode}_{cut}_F{idx}` 고정 + `upsert` 멱등성.

### Step 3 + 4 — LLM 장면/화자/서사 분석 (활성 웹툰만, ✅ 구현) — 설계 원칙 §9, 스테이지 §17.4, 모델·로스터 §18
- `step3a_extract`(Stage V, 컷별 비전) → `step3b_resolve`(**roster**(에피소드 로스터 추출)→**R**(정체·화자)→**N**(서사), 텍스트 전용) → `step3c_apply`(결정론 커밋+소급전파, **LLM 미사용**).
- Step4(회차 요약)는 별도 단계가 아니라 apply 커밋에 흡수되어 `analysis_episode_report`로 매 에피소드마다 자동 산출된다(§9.6).
- 골든 회귀 테스트 3종으로 mis-ID distrust/책략 탐지/교차에피소드 정체성 prior 핵심 동작이 고정됨(§9.10).

---

## 4. 아키텍처 — Temporal durable workflow (Faust/Kafka 피벗 완료)

Kafka/Faust는 완전히 제거됐고 **Temporal이 유일한 오케스트레이션 경로**다(2026-07-01 운영 확인). Temporal 서버는 `proxmox-configuration/temporal_repo`로 배포(ES 없는 Postgres visibility 최소 구성 — §14-1), `pipeline_repo` configmap에는 `TEMPORAL_ADDRESS`만 있다.

- 웹툰 = 워크플로 인스턴스 1개(`workflow_id={source}_{title_id}`) → 웹툰당 순차/웹툰 간 병렬. 에피소드 체이닝은 워크플로 내부, `continue_as_new`로 history 관리.
- 단계별 재시도는 activity `RetryPolicy`, 중간 재개는 workflow history + heartbeat_details(resume).
- service 트리거: `config/temporal.py::send_phase1_trigger`가 웹툰당 1회 멱등 kick(`client.start_workflow`). Kafka + Faust + Celery beat 3개가 Temporal 하나로 통합됨.

```
WebtoonWorkflow (id="{source}_{title_id}")        # 웹툰당 1개 → 순차/병렬
  └─ EpisodeWorkflow (child)                       # 에피소드 순차
       └─ 컷 루프: ocr ∥ yolo (각자 추론+저장 독립)  # OCR/YOLO 분리 병렬
       └─ (컷 완료 후) face_identify → chroma_sync   # 얼굴식별 = 에피소드 단위
```

결정 배경(Faust 한계·비교표·트리거 전환 경위)은 `prd-history.md` §H2.

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

## 7. DB 스키마 (PostgreSQL, service 관리) — v4.0 현행

정의 정본은 service `apps/api/toon/models.py`. 설계 근거·레이어링 원칙은 §17.1~17.3. v4.0부터 분석 산출은 `analysis_`, 설정은 `config_` prefix(콘텐츠 도메인은 prefix 없음·불가침) — **마이그레이션 0022(wipe)~0026 prod 적용 완료(2026-07-05~06)**. Region(위치)과 Annotation(해석) 분리, source 레이어링(`human > llm/step2 > paddle`), human 레이어는 run 밖(불멸)이 관통 원칙. 구 v3.x 스키마 서술은 `prd-history.md` §H7.

### 콘텐츠 도메인 (불가침, prefix 없음)
- **webtoon**(source, title_id) / **webtoon_episode**(webtoon FK, no) / **webtoon_cut**(episode FK, cut_number, human_modified_at) — 컷 행은 Step1 산출. `image_url` 없음(S3 경로 재현). 진행도/stale 컬럼 없음(run 도출, §17.1).

### 분석 도메인 (`analysis_`, run 단위 쓰고-버리기)
- **analysis_run**(webtoon FK, episode FK NULL, kind `step1|step2|vision|resolve|arc`, llm_model FK, status `running|succeeded|failed`, vision_run FK, stats) — **진행도/stale의 정본**: "step3 됐나"=succeeded resolve run 존재, "stale"=human_modified_at>run.finished_at.
- **analysis_character**(kind `cluster|character`, name, is_confirmed, significance, is_match_excluded) / **analysis_character_appearance** / **analysis_character_profile**(character FK, source `llm|human`, unique(character,source), gender/age_group/affiliation/role/personality/traits/key_facts) — 인물(클러스터→승격) + 도감. 서빙은 필드 단위 human 우선 병합.
- **analysis_face_detection**(cut FK, face_idx, bbox, conf, is_used — Step1 산출, 불변) + **analysis_face_identity**(detection FK, source `step2|human`, appearance FK, score, run FK) — human>step2 레이어링. **analysis_face_embedding**(detection FK, embedding_model).
- **analysis_text_region**(cut FK, index, bbox, is_excluded) + **analysis_text_annotation**(region FK, source `paddle|llm|human`, text, type, speaker_id, confidence, resolution_status `unresolved|resolved`).
  - **`type`(TextBlockType)**:

    | 값 | 의미 | speaker |
    |---|---|---|
    | `speech` | 대사 — 입 밖으로 낸 말(일반 말풍선) | 있음 |
    | `monologue` | 독백 — 속마음(구름/각진 말풍선) | 있음 |
    | `narration` | 나레이션 — 세계관/상황 해설(사각 박스) | 없음(null) |
    | `system` | 시스템/캡션 — 상태창·서적 글귀·편지·연도 표시 등 | 없음 |
    | `other` | 그 외(효과음 OCR 원문 등). RAG 제외 후보 | - |

    독백/나레이션 분리 이유: 화자 유무가 본질적으로 달라 의도/감정 추론에서 분리 필수. 효과음은 별도 type 없이 `other`, *의미*는 scene_meta로 흡수.
- **analysis_cut_scene_meta**(cut, action_summary, key_objects, run FK) — 상황 서술 레이어(OCR 텍스트 아님).
- **analysis_episode_report**(episode, summary, **teaser**, appeal_point, cliffhanger, foreshadowing, character_timeline, run FK) / **analysis_episode_beat**(hook_type free-form, appeal_point, intensity, stable_key) / **analysis_narrative_thread**(떡밥 심음/회수) / **analysis_character_claim**(deceptions 영속화, is_deception).
- **analysis_suggestion**(webtoon FK, type `name|merge|face_reassign|label_conflict`, character/detection/episode/cut ref, payload, confidence, run FK, status `pending|accepted|rejected`) — AI 제안 통합 검토 큐.
- **analysis_llm_usage**(webtoon/episode/cut FK, stage `vision|roster|resolve|narrative|arc`, llm_model FK, 토큰 3종, image_count, finish_reason, run FK) — 콜당 1행.

### 설정 테이블 (`config_`)
- **config_llm_model**(name, provider, model_id, params json, supports_vision, is_default, is_active, **fallback self-FK**) — modality당 활성 기본 1개 partial-unique(§18.1). params는 `context_window`만 권장(max_tokens 비움 — §18.3). 시드: glm-4.6v(vision default, cw=32768) / glm-5.2(text default, cw=131072) / qwen-vl(공용 fallback, cw=131072).
- **config_embedding_model**(metric_type `cosine|ccip`, default_threshold, is_default) — 시드 `clip`(cosine,0.25), `CCIP`(ccip,0.16). / **config_webtoon_embedding_setting**(웹툰별 override).
- **config_webtoon_pipeline_state**(phase3_enabled, processable_max_episode 등 설정성 필드만 — 진행 카운터는 폐기).

> `WebtoonLLMSetting`(웹툰별 LLM override)은 v4.1에서 폐기(전역 modality 2-슬롯만, §18.1). Chroma 컬렉션명은 파이프라인·reset 모두 DB `config_embedding_model.name`(=`CCIP`)을 사용해 일치.

---

## 8. 임베딩 모델 / Threshold 설정화 (CLIP·CCIP)

### 8.1 실험 근거 (face-embed-lab, 1292 crop)
- CLIP: 식별력 약함(여러 인물 한 덩어리). CCIP(deepghs, 애니 동일인 판별 전용) 채택.
- CCIP avg linkage 스윕 → **threshold 0.16** 기준값. ⚠️ 이 값은 **평균 거리(avg-linkage) 기준** — v2 매칭(top-3 평균 통계량, §18.5)에서는 **0.12**가 대응값이다(min 통계량에 0.16을 쓰던 v1이 magnet의 원인). threshold는 통계량과 세트로만 의미가 있다.

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

> 이 섹션은 `prd-step3.md`(2026-06-29~30 재설계 draft)의 최종 합의를 v3.3에서 전면 흡수한 것으로, 2-pass 설계의 **원칙·계약**(전역 해소, provisional→confirmed, belief state, 비트 계층, 신뢰성 규칙, 정답 취급)의 정본이다. **Step3(장면/화자)와 Step4(회차 요약)는 하나의 에피소드 추론 단계로 통합**됐고 `webtoon-pipeline/src/core/step3.py`에 구현·운영 중이며, 골든 회귀 테스트로 핵심 동작이 고정됐다(§9.10). `prd-step3.md`는 삭제됨(2026-07-07, 원문 git 이력).
>
> **⚠️ v4.0/v4.1 갱신 주의 — 이후 재편으로 본문 일부가 대체됐다**: ① 스테이지 분할 — Pass-2a(단일 텍스트 콜)가 **roster→R→N 3콜**로 분리(§17.4, §18.1~18.2). ② 모델 — 텍스트 스테이지는 glm-5.2, 비전만 glm-4.6v(§18.1); §9.2의 모델 표와 §9.4의 `max_tokens>=16384` 고정 하한은 **max_tokens 기본 미전송 + context_window 정책**(§18.3)으로 대체. ③ 스키마 — 본문 속 `WebtoonNarrativeState`/`NameDiscoverySuggestion`/`FaceRecord`/`is_stale` 등은 구명칭(v4.0 폐기·대체 목록 §17.3; prior는 정본 테이블 조인으로 조립). 원칙은 유효하되 수치·모델명·테이블명은 §7/§17/§18이 정본.

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
- **모델 역할**: 주력=**GLM**(무제한 플랜, 비용 비제약 → 병목은 rate-limit/지연), rate-limit·장애 시 폴백=**Qwen3-VL-32B 로컬**. 전환은 DB config로 — 코드 변경 없이 모델 교체(현행 배선은 §18.1: modality 2-슬롯 + fallback self-FK).

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
- **모델 A/B 결론**(`qwen-vl/_vltest.py`·`_pass1.py`·`_pass2.py` 하니스, naver 769209 실측): Pass-1 엔진은 **GLM-4.6v 우선**(OCR region 1:1 바인딩 엄수, Pass-2 입력 토큰 1/4, 40% 빠름). Qwen3-VL-32B는 1:1 바인딩을 깨고(예: 4블록→1병합) 4배 verbose하지만 JSON 에러 0 — **견고한 폴백**으로 채택(v4.1에서 DB self-FK 기반 런타임 fallback으로 배선됨, §18.4). 텍스트 스테이지 모델 재판정(glm-5.2 채택)은 §18.1/`prd-history.md` §H6.
- 당시 실험 로그·인용 근거 전문은 git 이력의 `prd-step3.md`(2026-07-07 삭제) 참조.

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

- **웹툰별 독립 컬렉션** `character_faces_{source}_{title_id}_{model.name}`(예: `character_faces_naver_769209_CCIP` — 모델명은 DB `config_embedding_model.name` 그대로, 대문자 포함), `hnsw:space=cosine`. 60만 규모에서 metadata 필터 후처리 성능 저하 회피 + 웹툰별 drop/재생성 가능. 파이프라인과 reset 태스크가 같은 규칙을 써서 정확히 일치.
- doc_id `{webtoon_id}_{episode}_{cut}_F{face_idx}` 고정 + `upsert` 멱등.
- 배포: `oci-croma.prup.xyz:8000`(운영, OCI 호스팅 — k3s 클러스터 밖) / docker-compose(개발). 토큰 인증. env `CHROMA_HOST/PORT/AUTH_TOKEN`.
- metadata: webtoon_id, episode, cut, face_idx, character_id, appearance_id, appearance_label, character_name, is_confirmed, bbox, conf, created_at.
- **⚠️ v1 REST API는 서버에서 완전히 제거됨(2026-07 실측 확인, `HTTP 410 "The v1 API is deprecated. Please use /v2 apis"`)**. `data-pipeline`은 공식 `chromadb==1.5.9` 클라이언트로 이미 v2(tenant/database 경로, `default_tenant`/`default_database`)를 쓰지만, `service` 쪽 수기 REST 호출은 v1을 쓰고 있었다가 2026-07-03에 v2로 이관(`prd-history.md` §H4.3). **앞으로 Chroma REST를 직접 호출하는 코드를 새로 짤 때는 반드시 v2(`/api/v2/tenants/{tenant}/databases/{database}/collections/...`)를 쓸 것** — v1은 404가 아니라 410을 반환하므로 "존재 안 함"과 오인하기 쉽다.

---

## 11. Human-in-the-loop

### 11.1 Human Checkpoint
`WebtoonPipelineState.phase2_processable_max_episode`로 Step2 처리 범위를 ep 번호로 제어(`null`=전체, `10`=10화까지 후 idle). 도달 시 다음 이벤트 미발행 → 자연 대기. 검토 후 값 올려 resume. (Temporal에선 Schedule/signal 또는 워크플로 가드로 동일 구현.)

### 11.2 재처리 (Human Correction → 일괄 재분석, **에피소드 단위로 재설계됨 — §9.11**)
- Step3가 컷 즉시확정에서 에피소드 전역 해소로 바뀌면서 **재해소 단위도 컷이 아니라 에피소드**다 — human 수정 1건이 에피소드 전체 화자/소구포인트 해소를 바꿀 수 있기 때문.
- stale은 컬럼이 아니라 **도출**(v4.0, §17.1): human 수정 API가 `webtoon_cut.human_modified_at`을 마킹하고, "stale" = human_modified_at > 최신 succeeded resolve run.finished_at.
- 재해소 실행: `python -m src.tools.reresolve <source> <title_id> <ep_no|stale> [--rerun-extract]` CLI — **얼굴↔캐릭터 매칭을 고친 경우 `--rerun-extract` 필수**(identified_faces 입력 자체가 바뀜). Temporal 자동 트리거는 미구현(오픈, §18.8).
- **부분 재처리(reapply)**: 이름 테이블만 바뀐 경우(예: suggestion 수락)는 LLM 없이 `reapply_episode`(apply만 재실행)로 결정론적 재투영 — run을 새로 만들지 않고 최신 succeeded resolve run id를 스탬프하며, **suggestion 큐는 건드리지 않는다**(§17.6).
- confidence 게이팅: 저신뢰 type/speaker/name은 provisional 유지. 자동 이름/중요도/병합은 제안만(자동 수행 금지), `human`/`is_confirmed`는 항상 동결.

### 11.3 이름 자동 확정
§9.5(name_evidence 누적) 참조. 주요 캐릭터는 여러 컷 증거가 쌓이며 `NameDiscoverySuggestion`으로 확정 제안(human은 confirm만), 조연은 제안 큐, 단역(`significance=extra`)은 soft-exclude 유지.

---

## 12. webtoonmoa 기능 요구사항

> 표의 데이터 소스는 작성 당시(v3.x) 모델명 — v4.0 대응은 §17.2~17.3(FaceRecord→analysis_face_detection/identity, NameDiscoverySuggestion·label_conflict·face_reassign→**analysis_suggestion 통합 큐**, is_stale→run 도출, profile→analysis_character_profile). 검토 큐/도감/로스터 화면 신설이 다음 프론트 작업(§17.7-4). 인물 로스터·등장/미등장·근거 표시 UI 레퍼런스로 `roster-viewer.html`(레포 루트, 자족형 HTML — glm-5.2 vs qwen 5회차 비교 뷰어) 참고.

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

완료 이력(스키마·Temporal 이관·Step3+4 구현·R2~R4 신뢰성 트랙·v4.0 적용)은 `prd-history.md` §H3. **남은 항목**:

| 단계 | 작업 | 상태 |
|---|---|---|
| V1 | v4.0 전량 재분석 완주 + 검증 지표 확인(§17.7-5: speaker 부착률, teaser 품질, suggestion 큐, profile 생성) | 🔄 재분석 진행 중 |
| V2 | 트랙 C 재실행 검증 — 화산귀환 wipe→재실행으로 CCIP blob 재발 여부 확인(§18.5) | 🔲 절차 준비됨 |
| A1 | Stage A(아크 종합) 신설 — story_arc 생산자, 트리거 주기 논의 후 구현(§17.7-3) | 🔲 미설계 |
| L3 | webtoonmoa 관리 UI — suggestion 통합 검토 큐/도감/로스터 화면(§12, §17.7-4) | 🔲 |
| T3 | chroma_sync/rematch/reembed → Temporal 워크플로/signal 패리티 (Faust 에이전트 삭제분 재구현) | 🔲 미확인 |
| S1 | Step3 오픈 리스크 실측(§9.12: 로컬 16K 품질 격차, belief state 압축, 비트 경계 안정성 — GLM 토큰 예산 항목은 §18.3으로 종결) | 🔲 |
| B1 | (보류) operator 라이브러리화 마무리 + 70만 배치 백필 | ⏸ |
| R1 | model-api `HF_HUB_OFFLINE=1` 추가(라우터 `run_in_threadpool` 수정은 완료 — §H4.1) | 🔲 |
| R4' | `LLM_MAX_CONCURRENCY`를 1보다 올려도 되는지 vllm 동시호출 허용치 실측 | 🔲 |
| R5 | `step3_episode`(+`analyze_cut_scene` 계열) 죽은 코드 정리 여부 결정 | 🔲 삭제 여부 미결정 |

세부 백로그(검출 위생, character_claim, 로스터 영속 등)는 §18.8.

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
| 2026-07-07 | **CCIP 매칭 v2(§18.5)** — v1 마진 룰의 과분할 자기강화(876얼굴→773클러스터) 실증 후 교체: ①통계량 min→**top-k(3) 평균**(0.16이 avg-linkage 캘리브레이션인데 min에 적용된 게 magnet 근본, `CCIP_MATCH_TOPK`) ②마진 **면제형**(2등도 in-threshold면 중복 경합 — 배정+`ambiguous_with`→step2 merge 제안 자동 발행; 2등 out이면 기존 보류) ③threshold **0.12**(top3평균 스케일, DB 변경 필요). 근거: 873 feature 72조합 오프라인 시뮬(현행 재현 750≈773, 채택안 67클러스터·F1@16 최고). 잔여 외형모드 파편은 human 병합(사용자 승인). 구현·테스트 완료, 배포 대기 | §18.5 |
| 2026-07-07 | **병합 시맨틱 확정(§19)** — 계기: 화산귀환 "청명" 확정 캐릭터 19명 사건. ①병합은 **undo 없음**(비가역 human 판단, 정정=재배정·물리삭제 금지) ②human 명시 병합 시 **확정(is_confirmed) 캐릭터도 흡수 허용** ③name 제안 수락 시 동명 kind=character 존재하면 rename 아닌 **자동 병합** ④병합 시 FK(speaker/claim/profile) 즉시 이관 + **Chroma 메타 재투영**(파생물 취급, 실패 허용), jsonb 산출은 stale→재해소 재생성 ⑤human 프로필 충돌=primary 우선(absorbed는 soft-delete 보존). merge log/이벤트소싱은 안 만듦 | §19 |
| 2026-07-07 | **문서 재구성** — 변경 경위·세션 로그를 `prd-history.md`로 분리, `prd-step3.md`(v3.3에서 §9로 흡수 완료)·`prd-identity-roster.md`(§18로 흡수) 삭제, §7을 v4 현행 스키마로 갱신. litellm 요청 프롬프트는 UI 열람 가능·DB(SpendLogs.messages) 미영속 확인(응답 길이 절단은 수용) | 전반 |
| 2026-07-06~07 | **v4.1 정체성·서사 로스터 + 신뢰성(§18)** — ①로스터=Stage R 흡수가 아닌 **별도 텍스트 콜**(에피소드당 텍스트 3콜: roster/R/N, §17.4 계약 개정) ②모델 배선=**modality 2-슬롯**(is_default+supports_vision 도출, per-webtoon override 폐기) ③fallback=`config_llm_model.fallback` self-FK 런타임 전환 ④max_tokens=**기본 미전송**(context_window는 명시 cap 안전장치로만 — 고정값 100k안 폐기, 400/조기절단 실전 결함) ⑤CCIP 과병합=매칭 magnet으로 진단(CCIP 정상), 앵커캡+마진 룰 채택 ⑥bleed 데이터 복구는 HITL 아닌 wipe→재실행 ⑦qwen-vl 로스터 비권장(불안정+느림), glm-5.2 채택 | §18 |
| 2026-07-05 | **v4.0 설계 확정(§17)** — "분석 데이터는 전량 폐기·재생성 가능, human 노동분만 불멸" 전제(사용자 결정)로 분석 도메인 재설계: ①제자리 멱등 갱신 → **AnalysisRun 단위 쓰고 버리기**(진행도/stale 플래그는 저장 않고 도출), ②`character.kind(cluster\|character)` 판별자(NEW_CHAR 관습 폐기), ③얼굴 레이어링 `face_detection`+`face_identity`(human>step2), ④`character_profile`(source `llm\|human` 레이어링, 필드 단위 human 우선 병합), ⑤`suggestion` 통합 검토 큐, ⑥LLM 스테이지 **V→R→N→apply**(+주기 A로 story_arc 부활, 에피소드당 LLM 3콜 상한), ⑦summary/teaser 분리+데이터 기반 스포 차단, ⑧`webtoon_narrative_state`/진행도 3원화/`name_discovery_suggestion` 폐기. 앙상블·Graph DB는 보류. §14-9~11은 이 결정으로 종결 | §17 |
| 2026-07-05 | **v3.6 — 화자 매칭 구조 결함 수정(Pass-1 화자 영속 + Pass-2a 전수 테이블 + Pass-2b 승격 안전망), confirmed 플래그 모델 입력 배선, 인물도감 profile(extra['llm_profile']), HITL stale 마킹(service 3개 API) + `src.tools.reresolve` CLI, narrative 캐시 슬림화, max_tokens 16384 재상향, deception/요약 프롬프트 보정** — naver/820097 전 30회차 화자 부착률 0~7% 실측이 계기 | §7,§9,§11,§16 헤더 v3.6 |
| 2026-07-04 | **v3.5 — `naver/820097` end-to-end 검증 + 회귀 버그 3건(Step2 자기-런 스냅샷/Step3 워커 미등록/narrative fold 캐시 불일치) 발견·수정 + Step3 신뢰성·품질 개선**: ep2 재실행으로 신뢰성 수정이 실제로 통함을 확인(R2 종결). 그 과정에서 새 버그 3건 발견·수정. Step4 별도 구현 계획은 철회(이미 Pass-2b에 흡수됨을 재확인). vllm 502/530 재시도, Pass-1 병렬화, max_tokens 절단 수정(4096→8192), 프롬프트 한국어 강제, `LLM_MAX_CONCURRENCY`/`PASS1_WORKERS` config화 | 경위 §H4(prd-history) |
| 2026-07-03 | **v3.4 — 홈랩 배포 환경 신뢰성 장애 대응**: 신규 §16에 사용자 배포 환경(k3s 홈랩+Cloudflare Tunnel+불안정 홈 네트워크)과 실제 장애 3건(Step1 `resolution_status` NOT NULL, Step1 재시도 비멱등성, Step2/Chroma/DB 정합성 드리프트) 원인·수정·검증 상태 기록. `service`의 Chroma REST v1→v2 이관(§10). model-api async/blocking 리스크 발견(후에 run_in_threadpool로 수정) | 경위 §H4(prd-history) |
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

> 버전별 상세 경위는 `prd-history.md`, v2.0 이전 원본 3개 문서는 `docs/archive/` 참조.

---

## 16. 인프라 환경 & 신뢰성(Reliability)

> 새 세션/에이전트가 "왜 이런 재시도·멱등성 설계가 들어갔는지" 맥락을 잃지 않도록 남긴다 — 16.1은 환경 전제, 16.2는 현행 규칙 요약. 규칙이 도출된 장애 경위(2026-07-03~04)는 `prd-history.md` §H4.

### 16.1 배포 환경 — 홈랩 특성 (신뢰성 설계 전제)
- 전체 워크로드(§2.4 `proxmox-configuration`)가 **사용자의 홈랩 k3s 클러스터**에서 구동된다. 클라우드 매니지드 인프라가 아니다.
- 외부에 노출되는 도메인은 전부 **Cloudflare Tunnel**을 경유한다(직접 공인 IP 아님) — 그래서 일반적인 5xx 외에 **Cloudflare 고유 에러(520~526, 특히 522 Connection timed out)**도 발생한다. 숫자상 전부 5xx라 "상태코드 >= 500이면 재시도" 규칙 하나로 같이 걸러진다.
- **홈 네트워크 회선 자체도 가끔 불안정**(사용자 확인, 2026-07-03) — Cloudflare Tunnel을 거치는 것과 별개로 근본적인 원인. 순수 5xx 상태코드 응답이 아니라 **커넥션 자체가 끊기는 경우**(`httpx.ConnectError`/`ReadError`/`RemoteProtocolError`/타임아웃)도 정상적으로 발생한다고 가정해야 한다 — HTTP 상태코드 체크만으로는 부족하고 커넥션 레벨 예외도 재시도 대상에 포함해야 함.
- 외부 인프라 위치:
  - GPU 서버(OCR/YOLO 추론): `gpgpu.prup.xyz` — 홈랩 밖(§5 배치 백필 노드와는 별개).
  - Chroma: `oci-croma.prup.xyz:8000` — **OCI(Oracle Cloud Infra)에 별도 호스팅, k3s 클러스터 밖**(§10).
  - 나머지(Temporal 서버, model-api clip/ccip, Django/Celery, Postgres 등)는 k3s 안(`proxmox-configuration`).
- **PaddleOCR 주기적 재시작**: 사용자가 메모리 누수 완화를 위해 의도적으로 구성(model-api `--max-requests`/`--max-requests-jitter`, `model-api/Dockerfile:69-77`). 재시작 윈도우 동안 OCR 호출이 일시적으로 502를 반환하는 것은 **의도된 정상 동작**이지 버그가 아니다 — 다만 이게 Step1의 재시도 트리거가 되므로 재시도 경로 자체는 견고해야 한다(§16.2).
- **결론**: 이 프로젝트의 재시도/타임아웃/멱등성 설계는 전부 "가끔 502/522/커넥션에러가 나는 게 정상"이라는 전제 위에 있다. 새로 짜는 외부 호출 코드는 기본적으로 재시도+백오프를 갖춰야 한다(예외: 진짜 버그를 감추면 안 되는 4xx).

### 16.2 신뢰성 설계 현황 — 살아있는 규칙 요약

> 아래 규칙들이 나온 장애별 원인·수정 경위(2026-07-03~04: Step1 비멱등성, Chroma v1/유령 벡터 드리프트, Step2 자기-런 스냅샷 회귀, Step3 워커 미등록, fold 캐시 불일치, model-api 이벤트루프 블로킹 등)는 `prd-history.md` §H4.

- **외부 HTTP 호출 공통 재시도 규칙**: 5xx(Cloudflare 520~526 포함) + `httpx.TransportError`(커넥션/타임아웃)만 최대 10회 지수 백오프(1s→8s 캡), **4xx는 즉시 실패**(우리 쪽 문제를 숨기면 안 됨) — `ocr_yolo_client.py`/`llm_client.py` 공통 패턴. 새로 짜는 외부 호출 코드도 이 패턴을 따를 것.
- **LLM 콜**: 스트리밍 호출(Cloudflare 터널 idle timeout 회피) + 전역 세마포어(`LLM_MAX_CONCURRENCY`, 기본 1 — 상향 가능 여부 미실측, §13 R4') + primary 재시도 소진 시 **fallback 모델 런타임 전환**(§18.4). max_tokens는 기본 미전송(§18.3).
- **Step1 멱등성**: resume(커밋된 region/face에서 복원, heartbeat_details 전달) + `ON CONFLICT DO NOTHING` 안전망(human 리뷰 필드 보호를 위해 DO UPDATE 금지).
- **Step2 방어**: `_get_valid_appearance_ids` 유령 appearance 방어(앵커 필터+루프 내 재검증) — 단 같은 런에서 만든 신규 캐릭터는 즉시 반영. DB/Chroma 리셋은 **비원자적**이므로 "리셋이 완벽했다"고 가정하지 말 것.
- **Temporal heartbeat**: 긴 텍스트 콜(roster/R/N)과 대용량 apply는 서브스레드 + 30초 주기 heartbeat(`_run_with_heartbeat`, §18.4) — heartbeat_timeout 초과로 인한 재시도 루프 방지. step1은 백오프 총 소요(<~55초/콜)가 heartbeat 5분보다 짧게 설계됨.
- **model-api**: 전 라우터 동기 추론을 `run_in_threadpool`로 오프로드(이벤트루프 블로킹 해소). 클라이언트 동시 요청은 서버 워커 수에 맞춤(`_EMBED_WORKERS=2`). `HF_HUB_OFFLINE=1`은 미적용(§13 R1). PaddleOCR 주기 재시작(--max-requests)은 의도된 동작 — 재시작 윈도우의 502는 정상.
- **Chroma REST 직접 호출은 반드시 v2**(`/api/v2/tenants/{tenant}/databases/{database}/...`) — v1은 404가 아니라 **410**을 반환해 "존재 안 함"과 오인하기 쉽다(§10).
- **프롬프트**: 자연어 출력은 한국어 강제(⚠️ 강조 지시). Pass-1 병렬화는 `PASS1_WORKERS`(configmap 노출), 실질 동시성 상한은 `LLM_MAX_CONCURRENCY`.

---

## 17. v4.0 재설계 (확정 방향, 2026-07-05) — 분석 도메인 신규 스키마 + LLM 스테이지 재편

> **전제(사용자 결정)**: 분석 산출 데이터는 전량 폐기·재생성해도 된다. 불가침은 콘텐츠 도메인(`webtoon`/`webtoon_episode`/`webtoon_cut` 등)과 **human 노동분**뿐이다. 따라서 마이그레이션 호환이 아니라 "백지에서 다시 설계해도 같은 걸 만들 것인가"를 기준으로 분석 도메인을 재설계한다. **본 섹션은 설계 확정본이며, 구현·prod 적용 완료 상태다**(마이그레이션 0022~0026, 2026-07-05~06 — 현황은 §17.6).

### 17.1 핵심 전환: 제자리 멱등 갱신 → AnalysisRun 단위 쓰고 버리기

현행 복잡도의 큰 부분(멱등 upsert, scope delete-reinsert, `stable_key`, `resolution_status` 단방향 전이, 리셋 태스크의 부분 실패 — `prd-history.md` §H4.3)은 전부 "기존 데이터를 보존하며 제자리 덮어쓰기"에서 온다. 잘못된 분석을 과감히 버리는 운영 철학에서는 **run 단위 교체**가 정답이다.

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

유지(형태 유지, run FK 추가): `text_region`, `text_annotation`, `cut_scene_meta`, `episode_report`(+`teaser` 필드 신설), `episode_beat`, `narrative_thread`, `character_claim`, `llm_usage`(+run FK), `character_appearance`, 설정 테이블(EmbeddingModel/WebtoonEmbeddingSetting/LLMModel — `WebtoonLLMSetting`은 v4.1에서 추가 폐기, §18.1).

### 17.4 LLM 스테이지 재편: V → R → N → apply (+주기적 A)

Pass-2a 한 콜(정체+화자+비트+요약+떡밥+책략)의 attention 분산과 출력 비대(v3.6 전수 화자 테이블화 이후)를 해소하기 위해 텍스트 해소를 둘로 분리한다. **에피소드 단위 텍스트 콜 상한은 3콜** — v4.1에서 roster 스테이지가 추가되어 roster/R/N 3콜로 상한을 채웠고(§18.1~18.2), 이 이상의 분해는 금지(§9.3 과분해 원칙 유지). 실제 스테이지 순서는 **V → roster → R → N → apply**.

| Stage | 콜 | 입력 | 출력 |
|---|---|---|---|
| **V** 컷 비전 | 컷당 1 | 오버레이 1장 + OCR + identified_faces(confirmed 포함) | 현행 Pass-1 유지(v3.6 수정 승계: provisional 화자 영속, 1:1, 한국어) |
| **R** 정체·화자 | 에피소드당 1 | V 트랜스크립트 + 도감 prior(profile) + confirmed 앵커 | characters(승격 제안 포함), **전수 화자 테이블**, name/merge/face_reassign/label_conflict → suggestion |
| **N** 서사 | 에피소드당 1 | **R로 정정된 트랜스크립트**(화자 확정 상태) | beats / threads / deceptions / episode{summary, **teaser**, appeal_point, cliffhanger} / profile delta |
| apply | 0 (결정론) | R+N 산출 | 커밋(human 동결), suggestion 적재, profile llm 행 병합 |
| **A** 아크 종합 | 주기적(매 N화 또는 아크 경계) | 해당 구간 episode_report들 | `story_arc` — 웹툰 전체 줄거리는 아크 요약의 연결(1~30 fold 나열식 늘어짐의 근본 해법) |

- R/N 분리 이점: 서사 분석이 정체 정정 **후의** 텍스트를 읽음, 출력 분산, 실패 격리(N이 죽어도 화자 데이터 착지). 비용은 에피소드당 텍스트 콜 증가(무제한 플랜에서 무시 가능).
- **보류 결정**: 앙상블/교차검증(단일 모델 튜닝이 먼저 — "두 번째 의견"은 suggestion 큐+human이 담당), 도감용 Graph/Vector DB(규모상 Postgres 조인으로 충분, 신규 인프라 운영 부담 회피 — 도감 정본은 Postgres, 얼굴 벡터만 Chroma 유지). (설계 당시 "민감물 로컬 라우팅은 `WebtoonLLMSetting`으로 충족"이라 했으나 v4.1에서 per-webtoon override 자체를 폐기 — 필요해지면 재도입 논의, §18.1.)

### 17.5 요약/티저 품질 규칙 (Stage N 프롬프트 계약)

실측 문제(820097 1~30 요약): "파문당한 귀족 에드 로스테일러" 식 수식어 반복으로 늘어짐 + 정보성 요약과 궁금증 유발 카피의 목적 혼재.

1. **기지 인물 수식어 금지**: roster/도감에 이미 있는 인물은 이름만. 소개 문구는 그 인물의 **첫 등장 회차 요약에서 단 1회**.
2. **summary(정보성, 스포 OK)와 teaser(궁금증 유발, 스포 금지) 필드 분리** — `episode_report.teaser` 신설.
3. **데이터 기반 스포 차단**: teaser는 이번 화에 resolved된 `narrative_thread`의 답과 `deceptions`의 진실 언급 금지, 미회수 떡밥은 암시만 — 스포일러 목록을 프롬프트 감이 아니라 구조 데이터로 강제.
4. summary 2~3문장 상한. v3.6 규칙(근거 기반, 평가어 금지, deception은 기만 의도 speech만) 승계.

### 17.6 구현·적용 현황 + 구현 중 확정된 계약 (2026-07-07 확인)

**상태**: §17.7 구상의 1~2단계(+service API)가 구현·커밋됐고(data-pipeline `24f573b`~, service 마이그레이션 `0022`~`0026`), **prod 적용 완료**(0022 wipe+0023 스키마 2026-07-05, 0024~0026 2026-07-06). 신 워커 배포·가동 중, **전량 재분석 진행 중**(wipe 전략 — human 노동분도 이번 전환에 한해 폐기, §15 결정. 유실 실측: face 확정 1,696건·이름 확정 84건·human 주석 36건·제외 마킹 49건, 재작업 감수). 남은 단계: Stage A(§13 A1), webtoonmoa(§13 L3), 검증 지표 확인(§13 V1). 당시 세션 인수인계·마이그레이션 작성 경위 전문은 `prd-history.md` §H5.

**구현 중 확정된 살아있는 계약**:
- **run kind에 `step1`/`step2` 추가**: step1/2는 산출물 run FK 귀속 없이 **완료 원장 행만**(`runs.record_completed_run`). 체인 진행 판정 step→kind 매핑은 `shared.STEP_RUN_KIND`(step3→resolve).
- **R/N run 공유**: step3b가 resolve run 시작(roster/R/N usage 귀속), **step3c apply 성공이 succeeded 전이** — "에피소드 step3 완료"의 정본 시각. N만 실패하면 화자 데이터 유지+서사 필드만 빈 값(실패 격리).
- **face_reassign suggestion**: Stage R 출력 `face_reassignments[{cut, face, to_character_id|null, evidence, confidence}]` → apply가 detection_id로 해석해 `suggestion(type=face_reassign)` 적재(수락 시 service가 human FaceIdentity 생성). human 확정 얼굴 동결, 무효 (cut,face) 무시, 무효 to_character_id는 null 강등, 동일/무의미 제안 드롭, 윈도우 병합은 (cut,face) dedup.
- **reapply(LLM 없는 재투영)는 run을 만들지 않고 suggestion 큐 불가침**: `apply_resolution(refresh_suggestions=False)` — pending 제안 재생성 원료가 비영속이라 delete-reinsert 시 통째 유실되기 때문. 제안 재생성은 새 resolve run의 apply만.
- **테이블 prefix**: 분석 `analysis_`/설정 `config_`(콘텐츠·추천 불변). **constraint 이름은 불변** — 파이프라인이 `ON CONFLICT ON CONSTRAINT` 이름 참조(`uniq_face_record_cut_idx`가 analysis_face_detection에 legacy 이름으로 유지).
- **클러스터 이름**: `name=""` + 표시 라벨 `cluster#{id}`. `_find_character_by_name`은 `kind='character'`만 후보.
- **Stage N 윈도잉 없음**: 입력이 컴팩트해 단일콜. 로컬 16K 폴백에서 긴 에피소드 절단 위험 — 실측 후 필요 시 후속(§13 S1).

---

## 18. v4.1 — 정체성·서사 로스터 & 파이프라인 신뢰성 (2026-07-06~07, 구현·배포 완료)

> **계기**: Step3 산출의 "핀트 어긋남" — 죽은 천마를 산 것으로 요약, 놀란 주체·정보 전달자 뒤바뀜, 미등장 인물을 화면 인물로 오귀속, 오류가 프로필에 박제·전파. **조사 결론**: 뿌리는 얼굴 임베딩도 이미지 부재도 아니라 ① 추론 스테이지에 권위 있는 **서사 로스터 부재** + ② 텍스트 스테이지에 **비전 모델(glm-4.6v) 오용**. 손-로스터 주입 시 who-is-who 혼동 7/12→0/6, glm-5.2 자동 로스터가 3웹툰·3장르(화산귀환/아카데미에서 살아남기/참교육) 전부 정확. 조사 경위·실측 수치·가설 반전 이력은 `prd-history.md` §H6.

### 18.1 스테이지·모델 배선 — modality 2-슬롯

- **스테이지 순서: V(컷 비전, 컷당 1콜) → roster(에피소드 로스터 추출, 신설) → R(정체·화자) → N(서사) → apply(결정론)**. roster/R/N은 이미지 없는 텍스트 콜로 에피소드당 3콜(§17.4 상한 충족). roster/R/N은 step3b 한 액티비티 안에서 순차 실행, resolve run 공유.
- **모델 해석**: `resolve_llm_model(webtoon_id, role)`, role∈{`vision`,`text`} — 전역 기본 = `config_llm_model WHERE is_default AND is_active AND supports_vision=(role=='vision')`(modality당 활성 기본 1개 partial-unique 제약). **per-webtoon override(`WebtoonLLMSetting`) 폐기**(전역 전용 — 필요해지면 재도입 논의). role 전용 기본이 없으면 아무 활성 기본으로 강등(시드 전/롤백 안전망). 모델명 하드코딩 없음.
- **시드**: glm-4.6v(vision default) / **glm-5.2(text default)** / qwen-vl(공용 fallback, `fallback` self-FK로 지정). 근거: 텍스트 추론에 비전 모델을 쓰면 반어·욕설·장르 트로프를 사실로 오독(glm-4.6v 로스터 정확도 ~1/7 vs glm-5.2 5/5). qwen-vl은 로스터 추출에서 회차별 불안정 + 2.6~10배 느려 fallback 전용.
- 코드: `llm_resolver.py`(role·전역 도출·fallback 해석) / `step3.py`(V=vision, roster·R·N=text) / `activities.py`(step3a=vision run, step3b=text).

### 18.2 로스터 스테이지 계약 (who-is-who)

- **`extract_roster`**(step3b, R 앞 텍스트 1콜): 에피소드 전체 트랜스크립트(+prior)로 권위 인물 로스터 산출 — `{name, aliases[], present_now, status(생존|사망|불명+근거), role, evidence(컷·원문)}`.
- **프롬프트 규칙**(전부 실측 실패 사례에서 도출): **언급≠등장**(호명·회상만이면 present_now=false), **반어·욕설 오독 금지**("얼어 죽을 X", "너 X 아냐?!"를 정체/생사로 단정 금지), **회상/현재 분리**, **환생/빙의는 aliases로 동일인 묶음**(원래 이름+현재 몸 이름 한 인물, 정보 전달자 조연과 구분), 사망/부재는 status에 근거와 함께.
- **주입**: `episode_roster`를 R·N 페이로드에 삽입 + 두 시스템 프롬프트에 `_ROSTER_GUIDANCE`("present_now=false 인물을 현재 화면 인물/화자로 지목 금지, status 사망/부재 인물을 산 것처럼 다루지 말 것, aliases는 동일인").
- **격리·영속**: 추출 실패 시 빈 로스터로 R/N 진행(run 중단 없음). **영속하지 않음**(`ResolveResult.roster`는 관찰용, apply 미커밋) — 회차 로스터를 다음 회차 prior에 합류시킬지는 오픈(§18.8-4). usage는 `stage='roster'`로 콜당 1행.

### 18.3 max_tokens 정책 — 기본 미전송 (§9.4의 고정 하한 대체)

- 고정 max_tokens(하한 16384든 상향 100k든)는 실전 결함 2건: 입력 큰 회차에서 입력+출력 > context_window → **400 ContextWindowExceeded**, 반대로 작으면 **조기절단(finish='length')**(추론형 모델은 reasoning이 예산 선소모).
- 게이트웨이(vllm) 실측: **max_tokens 미전송 시 남은 컨텍스트만큼 자동 허용 + 자연 종료(finish='stop')**. 따라서 `llm_client._resolve_max_tokens`는 **기본 None(미전송)**. `params.max_tokens` 명시 시에만 cost cap으로 쓰되 `params.context_window` 기준(입력 추정+마진)으로 축소해 400을 막는다. `step3._stage_ctx`는 temperature만 clamp.
- **DB 규칙**: `config_llm_model.params`에 `context_window`만 설정(glm-4.6v=32768, glm-5.2/qwen-vl=131072 — 게이트웨이 실측), `max_tokens`는 비움. **prod 적용 확인됨(2026-07-07)**.

### 18.4 강건성 — json-repair / 런타임 fallback / heartbeat

- **json-repair 파싱 폴백**(`llm_client._parse_json_content`): strict `raw_decode` 실패 시 `json_repair.repair_json`으로 콤마/괄호 누락·미이스케이프 따옴표 등 경미한 손상 복구 — finish='stop'인데 깨진 JSON으로 컷이 통째 스킵되던 문제 해소(실측: ep4 cut80 미이스케이프 따옴표를 5블록 손실 없이 복구). 라이브러리 부재 시 자동 비활성. 의존성 `json-repair` pyproject/uv.lock 추가(이미지 rebuild 필요).
- **런타임 fallback**(`call_llm_json`): primary 재시도(10회) 소진 시 `ctx["fallback"]`(resolver가 self-FK 1홉 해석, 순환 방지·비활성 무시) 모델로 1회전 더 — glm-5.2/glm-4.6v → qwen-vl. 폴백 모델명은 DB로만 지정(코드 하드코딩 없음).
- **Temporal heartbeat**(`activities._run_with_heartbeat`): 실제 작업을 서브스레드에서 돌리고 액티비티 본 스레드가 30초마다 heartbeat — step3b(roster/R/N, 대사폭탄 회차 콜 ~14분 실측)와 step3c(대용량 apply)를 감싸 heartbeat_timeout(10분) 초과→재시도 무한루프를 방지.

### 18.5 CCIP 매칭 — 과병합(magnet)·과분할(파편화) 수정 + 재실행 검증 절차 (트랙 C)

> **⚠️ v2로 대체됨(2026-07-07)**: 아래 v1(앵커캡+무조건 마진)은 배포·검증 결과 **과분할 자기강화**를 일으켰다 — 마진 룰이 "2등=다른 인물"을 전제해, 같은 인물이 중복 클러스터로 쪼개진 순간 1·2등 모두 그 인물이라 영구 기각→파편 무한생산(실측: 876얼굴→773클러스터, 수락률 ~12%, 주인공 얼굴 1개짜리 클러스터 710개 → "청명 19명" 사건의 공급원, §19). **v2 (실험 근거로 채택·구현됨, 배포 대기)**:
> - **통계량**: 인물별 min → **가까운 top-k(기본 3)개 diff의 평균**(env `CCIP_MATCH_TOPK`, 1=옛 min 롤백). 근거: 0.16은 애초에 avg-linkage(평균) 기준으로 캘리브레이션된 값인데 min(N이 클수록 요행으로 하락)에 적용돼 magnet이 생겼던 것 — 통계량을 캘리브레이션 의미에 맞춤. 전체 평균은 외형 멀티모달(거지꼴/도복)에서 손해라 top-k 평균 채택.
> - **마진 룰(면제형)**: 2등도 threshold 이내면 "중복 클러스터 경합"이므로 마진 없이 1등 배정 + 근소 차(<margin)면 `ambiguous_with` 신호 → **step2가 `suggestion(type=merge)` 자동 발행**(episode_id NULL — apply의 에피소드 스코프 재적재에 안 쓸림, 웹툰 내 쌍 dedup). 2등이 threshold 밖일 때만 기존 마진 보류 유지(경계 얼굴 보호).
> - **threshold: 0.12** (top3평균 기준 — min 기준 0.16과 스케일이 다름). DB 변경은 admin 수동이 아니라 **service 마이그레이션 `0027_ccip_threshold_topk_mean`**(0.16→0.12, 가역)으로 — 배포 시 자동 적용돼 누락 불가. ⚠️ data-pipeline v2 배포와 세트(한쪽만 반영된 과도기 산출은 wipe로 폐기 전제).
> - **실험 근거**: Chroma 실 feature 873개로 증분 매칭 오프라인 재현, 통계량 3×마진 3×threshold 8 = 72조합. 현행(min+strict@0.16)은 시뮬 750 클러스터로 prod 773 재현(시뮬 검증). strict 마진은 모든 조합에서 631~780 클러스터(구조 문제 확증). 채택안(top3평균+exempt@0.12): 67 클러스터, F1@16 0.587(최고), 주인공 파편 7 — 잔여 파편(외형 모드 단위 2~3개)은 human 병합으로 수습(사용자 승인). 하니스 `_margin_sim.py`(throwaway), 리포트 artifact "margin-sim-report".
> - 코드: `matching.py`(`_find_match_ccip` v2) + `step2.py`(`_record_merge_candidates`) + `tests/test_matching_topk.py`(판정 규칙 7케이스 고정).

**아래는 v1 기록(경위 보존용) — 진단·검증 절차는 여전히 유효:**

- **진단(실측)**: 화산귀환 char7 blob(207얼굴, 주인공 165보다 많음, 전 7화 고른 분포)의 내부 pairwise CCIP diff median **0.206**(=서로 다른 인물 수준) — **CCIP 임베딩 자체는 정상**(다른 인물 medoid 간 0.231 분리, 동일 인물 코어 <0.10대). 근본은 **매칭 magnet**: 앵커 무제한 누적 + greedy 1-NN이라 얼굴 많은 인물의 acceptance 영역이 커져 비슷한 얼굴을 다 빨아들임.
- **수정**(`matching.py`, env 튜닝·하드코딩 없음): ① **앵커 캡** — `load_ccip_anchors`가 인물(appearance)당 conf 상위 K개만(기본 12, env `CCIP_MAX_ANCHORS_PER_APPEARANCE`, ≤0이면 무제한=옛 동작). ② **마진 룰** — `_find_match_ccip`가 appearance별 최소 diff를 구해, 최근접이 threshold 이내 **AND** 2등(다른 인물)보다 margin(기본 0.03, env `CCIP_MATCH_MARGIN`) 이상 가까울 때만 확정, 애매하면 신규 보류(오병합보다 안전). `ccip_compare`가 전체 diffs를 반환해 model-api 변경 불필요.
- **데이터 복구는 안 함(사용자 결정)** — HITL 재배정 대신 배포 후 **화산귀환(webtoon_id=17) wipe→전량 재실행**으로 검증.
- **검증 절차**: ① 선행 — 배포 워커가 새 matching.py를 쓰는지 확인, env 2종을 `proxmox-configuration` pipeline configmap에 노출(무배포 튜닝), (선택) `config_embedding_model` threshold 0.16→0.13 스윕. ② wipe — service admin "분석데이터 초기화하기"(`reset_analysis_action`: DB 분석분+R2 face crop+Chroma 컬렉션 삭제, 컷까지 지워지므로 **phase1부터 전량 재실행** 필요). ③ 판정 쿼리(읽기전용) — 인물별 step2 얼굴 수(한 인물이 주인공보다 많고 전 회차 고르게 깔리면 magnet 재발) + 최다 인물의 에피소드별 분포(실제 등장 arc를 따라야 정상). ④ CCIP 실측 — `_ccip_bleed.py`로 최다 클러스터 내부 median diff **<0.16**(단일 인물 수준)이면 성공(`dghs-imgutils` 로컬 설치, `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python` 필요 — 절차 상세는 스크립트 주석과 `prd-history.md` §H6). ⑤ 튜닝 루프 — blob 재발/과분할 시 K 8~16, margin 0.02~0.05, threshold 0.13~0.16 스윕. 목표는 완벽 분리가 아니라 "char7급 blob 소멸 + 주요 인물 대략 분리"(대인/거지 구간은 본질적 모호).
- **진행 상태(2026-07-07)**: **wipe는 이미 실행됨** — 화산귀환 run이 118부터 새로 시작(구 run·컷 전부 삭제), phase1 재실행이 **ep1~3까지** step1/step2/step3 완료(컷 146/117/100 재생성). **ep4 이후는 컷 0개**(step1 미도달). ⚠️ **재실행에 쓰인 배포 이미지가 `bfd83f5`(matching.py 앵커캡/마진)를 포함하는지 미확인** — 미포함이면 ep1~3 재실행은 구 매칭으로 돈 것이라 트랙 C 검증으로 무효, 이미지 확인 후 필요 시 재차 wipe→재실행. 판정 쿼리(③④)는 이미지 확인 후 실행할 것.

### 18.6 테스트 오라클 — 화산귀환 정본 인물 로스터 (사용자 확정, 2026-07-06) ⭐

향후 정체성/화자/서사 품질 회귀의 **정답 기준**(naver/769209 초반부):
- **청명** — 주인공. 대화산파 13대 제자·매화검존. **천마를 죽이고 100년 후 환생**. 환생한 **거지 소년 몸의 원래 이름이 '초삼'**(거지판에서 청명은 초삼으로 불림) — 즉 **청명 = 초삼**(동일 인물, 몸의 이름).
- **구칠** — 거지. 청명에게 몸의 원래 이름(초삼)과 천마의 죽음을 알려준 **정보 전달자**.
- **왕초, 종팔** — 거지 무리(구칠과 같은 걸개 패거리).
- **운암** — 화산파 **제자**(장문인에게 보고, 처소/입관식 준비). 장문인 아님.
- **장문인** — 화산파 수장. ep2 말~ep3에서 청명의 입문 허락.
- **천마** — 마교 교주. **100년 전 청명에게 죽음**(회상 속 인물, 현재 시점 미등장. ep2/ep3엔 언급조차 없음).
- 청진 — 전쟁 중 실종된 화산 무인. ep3에서 청명이 "청진 후손" 거짓 각본에 이용(미등장).

핵심 함정: 환생 트로프(청명=초삼 동일인, 구칠과 혼동 금지), 천마 생사(사망), 운암≠장문인, 회상/현재 분리.

### 18.7 적용 상태 (2026-07-07 prod 실측)

- 마이그레이션 `0024`(모델 스키마: fallback FK+modality 제약, WebtoonLLMSetting 삭제)·`0025`(glm-5.2 시드)·`0026`(LLMUsage stage에 roster) 적용 확인. `config_llm_model` params가 context_window만 보유(max_tokens 제거) 확인.
- **`stage='roster'` 콜이 2026-07-06 15:57 KST부터 prod 적재 중** — 로스터 포함 빌드(커밋 `a5c777c` 이상) 배포·가동 확인.
- **미확인**: 이후 커밋 `cc27a69`(max_tokens 정책)·`bfd83f5`(heartbeat+**matching.py 앵커캡/마진**)·`b29ab87`(이름 제안 중복 방지)까지 포함된 이미지인지 — DB만으로 판별 불가, **재배포로 최신화 필요**(특히 트랙 C 검증 전 matching.py 배포가 선행조건). CCIP env 2종의 configmap 노출도 미확인.
- litellm 게이트웨이: 응답(response)은 `LiteLLM_SpendLogs`에 적재(길이 초과분 절단 — 수용, 앞/뒤는 남음). 요청 프롬프트는 **litellm UI에서는 보이지만 DB에는 영속되지 않음**(2026-07-07 실측: 최신 콜도 `messages={}`, `metadata.proxy_server_request`도 빈 값) — SQL로 과거 프롬프트 복구는 불가, 요청 확인은 UI로. UI 표시가 프록시 재시작 후에도 유지되는지 미확인.

### 18.8 백로그 (미착수 — 우선순위·설계 논의 후 착수)

1. **검출 위생 — 흐름과 안 맞는 텍스트/얼굴 정리** (구 §5.6 확장):
   - (a) **깨진 효과음 OCR**(예: '하아…'→'Sror', 'Sof', '스록' 같은 정체불명 노이즈): LLM이 `type='other'`로 분류는 하나 파이프라인이 제외를 커밋하지 않음. `other` 일괄 제외는 과삭제(정상 효과음 원문 포함) — 자모 깨짐/라틴 노이즈 비율 또는 LLM garbage 플래그 등 **별도 판정 신호** 필요.
   - (b) **파편 텍스트 region dedup(2026-07-07 신규)**: OCR이 글자 획 일부를 별도 region으로 뽑는 사례 — 예: 'ㅏ'에서 '-'를 별도 검출. 파편 bbox가 원 region bbox와 IoU상 거의 완전히 겹치는데도(사실상 포함) 별도 region으로 살아남아 `type='other'`로 커밋됨. garbage 텍스트 판정과 별개로 **bbox 포함관계(IoU/containment) 기반 dedup·제외**가 필요.
   - (c) **오탐 얼굴**(예: conf 0.435 비얼굴): model-api `FACE_CONF_THRESHOLD=0.3`뿐, 파이프라인 추가 하한 없음. 경계값이 애매해 자동 하한보다 **HITL 위주** 권장.
   - 삭제 방식: **도메인 플래그**(텍스트=`is_excluded`, 얼굴=`is_used=false` — HITL UI에 계속 보이며 가역) > `deleted_at`(전역 숨김이라 복구 어려움).
2. **character_claim 생사 평탄화 방지** (구 §5.3): 생사류 사실을 "누가·언제 그렇게 주장"+근거 인용으로 커밋해 장르 트로프가 명시 사실을 덮지 않게. glm-5.2 전환으로 급성 문제는 해소돼 후순위.
3. **트랙 C 재실행 검증**(§18.5) — 배포 확인 후 실행.
4. **로스터 영속/prior 합류**: 회차 로스터를 저장해 다음 회차 prior에 합류시킬지(현재 인메모리 전용). 정본 인물은 여전히 `analysis_character`.
5. **step3의 컷 0개 게이트 부재(2026-07-07 실사고)**: step3 체인은 `is_downloaded`만 확인하고 step1 산출(컷) 존재를 게이트하지 않음 — wipe 후 step1이 아직 안 돈 ep4(1101)에 step3 범위 체인이 돌아 **빈 트랜스크립트로 roster/R/N 실행 + "컷 데이터가 제공되지 않아…" 리포트를 succeeded resolve run(132)으로 커밋**. succeeded run이 생기면 자동(unbounded) 체인이 그 회차를 "완료"로 건너뛰므로 **자연 치유 안 됨** — ep4는 step1/2 후 step3 명시 재실행 필요. 근본 대책(컷 0이면 skip/fail 게이트) 논의 필요.
6. 이월: 재해소 Temporal 자동 트리거(§11.2), Stage N 로컬 16K 절단 실측(§13 S1), `LLM_MAX_CONCURRENCY` 실측(§13 R4'), `HF_HUB_OFFLINE`(§13 R1), 죽은 코드 정리(§13 R5).

### 18.9 새 세션 주의사항

- 사용자 워크플로: **논의 먼저, 코드 수정은 명시 승인 후.** 스키마/설계 변경은 PRD에 결정 기록이 선행된다.
- 진행도/stale은 컬럼이 아니라 도출(§17.1) — "분석 됐나"는 `analysis_run` 조회.
- 모델 추가/교체는 DB(`config_llm_model`)로만 — params에는 `context_window`만 넣고 `max_tokens`는 비울 것(§18.3). 이미지 필요하면 vision 슬롯, 텍스트 전용이면 text 슬롯.

---

## 19. 캐릭터 병합 시맨틱 (2026-07-07 확정) — undo 없음, FK 이관 + Chroma 재투영

> **계기**: 화산귀환(webtoon 17)에 이름 "청명"인 캐릭터 19명(18명 is_confirmed=True) 발생. 원인 3겹: ① Step2 CCIP 과분할(876 얼굴→773 캐릭터, 710개가 얼굴 1개 — §19.4) ② service의 name 제안 수락이 동명 존재를 확인하지 않고 rename+승격+확정 ③ `_merge_characters`가 얼굴만 옮기고 화자/프로필/claim/Chroma를 이관하지 않으며 확정 캐릭터는 흡수 불가(청명끼리 병합 자체가 안 됨). 구현 대상은 전부 `service`(webtoonmoa UI 일부).

### 19.1 원칙 (사용자 확정)

1. **병합은 비가역(undo 없음)** — merge log/이벤트소싱을 만들지 않는다. 정정 수단은 역병합이 아니라 **또 다른 human 판단**(face reassign 재배정). 근거: 콘텐츠 도메인 불가침 + 분석 산출은 재생성 가능(§17.1) + human 레이어는 upsert 덮어쓰기 가능이라 "복구 불가능한 손실"이 구조적으로 없음.
2. **물리 삭제 금지** — 밀려나는 human 노동분(absorbed의 human 프로필 등)은 soft-delete로 보존("롤백은 없지만 증거는 남는다").
3. **즉시 이관 = FK 컬럼 + Chroma 메타. jsonb 산출(character_timeline 등)은 이관하지 않는다** — stale 마킹→재해소가 재생성(v4 쓰고-버리기 유지).
4. **Chroma는 파생물** — 병합 DB 커밋 후 메타 재투영을 시도하되 **실패해도 병합은 성립**(§16 전제상 실패 정상). 정본은 Postgres face_identity. 수습은 2단: ①병합 인라인 갱신(옮긴 doc만, 뷰 안에서 동기) 실패 시 **웹툰 단위 재투영 celery 태스크 자동 큐잉**(`fallback_task_id`로 응답 노출, 태스크는 실패 컬렉션 잔존 시 지수 백오프 1→16분 최대 5회 재시도 — Chroma 복구되면 자가 수습) ②admin 액션 "Chroma 메타 재투영"으로 수동 실행(과거 병합 드리프트 소급 수습). 구현: `service.chroma_reproject.reproject_webtoon_chroma`(동기 함수, API/admin 직접 호출 가능) + celery `tasks.reproject_chroma_metadata`(운영 액션은 management command가 아니라 admin/celery 경유가 이 레포 관례). 재투영은 멱등.

### 19.2 병합 동작 (`_merge_characters` 확장)

- **확정(is_confirmed=True) 캐릭터도 흡수 허용** — human 명시 병합은 최신 human 판단이므로. 흡수된 캐릭터는 종류 불문 soft-delete.
- **FK 이관**(병합 트랜잭션 내 old→primary UPDATE): `analysis_text_annotation.speaker_id`, `analysis_character_claim.character_id`.
- **프로필**: absorbed의 llm 행 soft-delete(다음 재해소가 primary 기준 재생성). human 행은 primary에 활성 human 행이 **없으면** 내용을 primary로 이전, **있으면 primary 우선** — absorbed 것 soft-delete 보존.
- **이름/aliases 합류**: absorbed의 이름(비어있지 않고 primary와 다르면)과 aliases를 primary.aliases에 합집합 — 이름 정보 유실 방지(`_find_character_by_name`이 aliases도 매칭).
- **Chroma 메타 재투영**: 옮긴 detection들의 `chroma_doc_id`를 embedding_model별 컬렉션으로 묶어 v2 REST update — `character_id/appearance_id/appearance_label/character_name/is_confirmed`를 primary 기준으로. 실패 시 로깅+응답에 상태 표기.
- suggestion(absorbed 참조 pending)은 soft-delete 유지(의미 소멸), 컷 human_modified 마킹 유지.

### 19.3 name 제안 수락 = 동명 존재 시 병합

`SuggestionDetailAPIView` 수락 시 동명(이름/aliases, kind=character) 캐릭터 존재 확인 — 없으면 현행(rename+승격+확정), **있으면 그 캐릭터로 병합**(§19.2 재사용, 응답에 merged_into). "이 클러스터는 청명" 수락의 실질 의미가 병합이므로. UI는 수락 전 "기존 ○○로 병합됩니다"를 표시(제안 응답에 existing_character 동봉). 동명이인 실존 웹툰은 드묾 — 필요 시 별도 유지 선택지는 후속.

### 19.4 범위 외 / 연계

- ~~Step2 margin 자기강화 과분할~~ → **§18.5 v2로 해결(2026-07-07)**: top-k 평균 통계량 + 면제형 마진 + threshold 0.12 + step2 merge 제안 자동 발행. 구현·테스트 완료, 배포 대기(배포 시 DB threshold 0.16→0.12 변경 필요). CCIP env 3종(`CCIP_MAX_ANCHORS_PER_APPEARANCE`/`CCIP_MATCH_MARGIN`/`CCIP_MATCH_TOPK`)의 configmap 노출은 여전히 미완(코드 기본값으로 가동).
- 기존 청명 19명의 실제 정리는 **트랙 C 재wipe 결정 전 보류**(human 병합 노동이 wipe로 증발).
- 재해소 자동 트리거(§18.8-6)는 여전히 범위 외 — 병합 후 stale 해소는 수동 CLI.
