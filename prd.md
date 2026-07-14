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
- **(2026-07-14) speaker_resolution 차분(diff) 계약으로 재개정**: 전수 테이블이 출력 비대의 주범이 됨(화산귀환 실측 — R 1콜 completion 26k~65k tok, 65,536 캡 도달 `finish=length` + json-repair 의존, 50tok/s에서 콜당 20~37분). v3.6이 두려워한 "확신 블록 미출력 시 화자 유실"은 그 사이 **provisional 화자 승격**(§9.6)이 생겨 더 이상 성립하지 않으므로, 계약을 뒤집는다: **생략=spk_cid 승인**(승격 경로가 확정), 출력은 ①hint 없는 블록(필수 판정) ②교체 ③명시 null(판단 불가 — null 행이 승격을 차단)만. v3.6의 안전성은 유지하되(null 판정 존중 §9.6 그대로) 출력이 확신 블록 수만큼 줄어든다. Stage N 입력은 종전부터 R 미언급 블록에 provisional 폴백이라 무변경.
- **(v3.6) deception 판정 규칙 강화**: "다른 인물을 속이려는 의도가 있는 speech"만 — monologue/혼잣말/자조/한탄/수사적 표현은 deception 아님(속일 상대 부재. ep2에서 독백 "운명의 신이 날 조롱하는 기분"이 deception으로 오판된 실사례). episode.summary/appeal_point는 narration·실제 컷 사건에만 근거, 근거 없는 낙인·평가어("분탕", "배신자" 등) 금지.

### 9.6 Pass-2b — 결정론적 커밋 & 소급 전파 (LLM 없음, Step4 흡수 지점)
- Pass-2a의 최종 이름/화자 테이블을 **LLM 호출 없이** 결정론적으로 에피소드 전체에 투영: `TextAnnotation.speaker`+`resolution_status=resolved` 커밋, 이름 테이블을 character_id 키로 전 컷에 **소급(backward) 투영**.
- **(v3.6) provisional 화자 승격 안전망 (범위 정정 2026-07-13, diff 계약으로 정식 경로 승격 2026-07-14)**: Pass-2a가 **아예 언급하지 않은** speech/monologue 중 Pass-1이 영속한 provisional `speaker_id` 보유 블록만 그 화자로 resolved 승격 — 2026-07-14 R 출력이 차분(diff) 계약(§9.5)이 되면서 이 경로가 안전망이 아니라 **정식 확정 경로**다(생략=승인). 단 **R이 명시 판정한 블록(character_id=null "판단 불가"·무효 id·저신뢰)은 승격에서 제외**하고 provisional(unresolved)로 유지한다 — R의 에피소드 전역 맥락 기반 부정 판정(mis-ID distrust 포함)을 컷 단독 추정이 되덮지 않게(종전 구현은 이 구분 없이 전부 승격해 R의 null 판정이 무시되는 결함이 있었음).
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
`EpisodeChainWorkflow`의 step3 단계가 `step3a_extract`(STEP3_QUEUE, 컷 루프, 멀티모달, heartbeat, `start_to_close=2h`) → `step3b_resolve`(에피소드 텍스트 해소, 윈도우, `1h`) → `step3c_apply`(결정론 커밋, `15m`) 순으로 실행된다. 단계 간 데이터는 activity 반환값/입력으로 스레딩(step3a의 `ExtractResult` dict → step3b 입력, step3b의 `ResolveResult` dict → step3c 입력). `phase3_enabled` 게이트 그대로. **동시성(2026-07-13 개정)**: STEP3_QUEUE는 동시성 2 + 액티비티 진입 시 **webtoon_id별 프로세스 내 락**(`activities._webtoon_serialized`, replicas=1 전제 — 늘리려면 pg advisory lock으로 교체) — 같은 웹툰의 step3류 작업(체인 step3a/b/c·regen reresolve/profile·정리 패스 심판)은 직렬화되고 **다른 웹툰끼리만 병렬 2**. 락 대기 중에는 heartbeat 유지(대기 슬롯 점유는 감수 — 경합 시 종전 동시성 1 수준으로 강등될 뿐). step3c 최종 실패(재시도 소진) 시 워크플로가 resolve run을 failed로 닫는다(`mark_resolve_run_failed`, running-가드 — running 좀비 방지). ⚠️ 락 대기 시간은 액티비티 start_to_close에 포함된다 — 심판(수 시간)이 락을 쥐면 대기하던 step3a가 2h 타임아웃→attempt 취소(CancelledError)→재시도로 이어질 수 있으며 **무해**(2026-07-13 첫 정리 패스에서 실전 관측, 재시도가 락 획득 후 정상 진행). **우선순위(2026-07-14 개정)**: 락 대기열은 **정규 체인(step3a/b/c) > regen/정리 패스**, 같은 순위는 FIFO(`_WebtoonLock` — Condition+heap, 취소 시 대기 항목 정리로 락 유실 방지). 근거: 2026-07-13 화산귀환 16h 백로그에서 신규 ep7이 regen 뒤에 10시간 밀림 — regen/정리는 소급 정정이라 늦어도 무해, 신규 회차 진행이 항상 먼저다.

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

### 11.4 수동 정정 운영 가이드 (2026-07-12, 화산귀환 조걸↔윤종 스왑에서 정립)

- **두 캐릭터 이름이 서로 바뀐 경우(스왑)**: 병합 금지(별개 실존 인물) — **이름 맞교환**이 정답. 수동 rename(PATCH)은 §19.3 동명-병합 로직을 **타지 않으므로**(그건 제안 *수락* 경로 전용) 순간적 동명 중복은 무해하나, 그 사이 resolve가 돌지 않게 할 것. 수동 rename은 is_confirmed=True 동결. 이름 교환 후 **각자 "프로필 재생성"**(프로필 본문이 서로의 서사로 쓰여 있음). 과거 회차 리포트의 옛 이름 문장은 재해소 때 자연 재생성.
- **잘못 들어온 얼굴인데 누군지 모를 때** 3옵션:
  1. **재등장 예상 인물 → 새 캐릭터 생성 후 이동(추천)**: 이름은 인상착의 서술명(시스템 관례 — '복면 노인', '현지 거지 두목'). CCIP 앵커가 생겨 이후 회차 얼굴이 자동 귀속, 실명 확인 시 rename 1회.
  2. **일회성 잡얼굴 → 재배정 대상 비우기(미배정)**: human FaceIdentity(appearance=NULL) = "인물 아님/미배정" 판단. ⚠️ human 레이어 동결 — 이후 자동 재매칭 안 됨(중요해지면 수동 재배정).
  3. **얼굴 오탐 → is_used=false** (§18.8-1c).
- **미배정 얼굴 UI**: 캐릭터 상세에 미확인/확인됨/이동됨과 별개의 **"미배정" 섹션**(2026-07-12 신설 — service `human_unassigned` 필드 + webtoonmoa 섹션, 커밋 `8d893d4`/`21d1422`). 주의: 마스터 '미배정' 목록(WebtoonUnassignedFaces)은 여전히 "누구에게도 배정된 적 없음" 기준이라 human-미배정 얼굴은 안 나옴(구 캐릭터 상세의 미배정 섹션에서 보임) — 통합은 필요 시 후속.
- **LLM 가동 중 수동 병합/이동 가능 여부 (2026-07-13 정리)**: 시스템은 안 깨진다(human 동결·soft-delete FK·apply valid_ids 스킵·수락 API의 소멸 제안 skip). 단 마찰 3종 — ①apply가 그 에피소드 pending 제안을 delete-reinsert하므로 보던 제안/심판 배지가 사라질 수 있음(수락 no-op) ②resolve 진행 중 병합하면 해당 회차 apply가 옛 cid 스냅샷 기준이라 일부 커밋 스킵/구식 — 자연 치유 없음, 그 회차 재해소 필요(§11.4 "그 사이 resolve가 돌지 않게" 원칙과 동일) ③병합 수락→profile 재도출, face_reassign 수락→reresolve 자동 훅이 웹툰 락을 놓고 체인과 경쟁해 진행이 느려짐 — **2026-07-14 완화**: 자동 훅 reresolve는 텍스트 전용(~25~35분/회차, §20.3 개정), 정리 패스 수락분은 웹툰 단위 배치로 중복 제거(§20.9), 락 대기열에서 체인이 우선(§9.9 개정)이라 신규 회차는 밀리지 않는다. **권장: 큰 수술은 체인 idle 때 몰아서, 급한 정정은 "그 회차 나중에 재해소" 전제로만.**

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
| R4' | `LLM_MAX_CONCURRENCY`를 1보다 올려도 되는지 vllm 동시호출 허용치 실측 | ✅ 운영 10으로 상향 가동 중(2026-07-13 확인) |
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
| 2026-07-14 | **regen 증폭 구조 개선 5종 (16h 백로그 실사고 대응, 두 레포 구현 — 미배포)** — 계기: 07-13 정리 패스 수락 9건 → 캐릭터별 regen 8개 → 같은 회차 2~4회 중복 재해소(rerun_extract=True ~1.5h/회차) × 웹툰 직렬화 락 = 16시간, 신규 ep7/ep8 밀림 + 재해소가 만든 pending 33건을 다음 심판이 또 수락하는 자가 증폭 루프 노출. ①**배치 regen**(§20.9): 심판 수락 훅을 hook_collector로 모아 캐릭터별 coalesce → `RegenerateBatchWorkflow` 1개가 등장 에피소드 **합집합을 1번씩만** 재해소 ②**자동 훅 reresolve 텍스트 전용화**(§20.3 개정): `_invalidate_llm_speakers`(연루 캐릭터[from+to] 옛 llm 화자 에피소드 스코프 리셋)가 hint 재주입을 대체 — rerun_extract=True는 수동 버튼/admin 깊은 모드로만(§20.5 실측 근거: True 추가 이득 ~5% marginal) ③**수렴 가드**(§22.6): regen 산출 run에 stats.origin='regen' 마킹, 심판이 그 pending 자동판정 제외(루프 사이클 1 차단) ④**락 우선순위**(§9.9 개정): 정규 체인 > regen/정리, 같은 순위 FIFO(`_WebtoonLock`) ⑤**Stage R 화자 차분 계약**(§9.5 재개정): 생략=spk_cid 승인(승격이 정식 경로) — completion 26k~65k tok·65,536 캡 도달·콜당 20~37분 해소. 합산 효과 추정: 동일 시나리오 16h→~2h. 검증: smoke_test(배치·깊은 모드·no-op) + 락 단위테스트(우선순위/FIFO/취소) 통과. ⚠️ 배포 시 in-flight regen 워크플로 Terminate 필요(§20.9) | §9.5, §9.6, §9.9, §11.4, §17.4, §20.3, §20.9, §22.3, §22.6 |
| 2026-07-13 | **재실행 품질 조사 + CCIP 재실측 + 정리 패스 첫 실전 (2차)** — ①품질 저하 신고('봉방' 요약·초삼 미검출) 조사: 프롬프트/모델 무변경 확인, 원인=OCR 파편 '봉방'의 vision 1콜 오판이 로스터→prior로 증폭(§18.8-8 가드 백로그화) + 재실행이 ep2 시점의 중간 상태였던 것 ②시뮬 재실측(687얼굴): **0.12×topk3 유지 확정, threshold 하향은 파편만 증가 — wipe 불필요**. 잔여 혼합의 실체=feature 판별력 한계(천마)·이름 바인딩 오류(운암)·외형 모드 파편(청명) → human 몫(§18.5 재검증) ③정리 패스 첫 실전 E2E 성공(run 697: 판정 275·자동병합 1·기각 30, mixed 가드 25 차단, §22.5 관찰 완료) ④웹툰 락 실전 검증(심판↔step3a 직렬화, 락 대기 2h 타임아웃 재시도는 무해 — §9.9) ⑤LLM 가동 중 수동 병합/이동 가이드(§11.4) | §9.9, §11.4, §18.5, §18.8, §22.5 |
| 2026-07-13 | **PRD-코드 전수 감사 후속 3건** — ①STEP3 동시성 공식화: 동시성 2 + **webtoon_id별 프로세스 내 락**(`_webtoon_serialized`, 같은 웹툰 직렬/다른 웹툰끼리만 병렬 — 심판↔apply sid 경합·동명 승격 TOCTOU·run supersede 상호덮기 차단, replicas=1 전제) ②**승격 안전망 범위 정정**: R이 명시 판정한 블록(null/무효 id/저신뢰)은 provisional 승격 제외 — R의 전역 맥락 부정 판정을 컷 단독 추정이 되덮던 결함 수정(§9.6) ③**step3c run 좀비 수정**: attempt 단위 failed 전이 없이 재시도 소진 시 워크플로가 `mark_resolve_run_failed`(running-가드)로 닫음(§17.6). `LLM_MAX_CONCURRENCY` 운영 10 확인(R4' 종결) | §9.6, §9.9, §17.6 |
| 2026-07-10 | **재도출 프롬프트·스키마 확정(§20.6~20.7)** — ①프롬프트 v3 확정: role=항상적 정체(회차사건 금지), **progression 신규 필드**(변천사 `[{when,change}]` 자유형 — 판타지=스탯/드라마=관계·심경, 두 장르 실측), 장르 addendum 합성(스키마 포크 X, `webtoon.genre` 버킷), 교차인물 혼동 가드 ②모델=DB glm-5.2+self-FK 통일(모델차 아니라 프롬프트차 실증: v2로 qwen=glm급, v3로 glm 최상) ③**캡 제거 필수**(`_commit_profiles` 8/12 슬라이딩윈도우=지식폐기, 질문봇 no-discard·§17.1 "재생성≠휘발"). ④구현순서 §20.7(스키마→재도출워크플로→service훅→webtoonmoa). 구현 미착수 | §20.6~20.7 |
| 2026-07-09 | **캐릭터 재분석(재도출) 설계(§20)** — 계기: webtoon 23 바바리안 제안검토 병합이 역방향으로 돼 에르웬·비요른 프로필 소실(근본=`_merge_characters`가 absorbed llm 프로필 무조건 폐기, 양쪽 다 있으면 손실). ①프로필은 파생물 → 두 프로필 재봉합이 아니라 **원천 근거에서 재도출**(re-derive>stitch 실측; 재생성 모델 **glm-5.2**>qwen3.5-122b) ②프로필=텍스트(대사+장면) 도출, 얼굴 피쳐는 Step2 정체성 전용 ③대사는 speaker_id로 캐릭터에 묶임 → 얼굴 이동해도 자동 미변경, 화자 재귀속은 re-resolve로만(얼굴 교정엔 `rerun_extract=True` 필요 — provisional block이 옛 speaker_id 재주입) ④두 모드(병합→profile 1콜 / 얼굴이동→reresolve) Temporal 워크플로+celery 트리거+자동 훅, 모델은 resolve_llm_model(text) self-FK fallback ⑤LLM 비용 무제한 정책. 즉시 복구는 glm 재도출본으로 완료(미커밋 데이터), 기능 구현 미착수 | §20 |
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
- **LLM 콜**: 스트리밍 호출(Cloudflare 터널 idle timeout 회피) + 전역 세마포어(`LLM_MAX_CONCURRENCY`, 코드 기본 1 — **운영은 10으로 상향 가동 중**, 2026-07-13 확인 → §13 R4' 종결) + primary 재시도 소진 시 **fallback 모델 런타임 전환**(§18.4). max_tokens는 기본 미전송(§18.3).
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
| **R** 정체·화자 | 에피소드당 1 | V 트랜스크립트 + 도감 prior(profile) + confirmed 앵커 | characters(승격 제안 포함), **화자 차분 테이블**(2026-07-14 §9.5 — 생략=spk_cid 승인, 승격이 확정), name/merge/face_reassign/label_conflict → suggestion |
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
- **R/N run 공유**: step3b가 resolve run 시작(roster/R/N usage 귀속), **step3c apply 성공이 succeeded 전이** — "에피소드 step3 완료"의 정본 시각. N만 실패하면 화자 데이터 유지+서사 필드만 빈 값(실패 격리). **(2026-07-13)** step3c는 attempt 단위로 failed 전이하지 않고(재시도 성공 시 failed↔succeeded 왕복 방지) 재시도 소진 시 워크플로가 `mark_resolve_run_failed`(running일 때만 전이)로 닫는다 — 종전엔 예외 시 run이 영구 running 좀비로 남았다.
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
>
> **v2 재검증 (2026-07-13, wipe 후 신 재실행 데이터 실측 — threshold 재조정 논의 종결)**: 재실행 ep1~6의 실 CCIP feature 687개로 증분 매칭 시뮬 재실시(threshold 0.09~0.13 × topk 1/3/5, v2 규칙 복제. 하니스 throwaway).
> - **신뢰성**: 0.12×topk3 시뮬 82클러스터 ≈ prod 실측 83 — 재현 확인.
> - **threshold 하향은 손해**: 임베딩 레벨 혼합(클러스터 내부 median diff>0.16)은 **전 구간 0**이고, 내리면 파편만 1.7~2.3배(0.10→142, 0.09→187 클러스터). 올리면(0.13×topk1) 진짜 혼합 발생 시작. → **0.12×topk3 유지 확정, wipe→재실행 불필요 결정.** (참고: P7 v2+CCIP v2 효과 — 687얼굴→83클러스터, v1 시절 876얼굴→773클러스터 대비 대폭 개선.)
> - **잔여 문제의 정체 (threshold로 못 고침, 층위가 다름)**: ①**천마 매그넷**(36얼굴, 내부 median 0.100) — 흑발 성인 남캐들이 이 그림체에서 CCIP 임베딩상 동일인. feature 판별력 한계라 해법은 human `is_match_excluded`(회상 전용 인물이라 실익 손실 없음). ②**운암=장문인 얼굴**(2728) — 클러스터 자체는 균질(median 0.093)하고 장문인과 교차 0.265로 분리 가능한데, **R이 "운암입니다" 대사 근거로 남의 얼굴 클러스터에 이름을 바인딩**한 오류. wipe해도 재발 유형 — human rename/재배정 영역. ③**청명 외형 모드 파편**(본체 103얼굴 중 >0.16쌍 20.6% + 파편 2708 등 교차 0.209) — 예측대로 human 병합/심판 무명→named 영역.

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
6. **이름 없는 얼굴만 CCIP 재배치(2026-07-10 논의, 의견만·미착수)**: step2 소스(비확정) 얼굴 풀만 **현재 confirmed/named 앵커에 재매칭** → 확신 매치면 흡수, 아니면 클러스터 유지(human FaceIdentity 불가침). 효과: 대표 하나만 명명하면 나머지 이름 없는 파편이 그 앵커로 흡수돼 **과분할 청소**. **전제: CCIP v2 매칭(§18.5) 필수**(현행 min+strict 룰로 재매칭하면 magnet/과분할 재생산). **한계**: 외형 모드(측/후면) 파편은 임베딩 거리가 멀어 앵커에 안 붙음 → 여전히 human 병합. **안전**: 자동 흡수는 보수적 threshold+margin, 경계값은 `suggestion`(merge/face_reassign) 경유(§18.5 ambiguous 발행 재사용). **워크플로 위치**: 재도출(§20) **앞단** 정체성 정리 레버, 웹툰 단위 온디맨드 버튼.
7. 이월: 재해소 Temporal 자동 트리거(§11.2) → **§20.4로 설계 정식화(구현 미착수)**, Stage N 로컬 16K 절단 실측(§13 S1), `LLM_MAX_CONCURRENCY` 실측(§13 R4'), `HF_HUB_OFFLINE`(§13 R1), 죽은 코드 정리(§13 R5).
8. **로스터 이름 채택 가드 (2026-07-13 실사고 '봉방')**: 재실행 ep1 cut86의 OCR 파편 '봉방'(왕초 대사 조각이 별도 region으로 검출 — 1(b) 파편 dedup의 실사례)을 vision 콜 1회가 주인공 이름으로 오해석 → `name_evidence` 영속 → **로스터가 몸의 이름으로 채택** → ep1 리포트·프로필·prior로 전파, ep2 로스터도 aliases로 승계(정답 '초삼'은 ep2에서 병기됨). 같은 컷을 처리한 이전 런 4회는 전부 대사로 처리 — 프롬프트/모델 결함이 아니라 **확률적 오판 1회가 로스터→prior 체인으로 증폭**되는 구조 문제. 단일 컷 name_evidence만으로 로스터 이름 채택을 막는 가드 없음 → "이름 채택은 복수 컷 증거 또는 명시적 호명 문맥 필수" 프롬프트/결정론 가드 필요. 이미 커밋된 ep1 리포트는 정리 후 재해소로 수습(§11.2).

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
- 재해소 자동 트리거(§18.8-6)는 **§20.4로 정식화** — 병합→profile, 얼굴 이동→reresolve를 celery/Temporal로 자동 큐잉(구현 미착수). 그 전까지는 수동 CLI(`src.tools.reresolve`).

---

## 20. 캐릭터 재분석 (재도출) — 병합/얼굴교정 후 프로필·화자 재생성 (2026-07-10 설계 확정, **같은 날 구현 완료**)

> **⏩ 재개 anchor**: 설계 확정 + **2026-07-10 구현 완료(세 레포, §20.7 구현 내역) + 같은 날 prod 실측 검증 완료** — ①수동 profile(에르웬 1858: key_facts 38·progression 7·role 항상적, run 363) ②병합 자동 훅 E2E(중복 1862→1858 실병합 → on_commit→celery(lopri)→Temporal 자동 발화, run 367 succeeded) ③reresolve E2E(수정 연합 간부 1864 실 API 트리거: ep9 vision·resolve 재실행(run 370/371 succeeded)→프로필 재도출, run 368 succeeded) ④regen-status API shape 확인. 소소한 알려진 동작: umbrella run 종료 시 stats가 최종 결과로 replace → episodes_done 진행값은 running 중에만 표시(의도 허용). 실측 하니스/결과는 세션 scratchpad(throwaway): `test_profile_regen.py`·`test_qwen_prompt.py`(v2/v3)·`test_drama.py`, 결과 `regen_results.json`·`qwen_prompt_v2.json`·`qwen_prompt_v3ds.json`·`drama_glm_v3.json`, 근거 `evidence.json`, 재해소 스냅샷 `snap_ep13_*.json`. 최종 목표는 **웹툰 분석+질문봇(RAG+tool)** — 분석 산출=지식베이스라 "버리면 안 됨"(§20.6 캡 제거의 근거).

> **계기**: webtoon 23("게임 속 바바리안으로 살아남기") 제안검토 병합이 **역방향**(이름/풍부한 쪽이 absorbed로 흡수)으로 수행돼 에르웬·비요른 프로필이 소실. 근본은 §19.2 프로필 규칙 — `_merge_characters`가 absorbed의 llm 프로필을 **무조건 soft-delete**하고 primary llm만 유지 → **양쪽 다 프로필이 있으면**(llm 프로필은 캐릭터마다 자동 생성되므로 사실상 기본값) 한쪽 정보가 통째로 소실. 얼굴/화자 FK는 이미 생존자로 정상 이관됐으므로 손실은 프로필뿐이고, soft-delete라 내용은 복구 가능.

### 20.1 원칙 — 프로필은 파생물, "재봉합"이 아니라 "원천 재도출"

- 손실 방지의 정답은 두 프로필을 AI로 합치는(stitch) 게 **아니라**, 교정된 정체성 위에서 **원천 근거(귀속 대사·장면 서술·트랜스크립트)로 다시 계산(re-derive)**. §17.1 "분석 산출은 재생성 가능, human 노동분만 불멸"의 직접 적용.
- **실측(2026-07-09, LLM 비용 무제한 전제)**: 에르웬/비요른의 귀속 대사 + 장면 action_summary + 과거 프로필 조각을 전량 주입해 단일 프로필 재생성, **glm-5.2 vs qwen3.5-122b** 비교. **glm-5.2 우월**(에르웬 key_facts 17·비요른 18개, 저장된 두 프로필 **어디에도 없던 사실까지 복원** — 정식명·서사·스탯 등), qwen은 유효 JSON이나 희박(5·6개)·포맷 흠. → **재도출 > 저장 프로필 union 실증**, 재생성 모델은 **glm-5.2** 채택. 하니스는 scratchpad throwaway.
- 결정론적 필드-union은 폐기가 아니라 **재도출 완료 전 0-지연 자리표시자**로만 남긴다.

### 20.2 두 레이어 구분 (오개념 방지 — 핵심)

- **정체성(누구 얼굴이냐)** = CCIP **임베딩(피쳐)** → Step2 매칭. human이 옮기면 human FaceIdentity로 동결(정답 앵커).
- **프로필(성격·사실)** = **텍스트**(귀속 대사 + 얼굴이 등장한 컷의 장면 서술)에서 도출. **얼굴 피쳐(임베딩)는 프로필 생성에 전혀 안 들어간다.**
- **대사는 얼굴이 아니라 캐릭터에 묶인다**: `analysis_text_annotation.speaker_id`(→character)만 있고 detection 링크 없음. 화자는 Stage R이 "이 컷에 누가 있나(얼굴 정체성)"를 보고 결정.
- 귀결: **얼굴만 옮기면 대사(speaker_id)는 따라오지 않는다.** 섞였던 인물의 대사가 옛 화자로 잔존 → 화자 재귀속은 **Stage R 재실행(re-resolve)** 으로만 된다. 얼굴 정리 후 프로필-only 재생성만 하면 옛 화자귀속을 그대로 읽어 **재오염**.

### 20.3 두 모드 (2026-07-14 개정 — 자동 훅 reresolve는 텍스트 전용이 기본)

| 액션 | 모드 | 이유 |
|---|---|---|
| **병합**(동일인 합침) | **profile** (경량 1콜) | 대사 합집합이 전부 정당 → 화자 재해소 불필요 |
| **얼굴 이동/섞임 풀기 — 자동 훅**(제안 수락) | **reresolve** (`rerun_extract=False` + **옛 화자 무효화**) | 교정 얼굴 기준 화자 재귀속 — 연루 캐릭터를 아는 경로라 무효화로 충분(회차당 ~25~35분, True 대비 ~3배 절감) |
| **얼굴 이동/섞임 풀기 — 수동 버튼/admin** | **reresolve** (`rerun_extract=True` 깊은 모드) | 대량 수동 정리 후라 연루 캐릭터 미상(무효화 대상 특정 불가) → Pass-1부터 재실행(~1.5h/회차) |

- **(구) `rerun_extract=True` 필요 근거였던 것(코드 확인)**: `_load_faces`는 현재 face_identity(human>step2)를 fresh 읽으므로 텍스트 전용도 얼굴 자체는 반영하나, **`_load_provisional_blocks`가 옛 speaker_id를 hint(`spk_cid`)로 재주입** + §9.5 diff 계약의 생략=승인 경로로 옛 resolved 값이 잔존 → 섞임 화자가 되살아날 수 있었다.
- **(2026-07-14 개정) 무효화가 비전 재실행을 대체**: `step3._invalidate_llm_speakers` — 재해소 전에 **연루 캐릭터(얼굴을 잃은 쪽+얻은 쪽)에 귀속된 llm 화자를 에피소드 스코프에서 NULL/unresolved로 리셋**(human 동결). hint 재주입·생략=승인 둘 다 원천 차단되고, R이 fresh 얼굴 근거로 재판정한다. "잃은 쪽"은 face_reassign 수락 시점(FaceIdentity 덮기 전)에만 알 수 있어 service 수락 경로가 `invalidate_character_ids=[from, to]`를 훅에 동반한다. §20.5 실측(True의 추가 이득 ~5% marginal, False가 un-mixing 100%)이 이 개정의 근거 — "비용 무제한이면 True 정석"은 회차 1개 전제였고, 훅이 전 회차×중복으로 곱해지는 순간(2026-07-13 16h 실사고) 성립하지 않는다.
- 재사용 자산: `step3.reresolve_episode(rerun_extract=, invalidate_speaker_character_ids=, run_origin=)`, CLI `python -m src.tools.reresolve <src> <title> <no|stale> [--rerun-extract]`.

### 20.4 기능화 계획 (Temporal, 프론트 버튼 → celery — 2026-07-10 구현 완료, §20.7)

```
프론트 버튼 → service API → celery task → Temporal 트리거
  → data-pipeline 워크플로 regenerate_character(character_id, mode)
      profile:   근거(대사 by speaker + 장면 by 현재 FaceIdentity + soft-deleted 흡수분 key_facts) → LLM 1콜 → character_profile(llm) upsert
      reresolve: 캐릭터 등장 에피소드 집합 → 각 에피소드 reresolve_episode(rerun_extract=True) → 화자·프로필·리포트 재생성
```

- **모델**: 하드코딩 없이 `resolve_llm_model(webtoon_id, TEXT)` → DB glm-5.2 + self-FK fallback(§18.3/§18.4).
- **자동 훅**: `_merge_characters` 커밋 후 → 생존자 **mode=profile** enqueue(+union 자리표시자); 얼굴 이동/`face_reassign` 수락 경로 → **mode=reresolve** enqueue.
- **레포 책임**: LLM 로직(프롬프트·모델해석·호출)은 data-pipeline에 두고 **service celery는 트리거만**(중복 방지). service 관례는 management command 금지 → service 함수 + celery task + admin 액션 + 프론트 API.
- webtoonmoa: 캐릭터 관리 버튼(문구는 "재분석"보다 "얼굴 정리 반영 재해소"류로 의미 정합), 진행 상태(run) 표시.

### 20.5 현황 / 잔여

- **즉시 복구(2026-07-09)**: 에르웬(1858)·비요른(1883) llm 프로필을 glm-5.2 재도출본으로 복구(docker exec service ORM, 직접 SQL 없음). ⚠️ 에르웬 1858은 **무명 cluster + 중복 1862** 존재 — 얼굴 정리 후 승격/병합 + reresolve로 깨끗이 덮을 예정.
- **re-resolve 얼굴교정 반영 실측(2026-07-09, webtoon 23 ep13, `rerun_extract=False`, 텍스트 3콜 ~23분)**:
  - **화자 재귀속 완벽(라인 단위 검증)**: 아이나르에 섞였던 에르웬 대사(cut82 '상처가/아저씨 얼른 포션 마시세요' 등)가 **에르웬으로**, 비요른 명령 대사(cut91 '옷을 벗겨라/에르웬')가 **비요른으로**, 분산됐던 에르웬 중복(1862)이 **1858로 통합**. 화자 부착 103→183줄. `_load_provisional_blocks`의 옛 speaker_id hint(conf 0.0)는 현재 얼굴에 덮여 무해 → 텍스트 전용도 un-mixing은 **완벽**.
  - **True(rerun_extract) 비교(같은 ep13, ~1.5h)**: 44줄 상이. True가 **어려운/애매 화자에서 더 정확**(cut84·93 '저 모험가들 시체'→에르웬, cut65 전략독백→비요른, 적 1770을 '방패쟁이'로 명명 — 근거 대조 모두 True 맞음), 대신 약간 보수적(cut98 '포탈이에요!' 에르웬을 미해소로 놓침). **결론: False가 un-mixing 100%+대부분 화자를 잡고, True는 남은 ~5% 애매 화자를 clean provisional+재생성 cut_summary로 더 정확히**. docstring "얼굴 교정엔 True" 실측 뒷받침 — 단 차이 marginal. **비용 무제한이면 얼굴이동 재해소=True 정석**, 빠른 처리면 False가 가치 대부분.
  - **프로필은 re-resolve로 정리되지 않음(중요)**: `_commit_profiles`가 key_facts=append 후 **최근 12개만**(캡 12), personality 최근 8, 스칼라(role)=**최신 회차값 replace**. 즉 프로필은 누적 dossier가 아니라 **최근 12사실 슬라이딩 윈도우**. 귀결: ①union은 오염 사실을 **제거 못함**(추가만) ②단일 옛 회차 재해소는 role을 그 회차 사건으로 **회귀**시킴(ep13 재해소가 1858/1883 role을 ep13 값으로 덮음; 전 회차 순차 재해소면 최신 회차로 끝나 완화). → **프로필 정리는 re-resolve(union)가 아니라 §20.4 mode=profile(전량 재도출·replace)이 정답**임을 실측 확증. 리치 프로필을 원하면 **12/8 캡 상향**이 선결(현행 캡은 임의값 — LLM 비용 무제한 전제와 상충).
  - 정리: **화자=re-resolve(에피소드), 프로필=mode=profile(전량 재도출)** 로 도구가 갈린다는 §20.3 두 모드 설계가 실측으로 뒷받침됨.
- **LLM 비용 무제한**(사용자 확정): 재도출은 비용 절감형(요약 재봉합·저렴 모델·토큰 캡)이 아니라 원천 근거 전량 주입 + 최상위 모델을 기본으로 한다.

### 20.6 확정된 재도출 프롬프트·스키마·모델 (2026-07-10, 실측 확정)

- **모델(확정)**: 전부 **DB `config_llm_model` 값 + self-FK fallback으로 통일, 기본 `glm-5.2`**(재도출 전용 특별 배선 없음 — `resolve_llm_model(webtoon_id, TEXT)` 그대로). *일단 구현하고 성능은 프롬프트로 올린다.* (테스트에서 qwen3.5-122b·deepseek-v4-flash도 썼으나, **모델차가 아니라 프롬프트차**였음이 실증 — v2 프롬프트로 qwen이 glm 동급, v3로 glm이 최상. glm-5.2 context=100k로 충분.)

- **프롬프트 = v3(확정)**, 세 축:
  1. **role = 항상적 정체**(장기 불변: 정체·핵심 목표·서사적 위치). **특정 회차 사건 서술 금지**(예 'n화에서 교전 후 탈출' 금지 — 그건 key_facts). 이유: `_commit_profiles`가 role을 최신 회차값으로 replace해 회차 사건이 role을 오염시켰음(qwen v2 실측). 재도출(mode=profile)은 1콜이라 항상성 유지 가능.
  2. **progression(신규 필드) = 변천사** `[{"when":"회차/국면","change":"무엇이 어떻게 바뀌었나"}]` 연대순. traits(현재값 스냅샷)·key_facts(사실 로그)와 구분되는 **변화 궤적**. **자유형 확정**(구조화 `[{stat,from,to}]` 반대) — 장르 불문 동작 실증: **판타지=스탯/스킬/파워 성장**(비요른 12단계: 아이템레벨+13/정신+1/부상심화), **드라마=처지·관계·심경 변화**(김진현 9단계: 왕따→파산→환생결의→원수 재회). 구조화였으면 드라마에서 텅 빔.
  3. **장르 addendum(합성)** = 유니버설 base + 짧은 장르 스니펫(`_ROSTER_GUIDANCE` 패턴). **스키마는 통째로 포크하지 않음**(traits free-form이 이미 장르중립). `webtoon.genre`(단일 컬럼, 존재) → 버킷: **판타지/게임/무협→스탯·스킬·아이템·파워 성장**, **로맨스/로판→관계·호감·감정선 변화(스탯 만들지 말 것)**, **스릴러/드라마→비밀·처지·심경·갈등·반전(스탯 금지)**. addendum이 progression에 무엇을 담을지 조종.
  4. **교차 인물 혼동 가드**: 다른 등장인물 이름/정체를 이 인물에 섞지 말라 명시 + "이 인물 아님" 목록(로스터/타 캐릭터 이름) 주입. (실측 실패: deepseek가 비요른(얀델 둘째)에 별개 바바리안 '카락'(파눈 셋째)을 본래정체로 오귀속.)
  - **확정 v3 프롬프트 원문(scratchpad는 휘발 — 여기 영속)**:
    ```
    [BASE — 유니버설]
    당신은 웹툰 캐릭터의 모든 근거 자료를 읽고 철저하고 상세한 인물 도감 프로필을 재구성하는 분석기입니다. 한국어. 마지막에 JSON만 출력.
    입력 근거 3종: 1) dialogue(귀속 대사/독백 — speech 거짓·과장 가능, monologue 속마음, narration/system 객관진실) 2) scenes(등장 컷 행동 서술, 객관진실) 3) prior_profile_fragments(과거 조각, 중복·구식 가능).
    [절차] 1단계 전수추출: dialogue·scenes 처음~끝, 모든 개별 사실 빠짐없이 회차(ep)순. 2단계 중복만 병합(서로 다른 사실 생략 금지). 3단계 스키마 정리.
    [role 규칙] role은 항상적 정체·서사적 위치(장기 불변). 특정 회차 사건·상황 금지 — 사건은 key_facts로. 어느 시점에 봐도 성립하는 한 문장.
    [progression] 시간에 따라 변하는 값은 traits로 뭉개지 말고 progression에 연대순: [{"when":"회차/국면","change":"무엇이 어떻게 바뀌었나"}]. traits엔 최근 현재값만.
    [분량] 주요 인물이라 근거 풍부 — key_facts 15+, traits 8+, progression 변하는 값 모두. 요약으로 날리지 말되 근거 없는 창작 금지.
    [교차 인물 혼동 금지] 다른 등장인물 이름·정체·행적을 이 인물에 섞지 말 것(+"이 인물 아님" 목록 주입).
    [출력 JSON] {gender, age_group, affiliation, role(항상적 한 문장), personality[3~6], traits{현재값 8+}, key_facts[연대순 15+], progression[{when,change}]}

    [+ 장르 addendum — webtoon.genre 버킷으로 base에 이어붙임]
    판타지/게임/무협: 스탯 수치·스킬/능력 습득·아이템/장비·정수/파워 시스템·전투력 성장을 빠짐없이. 수치·능력 변화 시점은 progression에 연대순.
    로맨스/로판: 관계·호감/애정 변화·감정선·오해와 화해 중심. 스탯 만들지 말 것. 관계 진전/후퇴는 progression에.
    스릴러/드라마: 비밀·진짜 동기·처지·심경·관계 갈등·반전 중심. 스탯 만들지 말 것. 처지·내면 변화는 progression에.
    ```

- **출력 스키마**: `{gender, age_group, affiliation, role(항상적), personality[3~6], traits{현재값 8+}, key_facts[연대순 15+], progression[{when,change}]}`.

- **스키마 변경(필요)**: `analysis_character_profile`에 **`progression` JSONField(default=list) 추가**(service 마이그레이션). CharacterProfileSerializer/도감 서빙에 노출.

- **캡 제거(필수, 단순 최적화 아님)**: `_commit_profiles`의 `personality[-8:]`/`key_facts[-12:]` 슬라이딩 윈도우는 **지식 폐기** — 질문봇 코퍼스(no-discard 목표)와 §17.1 재해석("재생성 가능 ≠ 휘발, 최신본은 항상 보존")상 **제거/대폭 상향**. `mode=profile`(전량 재도출)은 **무캡 replace**로 쓴다(role/personality/traits/key_facts/progression 전체 교체). 증상: glm 복원 17facts가 다음 apply에서 12로 잘림.

### 20.7 구현 내역 (2026-07-10 구현 완료 — 세 레포)

1. **service 스키마**: `CharacterProfile.progression` JSONField(마이그레이션 0028, **`db_default=[]` 유지** — data-pipeline raw INSERT가 컬럼을 몰라도 안 깨지게/배포 순서 무관) + `AnalysisRunKind.PROFILE`/`LLMStage.PROFILE`(0029). 서빙 `merge_character_profile`(human 우선 병합)에 progression 포함, human patch allowed에도 추가.
2. **data-pipeline 재도출**: 신규 **`src/core/regen.py`** — `regenerate_character_profile(character_id, absorbed_character_ids=)`: 근거수집(유효 대사 by speaker[human>llm] + 장면 by 현재 FaceIdentity[human>step2] action_summary+narration/system + prior 조각[본인 활성 + 흡수 soft-delete분]) → genre 버킷 addendum 합성(v3 프롬프트) → `resolve_llm_model(TEXT)` 1콜 → llm 행 **무캡 replace** upsert(progression 포함). usage stage='profile'. `_commit_profiles` 캡(8/12) **제거**. Temporal `RegenerateCharacterWorkflow(RegenInput{character_id, mode, absorbed_character_ids})`: profile=재도출 1콜 / reresolve=등장 에피소드(얼굴∪화자) 전부 `reresolve_episode(rerun_extract=True)` 순차 **후 프로필 재도출로 마무리**(union 재봉합은 오염 제거 불가 — §20.5). *(2026-07-14 개정: RegenInput에 rerun_extract/invalidate_character_ids 추가 — 자동 훅은 False+무효화, §20.3 개정·§20.9 배치 참조.)* 무거운 액티비티는 STEP3_QUEUE(동시성 2 + 웹툰 단위 락 — §9.9 2026-07-13 개정). 진행표시 정본 = **umbrella run(kind=profile, episode NULL, stats.character_id/mode/episodes_total/episodes_done)** — supersede는 같은 character_id만. smoke_test.py에 오케스트레이션 검증 포함(통과).
3. **service 트리거/훅/API**: `send_regenerate_trigger`(temporal.py, workflow_id=`regen_char_{id}_{mode}` 멱등) + celery `trigger_character_regenerate`(재시도 백오프) + 자동 훅(`transaction.on_commit`): 병합 3경로(수동 merge API·merge 제안 수락·name 수락=동명 병합) → mode=profile(+흡수 id), **face_reassign 제안 수락** → mode=reresolve. 프론트 API `POST /character/{pk}/regenerate/`(202) + `GET /webtoon/{s}/{t}/regen-status/`(kind=profile run 최근 30건) + admin 액션 2종.
   - **결정**: 수동 얼굴 이동(FaceRecordReassign/BulkReassign)은 자동 enqueue **안 함** — 라벨링 세션에서 이동마다 수 시간짜리 재해소가 폭주하는 것 방지. 얼굴 정리 후 프론트 버튼("얼굴 정리 반영 재해소")이 의도된 트리거.
4. **webtoonmoa**: 도감 버튼 2종("프로필 재생성"/"얼굴 정리 반영 재해소"+confirm) + 진행 배지(재해소 중 n/m화, 5초 폴링 — running 있을 때만) + 도감 **변천사(progression) 타임라인** 렌더.

**배포 순서 주의**: service 먼저(0028/0029 migrate) → data-pipeline(regen 코드가 progression 컬럼에 INSERT) → webtoonmoa. 최종 목표(웹툰 분석+질문봇/RAG)는 §20.6 캡 제거의 상위 근거.

### 20.8 supersede 좀비 수정 (2026-07-12 실사고 → 커밋 `de022ce`, 배포 대기)

- **사고**: 청명(1651) 재해소(28ep×~1.5h) 진행 중 같은 캐릭터의 프로필 버튼 클릭 →
  `begin_profile_run`의 supersede가 **mode 무구분**이라 재해소 umbrella run을 failed로 덮음.
  결과: 진행표시는 죽고 Temporal 워크플로는 STEP3_QUEUE(동시성 1)를 ~42h 계속 점유하는 **좀비**
  — 뒤에 줄 선 프로필 run들이 "재갱신 중"으로 무한 대기(프론트는 사실대로 보여준 것).
- **수정 2축**: ① supersede를 **같은 character_id + 같은 mode**로 한정(같은 mode 중복 기동은
  workflow_id 멱등이 이미 차단 — supersede 본 목적은 워커 크래시 잔재 정리).
  ② `regen.run_is_live(run_id)` — 무거운 액티비티(regen_reresolve_episode/regen_profile)가
  작업 전 확인, superseded면 워크플로 **자기 조기 종료**(좀비의 큐 점유 원천 차단).
- **운영 노트**: 수정 배포 전의 기존 좀비는 Temporal UI에서 수동 Terminate. umbrella run이
  running이어도 실제로는 큐 대기일 수 있음(진행표시 ≠ 실행 중 — 대기/실행 구분 표시는 후속 여지).

### 20.9 웹툰 단위 배치 재분석 (2026-07-14 — 정리 패스 수락 실행의 자동 훅 전용)

> **계기(실사고)**: 2026-07-13 저녁 정리 패스가 face_reassign 9건·merge 2건·name 5건을 수락 →
> §20.4 자동 훅이 캐릭터별 개별 regen 8개를 큐잉(reresolve 3: 청명 포함) → 캐릭터별로 등장
> 에피소드를 각자 순회하니 **겹치는 회차가 2~4회 중복 재해소**(ep6 4회·ep5 3회, vision 16 +
> resolve 15 run) × 회차당 `rerun_extract=True` ~1.5h × 웹툰 직렬화 락 = **16시간 백로그**,
> 신규 ep7/ep8이 그 뒤로 밀림.

- **구조**: `execute_adjudication`(service)이 수락 건별 훅을 즉시 큐잉하지 않고
  `hook_collector`로 모아 **캐릭터별 coalesce**(reresolve ⊃ profile — 재해소가 프로필 재도출로
  끝나므로 흡수; absorbed/invalidate는 union) 후 **`RegenerateBatchWorkflow` 1개**를 발화
  (celery `trigger_webtoon_regenerate_batch` → `send_regenerate_batch_trigger`,
  workflow_id=`regen_batch_w{webtoon_id}_{consolidate run id}` — 판정 1회당 1배치).
- **배치 동작**: `regen_batch_begin`(캐릭터별 umbrella run 생성 — 프론트 regen-status 계약
  유지, reresolve run들의 episodes_total=합집합 크기) → reresolve 대상들의 **등장 에피소드
  합집합을 회차순으로 1번씩만** `reresolve_episode(rerun_extract=False,
  invalidate_speaker_character_ids=union, run_origin='regen')` → 항목별 `regen_profile`
  (기존 액티비티 재사용, run_is_live로 supersede 자체 스킵).
- **개별 워크플로(§20.4)는 존치** — 뷰 단건 수락/수동 버튼/admin 경로. 단건 수락도
  텍스트 전용(invalidate 동반), 수동 버튼·admin만 깊은 모드(§20.3 개정 표).
- **효과**: 같은 사고 시나리오 기준 중복 제거(~3×) × vision 생략(~3×) ≈ **16h → 2h 안팎**.
  + 락 우선순위(§9.9 개정)로 신규 회차는 그마저도 기다리지 않는다.
- **배포 주의**: `RegenerateCharacterWorkflow`의 액티비티 인자가 바뀌어(6개) **in-flight
  regen 워크플로는 배포 시 replay 비결정성으로 죽을 수 있음** — 배포 전 Temporal UI에서
  진행 중 regen/consolidate 워크플로 Terminate 권장(§20.8 운영 노트와 동일 절차).

---

## 21. 최종 목표 — 웹툰 분석 + 질문봇 (RAG + tool chain) (2026-07-10, 방향만·상세 미논의)

> **제품 방향(사용자)**: 웹툰을 분석하고 그 위에 **질문봇**을 얹는다. 개인 선호로 **RAG 구축 + tool chain**(모델이 필요하면 RAG 검색을 tool로 호출).

### 21.1 "버리는 건 안 됨" — §17.1 재해석
- 분석 산출(프로필·progression·key_facts·episode_report·thread·claim·화자귀속 대사·cut_scene_meta)이 곧 **질문봇의 지식베이스**다. 폐기하면 지식 손실.
- **§17.1 "분석 데이터 전량 폐기·재생성 가능"은 재계산 경로가 있다는 뜻이지 "휘발해도 됨"이 아니다** — 최신 재생성본은 **항상 보존·인덱싱**. *재생성 가능 ≠ 버려도 됨.* (§17을 읽을 때 이 단서를 함께.)
- 직접 영향: `_commit_profiles` 슬라이딩 윈도우 캡 제거(§20.6), progression·전체 key_facts 영속.

### 21.2 아키텍처 방향 (의견 — 상세 설계 미논의)
- **순수 텍스트 RAG 지양.** 강점은 이미 **구조화된 분석**(프로필/progression/thread/화자귀속 대사/episode_report)을 가진 것. "비요른이 몇 화에서 정수 얻었나" 같은 질문은 **progression 구조 질의**가 벡터 검색보다 정확 → **구조화 tool(스키마 직접 질의) + 시맨틱 RAG 하이브리드**, 모델이 tool chain으로 선택.
- 벡터스토어: **Chroma 이미 배포**(얼굴 임베딩) → 텍스트 컬렉션 추가 또는 pgvector.
- **스포일러/시간 스코프**: 독자용이면 "N화까지" 제한 필요 — 기존 prior/roster 시간 스코프(§18.7 등) 재사용.
- 연결: **재도출(§20) 품질 = RAG 코퍼스 품질** — role 항상성·progression·캡 제거가 봇 답변 품질로 직결.
- **상태**: 방향만 합의, 상세 설계(스키마·인덱싱·tool 정의·시간 스코프 구현) 미논의.

---

## 22. 정체성 유지보수 — Property 7 재설계 + 5회차 정리 패스 + 제안검토 AI (2026-07-12 설계 확정, 구현 미착수)

> **계기**: 화산귀환(w17) 인물정리 붕괴 진단(30화 시점: 캐릭터 190개 중 무명 클러스터 175, pending 제안 723건, 주요 인물 얼굴 혼입 다수). 사용자 발의 "5회차마다 캐릭터 reresolve"에서 출발 → 진단 결과 재해소(R/N 재실행)가 아니라 **정체성 레이어의 3중 자기파괴 루프**가 뿌리: ① P7 편도 제외가 step2를 역오염 ② 자동 승격이 혼합 클러스터에 이름 조기 박제 ③ 교정 신호(제안 큐)가 소비되지 않음. 설계는 전부 **드라이런 실측 + 사용자 눈검증(GT)으로 캘리브레이션 완료**. 하니스 `_suggest_dryrun.py`(throwaway).

### 22.1 진단 실측 (2026-07-12, prod)

- **P7 편도 제외 = 파편 폭발 주범**: `step3._project_characters`가 R의 **회차-국소** significance='extra' 판정 시 `is_match_excluded=true`를 편도 세팅(승격 시 해제 없음). 실측: 42개 제외(현재 sig가 main/supporting인데 제외 잔존 20+ — 운암·장문사형·화산 장문인·복면 노인·조걸 등). significance는 매 apply **replace 라벨**임을 라이브 관측(조걸 ep13 extra→ep14 supporting 전환, 제외는 잔존). 제외되면 step2 앵커 로드에서 빠져 **그 인물 이후 회차 얼굴이 전부 새 클러스터 발급** — 7/11 하루에 신규 클러스터 92개/364얼굴(step2가 11~30화를 도는 동안 resolve는 4~13화, 17화 선행 랙).
- **자동 승격의 이름 박제**: conf≥0.85 즉시 승격이 혼합 클러스터에 오명 부여(1697 '백매관 대사형'=실제 운검+장문인+청명+공 루주 혼입, label_conflict 5건+·"운검" name 제안 4회 반복 — 전부 pending 방치). 조걸/윤종 혼선 동일 패턴.
- CCIP v2(§18.5)는 **정상 가동 확인**(0027 적용·threshold 0.12·step2 자동 merge 제안 발행 중) — v1급 붕괴(773클러스터) 재발 없음. 잔여 파편화의 주인은 매칭이 아니라 P7.
- 제안 큐 구성: CCIP 경합쌍 207(서사 근거 없음 — LLM 판정 불가 영역) / **혼합 대형 클러스터 ~30개**(c1661은 12개 인물과 경합, 191얼굴) / 회차-국소 파편의 1회성 고신뢰 제안. **쌍 단위 회차 반복은 구조적으로 희소**(파편이 회차마다 새로 생성) → "K회 반복 자동수락"은 수확 없음(K=2에서 1건).

### 22.2 결정 A — Property 7 파생값 전환 (편도 폐지)

1. **AI 몫의 `is_match_excluded`는 상태가 아니라 파생값**: 매 apply마다 `excluded = (significance=='extra')`로 **양방향 동기화**(extra→supporting 승격 시 자동 해제).
2. **named(kind='character')는 AI가 제외 불가** — 어떤 시나리오에서도 실익 없음(실측 피해자 전원이 named/주요 인물).
3. **human 세팅 절대 우선(동결)** — 단일 boolean이라 출처 구분 불가 → **출처 레이어링 컬럼 추가**(service 스키마, §17.2 얼굴 레이어링과 동형. 예: `match_excluded_source` human|llm).
4. **백필**: w17의 AI 세팅 42건 리셋을 **재설계 코드와 같은 배포로**(데이터 마이그레이션) — 백필만 먼저면 편도 코드가 재오염, 코드만 먼저면 42건 잔존.

### 22.3 결정 B — 5회차 정리 패스 (consolidation)

- **트리거**: 에피소드 번호 모듈로(%5)가 아니라 **"마지막 정리 run 이후 succeeded resolve ≥5"** — §17.1대로 컬럼 없이 `analysis_run`에서 도출(정리 패스 자체를 umbrella run으로 남김, §20.7 패턴). 예약을 1개씩 쪼개든 30개 묶든 카운트가 원장에 누적되므로 동작 동일. 잔여 <5 방치분은 **온디맨드 버튼**(웹툰 단위, §18.8-6 구상과 같은 자리)으로.
- **훅 위치**: `EpisodeChainWorkflow` 회차 진행 지점. 무거운 실행은 STEP3_QUEUE — 같은 웹툰과는 웹툰 단위 락으로 직렬화(§9.9 2026-07-13 개정: 심판이 apply의 pending suggestion delete-reinsert와 겹치면 표결 중이던 sid가 소멸하므로 직렬화 필수). 패스는 멱등(같은 상태 재실행=no-op).
- **패스 내용**: ①제안 판정(§22.4 — 단 regen 산출 pending은 자동판정 제외, §22.6) ②수락분 실행 — **service 기존 수락 경로 경유**(§19 병합 시맨틱·name수락=병합·face_reassign수락). §20 자동 훅은 건별 큐잉이 아니라 **hook_collector로 모아 웹툰 단위 배치 1개**(§20.9, 2026-07-14 — 중복 재해소 제거)로 발화 ③human 잔여는 우선순위 정렬된 검토 큐로.
- 연계: CCIP 경합쌍(무명↔무명)은 이 패스가 아니라 §18.8-6 "이름 없는 얼굴 CCIP 재배치"의 영역. 혼합 클러스터는 얼굴 단위 수술이라 human 큐 상위 정렬 대상.

### 22.4 결정 C — 제안검토 AI (3단 구조) + 실측 캘리브레이션 ⭐

**구조**: 결정론 가드(차단 전용) + LLM 심판(부피 처리) + human(심판이 넘긴 것만 — "억지 자동화 금지" 사용자 원칙).

**LLM 심판 계약** (glm-5.2, 도시에=대상 인물 1명+관련 클러스터+제안 전문+도감 prior — 2라운드 실측에서 확정):
- **판정은 suggestion_ids 기반으로 수집이 정본** — 심판의 cluster_id 필드는 신뢰 불가(묶음 판정 시 대상 id 오기입, rename으로 병합 우회 실측). sid→쌍 복원 후 가드 적용.
- **rename-to-기존이름 = 병합으로 정규화**(§19.3과 동일 시맨틱). accept된 rename/promote 간 **이름 충돌 가드**(실측: c1731 promote '현종' vs c1659 rename '현종' 동시 accept).
- **도시에 의도적 중복 배치 = 다관점 검증**(같은 클러스터를 후보 대상별 도시에에 중복 노출 → 모순이 표결로 드러남. 실측: c1712 조걸자칭 vs 청명 모순 검출). 도시에당 제안 수 캡 필요(88k자 도시에 3콜이 게이트웨이 재시도로 2h+ — 분할 시 웹툰당 ~40분/4-way).
- 프롬프트 필수 규칙: named 통째 병합 accept 금지(근거가 '일부 얼굴 오식'이면 face_reassign으로), CCIP-only 근거 accept 금지, 혼합 클러스터 통째 처리 금지, 무효 제안은 needs_human이 아니라 reject(큐 제거).

**결정론 가드(자동 실행 조건)** — 전부 만족 시에만:
1. 방향 **무명 클러스터 → named** 한정 (named↔named는 항상 human — 눈검증 8쌍 중 7쌍이 '다른 인물', 유일한 같은 인물=복면 노인=청명 변장으로 정확히 human 안건이었음).
2. 어떤 도시에서도 **혼합(mixed) 플래그 없음** / **다대상 accept 없음** / 과거 human reject 쌍 아님.
3. **표결 `accept ≥ 2×(reject+needs_human)`** ⭐ — GT 캘리브레이션 확정값. 화산귀환 자동후보 22건 눈검증(같은 14/다른 6/모름 2)에서 **오병합 0·참병합 9**인 유일 규칙군. 비교: 단순 다수결(acc>rej)은 오병합 6(27%) — needs_human 표를 무시한 게 원인(오판 전건이 hum≈acc 또는 hum≫acc). strict(rej==0)도 오병합 4. 놓친 참병합 5건은 human 큐에서 고표결 순으로 노출(빠른 수동 승인).

**전체 큐 드라이런 성적(w17, 35도시에)**: pending 749 중 731 판정 — 자동병합(최종 규칙) 9 + 만장일치 reject 328 = **큐 45% 자동 해소, 오병합 0**. human 잔여 ~292의 실체는 혼합 클러스터 29개(얼굴 수술 영역, 텍스트로 축소 불가) + named↔named 변장/오식 판단.

### 22.5 구현 순서 / 상태

1. **P7 재설계+백필**(§22.2) — 선결. 이것만으로 파편 재생산이 멎음. ✅ **구현 완료(2026-07-12)**:
   - service: `Character.match_excluded_source`(llm|human, db_default='llm' — raw INSERT 안전) + human PATCH 경로가 source='human' 세팅 + admin/serializer 노출. 스키마 `0033`(AddField)은 커밋 `4a3d666`으로 **prod 적용 완료**(2026-07-12 14:17 KST).
   - **백필은 마이그레이션 없이 사용자가 prod에 직접 실행**(2026-07-12): llm 몫 제외 중 named 또는 sig≠extra 해제 — 실행 후 실측 규칙위반 잔존 0(남은 제외는 전 웹툰 extra 클러스터뿐, w17은 49→24).
   - data-pipeline: `step3._sync_match_exclusion`(순수 규칙: 양방향 동기화/named 금지/human 동결) + `_project_characters` 재배선(승격 시 즉시 해제 포함) + `tests/test_match_exclusion_sync.py` 8케이스. 스위트 84 passed(orchestration 1건은 테스트서버 기동 플레이크 — 단독 재실행 통과). 커밋됨 — **배포만 남음**(컬럼·백필 모두 prod 반영돼 배포 순서 제약 없음).
2. 정리 패스 + 심판(§22.3~22.4) — ✅ **구현 완료(2026-07-12, 세 레포 — 미배포)**:
   - **data-pipeline**: `src/core/adjudicate.py`(도시에 구성→glm-5.2 심판(순차, 도시에당 제안 60 캡 분할)→sid 기반 교차대조+가드→`payload['judge']` 권고 영속, usage stage='judge') / `ConsolidateWebtoonWorkflow`(begin→adjudicate[STEP3_QUEUE, heartbeat]→finish) / 체인 훅(step3c 후 `consolidation_due` 판정→자식 워크플로, `consolidate_webtoon_{id}` 멱등+ABANDON, env `CONSOLIDATE_EVERY_N_RESOLVES` 기본 5) / `service_bus.send_service_task`(celery send_task — **의존성 celery[redis] 추가, 이미지 재빌드 필요**). 테스트: reconcile 가드 9케이스 + 워크플로 오케스트레이션.
   - **service**: 수락 로직을 뷰에서 `service/suggestion_adjudication.py`로 추출(`apply_suggestion_status` — 뷰/celery 공용, §19/§19.3/§20 훅 그대로) + `execute_adjudication`(건별 격리, 병합 primary=named 쪽 명시, 결과를 run stats.execution에 기록) / celery `execute_consolidation`(pipeline이 send_task)·`trigger_webtoon_consolidation` / `send_consolidation_trigger`(temporal.py) / API `POST consolidate/`·`GET consolidate-status/` / admin 액션 "제안 정리 패스 실행" / `AnalysisRunKind.CONSOLIDATE`+`LLMStage.JUDGE`(마이그레이션 `0034`, choices-only).
   - **webtoonmoa**: suggestions 페이지 — 심판 배지(payload.judge: 자동수락/자동기각/권고/사람판단, 표결·사유 툴팁) + "제안 정리 패스 실행" 버튼 + 최근 run 상태(5초 폴링, running일 때만). 서버측 judge 정렬은 후속(JSONB order_by — §22.4 주의 참조).
   - **배포 요건**: ①service 먼저(0034 migrate) ②data-pipeline 이미지 재빌드(celery dep) + **워커 env `BROKER_URL_`/`BROKER_PORT_`/`BROKER_PASSWORD` 추가 필요(proxmox-configuration pipeline_repo — 미노출 시 판정·권고까지만 되고 실행 위임이 enqueue_failed로 run failed, 수동 수습 가능)** ③webtoonmoa.
3. §18.8-6 CCIP 재배치(경합쌍 207 해소)는 별도 트랙.
- 드라이런 결과물: `~/.claude/jobs/5b9e9531/tmp/suggest_{judge_full,sid_reconciled,visual_check}.json`, 눈검증 artifact "merge-visual-check"(GT는 이 절에 영속됨).

**잔여 TODO (2026-07-12 세션 마감 — 2차 갱신)**:

완료(같은 날): ~~좀비 Terminate~~(사용자 수동 종료) / ~~P7 push·배포~~(`de022ce` 이미지 가동 확인) / ~~P7 백필~~(사용자 prod 직접 실행, 위반 잔존 0) / ~~정리 패스+심판 구현~~(§22.5-2 ✅). **화산귀환은 사용자가 분석데이터 초기화 → 새 이미지(P7 v2+CCIP v2)로 전량 재실행 중** — §18.5 트랙C wipe 검증 겸함. 조걸↔윤종 스왑·프로필 재생성 등 human 노동분은 wipe로 증발(§11.4 가이드는 유효).

- [x] **정리 패스 배포 (2026-07-13 실측 확인, ①~③ 완료)**: ①pipeline_repo env — `BROKER_PASSWORD`(infisical-secret) + `BROKER_URL_`/`BROKER_PORT_`/`CONSOLIDATE_EVERY_N_RESOLVES`(configmap) 노출 확인 ②service 0034 migrate 적용 확인(prod django_migrations, 07-12 10:13 UTC) ③pipeline 배포 태그 `13a796e`(=0b7d6e0 celery dep 포함) 확인. ④webtoonmoa `23eb0ad`(심판 배지 UI)만 배포 여부 미확인 — 비차단. ⚠️ 배포 이미지에 동시성 2가 포함되나 웹툰 락(`1679b03`)은 미포함 — 정리 패스 첫 발동(resolve 5개) 전에 락 커밋 배포 권장(심판↔apply pending suggestion sid 경합 방지, §9.9).
- [x] **재실행 관찰(화산귀환) — 2026-07-13 전 항목 확인**: (a) ✅ P7+CCIP v2 효과 — ep6 시점 687얼굴→클러스터 83(v1 시절 876얼굴→773 대비 대폭 개선), llm 제외는 extra 클러스터 8건뿐(named 제외 0). (b) ✅ ep5 resolve(5개째) 직후 `consolidate_webtoon_17` 자동 발화 — run 697, 30도시에 ~3h(judge 콜별 usage 적재 확인). (c) ✅ E2E — 자동수락 1건(무명 2707→청명) celery 실행→실병합(soft-delete)→§20 프로필 재도출 훅(run 700) + 자동기각 30건, run 697 stats.execution에 결과 기록·errors 0. **자동 수확이 적은 이유 = 심판 mixed 가드(25 클러스터 혼합 판정)로 차단 — 실체는 §18.5 v2 재검증 참조(threshold 문제 아님, 천마=feature 한계/운암=이름 바인딩 오류/청명=모드 파편 → human 몫). 방향: wipe 없이 ep73(1부 종료)까지 완주 후 human 큐레이션(§18.5 재검증 결론 — 7~73 체인 kick은 사용자 실행).**
- [ ] 후속 백로그: suggestions **서버측 judge 정렬**(JSONB order_by — 현재 배지만, §22.4 주의) / 마스터 '미배정' 목록에 human-미배정 얼굴 포함 여부(§11.4 주의) / §18.8-6 CCIP 재배치(경합쌍 해소) / Stage A(아크 종합)는 여전히 미착수.
- 참고: 파이프라인 신규 테스트(reconcile 가드 9·워크플로 오케스트레이션·P7 동기화 8케이스)는 `tests/`가 gitignore라 로컬 보관(기존 테스트 전부와 동일 관례) — 커밋엔 미포함.

### 22.6 수렴 가드 — regen 산출 제안은 자동판정 제외 (2026-07-14)

> **계기**: 정리 패스는 자가 증폭 루프가 열려 있었다 — 심판이 face_reassign을 수락 → §20 자동
> 훅이 reresolve → 재해소 apply가 **새 pending suggestion을 재생성** → 다음 심판이 또 수락 →
> 또 reresolve… (2026-07-13~14 화산귀환에서 사이클 1이 실제 관측: 재해소들이 pending 33건을
> 새로 만들었고 다음 consolidate가 대기 중이었다). 2026-07-10의 "수동 얼굴 이동은 자동 enqueue
> 안 함(폭주 방지)" 결정과 같은 우려를 **정리 패스 경로가 대량으로 재도입**한 셈.

- **가드**: regen 재해소가 만드는 vision/resolve run에 `stats.origin='regen'` 마킹
  (`reresolve_episode(run_origin=)` → `runs.start_run(stats=)`, `finish_run`은 merge로 보존).
  심판(`adjudicate_webtoon`)은 **run의 origin이 regen인 pending을 자동판정에서 제외**
  (stats.regen_held로 카운트) — 이런 제안은 human 검토 큐로만 흐른다. 루프가 사이클 1에서 끊긴다.
- **자격 회복**: 정규 체인 resolve가 같은 에피소드를 다시 돌면 apply의 delete-reinsert가
  제안을 새 run(origin 없음)으로 재생성하므로 그때 자동판정 자격을 회복한다 — 영구 배제가 아니라
  "regen이 만든 신호는 한 박자 쉬고 사람이 먼저 본다".
- 쿨다운(웹툰당 정리 패스 최소 간격) 방식과 비교해 이쪽을 채택 — 트리거 빈도를 건드리지 않고
  증폭의 원인(regen 산출의 즉시 재소비)만 정확히 차단한다.
