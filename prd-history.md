# 웹툰 분석 파이프라인 — PRD 변천사 아카이브

> **성격**: `prd.md`(마스터)에서 "결론은 이미 났고 경위만 남은" 기록을 이관해 보존하는 **append-only 아카이브**. 현행 설계·계약·백로그의 정본은 언제나 `prd.md`다. 여기 문서의 상태 기술("미커밋", "미적용" 등)은 **작성 당시 스냅샷**이며 이후 갱신하지 않는다.
> **분리일**: 2026-07-07 (prd.md v4.1 최신화 작업의 일부)
> **삭제된 문서** (원문은 git 이력으로 열람 가능):
> - `prd-step3.md` — Step3 재설계 working draft(2026-06-29~30, 실험 로그·모델 A/B·인용 근거 포함). v3.3(2026-07-01)에서 prd.md §9로 **전면 흡수** 완료, 2026-07-07 삭제.
> - `prd-identity-roster.md` — 정체성·로스터 조사+구현 문서(2026-07-06~07). 확정 스펙·백로그는 prd.md §18로 이관, 조사 경위는 본 문서 §H6에 보존, 2026-07-07 삭제.
> - v2.0 이전 원본 3개 문서(`prd.md` v2.0 / `prd-renew.md` v1.4 / `prd_embedding.md`)는 종전대로 `docs/archive/`.

---

## H1. 버전 헤더 변경 로그 (v3.0~v3.6, prd.md 헤더에서 이관)

> prd.md 헤더에 누적되던 버전별 상세 불릿. 한 줄 요약은 prd.md §15 결정 로그에 유지.

**v3.0 핵심 변경 (2026-06-21)**:
1. 스트림 처리 레이어 **Faust/Kafka → Temporal 워크플로**로 피벗 결정 (§H2)
2. model-api **OCR/YOLO 엔드포인트 분리** 적용 (`/ocr`, `/yolo`, 모드 `ocr`/`yolo`)
3. **LLM(Step 3/4)를 1급 섹션으로 승격** — 당시 미구현, GLM-4.6v로 구현 예정
4. 70만 배치 백필은 **범위에서 보류**(증분 경로 집중)

**v3.1 변경 (2026-06-28)**:
- **`TextBlockType` 개편(A)**: 독백(Monologue) 신규 추가, 효과음(SFX) 타입 제거(→ OTHER + soft-exclude), "상황 서술"은 `TextBlockType`이 아니라 `CutSceneMeta`(장면 서술) 레이어로 분리.
- (보류) 엑스트라/효과음 soft-exclude 정리 정책(Human/VL 판정)은 별도 개정에서 확정.

**v3.2 변경 (2026-07-01) — 실제 코드/배포 상태 반영**:
- **Faust→Temporal 전환 완료**: `service`는 이미 `config/temporal.py`만 사용(`config/kafka.py` 삭제됨), `proxmox-configuration`의 `pipeline_repo` configmap도 `TEMPORAL_ADDRESS`만 있고 Kafka 관련 설정 없음. 문서 곳곳의 "미구현/POC/목표 아키텍처" 표현은 과거 스냅샷이었음.
- **Step 3 구현 완료**: `webtoon-pipeline/src/core/step3.py`가 extract(pass1)→resolve(pass2a)→apply(pass2b, LLM 없는 결정론적 커밋) 2-pass로 동작 중. 상세 설계는 `prd-step3.md`로 이관(→ v3.3에서 §9로 재흡수).
- **레포 4개로 분리 명시**: 이전 버전은 `webtoonmoa`를 `service`(Django)에 묶어 표기했으나, 실제로는 별도 SvelteKit 프론트엔드 레포다.

**v3.3 변경 (2026-07-01) — `prd-step3.md` 전면 흡수 + Step4 판단 정정**:
- **§9를 `prd-step3.md`의 최종 설계로 전면 교체**: 문제정의·목표·모델 토큰예산/윈도우 적응 설계·2-pass 아키텍처·Pass-1/2a/2b 계약·belief state·소구포인트(비트) 계층·캐릭터 중요도 티어링·mis-ID distrust/책략 탐지/교차에피소드 prior 신뢰성 규칙·정답 데이터 취급·Temporal 배선까지 전부 흡수. `prd-step3.md`는 실험 로그·인용 근거를 보존하는 이력 문서로 유지(→ 2026-07-07 삭제, git 이력 보존).
- **v3.2의 "Step4 미착수" 판단을 정정**: Step3+Step4는 하나의 에피소드 추론 단계로 통합됐고, Pass-2b(`apply_resolution`)가 매 에피소드 처리마다 `EpisodeReport`(summary/appeal_point/cliffhanger/foreshadowing/character_timeline)를 자동 커밋한다 — 즉 Step4는 이미 구현·운영 중이다. `episode-summary/main.py`는 이 통합 이전 시점의 요약 품질 비교용 레거시 실험 스크립트로 재확인.
- **DB 스키마 8종 추가 반영**: `TextAnnotation.resolution_status`, `Character.significance`, `EpisodeReport`, `EpisodeBeat`, `NameDiscoverySuggestion`, `StoryArc`, `NarrativeThread`, `CharacterClaim`, `LLMUsage`, `WebtoonNarrativeState` — 전부 마이그레이션·코드 반영 완료.
- **골든 회귀 테스트 3종 작성·통과**: mis-ID distrust(ep2 천마→운암), 책략 탐지(ep3 청진, Property 10), 교차에피소드 정체성 prior(ep3 418=청명) — `webtoon-pipeline/tests/`. 부수적으로 `test_workflow_orchestration.py`의 stale 스텁(`step3_episode` 단일 액티비티 잔존) 버그를 step3a/b/c 3-스텁으로 갱신해 수정.
- **재처리를 에피소드 단위로 재설계**: 컷 단위 short-circuit → 에피소드 단위 `reresolve_episode`/`reapply_episode`.

**v3.4 변경 (2026-07-03) — 홈랩 배포 환경 신뢰성 장애 대응 세션**:
- **신규 §16**: 사용자의 실제 배포 환경(홈랩 k3s + Cloudflare Tunnel + 불안정 홈 네트워크)을 문서화하고, 이 환경에서 실제로 터진 3개 버그(Step1 `resolution_status` NOT NULL / Step1 재시도 비멱등성 / Step2·Chroma·DB 정합성 드리프트)의 원인·수정 내역·검증 상태를 기록(→ 본 문서 §H4). `data-pipeline` + `service` 양쪽 레포 수정.
- **model-api 구조적 리스크 발견(당시 미수정)**: 전 라우터가 `async def` 안에서 동기 CPU-bound 추론을 직접 호출 — 부하 시 이벤트루프 블로킹으로 gunicorn WORKER TIMEOUT/SIGKILL 유발 가능(§H4.1).

**v3.5 변경 (2026-07-04) — `naver/820097` end-to-end 검증 + 회귀 버그 3건 수정 + Step3 신뢰성/품질 개선**:
- `naver/820097` ep2를 실제로 재실행해 step1→step2→step3 전 구간 완료 확인(R2 종결).
- **Step2 자기-런 스냅샷 버그 발견·수정**(§H4.5): 드리프트 방어 로직(`valid_appearance_ids`)이 루프 시작 전 스냅샷이라, 같은 에피소드 처리 중 새로 생긴 캐릭터를 "유령"으로 오판해 42/42 얼굴이 전부 신규로 쪼개지는 회귀 — 즉시 반영하도록 수정.
- **Step3 Temporal 워커 액티비티 미등록 버그 발견·수정**(§H4.6): `worker.py`가 옛 단일 패스 `step3_episode`만 등록 — step3a/b/c 2-pass 액티비티로 교체.
- **`narrative_context.fold` 캐시 불일치 버그 발견·수정**(§H4.7).
- **Step4(회차 요약) 별도 프로덕션 연결 불필요로 확정**(이미 Pass-2b에 흡수됨을 재확인, 계획 철회).
- **vllm 502/530 재시도, Pass-1 병렬화, max_tokens 절단 수정(4096→8192), 프롬프트 한국어 강제, LLM 동시성 config화**(§H4.8).

**v3.6 변경 (2026-07-05) — 화자 매칭 구조 결함 수정 + 인물도감 profile + HITL stale 배선 + 캐시 슬림화**:
- **화자 매칭률 1~2% 구조 결함 발견·수정**: naver/820097 전 30회차 실측 — llm speech/monologue 블록의 speaker_id 부착률이 회차당 0~7%. 3중 원인: ① Pass-1이 얼굴 기반으로 확신한 화자 후보를 **DB에 저장 안 함**(speaker_id=NULL 강제), ② Pass-2a 프롬프트가 "provisional speaker가 null/불확실한 블록만" speaker_resolution으로 내라고 지시 → 확신 블록은 재출력 안 됨, ③ Pass-2b는 speaker_resolution만 커밋 → 확신 화자가 전부 유실. 수정: Pass-1 화자 후보(conf≥0.5)를 provisional speaker_id로 영속, Pass-2a는 **전수 화자 테이블**(모든 speech/monologue, confirm-or-override) 출력으로 변경, Pass-2b에 provisional 화자 resolved 승격 안전망 추가.
- **is_confirmed가 모델 입력에 실리지 않던 버그 수정**: 프롬프트는 "is_confirmed는 진실로 동결"을 지시하는데 `_load_faces`/페이로드에 그 플래그가 아예 없었음 — Pass-1 `identified_faces`와 Pass-2a `faces`에 `confirmed` 추가.
- **인물도감(범용 profile)**: Pass-2a characters에 `profile{gender, age_group, affiliation, role, personality[], traits{}}` 추가 — `character.extra['llm_profile']`에 병합 커밋(과도기, → v4.0 `character_profile` 모델로 대체됨).
- **HITL stale 배선**: service 얼굴 확정/일괄 재배정/텍스트 어노테이션 API가 `webtoon_cut.is_stale`/`human_modified_at`을 마킹하도록 수정. 2-pass가 `llm_analyzed_at`/`is_stale=false`를 전혀 안 찍던 것도 수정. 재해소 실행용 CLI `python -m src.tools.reresolve` 추가. (→ v4.0에서 is_stale/llm_analyzed_at 컬럼 폐기, run 도출로 대체)
- **WebtoonNarrativeState 캐시 슬림화**: persist_state 쓰기 시점에 roster를 유의미 인물로 한정 + key_facts 인물당 12개 캡 + running_summary 최근 30화 캡(실측 ep30에 roster 69명 대부분 NEW_CHAR 엑스트라). (→ v4.0에서 테이블 자체 폐기)
- **max_tokens 재상향**: `_PASS1_MIN_MAX_TOKENS`/`_PASS2_MIN_MAX_TOKENS` 8192→16384. (→ v4.1에서 고정 하한 자체를 폐기, 기본 미전송 정책)
- **요약/책략 프롬프트 보정**: deception은 "다른 인물을 속이려는 의도가 있는 speech"만(monologue/자조/한탄 제외), summary/appeal_point는 narration·실제 사건 근거만(근거 없는 낙인·평가어 금지).

---

## H2. Faust → Temporal 피벗 — 결정 배경 상세 (구 §4, 2026-06-21 결정 · 2026-07-01 완료 확인)

> 결론과 현행 요약은 prd.md §4. 아래는 결정 당시 기록.

### H2.1 배경 (결정 당시)
당시 현행은 Faust+Kafka. 이 워크로드의 본질은 "**엔티티(에피소드) 단위 다단계 durable workflow** + 웹툰당 순차/웹툰 간 병렬 + 중간 재개 + 단계별 재시도"이며, 일 ~1000컷으로 스트리밍 처리량 요구는 없다. Faust의 한계:
- faust-streaming 생태계 유지보수 불안.
- 컷/에피소드 진행을 **메시지 자가 재발행**으로 구현 → 암묵적 상태머신, 추적 난해.
- OOM 재시작 시 **Kafka 오프셋 미커밋 → 컷1부터 재처리**(체크포인트 부재).

### H2.2 결정 비교표
| 관심사 | Faust(당시 현행) | Temporal(목표) |
|---|---|---|
| 웹툰당 순차 / 웹툰 간 병렬 | Kafka 파티션 키 | 웹툰=워크플로 인스턴스 1개(`workflow_id={source}_{title_id}`) |
| 중간 재개 | 오프셋+자가 재발행 | 워크플로 history 영속(activity 완료 단위 재개) |
| 단계별 재시도 | 수기 `retry_count` | activity `RetryPolicy` |
| 트리거/스케줄 | Celery beat | Temporal Schedule / 멱등 start |
| history 무한 증가 | 해당 없음 | `continue_as_new` (컷·에피소드 단위) |

- Kafka + Faust + Celery beat **3개를 Temporal 하나로 통합**. 배치 백필이 필요해지면 별도 재개형 스크립트로 분리.
- POC `temporal-pipeline/`으로 워크플로 계층(WebtoonWorkflow → EpisodeWorkflow → 컷 루프) 구현·검증 후 이관.

### H2.3 service 트리거 전환 (완료)
`config/kafka.py` 프로듀서 → Temporal 클라이언트로 교체: `send_phase1_trigger(...)`가 `client.start_workflow(WebtoonWorkflow.run, ..., id="{source}_{title_id}")` 호출(멱등 kick). `backend/config/kafka.py` 삭제, `config/temporal.py`만 유지. 에피소드 체이닝은 워크플로 내부로 내려가 service는 웹툰당 1회 kick으로 단순화.

---

## H3. 마이그레이션 완료 이력 (구 §13 "완료" 블록)

- Django 스키마, Step1(OCR/YOLO) + Step2(face_identify) 처리 로직, 에피소드 게이팅(EpisodePipelineProgress).
- 이중 임베딩 제거(임베딩+매칭 1패스).
- model-api 모드 분리(clip/ccip) + CCIP 엔드포인트 + OCR/YOLO 엔드포인트 분리(v3.0, 이후 별도 GPU 서버로 이전).
- EmbeddingModel/WebtoonEmbeddingSetting + model_resolver/metric 분기(기본 CLIP 유지).
- LLM 스키마 반영(rename + LLMModel/WebtoonLLMSetting/llm_model), CutSceneMeta 마이그레이션(0009/0010).
- **Faust→Temporal 전면 이관(v3.0, v3.2 시점 운영 확인)**: Faust/Kafka 완전 제거(`service`의 `config/kafka.py` 삭제, `proxmox-configuration` configmap에 Kafka 설정 없음). `webtoon-pipeline`은 k3s에 Temporal 워커로 배포·운영 중, `proxmox-configuration/temporal_repo`에 Temporal 서버 배포됨. `service`는 `config/temporal.py`의 `send_phase1_trigger`로 웹툰당 1회 kick.
- **Step3+4 재설계·구현 완료(v3.3, `episode-scene-resolution` 스펙 — 2026-07-01 확인)**: 에피소드 단위 2-pass(extract→resolve→apply)가 서비스 스키마 + 코어(`core/step3.py`, `narrative_context.py`) + Temporal 배선(step3a/b/c)까지 전부 구현되어 운영. Step4(회차 요약)는 Pass-2b에 흡수돼 `EpisodeReport`로 자동 산출. 골든 회귀 테스트 3종 작성·통과.
- **R2~R4 신뢰성 트랙(2026-07-03~04)**: `naver/820097` ep2 end-to-end 재검증 완료(R2), Step2 자기-런 스냅샷/Step3 워커 미등록/narrative fold 캐시 불일치 3건 수정(R3), vllm 재시도·Pass-1 병렬화·max_tokens 절단 수정·한국어 강제·동시성 config화(R4). model-api 라우터 `run_in_threadpool` 수정 완료(R1 일부).
- **v4.0 스키마·스테이지 재편(2026-07-05~06)**: 마이그레이션 0022(wipe)~0026 prod 적용, 신 워커 배포. (경위 §H5, 현행 §17)
- **v4.1 정체성·로스터(2026-07-06~07)**: 커밋 24f573b~b29ab87, prod 가동 확인. (경위 §H6, 현행 §18)

---

## H4. 2026-07-03~04 홈랩 신뢰성 장애 대응 경위 (구 §16.2~16.9)

> 배포 환경 전제(§16.1)와 여기서 도출된 **살아있는 신뢰성 규칙 요약**은 prd.md §16에 유지. 아래는 장애별 원인·수정의 전체 경위.

### H4.1 model-api 구조적 리스크 — async 라우터의 동기 추론 블로킹 (구 §16.2)
- `model-api/src/routers/{ocr,yolo,ocr_yolo,embed,embed_ccip}.py` **전부** 라우터 핸들러가 `async def`인데, 그 안에서 동기(blocking) CPU-bound 모델 추론(`extract_ccip_feature`, PaddleOCR, YOLO 등)을 `await`/스레드 오프로드 없이 직접 호출. `UvicornWorker`는 이벤트루프 기반이라 이 호출이 도는 동안 그 워커 전체가 다른 요청도, gunicorn 마스터에 대한 생존 응답도 못 한다.
- **실측 사고(2026-07-03 01:34)**: embed-ccip 워커 2개가 거의 동시에 `WORKER TIMEOUT`(gunicorn `--timeout=120`) → `SIGKILL`/`SIGABRT`. 직전에 step2가 짧은 시간에 요청을 몰아 보낸 정황(`임베딩 진행 32/42`) — 부하가 몰리면 이벤트루프가 120초 넘게 막혀 재현 가능한 구조.
- **수정(2026-07-03)**: 전 라우터의 동기 호출을 `starlette.concurrency.run_in_threadpool`로 감싸 이벤트루프 블로킹 제거. 클라이언트 쪽 동시 요청 수도 서버 워커 수에 맞춰 제한(`step2.py::_EMBED_WORKERS` 8→2).
- **잔여**: `HF_HUB_OFFLINE=1` 미적용 — 가중치는 이미지에 baked-in이라 재다운로드는 없지만 워커 재시작마다 huggingface.co 캐시 확인 왕복 발생(불필요한 외부 의존). → prd.md §13 R1으로 이월.

### H4.2 Step1(OCR/YOLO) 신뢰성 강화 (구 §16.3)
**버그 2건(수정 완료)**:
1. **`text_annotation.resolution_status` NOT NULL 위반**: 마이그레이션 `0017`로 컬럼 추가됐는데(Django `default=`는 DB 레벨 기본값 아님) Step1 raw SQL INSERT(`_process_segment_ocr`)와 Step3 레거시 단일-pass 경로가 이 컬럼을 안 채워 터짐 → 명시적 값 지정.
2. **`step1_episode` 재시도 비멱등성**: 세그먼트 단위 즉시 커밋 + 재시도 시 처음부터 재실행인데 `prepare_episode`(기존 데이터 정리)는 1번만 실행 → 도중 실패 후 재시도가 이미 커밋된 `text_region` 재INSERT → `uniq_text_region_cut_index` UniqueViolation.

**수정**: ① 이어하기(resume) — `_load_resume_state`가 커밋된 region/face에서 인덱스·bbox 복원, `resume_from`은 heartbeat_details로 전달. ② HTTP 재시도+지수 백오프 — `ocr_yolo_client._post_image`, 5xx(Cloudflare 520~526 포함)+`TransportError`만 최대 10회 1s→8s, 4xx 즉시 전파. 백오프 총 소요(최악 ~55초/콜)는 `heartbeat_timeout=5분`보다 짧게 설계(5분 값 유지는 사용자 결정 — 정상 콜은 10초 내라 초과는 진짜 이상 신호). ③ UPSERT 안전망 — `ON CONFLICT DO NOTHING`(human 리뷰 필드 보호 위해 DO UPDATE 아님). ④ 로깅 강화 — print→logging, 컨텍스트(source/title_id/ep/cut) 포함.

### H4.3 Step2/Chroma·Postgres 정합성 드리프트 (구 §16.4)
**증상**: `face_identify_episode`가 `face_record.appearance_id` FK 위반으로 실패.

**근본 원인(실측 확인)**:
1. **리셋 액션이 Chroma 삭제를 통째로 스킵**: `_reset_chroma_collections`가 `webtoon.embedding_settings`(웹툰별 명시 오버라이드)만 순회 — 대부분 웹툰은 전역 기본 모델만 쓰므로 삭제 대상 컬렉션이 0개(로그 `{'chroma': {'status': 'success', 'collections': {}}}`). DB만 하드 삭제되고 Chroma엔 유령 벡터 잔존.
2. **Chroma v1 REST 완전 제거**(HTTP 410): service 수기 REST 호출(`chroma_client.py`/`ChromaCollectionsAPIView`)이 v1 경로 사용 — 명시 오버라이드 웹툰도 삭제가 실제로 성공한 적 없었을 가능성. 상위 상태 집계도 실패를 무시하고 항상 success.
3. **실제 오염**: webtoon 60(naver/820097) 컬렉션에 하드 삭제된 `appearance_id=491`(NEW_CHAR_005) 유령 벡터 20개 + 재시도가 그 순간에도 같은 유령 id에 새 벡터 추가 중.

**수정**: chroma_client/뷰 v1→v2 전환, 리셋 순회를 `EmbeddingModel.raw_objects.all()`로 확장, 컬렉션별 실패 시 chroma 단계 status=failed 집계, 오염 컬렉션 일회성 정리, `step2.py`에 `_get_valid_appearance_ids` 유령 방어(앵커 필터 + 루프 내 재검증 + 신규 재할당). **구조적 한계**: DB/Chroma 리셋은 여전히 비원자적 — "리셋이 완벽히 원자적"이라고 가정하면 안 됨.

### H4.4 검증 상태 스냅샷 (구 §16.5)
§H4.2/H4.3 수정은 py_compile 통과 + Chroma v1 410/v2 200 curl 실측. ep2 재실행 결과는 §H4.5.

### H4.5 `naver/820097` ep2 end-to-end 검증 + Step2 자기-런 스냅샷 회귀 (구 §16.6, 2026-07-04)
ep2 재실행으로 phase 1/2/3 전부 completed, region/annotation 1:1 정합, scene_meta/llm_usage/narrative_state 정상 — R2 종결. 단 재실행 로그 "매칭 42/42 (매칭=0, 신규=42)": §H4.3에서 추가한 `valid_appearance_ids`가 루프 시작 전 스냅샷이라 같은 에피소드 안에서 새로 만든 캐릭터를 유령으로 오판, 전부 신규로 쪼갬. **수정**: `_allocate_character` 직후 `valid_appearance_ids.add(...)` 즉시 반영 → 재실행 "매칭=29, 신규=13" 정상화. 잘못 생성된 42개 캐릭터/Chroma 정리(face_record는 Step1 산출물이라 보존, appearance_id만 NULL 리셋).

### H4.6 Step3 워커 액티비티 미등록 (구 §16.7, 2026-07-04)
`worker.py`의 step3_worker가 옛 단일 패스 `step3_episode`만 등록 — 워크플로는 이미 step3a/b/c 체인 호출이라 `NotFoundError`로 전부 실패. 등록 목록 교체로 수정. 부산물: `step3_episode`(+`analyze_cut_scene` 계열)는 호출자 전무한 죽은 코드로 확인(R5, 삭제 여부 미결정).

### H4.7 `narrative_context.fold` 캐시 불일치 (구 §16.8, 2026-07-04)
`webtoon_narrative_state.open_threads` 캐시의 `planted_episode: 1` vs `narrative_thread` 테이블의 ep2 — `_commit_threads`는 planted_episode를 현재 화로 강제 보정하는데 fold에 넘기는 값은 LLM 원본 그대로였음. `_normalize_threads_for_fold`로 동일 보정 적용 + 오염 캐시 삭제. (테이블 자체는 v4.0에서 폐기됨)

### H4.8 vllm 신뢰성 + Pass-1 병렬화 + max_tokens 절단 + 한국어 강제 + 동시성 config화 (구 §16.9, 2026-07-04)
- **vllm 502/530 재시도**: `llm_client.call_llm_json`에 재시도가 전혀 없었음 → ocr_yolo_client와 동일 패턴(10회, 1s→8s, 5xx+TransportError만) 추가. 스트리밍 1회 시도는 `_stream_llm_once`로 분리, 백오프 대기 중엔 `_LLM_SEMAPHORE` 반납.
- **Pass-1 병렬화**: `extract_cut`은 컷 간 belief 의존 없음 확인 → `ThreadPoolExecutor`(기본 4, `PASS1_WORKERS`) 병렬화, 순서 의존 후처리는 완료 후 재정렬. 실질 동시성 상한은 `LLM_MAX_CONCURRENCY`.
- **max_tokens 절단**: glm-4.6v가 추론형이라 reasoning_content가 예산 선소모 — `_PASS1_MIN_MAX_TOKENS` 4096→8192(→v3.6에서 16384→v4.1에서 고정 하한 폐기). 파싱 실패 에러에 finish_reason/completion_tokens/응답 길이 포함.
- **프롬프트 한국어 강제**: Pass-1/2a 시스템 프롬프트에 "반드시 한국어" 강조.
- **LLM 동시성 config화**: `LLM_MAX_CONCURRENCY`/`PASS1_WORKERS`를 `proxmox-configuration` configmap에 노출. 1보다 올려도 되는지는 vllm 실측 필요(미실측).

---

## H5. v4.0 구현 세션 로그 (구 §17.6/17.8/17.9, 2026-07-05 시점 스냅샷)

> ⚠️ **낡음 주의**: 아래 "미커밋/prod 미적용" 기술은 2026-07-05 세션 종료 시점 스냅샷이다. 이후 두 레포 모두 커밋됐고(마이그레이션 0022~0026 prod 적용 2026-07-05~06, data-pipeline 커밋 24f573b~b29ab87), 신 워커가 배포·가동 중이다 — 현행 상태는 prd.md §17.6/§18.7.

### H5.1 v3.6 코드 diff 처리 (구 §17.6)
- **v4.0에 그대로 승계**: Pass-1 화자 영속 + 전수 화자 테이블 + provisional 승격 안전망, confirmed 플래그 배선, max_tokens 16384(→v4.1에서 정책 변경), deception/요약 프롬프트 규칙, service human-수정 API의 수정 신호(is_stale 플래그 → human 타임스탬프 비교로 형태 변경), reresolve CLI(run 재실행으로 개념 승계).
- **v4.0에서 걷어냄**: `character.extra['llm_profile']` 커밋부 + `CharacterSerializer.profile`의 extra 참조(→ `character_profile` 모델로), `apply_resolution`의 `llm_analyzed_at/is_stale` 컷 마킹(→ run 도출로), `webtoon_narrative_state` 캐시 슬림화(테이블 자체 폐기로 무의미).

### H5.2 구현 노트 — §17.7 1~2단계 구현하며 확정된 보강 (구 §17.8)

> 여기 항목 중 **살아있는 계약**(run kind step1/2, R/N run 공유, face_reassign suggestion, reapply 규칙, 테이블 prefix/constraint 이름, 클러스터 이름, Stage N 윈도잉 없음)은 prd.md §17.6에 요약 유지 — 아래는 결정 경위 포함 전문.

- **AnalysisRunKind에 `step1`/`step2` 추가**: §17.1은 LLM 도메인(vision/resolve/arc)만 정의했으나, 진행도 3원화를 run으로 수렴하려면 step1/2 완료도 run이어야 한다(구 `EpisodePipelineProgress` 대체). step1/2는 산출물 run FK 귀속 없이 **완료 원장 행만** 남긴다(`runs.record_completed_run`) — 탐지/매칭 레이어는 run 교체 대상이 아니기 때문. 체인 진행 판정(`next_chain_episode`)의 step→kind 매핑은 `shared.STEP_RUN_KIND`(step3→resolve).
- **R/N run 공유**: step3b가 resolve run을 시작(R+N 2콜 usage 귀속), step3c apply 성공이 succeeded 전이 — "에피소드 step3 완료"의 정본 시각. N 콜만 실패하면 화자 데이터는 유지된 채 서사 필드만 빈 값(실패 격리 확인).
- **face_reassign suggestion 생산 구현(2026-07-05 후속 세션)**: Stage R 출력에 `face_reassignments: [{cut, face(F라벨), to_character_id|null, evidence, confidence}]` 섹션 추가(얼굴 단위 판단 — 인물 전반의 의심은 기존 label_conflict 유지, 프롬프트가 구분 지시). apply(step3c)가 `(cut_number, face_idx)`→`face_detection.id`로 해석(`_episode_face_detection_map`)해 `suggestion(type=face_reassign, detection_id, payload={to_character_id, evidence})`로 적재 — 수락 시 service가 human FaceIdentity 생성(기구현). 커밋 규칙: human 확정(confirmed) 얼굴 동결, 실재하지 않는 (cut,face) 무시, 웹툰에 없는 to_character_id는 null 강등(오배정 신호만 유지), 현재 배정과 동일/미배정+대상미상 제안은 드롭. 윈도우 병합은 (cut,face) dedup(confidence 우선).
- **reapply는 suggestion 큐 불가침(2026-07-05 후속 세션, 유실 버그 수정)**: `apply_resolution(refresh_suggestions=False)` — reapply(이름만 변경 시 LLM 없는 재투영)가 pending 제안을 delete-reinsert하면 스냅샷에 비영속인 원료(name 후보 confidence, face_reassignments)가 재생성 불가라 직전 run의 pending name/face_reassign 제안이 통째로 유실되던 문제. 재투영은 큐를 건드리지 않는다(제안 재생성은 새 resolve run의 apply만).
- **마이그레이션 전략 전환: 이식 → 전량 wipe(사용자 결정, 2026-07-05 후속 세션)**: §17 전제의 "human 노동분 불가침"을 이번 전환에 한해 완화 — 분석 데이터 전량 폐기·재생성을 택했다(유실 실측: face 확정 1,696건, 이름 확정 캐릭터 84건, human 주석 36건, 제외 마킹 49건 — 재작업 감수). 불가침은 콘텐츠 도메인(webtoon/webtoon_episode/webtoon_cut/webtoon_author)·설정 테이블·S3 원본뿐. 이에 따라 **구 손작성 0022(RenameModel pk 보존+이식)는 폐기**하고 `0022_v4_wipe_analysis_data`(TRUNCATE, postgres 벤더 가드) + `0023`(전부 makemigrations 자동 생성) 구성으로 재생성. sqlite 체인 검증을 위해 삭제 예정 모델(EpisodePipelineProgress/FaceRecord/NameDiscoverySuggestion)의 constraint/index를 RemoveField 앞에 명시 제거.
- **테이블 prefix 도입(사용자 결정, 2026-07-05 후속 세션)**: 분석 산출 테이블 17개에 `analysis_`(`analysis_run`은 기존 이름 유지), 설정 테이블 5개에 `config_`. 콘텐츠(webtoon*)·추천(reco_*)은 불변. 파이프라인 raw SQL 124곳 일괄 치환. **constraint 이름은 불변**(파이프라인이 `ON CONFLICT ON CONSTRAINT`로 이름 참조 — `uniq_face_record_cut_idx`는 analysis_face_detection 테이블에 legacy 이름으로 유지).
- **reapply는 run을 만들지 않는다** — 기존 run 산출의 재투영이므로 최신 succeeded resolve run id를 그대로 스탬프.
- **(폐기된 구안) 마이그레이션 안전장치**: `face_record`를 RenameModel로 이행해 pk 보존 + human 확정 이식 + `character.kind` 백필 — wipe 전략 채택으로 폐기.
- **클러스터 이름**: `name=""`(빈 문자열) + 로그/Chroma 메타 표시용 라벨은 `cluster#{id}`. `_find_character_by_name`은 `kind='character'`만 후보로.
- **Stage N 윈도잉 없음**: N 입력(정정된 트랜스크립트, 텍스트만)은 컴팩트해서 단일콜. 로컬 16K 폴백에서 긴 에피소드가 초과하면 절단 위험 — 실측 후 필요 시 후속.
- **기존 깨진 테스트 7건(step1 resume SQL 픽스처 미모델링) 수리 완료**: conftest FakeCursor에 resume 복원 3쿼리 핸들러 추가 + face_record→face_detection 매칭 문자열 전환. 파이프라인 테스트 스위트 전체 그린(2026-07-05).

### H5.3 작업 현황 & 인수인계 (구 §17.9, 2026-07-05 세션 종료 시점)

**당시 상태: §17.7의 1·2단계 + 3단계 일부(service API)가 구현 완료, 두 레포 모두 미커밋 working tree(사용자 결정: 유지), prod DB 미적용.** (→ 이후 전부 커밋·적용·배포됨)

#### 완료된 변경 (당시 미커밋)
- `data-pipeline`: `prd.md`(v4.0), `webtoon-pipeline/src/core/{step1,step2,step3,runs}.py`(runs.py 신규), `src/operators/narrative_context.py`(정본 조인 재작성), `src/temporal/{activities,workflows,shared}.py`, `src/tools/reresolve.py`, `tests/conftest.py`, `tests/test_workflow_orchestration.py`, `tests/test_step3_face_reassign.py`(신규), `smoke_test.py`.
- `service`: `models.py`(+테이블 prefix), `migrations/0022_v4_wipe_analysis_data.py`(손작성 wipe), `migrations/0023_...`(자동 생성 + constraint 선행 제거 3건 손보정), `views.py`/`serializers.py`/`admin.py`/`tasks.py`/`urls.py`/`service/face_crop.py`/`management/commands/sync_confirmed_face_embeddings.py`.
- 검증: 파이프라인 pytest 전체 그린, service `manage.py check` + sqlite 마이그레이션 체인 통과. 실 Postgres/실 LLM 미실행.

#### 당시 남은 작업 (순서 제안 — 이후 1·2는 완료됨)
1. 커밋 정리(단위 분리 권장). → **완료**
2. 배포 & 검증 — ⚠️ 순서: prod DB 백업 → migrate(0022 wipe+0023) → **파이프라인 워커 동시 배포**(구 워커는 구 테이블명 사용) → Chroma 컬렉션 리셋(TRUNCATE로 사라진 pk 참조) → step1부터 전량 재분석 → 지표 확인(speaker 부착률, teaser 품질, suggestion 큐, profile 생성). → **적용·재분석 진행 중(2026-07-06~)**
3. Stage A(아크 종합) 신설 — 트리거 주기 논의 후 구현(미설계). → 미착수
4. webtoonmoa — suggestion 큐 화면, 도감 화면(API는 준비됨). → 미착수
5. 이월: litellm 요청 프롬프트 로깅(→2026-07-07 활성화), Stage N 로컬 16K 절단 실측, `LLM_MAX_CONCURRENCY` 상향 실측, model-api `HF_HUB_OFFLINE`.

#### 새 세션 주의사항 (당시)
- 사용자 워크플로: **논의 먼저, 코드 수정은 명시 승인 후.** 스키마/설계 변경은 PRD에 결정 기록이 선행된다.
- 진행도/stale은 컬럼이 아니라 도출(§17.1) — `analysis_run` 조회(구 `episode_pipeline_progress`/`llm_analyzed_at` 쿼리는 무효).

---

## H6. 정체성·서사 로스터 조사 이력 (구 `prd-identity-roster.md` v0.2, 2026-07-06~07)

> 확정 스펙(스테이지·모델 배선·max_tokens·fallback·CCIP 매칭·백로그·검증 절차)과 테스트 오라클(화산귀환 정본 로스터)은 prd.md §18로 이관. 아래는 조사 경위·실측 수치·가설 반전 이력.

### H6.1 문제 정의 (실측 사례)
대상: **화산귀환(naver/769209, 무협)** ep1(id=1255)·ep2(1334)·ep3(1333) + **아카데미에서 살아남기(820097, 게임 빙의물)** ep2(11757)·ep3(11758) + **참교육(758037, 학원 액션)** ep1(7729) = 3웹툰·3장르.

핀트 어긋남 실측(ep1, 프로덕션 산출):
- `episode_report.summary`: "천마가 **살아있다는** 사실" ← 원문(컷123 "백년 전에 죽은 대마두잖아?")은 **사망**.
- `character_profile`: 놀란 주체·정보 전달자가 뒤바뀜(거지가 놀랐다고, 실제론 청명이 놀람).
- 회차를 넘어 **오류가 프로필에 박제·전파**: ep2/ep3에서도 char1(청명) 프로필에 "천마 살아있다" 잔존, char4가 "화산 장문인"인데 천마(마교 교주) facts를 그대로 보유.

### H6.2 조사 요약 — 가설이 여러 번 뒤집힘
전 과정 화산귀환 ep1 컷116–144(청명·거지 대화) + 전체 트랜스크립트 기반, LLM 콜 30+회.

| 가설 | 검증 | 결과 |
|---|---|---|
| (원안) step2 임베딩 빼고 VLM이 얼굴 crop으로 정체성 결정 | 112px crop 27개 몽타주 → glm-4.6v | **반증** — 24/27 과병합, 청명·거지 시각 구분 실패 |
| 추론 스테이지에 이미지 넣으면 교정 | 컷 이미지+텍스트 | 부분 — 관계 파악은 도움, but 천마 생사는 여전히 오답 |
| strict "사실 인용 강제" 프롬프트가 천마 생사 교정 | 재채점 | **반증** — 천마 생사는 텍스트만으로 11/12 이미 정답(문제 아님). strict는 who-is-who **악화** |
| **who-is-who 혼동의 뿌리 = 권위 로스터 부재** | 손-로스터 주입 | **확증** — 혼동 **0/6**, 사실 전부 교정 |
| "qwen ≫ glm(과추론)" | glm-5.2로 재검증 | **반증/정정** — glm-4.6v(**비전**)를 텍스트에 오용한 탓. **glm-5.2(텍스트)는 정답+빠름** |

핵심 수치:
- **천마 생사(사실)**: 로스터 없이도 텍스트만으로 11/12 정답 → 문제의 핵심 아님(사용자 지적이 옳았음).
- **who-is-who 혼동**(죽은/부재 인물을 화면 인물로 오인): 로스터 없음 **7/12**(strict 프롬프트가 최악, "거지=천마" 확신+자기검증 "모순없음"). **손-로스터 주입 시 0/6.**
- **로스터 자동추출 present_now 정답**(천마=미등장): glm-4.6v ~1/7, qwen 2/2, **glm-5.2 5/5**(회상/현재 분리·환생 인지까지 정확).
- **속도**: glm-5.2 로스터추출 ~2.5–4.5분/회차, 분석 ~0.5–1.5분. qwen(~15분)보다 몇 배 빠름.

### H6.3 일반화 결과 (glm-5.2 로스터접근, max_tokens 100k 실험 설정)

| 회차(장르) | 프로덕션(glm-4.6v)의 대표 오류 | 로스터접근(glm-5.2) 결과 |
|---|---|---|
| 화산 ep2 (무협) | "운암을 만나 입문"(운암=장문인 병합), char 프로필 천마 오류 잔존 | 운암=제자/장문인 분리 ✅, 종팔·사형 present=false ✅, 천마 없음 ✅, 초삼=청명 환생몸·구칠=정보전달자 정확 ✅ |
| 화산 ep3 (무협) | "청명과 초삼이 별개 동행", 대인/장문인 혼용 | 회상 cut11–72 / 현재 정확, 청진 present=false ✅, 운암≠장문인 ✅, 각본·객청 엔딩 정확 |
| 아카데미 ep2 (게임빙의) | (요약은 대체로 정확) | 에드=주인공, 페니아, 가주 present=false ✅ |
| 아카데미 ep3 (게임빙의) | — | 원작 주인공 테일리·원작 수석 루시 등 메타 참조 인물 present=false ✅, 회상/현재 정확 |
| 참교육 ep1 (학원 액션) | — | 회상(cut1–62)/현재(63–137) 분리, 박대석 present=false·사망 ✅, 우현식·담임 present=false ✅, 나화진·교감 present=true ✅ |

→ 로스터접근이 3웹툰·3장르 전부에서 회상/현재 분리 + 참조-부재 인물 present_now=false + 인물 역할 분리를 정확 처리, 프로덕션 who-is-who 오류를 제거. **qwen-vl 비권장(판정 확정)**: 회차 따라 불안정(화산 ep2 운암=장문인 병합, 참교육 회상전용 인물 present=true 오판) + glm의 2.6~10배 느림. glm-5.2는 5회차 전부 일관 정확 → 채택.

환생 트로프 처리 검증: 청명이 거지 소년(초삼) 몸으로 환생 → 초삼=청명의 환생 몸 이름(별개 인물 아님), 정보 전달자는 구칠 — glm-5.2 로스터가 정확 인지. (세션 중 사용자가 한때 "청명≠초삼"이라 정정했다가 재정정 — 정본은 청명=초삼.)

### H6.4 근본 원인 (2가지)
1. **서사 로스터 부재.** 당시 파이프라인은 회차 간 확정 이름만 `confirmed_roster_prior`로 나름 — 첫 회차·미확정 클러스터뿐인 초반엔 로스터가 비어, 모델이 애매한 in-scene 언급("천마! 너 천마 아냐?!", 욕설 "얼어 죽을 천마")으로 정체를 자유연상 → 현재 미등장인 천마를 화면 인물에 결부.
2. **텍스트 스테이지에 비전 모델 오용.** Stage R/N은 이미지 없는 텍스트 추론인데 glm-4.6v(비전)를 사용 — 반어·욕설·장르 트로프를 사실로 오독. glm-5.2(텍스트)로 교체 시 상당 부분 해소.

별개 문제(로스터로 안 고쳐짐): 얼굴 클러스터/프로필 bleed — step2 매칭(앵커 무제한 누적) + 프로필 누적 로직 문제 → 트랙 C(§H6.6, 현행 prd.md §18.5).

### H6.5 개선 제안 원문 요지 (구 §5 — 구현 결과는 prd.md §18)
- §5.1 Stage R/N 모델 glm-4.6v→glm-5.2 (모델명 하드코딩 금지). max_tokens 고정 100k 권장 → **구현 중 폐기**(400 ContextWindowExceeded/조기절단 실전 결함 2건 발견, 기본 미전송 정책으로 대체 — prd.md §18.3).
- §5.2 로스터 추출 스테이지 신설(에피소드 텍스트 1콜) + R·N 주입. 영속 옵션((a) 인메모리 / (b) 다음 회차 prior 합류)은 (a)로 시작.
- §5.3 (선택) `analysis_character_claim`으로 생사류 주장 근거 인용 커밋 — 미착수(백로그).
- §5.4 얼굴 클러스터/프로필 bleed — 당시 "범위 밖"이라 했으나 트랙 C로 실측·수정함.
- §5.5 Pass-1 JSON 파싱 실패 컷 드롭 — json-repair 폴백. 실태(완료 회차, scene_meta 없는 컷): 화산 1–6%, 아카데미 ep2 16%(17/109, 단 16컷은 무텍스트 액션 컷)·ep3 7%, 참교육 ep1 4%. 실측: naver/769209 ep4 cut80 실패는 glm-4.6v가 cut_summary에 미이스케이프 따옴표(`"네, 넷?!!"`) — json-repair가 5블록 손실 없이 복구.
- §5.6 불량 OCR/오탐 얼굴 soft-delete — 백로그로 이월(prd.md §18.8, 2026-07-07 파편 bbox dedup 증상 추가).

### H6.6 트랙 C — CCIP bleed 진단 실측 (구 §8.7)
- **실측**(화산귀환 char7 "초삼'C"=207얼굴 blob, 청명·대인·거지·조걸 혼재): 내부 pairwise CCIP diff **median 0.206**(=다른 인물 수준). **CCIP 자체는 정상**(청명 medoid vs 대인 medoid=0.231 분리, 청명+cluster8 재통합, 초삼 코어 ~50 순수). **근본 = 매칭 magnet**(무제한 앵커+greedy 1-NN), CCIP 한계 아님.
- 측정 방법: `dghs-imgutils`(model-api와 동일 CCIP metric) 로컬 설치 → Chroma 저장 feature로 diff 재현. 실컬렉션 `character_faces_naver_769209_CCIP`(케이스는 DB `EmbeddingModel.name=CCIP` 기준 — 앞선 "케이스 불일치" 우려는 실측 스크립트가 소문자 하드코딩한 탓으로 판명).
- ep4(=1101) 대사폭탄 회차에서 step3b가 heartbeat_timeout 10분 초과 → Temporal 재시도 → roster→R→N 무한 재실행 발견(heartbeat 수정 계기). ep4는 로컬 reresolve로 수동 커밋(run 112 succeeded, 406 speakers, finish=stop — max_tokens 미전송 정책 실전 검증).

### H6.7 재현 방법 & 산출물 (실험 스크립트, uncommitted throwaway)
- `webtoon-pipeline/_ident_experiment.py` — crop 몽타주/컷이미지/전체맥락 테스트. `_faith.py`~`_faith4.py` — 충실도·로스터 주입·자동추출 매트릭스. `_gen.py <episode_id> [qwen]` — 임의 회차 로스터추출→분석 + loose baseline. `_glm52_retest.py` — 로스터추출 모델 비교. `_rescore.py` — 재채점(자동 스코어러 키워드 오탐 주의). `_ident_gather.py` — 입력 수집. `_ccip_bleed.py`/`_ccip_recluster.py` — CCIP 실측(트랙 C 검증에 계속 사용, prd.md §18.5).
- 실행: `cd webtoon-pipeline && set -a && source ../prod.env && set +a && .venv/bin/python _gen.py <eid>` (읽기 전용).
- **분석 뷰어**: `roster-viewer.html`(레포 루트, 자족형 HTML) — 5회차 glm-5.2 vs qwen-vl 로스터·분석 비교. webtoonmoa 관리 화면 개선 시 UI 레퍼런스.

---

## H7. 구 DB 스키마 서술 (v3.x as-is, 구 §7 — v4.0 wipe로 대체됨)

> 2026-07-05 마이그레이션 0022/0023으로 아래 구조는 prod에서 소멸. 현행 스키마는 prd.md §7/§17.

- **WebtoonCut**(episode FK, cut_number, processed_at, is_stale, `llm_analyzed_at`, human_modified_at, `llm_model` FK) — is_stale/llm_analyzed_at은 v4.0에서 폐기(run 도출).
- **TextRegion**(cut FK, index, bbox, is_excluded) / **TextAnnotation**(region FK, source `paddle|llm|human`, text, type, speaker, confidence, model_version, resolution_status) — v4.0에서 `analysis_` prefix로 개명·유지.
- **Character**(webtoon_id, name, aliases, age, skills, first_seen_*, is_confirmed, is_name_auto_assigned, significance, notes) — NEW_CHAR placeholder 관습(→ v4.0 kind=cluster로 대체). (v3.6 과도기) 인물도감 메타는 `extra['llm_profile']` jsonb 병합 저장(→ `analysis_character_profile`로 대체).
- **CharacterAppearance**(character FK, label, description, first_seen_*, is_canonical) — 유지.
- **FaceRecord**(cut FK, face_idx, appearance_id FK/NULL, bbox, conf, chroma_doc_id, match_score, is_confirmed) — v4.0에서 `analysis_face_detection`+`analysis_face_identity`로 분해.
- **WebtoonPipelineState**(phaseN_status, phase2_last_completed_episode, phase2_processable_max_episode, phase3_enabled, ...) — 진행 카운터는 v4.0에서 폐기(run 도출), 설정성 필드만 `config_webtoon_pipeline_state`로 존치.
- **EpisodePipelineProgress**(episode FK, phase 값1~4, status, completed_at) — v4.0에서 폐기(analysis_run으로 수렴).
- Step3/4 스키마(v3.3): EpisodeReport / EpisodeBeat / NameDiscoverySuggestion(→ suggestion) / StoryArc(생산자 없음 → Stage A 산출로 재정의 예정) / NarrativeThread / CharacterClaim / LLMUsage / **WebtoonNarrativeState**(webtoon OneToOne, roster/open_threads/running_summary 캐시 — v3.6 슬림화 후 v4.0 폐기, prior는 정본 조인).
- 설정: EmbeddingModel(clip cosine 0.25 / ccip 0.16 시드) / WebtoonEmbeddingSetting / LLMModel(glm-4.6v 시드) / **WebtoonLLMSetting**(웹툰 단위 LLM 선택 — v4.1에서 폐기, 전역 modality 2-슬롯).
