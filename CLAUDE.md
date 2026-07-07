# 웹툰 분석 시스템 — 레포 구성

웹툰을 다운받아 분석하고 서비스하는 전체 시스템은 4개의 별도 Git 레포로 나뉘어 있다.

## data-pipeline (이 레포)
`/Users/jj/github/data-pipeline`

웹툰 컷 이미지를 분석하는 ML 파이프라인. 핵심 코드는 `webtoon-pipeline/`(Temporal 워커)에 있고, 4단계로 구성된다.

1. **Step 1 — OCR + YOLO** (`src/core/step1.py`): 컷 이미지에서 텍스트(PaddleOCR)와 얼굴 bbox(YOLO)를 로컬 모델로 추출. ✅ 구현됨.
2. **Step 2 — CCIP 인물 식별** (`src/core/step2.py`): 얼굴 crop을 임베딩(CLIP/CCIP)하고 Chroma 유사도 매칭으로 캐릭터에 귀속(신규면 NEW_CHAR 발급). ✅ 구현됨.
3. **Step 3 — LLM 스테이지 V→roster→R→N→apply** (`src/core/step3.py`): step3a=Stage V(컷당 비전 1콜, glm-4.6v) → step3b=roster(에피소드 인물 로스터 추출)→R(정체·화자)→N(서사) 텍스트 3콜(glm-5.2, fallback=qwen-vl) → step3c=apply(LLM 없이 결정론적 커밋). 모델은 DB `config_llm_model`의 modality 2-슬롯(vision/text)으로 해석. ✅ 구현·배포됨(v4.1, 2026-07-06).
4. **Step 4 — 회차 종합 요약**: 별도 단계가 아니라 apply 커밋에 흡수 — `analysis_episode_report`(summary/teaser 등)가 매 에피소드 자동 산출. ✅ (`episode-summary/main.py`는 통합 이전의 레거시 실험 러너, 프로덕션 경로 아님.)

모델 서빙은 `model-api/`(FastAPI, OCR/YOLO/CLIP/CCIP 모드 분리)가 담당.

자세한 배경/설계 결정은 `prd.md`(**정본** — 현행 설계·계약·백로그, §17 v4.0 스키마·§18 v4.1 로스터), `prd-history.md`(변천사 아카이브)에 있다 — 여기서는 각 레포/스텝의 역할만 정리. (`prd-step3.md`·`prd-identity-roster.md`는 2026-07-07 prd.md로 흡수 후 삭제됨.)

## service
`/Users/jj/github/service`

Django 백엔드. 웹툰 다운로드와 DB의 source of truth 역할.

- `backend/apps/api/toon/tasks.py`: 웹툰 회차를 다운로드하고 `config/temporal.py`의 `send_phase1_trigger`로 data-pipeline의 Temporal 워크플로를 트리거.
- `backend/apps/api/toon/models.py`: 웹툰/에피소드/캐릭터/텍스트/장면 등 전체 스키마 정의. data-pipeline이 분석 결과를 여기 정의된 테이블에 저장.

## webtoonmoa
`/Users/jj/github/webtoonmoa`

SvelteKit 프론트엔드. 분석된 웹툰을 조회하고, 분석 검증을 위한 사람 라벨링(human-in-the-loop)을 수행하는 화면 제공. service의 Django API를 소비.

## proxmox-configuration
`/Users/jj/github/proxmox-configuration`

k3s 클러스터에 ArgoCD로 배포되는 GitOps 설정(YAML) 모음. app-of-apps 구조로 위 레포들의 워크로드를 배포.

- `pipeline_repo/`: data-pipeline의 Temporal 워커 + model-api(ocr-yolo/clip/ccip) 배포
- `service_repo/`: Django backend + celery + redis + nginx 배포
- `temporal_repo/`: Temporal 서버 배포
- `envoy_repo/`, `monitoring_repo/`, `ollama_repo/`, `system/`: 게이트웨이/모니터링/로컬 LLM/클러스터 공통 컴포넌트

이 레포 push 시 CI(`.github/workflows/deploy.yaml`)가 `ghcr.io/strsu/*` 이미지를 빌드하고 **proxmox-configuration의 `pipeline_repo/kustomization.yaml` 태그를 자동 커밋**한다(`[skip-ci]`) — 태그 수동 수정 금지. 배포 리소스(nodeSelector: k3s-super-worker-01 고정, 리소스, Infisical 시크릿)를 바꾸려면 그쪽 레포에서 작업하고, 그쪽 `CLAUDE.md`·`docs/`를 먼저 읽을 것.

## 프로덕션 DB 직접 조회

접속 정보는 이 레포 루트의 `prod.env`(gitignore됨)에 있다. 로컬에 `psql`이 없으므로 `webtoon-pipeline/.venv`의 psycopg2로 조회한다(이미 설치돼 있음).

```bash
cd /Users/jj/github/data-pipeline/webtoon-pipeline
set -a && source ../prod.env && set +a
.venv/bin/python -c "
import psycopg2
conn = psycopg2.connect(host='$POSTGRES_HOST', port='$POSTGRES_PORT', dbname='$POSTGRES_DB', user='$POSTGRES_USER', password='$POSTGRES_PASSWORD')
cur = conn.cursor()
cur.execute('SELECT ...')
for r in cur.fetchall(): print(r)
"
```

주의: `POSTGRES_PORT`(5459)를 쓴다 — `PGBOUNCER_PORT`(5460)가 아니라 direct 포트. **읽기 전용 조회만** 하고, UPDATE/DELETE는 절대 직접 실행하지 않는다(source of truth는 `service` Django, 쓰기는 그쪽 마이그레이션/코드 경로로).

자주 쓰는 테이블(스키마는 `service/backend/apps/api/toon/models.py`가 정의). v4.0(2026-07-05)부터 분석 산출은 `analysis_`, 설정은 `config_` prefix — **prod 적용 완료(마이그레이션 0022~0026, 2026-07-05~06). 실제 DB가 신 스키마다**(구 face_record 등은 소멸):
- 콘텐츠(불가침, prefix 없음): `webtoon`(id, title_id, source) / `webtoon_episode`(id, webtoon_id, no) / `webtoon_cut`(id, episode_id, cut_number, human_modified_at)
- `analysis_run`(webtoon_id, episode_id, kind=step1|step2|vision|resolve|arc, status=running|succeeded|failed, vision_run_id, stats) — 진행도/stale의 정본("step3 됐나"=succeeded resolve run 존재, "stale"=human_modified_at>run.finished_at)
- `analysis_character`(id, webtoon_id, kind=cluster|character, name, is_confirmed, significance, is_match_excluded) / `analysis_character_appearance`(id, character_id) / `analysis_character_profile`(character_id, source=llm|human, gender, age_group, role, personality, key_facts) — 인물(클러스터→승격) + 도감
- `analysis_face_detection`(id, cut_id, face_idx, bbox, is_used) / `analysis_face_identity`(detection_id, source=step2|human, appearance_id, score, run_id) — 얼굴 탐지/정체 레이어(human>step2 우선) / `analysis_face_embedding`(detection_id, embedding_model)
- `analysis_text_region`(id, cut_id, index, is_excluded) / `analysis_text_annotation`(id, region_id, source=paddle|llm|human, type, speaker_id, resolution_status) — Step1 OCR + Step3 결과
- `analysis_cut_scene_meta`(cut_id, action_summary, key_objects, run_id) — Stage V 컷별 상황서술
- `analysis_episode_report`(episode_id, summary, teaser, appeal_point, cliffhanger, character_timeline) / `analysis_episode_beat` / `analysis_narrative_thread` / `analysis_character_claim` — Stage R/N 산출
- `analysis_suggestion`(webtoon_id, type=name|merge|face_reassign|label_conflict, character_id, detection_id, episode_id, cut, payload, confidence, run_id, status=pending|accepted|rejected) — AI 제안 통합 검토 큐
- `analysis_llm_usage`(webtoon_id, episode_id, cut_id, stage=vision|**roster**|resolve|narrative|arc, total_tokens, finish_reason, run_id) — 콜별 토큰/완료상태
- 설정: `config_llm_model`(+fallback self-FK, params.context_window만 — max_tokens 비움) / `config_embedding_model` / `config_webtoon_embedding_setting` / `config_webtoon_pipeline_state`(phase3_enabled 등). `config_webtoon_llm_setting`은 v4.1에서 폐기됨(전역 modality 2-슬롯).

### LiteLLM 원본 응답 조회 (confidence 등 우리 DB가 버리는 값 복구용)

우리 DB(`beldori`)는 Pass-1 블록별 confidence/tail_hint, Pass-2a의 name_confidence(자동 rename된 경우) 등을 **영속화하지 않는다**(design: belief state는 비영속). 그 값이 필요하면 같은 Postgres 서버의 `litellm`(LiteLLM 게이트웨이) DB에 원본 콜이 남아있다 — dbname만 바꿔서 동일 접속정보로 조회:

```bash
.venv/bin/python -c "
import psycopg2
conn = psycopg2.connect(host='$POSTGRES_HOST', port='$POSTGRES_PORT', dbname='litellm', user='$POSTGRES_USER', password='$POSTGRES_PASSWORD')
cur = conn.cursor()
cur.execute('SELECT request_id, \"startTime\", response FROM \"LiteLLM_SpendLogs\" WHERE \"startTime\" BETWEEN %s AND %s ORDER BY \"startTime\"', (start, end))
"
```

- `LiteLLM_SpendLogs.response`(jsonb)에 `choices[0].message.content`로 모델이 실제로 낸 원문 JSON(confidence/label_conflict/evidence 등 우리가 버린 값 포함)이 남는다. `startTime`은 UTC(우리 `llm_usage.created_at`과 동일 기준) — episode_id/cut별로 안 찍히므로 시간대 + 응답 내용(대사/인물명 등)으로 매칭해서 찾아야 한다.
- `messages`(요청 프롬프트, 즉 identified_faces에 뭘 넣어 보냈는지)는 **litellm UI에서는 보이지만 DB에는 여전히 빈 값(`{}`)**(2026-07-07 실측, `metadata.proxy_server_request`도 빈 값) — SQL로 과거 요청 프롬프트를 복구하는 건 불가, 요청 확인은 UI로.
- ⚠️ `messages`/`response` 모두 길면 중간이 잘려 저장된다(`...litellm_truncated skipped N chars...` 마커, `MAX_STRING_LENGTH_PROMPT_IN_DB` 초과분 — 길이 제한은 수용하기로 함, 2026-07-07). 앞/뒤는 남아있으니 필요한 필드가 앞쪽(예: characters[0])이면 보통 확인 가능.
