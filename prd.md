# 웹툰 분석 파이프라인 PRD

> **목적**: 현재 구현 상태와 목표 파이프라인을 한곳에 정리하고, 리뷰하면서 지속 업그레이드하기 위한 문서.
> **마지막 갱신**: 2026-05-28 (v2.0 — 리뷰 피드백 반영: threshold 보정, Step 2 멱등성 가드, OpenCV 스타일 확정, is_name_auto_assigned 추가, 404 vs 5xx 재시도 로직)
> **주요 코드**: `pipeline.py` (레거시), `webtoon-pipeline/src/` (Faust 앱)

---

## 1. 개요

웹툰 컷 이미지에서 **텍스트(OCR)**, **얼굴(캐릭터)**, **장면·화자(멀티모달 LLM)** 를 추출하고, 회차 단위로 서사를 요약하는 파이프라인.

| 구분 | 설명 |
|------|------|
| 입력 | S3 저장 컷 이미지 (`{S3_LOCATION}/{source_dir}/{title_id}/{ep}/{title_id}_{ep}_{cut}.jpg`) — boto3 직접 다운로드 |
| 로컬 모델 | PaddleOCR, [deepghs/anime_face_detection](https://huggingface.co/deepghs/anime_face_detection) (YOLO), ResNet50 임베딩 |
| 원격 모델 | GLM-4.6v (z.ai API) |
| 저장 | PostgreSQL (텍스트·얼굴 메타) + S3 (face crop: `{source}/{title_id}/face_crop/{pk}.jpg`) + Chroma (얼굴 임베딩, Step 2~) |
| **규모** | 웹툰 **30+종**, 누적 에피소드 **~6,000개**, 누적 컷 **~60만 장**, 일 신규 **~10 에피소드 (~1,000 컷)** |
| **스트림 처리** | **Faust + Kafka** — 에피소드 간 병렬, 웹툰별 에피소드 순차 |

---

## 2. 현재 구현 (`pipeline.py`)

실행: `python3 pipeline.py`

```
[1/3] OcrRefineOperator  →  *_result.json
[2/3] SceneOperator      →  *_scene.json
[3/3] FaceOperator       →  face_crops/ + face_db/
```

### 2.1 Operator 상세

#### OcrRefineOperator
- **입력**: 컷 이미지 URL
- **처리**: PaddleOCR → GLM-4.6v (타입 분류 + 맞춤법 교정)
- **출력**: `{webtoon_id}_{episode}_{cut}_result.json`
- **blocks 필드**: `index`, `bbox_2d`, `paddle_text`, `paddle_score`, `type`, `corrected_text`
- **type enum**: `narration` / `speech` / `sfx` / `caption` / `other`
- **특징**: 컷당 GLM 1회 호출. speaker 정보 없음.

#### SceneOperator
- **입력**: `*_result.json` (슬라이딩 윈도우 최대 3컷: N-2, N-1, N)
- **처리**: GLM-4.6v 멀티모달 (3장 이미지 + 블록 텍스트 + 이전 컷 scene 요약)
- **출력**: `{webtoon_id}_{episode}_{cut}_scene.json`
- **필드**: `scene_meta` (action_summary, key_objects), `speaker_mapping` (index, speaker, rationale)
- **특징**: 컷당 GLM 1회 추가 호출. **얼굴 정보 미사용**. blocks와 speaker가 **분리**된 파일.

#### FaceOperator
- **입력**: `*_result.json` 목록 (에피소드 단위)
- **처리**:
  1. YOLO 얼굴 탐지 ([deepghs/anime_face_detection](https://huggingface.co/deepghs/anime_face_detection) — Ultralytics YOLO `.pt` 포맷으로 사용)
  2. 크롭 저장 (`face_crops/`)
  3. ResNet50(IMAGENET) 임베딩 추출
  4. 에피소드 내 계층적 클러스터링 (cosine distance, threshold=0.45)
- **출력**: `face_db/embeddings.npy`, `metadata.json`, `clusters.json`, `cluster_grid.jpg`
- **특징**: Scene **이후** 실행. 컷 단위 인물명 할당 없음. Chroma 미사용.

### 2.2 보조 도구

| 파일 | 역할 |
|------|------|
| `web_manager.py` | Flask UI — 클러스터에 캐릭터 이름 수동 지정, 클러스터 이동/제거 (`labels.json`) |
| `main7.py` | `query_similar()` (코사인 top-k 검색) 프로토타입 — `pipeline.py` 미연동 |

### 2.3 현재 데이터 흐름

```
컷 N ── PaddleOCR ── GLM(refine) ──→ *_result.json
                │
                └──→ GLM(scene, 3컷) ──→ *_scene.json  (얼굴 정보 없음)

에피소드 ── YOLO ── ResNet50 ── 클러스터링 ──→ face_db/
                                              └── web_manager (수동 라벨링)
```

### 2.4 현재 한계

| # | 한계 |
|---|------|
| 1 | OCR·얼굴 탐지가 **감 단위 병렬**이 아님 (순차 3 Operator) |
| 2 | 얼굴 DB가 Scene/화자 분석에 **연결되지 않음** |
| 3 | GLM **2회/컷** (교정 + 장면) — 비용·지연 |
| 4 | 인물 식별 = 클러스터 ID + **수동** 이름 (자동 `카락` / `신규_인물_A` 없음) |
| 5 | **회차 종합 요약** 미구현 |
| 6 | JSON 파일 저장 — 쿼리·이력 추적 불편 |
| 7 | `full_text`를 result.json에 저장 (annotation 변경 시 동기화 이슈 가능) |

### 2.5 성능 (측정값)

- 약 **3분/컷** (OCR + GLM refine + GLM scene, 순차)
- 화당 ~100컷 → 순차 처리 시 **약 5시간/화**

---

## 3. 목표 파이프라인 (4단계)

```
Step 1 ── 로컬 추출 (OCR + YOLO, 병렬)          ← 모든 웹툰  ✅ 구현 완료
Step 2 ── 인물 식별 (Chroma 벡터 DB)             ← 모든 웹툰, 웹툰별 에피소드 순차
Step 3 ── GLM 통합 분석 (슬라이딩 윈도우, 1회/컷) ← 활성 웹툰만
Step 4 ── 회차 종합 요약                          ← 활성 웹툰만
```

> **Step 1·2 전체 대상 / Step 3·4 활성 웹툰만**: Step 1·2는 누적 60만 컷 전체 처리(백로그 소화 + 일 신규). Step 3 GLM은 비용·시간이 크므로 현재 연재 중이거나 서비스 중인 활성 웹툰만 실행. 비활성 웹툰은 Step 1·2 결과를 저장해 두고, 활성화 시 Step 3부터 이어서 실행.

### Step 1: 로컬 텍스트 및 얼굴 추출 — 모든 웹툰, 완전 병렬

| 항목 | 내용 |
|------|------|
| 대상 | **모든 웹툰** (30+종, 누적 60만 컷) |
| Input | 컷 이미지 (로컬 파일 또는 URL) |
| Process | PaddleOCR → `text_data_raw` (텍스트 + bbox) |
| | YOLO ([anime_face_detection](https://huggingface.co/deepghs/anime_face_detection)) → `face_bboxes` |
| | **에피소드 내 컷 순차** (cut1 → cut2 → 404 감지 → 에피소드 완료) |
| | **에피소드 간 완전 병렬** (Kafka 라운드로빈, N workers) |
| Output | `text_data_raw`, `face_bboxes` — **GLM 호출 없음** |

> **404 기반 에피소드 경계 감지**: 컷 이미지 API 또는 로컬 파일에서 cut N+1이 존재하지 않으면 에피소드 종료로 판단. `episode_phase1_complete` 이벤트 발행. 현재 `pipeline.py`의 `image_exists()` 로직과 동일.

> **YOLO 모델**: [deepghs/anime_face_detection](https://huggingface.co/deepghs/anime_face_detection)  
> 애니메이션·웹툰 얼굴 전용 탐지 모델. F1 ~0.94–0.97. 프로젝트에서는 Ultralytics YOLO 가중치(`.pt`)로 로드.

### Step 2: 인물 식별 (Chroma) — 모든 웹툰, 웹툰별 에피소드 순차

| 항목 | 내용 |
|------|------|
| 대상 | **모든 웹툰** (30+종) |
| Input | `face_bboxes`, 컷 이미지 |
| Process | 1. bbox 크롭 → 임베딩 추출 (ResNet50) |
| | 2. **Chroma** 벡터 DB에서 코사인 유사도 검색 |
| | 3. score ≥ threshold → DB 캐릭터명 할당 (예: `"카락"`) |
| | 4. 미매칭 → 임시 ID 생성 + DB 등록 (예: `"NEW_CHAR_001"`) |
| Output | `identified_faces = [{"face_id": "FACE_0", "name": "카락", "bbox": [...]}, ...]` |

**에피소드 순차 처리 (웹툰별)**

Step 2는 **웹툰 단위로 ep1 → ep2 → ep3 ... 순서를 반드시 지킨다.**  
ep1에서 확정된 캐릭터 정보가 ep2 매칭에 반영되어야 인물 식별 품질이 보장되기 때문이다.

```
Webtoon A worker: ep1 완료 → 이벤트 → ep2 완료 → 이벤트 → ep3 ...
Webtoon B worker: ep1 완료 → 이벤트 → ep2 완료 → 이벤트 → ep3 ...  (A와 독립 병렬)
```

- Kafka `webtoon_id` 파티셔닝 → 같은 웹툰은 항상 같은 worker → 에피소드 순서 자동 보장
- **웹툰 간 캐릭터 namespace 독립** → Race condition 없음, Redis lock 불필요
- 에피소드 완료 이벤트 수신 후 다음 에피소드 트리거 — 자세한 내용은 **§20 Human Checkpoint**

**현재 대비 변경점**
- 에피소드 일괄 클러스터링 → **컷 단위 실시간 검색·할당**
- numpy 파일 → **Chroma persistent collection**
- 수동 라벨링(web_manager) → **자동 할당 + UI에서 이름 확정/수정** (human override 유지)

### Step 3: GLM 멀티모달 장면/화자 분석 — 활성 웹툰만

| 항목 | 내용 |
|------|------|
| 대상 | **활성 웹툰만** (`WebtoonPipelineState.phase3_enabled = True`) |
| Input | 이미지: 컷 N-2, N-1, N (**원본 3장, 병합하지 않음**) |
| | 텍스트: `text_data_raw`, `identified_faces` |
| | 컨텍스트: N-1 컷 **마지막 대사** |
| Process | 1. **현재 컷(N) 이미지에 OpenCV 오버레이** — `identified_faces` bbox + `[FACE_0: 카락]` 라벨 렌더링 |
| | **오버레이 스타일 확정**: `cv2.rectangle` 흰색 filled rect → `cv2.putText` 검은 글씨(thickness=2). GLM은 텍스트를 이미지로 파싱하므로 배경 없는 단색 텍스트는 배경과 겹쳐 미인식 위험. |
| | 2. GLM 1회 — type 분류 + speaker 확정 + corrected_text + scene_meta + **name_discoveries** |
| | 3. 오버레이된 N번 이미지 + N-2·N-1 원본을 슬라이딩 윈도우로 전달 |
| | 4. 말풍선 꼬리 ↔ 오버레이 얼굴 라벨 매칭으로 화자 결정 |
| Output | 통합 JSON (아래 스키마) |

> **오버레이 전략**: `identified_faces` JSON만 텍스트로 넘기면 GLM이 text `bbox_2d`와 face `bbox` 공간 매칭에 토큰·추론 비용을 낭비한다. 현재 컷(N)에 bbox + 이름 라벨을 **시각적으로 박아 넣은 버퍼**를 전달하면 말풍선 꼬리 매칭 정확도가 크게 향상된다. N-2, N-1은 맥락용 원본 유지(종횡비 붕괴 방지).

```json
{
  "scene_meta": {
    "action_summary": "줄거리 요약",
    "key_objects": []
  },
  "blocks": [
    {
      "index": 0,
      "type": "speech",
      "speaker": "카락",
      "corrected_text": "대체 내가 왜 여기에..."
    }
  ],
  "name_discoveries": [
    {
      "name": "카락",
      "character_id": "NEW_CHAR_001",
      "confidence": 0.92,
      "evidence": "직접 호칭 — FACE_0에게 '카락, 잠깐만'"
    }
  ]
}
```

> **name_discoveries**: 대사·나레이션에서 GLM이 추출한 이름-얼굴 매핑. confidence ≥ 0.85 → 자동 반영, 미만 → webtoonmoa 검토 플래그. 자세한 내용은 **§19 인물 이름 자동 추출**.

**현재 대비 변경점**
- OcrRefine + Scene **2회 GLM → 1회 통합** (Step 1 GLM refine **제거** — §12.1 결정)
- `speaker_mapping` 분리 → `blocks[].speaker` 통합
- 얼굴 정보: JSON 나열 + **현재 컷 OpenCV 오버레이 이미지** 이중 전달
- **`name_discoveries` 출력 추가** (§12.9 결정)
- **활성 웹툰만 실행** (§12.10 결정)

### Step 4: 에피소드 최종 요약 — 활성 웹툰만

| 항목 | 내용 |
|------|------|
| 대상 | **활성 웹툰만** |
| Input | 해당 회차 모든 Step 3 JSON 배열 (타임라인 순) |
| Process | 텍스트 전용 모델 또는 GLM에 대사·장면 요약 전체 피딩 |
| Output | `{episode}_report.json` |

```json
{
  "episode_summary": "회차 전체 줄거리",
  "character_timeline": [
    {"name": "카락", "first_cut": 3, "last_cut": 87, "key_moments": []}
  ],
  "foreshadowing_objects": ["반지", "편지"]
}
```

---

## 4. 목표 데이터 흐름

```
컷 N ─┬─ PaddleOCR ──→ text_data_raw ─────────────┐
      └─ YOLO ───────→ face_bboxes ──┐             │
                                     ▼             │
                              Chroma 검색          │
                                     │             │
                              identified_faces ────┤
                                                   ▼
                              GLM(3컷 + overlay) ──→ *_analysis.json
                                                   │
모든 컷 JSON ──────────────────────────────────────┴──→ {ep}_report.json
```

---

## 5. 벡터 DB 설명세 (Chroma)

### 5.1 Collection 설계

**웹툰별 독립 컬렉션** (`character_faces_{webtoon_id}`) 방식 채택.

| Collection 패턴 | 예시 | 용도 |
|----------------|------|------|
| `character_faces_{webtoon_id}` | `character_faces_808482` | 웹툰별 캐릭터 얼굴 임베딩 |
| `character_profiles` | — | 캐릭터 메타 (name, aliases, first_seen_cut) — metadata 또는 별도 PG 테이블 |

> **단일 컬렉션 대신 웹툰별 분리 이유** (§12.12 결정):
> - 60만 건+ 규모에서 metadata 필터링(`webtoon_id`)은 전체 HNSW 인덱스를 스캔한 뒤 필터링하므로 성능 저하
> - 웹툰 재작업 시 컬렉션 drop·재생성이 가능 — 단일 컬렉션이면 타 웹툰 데이터까지 오염 위험
> - 웹툰 간 캐릭터 namespace가 완전히 독립적이므로 cross-collection 쿼리 불필요

```python
def get_face_collection(webtoon_id: int) -> chromadb.Collection:
    return client.get_or_create_collection(
        name=f"character_faces_{webtoon_id}",
        metadata={"hnsw:space": "cosine"},
    )
```

### 5.6 Chroma 배포 위치 및 연결 설정

| 환경 | 호스트 | 포트 | 비고 |
|------|--------|------|------|
| **운영** | `oci-croma.prup.xyz` | `8000` | OCI 인스턴스 독립 서버 |
| **개발** | `localhost` | `8000` | docker-compose `chromadb` 서비스 |

**환경 변수** (PostgreSQL과 동일 패턴):

```env
CHROMA_HOST=oci-croma.prup.xyz   # 개발: localhost
CHROMA_PORT=8000
CHROMA_AUTH_TOKEN=<token>
```

**클라이언트 연결 코드** (`croma-test.py` 패턴 기반):

```python
import chromadb
from chromadb.config import Settings

def get_chroma_client() -> chromadb.HttpClient:
    settings = Settings(
        chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
        chroma_client_auth_credentials=settings.CHROMA_AUTH_TOKEN,
    )
    return chromadb.HttpClient(
        host=settings.CHROMA_HOST,
        port=settings.CHROMA_PORT,
        settings=settings,
    )
```

**Collection 생성** (웹툰별 분리 — §5.1):

```python
collection = client.get_or_create_collection(
    name=f"character_faces_{webtoon_id}",
    metadata={"hnsw:space": "cosine"},
)
```

**개발 환경 docker-compose** (service `z_docker/` 또는 별도 `docker-compose.chroma.yml`):

```yaml
services:
  chromadb:
    image: chromadb/chroma:1.3.5
    container_name: chromadb
    restart: always
    environment:
      - IS_PERSISTENT=TRUE
      - ANONYMIZED_TELEMETRY=FALSE
      - CHROMA_SERVER_AUTHN_PROVIDER=chromadb.auth.token.TokenAuthenticationServerProvider
      - CHROMA_SERVER_AUTHN_CREDENTIALS=${CHROMA_AUTH_TOKEN}
    volumes:
      - ./chroma_data:/chroma/chroma
    ports:
      - "8000:8000"
```

> **유사도 메트릭**: `hnsw:space=cosine` → Chroma가 반환하는 `distance`는 **cosine distance (낮을수록 유사)**. **P0 시작값 0.25** — 0.45는 코사인 유사도 0.55로 환산되어 ResNet50(ImageNet) 기준 애니 캐릭터 False Positive 폭발 위험. 미매칭 과다 시 0.05씩 완화. P2 애니 특화 모델 교체 후 재보정 (§14).

### 5.2 Document / Metadata 스키마 (CharacterAppearance 반영)

```python
# Chroma document id — 재처리 멱등성 보장 (§5.5)
doc_id = f"{webtoon_id}_{episode}_{cut}_F{face_idx}"   # 예: 808482_1_16_F0

# Chroma add() 시
{
  "id": doc_id,
  "embedding": [...],                  # 2048-dim (ResNet50) → P2~P3 애니 특화 모델 교체
  "metadata": {
    "webtoon_id": 808482,
    "episode": 1,
    "cut": 16,
    "face_idx": 0,
    "character_id": "NEW_CHAR_001",    # 논리 캐릭터 ID (웹툰 글로벌)
    "appearance_id": 3,                # CharacterAppearance PK (외형 단위)
    "appearance_label": "현실",        # 빠른 조회용 denorm
    "character_name": "카락",          # null이면 미확정
    "is_confirmed": false,             # webtoonmoa 관리 페이지에서 human confirm
    "bbox": {"x1": 0, "y1": 0, "x2": 0, "y2": 0},
    "crop_path": "face_crops/808482_1_16_face00.jpg",
    "conf": 0.87,
    "created_at": "2026-05-23T..."
  }
}
```

### 5.3 검색·할당 로직

```
1. query_embedding = embed(crop)
2. results = collection.query(query_embeddings=[query_embedding], n_results=5)
3. best = results[0]
4. if best.distance <= MATCH_THRESHOLD (cosine):
     name = best.metadata.character_name or best.metadata.character_id
   else:
     with new_character_lock:                    # §Step 2 동시성 제어
       char_id = allocate_character_id()        # NEW_CHAR_{INCREMENT} (웹툰 글로벌)
       doc_id = f"{webtoon_id}_{episode}_{cut}_F{face_idx}"
       collection.upsert(id=doc_id, ...)         # add() 대신 upsert (§5.5)
5. return {"face_id": doc_id, "name": name, "bbox": ..., "match_score": ...}
```

| 파라미터 | 현재값 (FaceOperator) | 목표값 (검토) |
|----------|----------------------|---------------|
| MATCH_THRESHOLD | CLUSTER_DIST=0.45 (클러스터링) | **P0: 0.25** (ResNet50 ImageNet 기준 0.45는 유사도 0.55로 False Positive 폭발 위험 → §14) |
| FACE_CONF_THRESHOLD | 0.3 | 0.3 (모델 권장 threshold 참고) |
| 임베딩 모델 | ResNet50 (ImageNet) | P0: ResNet50 유지 → **P2~P3: 애니 특화 모델 교체 (핵심 마일스톤)** |

### 5.4 web_manager 연동 (유지·확장)

- human이 클러스터/캐릭터명 확정 → Chroma metadata `character_name`, `is_confirmed=true` 업데이트
- 잘못된 매칭 → face를 다른 character collection entry로 재할당
- `labels.json` → Chroma metadata로 **마이그레이션** 예정

### 5.5 인물 식별 오염 방지 및 예외 처리 (Edge Case)

1. **신규 인물 발급 스코프 (Identity Scope)**
   - `allocate_character_id()`로 발급되는 ID는 **웹툰 전체 스코프(Global)** 로 관리한다.
   - 포맷: `NEW_CHAR_{INCREMENT_ID}` 또는 `NEW_CHAR_{UUID_SHORT}`
   - 이유: 에피소드 단위로 자르면 1화 신규 인물이 2화에서 또 신규로 분리되어 Chroma DB가 파편화됨. 1화에서 발급된 임시 ID가 2화에서도 매칭되도록 글로벌 컬렉션을 유지한다.

2. **동일 컷 재처리 시 Chroma Entry 멱등성 (Idempotency)**
   - Chroma `id`를 `{webtoon_id}_{episode}_{cut}_F{face_idx}` 규격으로 **고정**한다.
   - 동일 컷 재처리 시 기존 `id`가 존재하면 `add()`가 아닌 **`upsert()`** 또는 `delete_by_id` 후 재삽입하여 유일성을 보장한다.

3. **Step 3 멀티모달 컨텍스트 주입 최적화**
   - 이미지 종횡비 붕괴 방지: N-2, N-1, N을 **병합하지 않고** 원본 배열로 GLM API에 전달한다.
   - **현재 컷(N)만** OpenCV로 `identified_faces` bbox + `[FACE_0: 카락]` 라벨을 오버레이한 버퍼를 추가 전달한다.
   - N-2, N-1은 맥락 파악용 **원본** 유지.

4. **신규 등록 레이스 컨디션**
   - §Step 2 동시성 제어 표 참조. Step 2 순차 처리 + Lock(단기) → Redis Lock(중장기).

---

## 6. 저장 구조

JSON 파일 출력 없음. 모든 결과는 PostgreSQL + S3에 직접 저장.

| Step | PostgreSQL | S3 |
|------|-----------|-----|
| Step 1 | `WebtoonCut`, `TextRegion`, `TextAnnotation(paddle)`, `FaceRecord` | `face_crop/{pk}.jpg` |
| Step 2 | `FaceRecord.appearance_id` 업데이트 | Chroma collection |
| Step 3 | `TextAnnotation(glm)`, `CutSceneMeta` | — |
| Step 4 | `EpisodeReport` | — |

### Face Crop S3 경로 규칙

```
{S3_LOCATION}/{source}/{title_id}/face_crop/{face_record_id}.jpg
# 예: media/kakao/808482/face_crop/1234.jpg
```

`FaceRecord.crop_s3_key` property로 항상 재현 가능 (DB 저장 불필요).

---

## 7. DB 스키마 (PostgreSQL)

Region(위치)과 Annotation(텍스트 해석) 분리 원칙 유지. `service` 레포 `apps/api/toon/models.py`에서 Django 모델로 관리.

> **웹툰·에피소드 모델**: `Webtoon(source, title_id)` + `WebtoonEpisode(webtoon FK, no)` 단일 통합 모델. Kakao/Naver 분리 없음.

### WebtoonCut
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | PK | |
| episode | FK → WebtoonEpisode | |
| cut_number | smallint | |
| processed_at | timestamp / NULL | Step 1 완료 시각 |
| is_stale | bool | Human 수정 후 재분석 필요 여부 |
| glm_analyzed_at | timestamp / NULL | Step 3 완료 시각 |
| human_modified_at | timestamp / NULL | |

> `image_url` 없음 — episode FK traversal + cut_number로 S3 경로 항상 재현 가능.

### TextRegion (bbox 불변)
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | PK | |
| cut_id | FK | |
| index | int | 컷 내 순서 |
| bbox_x1..y2 | int | |
| is_excluded | bool | human이 제외 지정한 영역 (간판·UI 등). GLM 입력 및 UI 표시에서 제외. |

> **is_excluded 사용 시나리오**: 간판, 배경 텍스트, 게임 UI 숫자 등 분석에 불필요한 텍스트 영역을 human이 webtoonmoa 관리 페이지에서 체크. `type=other`(GLM 자동 분류)와 달리 human 명시적 제외이므로 별도 필드로 관리. 제외 후 해당 컷은 `is_stale=True` 처리.

### TextAnnotation (레이어 적재)
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | PK | |
| region_id | FK | |
| source | ENUM | `paddle` / `glm` / `human` |
| text | text | |
| type | ENUM / NULL | narration / speech / sfx / caption / other |
| speaker | text / NULL | Step 3 이후 |
| confidence | float / NULL | |
| model_version | text / NULL | |
| created_at | timestamp | |

**최종 텍스트 우선순위**: `human > glm > paddle` (쿼리로 파생, full_text 컬럼 없음)

### Character (신규 — 논리적 캐릭터)
| 컬럼 | 타입 | 설명 |
|------|------|------|
| character_id | PK | |
| webtoon_id | int | |
| name | text | `"카락"` 또는 `"NEW_CHAR_001"` |
| aliases | text[] / JSONB | 이명·별칭 목록 (예: `["주인공", "검은 기사"]`) |
| age | text / NULL | 나이 (텍스트 — "약 20대", "알 수 없음" 허용) |
| skills | text[] / JSONB | 기술·능력 목록 |
| first_seen_episode | int / NULL | 최초 등장 회차 (전체 외형 통합) |
| first_seen_cut | int / NULL | 최초 등장 컷 번호 |
| is_confirmed | bool | human 확정 여부 |
| is_name_auto_assigned | bool | AI가 자동 지정한 이름 여부 — webtoonmoa UI에서 "AI 추천 이름 (검토 필요)" 배지 표시용 |
| notes | text / NULL | 기타 메모 (human 입력) |

> **자동 수집 vs human 입력**: `name`, `first_seen_*`은 파이프라인이 자동 채움. `aliases`, `age`, `skills`, `notes`는 webtoonmoa 관리 페이지에서 human 입력.
> **`is_name_auto_assigned` 용도**: `is_confirmed=False`는 "미확정"이지만 이름이 없는 NEW_CHAR 상태와 구분이 불가. `is_name_auto_assigned=True`이면 AI가 이름을 지정했지만 아직 human이 검토하지 않은 상태. 회상·변장·사칭 등 고신뢰도 오인식 방지를 위해 human이 최종 검토할 수 있도록 UI에서 명시.

### CharacterAppearance (신규 — 시각적 외형 단위)

동일 인물이 변장·이세계·성장 등으로 **외형이 달라지는 경우**를 대응하기 위해 `Character`(논리 인물)와 `CharacterAppearance`(시각 외형)를 분리한다.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | PK | |
| character_id | FK → Character | 논리적 동일 인물 |
| label | text | 외형 레이블 — 자유 텍스트 (예: `"현실"`, `"게임 아바타"`, `"변장"`, `"어린 시절"`) |
| description | text / NULL | 외형 상세 설명 |
| first_seen_episode | int / NULL | 이 외형으로 처음 등장한 회차 |
| first_seen_cut | int / NULL | |
| is_canonical | bool | 대표 외형 여부 (캐릭터 썸네일 등에 사용) |

**사용 예시**

```
Character: "카락"
├── CharacterAppearance(label="현실", is_canonical=true)   ← 기본 등장
├── CharacterAppearance(label="게임 아바타")               ← 게임 내 분신
└── CharacterAppearance(label="유년기")                    ← 회상 장면

Character: "이한수"
├── CharacterAppearance(label="현재", is_canonical=true)
└── CharacterAppearance(label="변장")                      ← 스파이 모드
```

> **Chroma 연동**: 각 `CharacterAppearance`는 별도 얼굴 임베딩 클러스터를 가진다. Chroma metadata에 `character_id` + `appearance_id` 함께 저장. 검색 시 같은 `character_id`의 모든 외형을 묶어서 "카락"으로 표시 가능.

### FaceRecord
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | PK | |
| cut_id | FK → WebtoonCut | |
| face_idx | smallint | 컷 내 얼굴 인덱스 |
| appearance_id | FK → CharacterAppearance / NULL | 매칭된 외형 (미매칭 시 NULL) |
| bbox_x1..y2 | float | YOLO 탐지 bbox |
| conf | float | YOLO confidence |
| chroma_doc_id | text | Chroma collection doc id |
| match_score | float / NULL | 코사인 유사도 (낮을수록 유사) |
| is_confirmed | bool | human 확정 여부 |

> `crop_path` 없음 — S3 경로는 `crop_s3_key` property로 재현: `media/{source}/{title_id}/face_crop/{id}.jpg`  
> `appearance_id` → `CharacterAppearance` → `Character` 경유 조회. `Character` 직접 FK 없음.

### WebtoonPipelineState
| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | PK | |
| webtoon | OneToOne → Webtoon | |
| phase1_status | ENUM | `idle` / `running` / `completed` / `error` |
| phase2_status | ENUM | `idle` / `running` / `checkpoint` / `completed` / `error` |
| phase2_last_completed_episode | FK → WebtoonEpisode / NULL | 마지막으로 완료된 Step 2 에피소드 |
| phase2_pending_next_episode | FK → WebtoonEpisode / NULL | 다음 실행 대기 에피소드 |
| phase2_processed_count | int | Step 2 완료 에피소드 수 (진행률 표시용) |
| phase2_processable_max_episode | int / NULL | **처리할 최대 에피소드 번호** (`WebtoonEpisode.no` 기준). `null` = 전체, `10` = 10화까지 |
| phase3_enabled | bool | Step 3 GLM 실행 여부 (활성 웹툰 플래그) |
| phase3_last_completed_episode | FK → WebtoonEpisode / NULL | 마지막으로 완료된 Step 3 에피소드 |

> **processable_max_episode 사용법**: 처음엔 `10` 설정 → 10화까지 처리 후 idle → webtoonmoa에서 얼굴 검토 → `20`으로 업데이트 → 재개. `null`이면 전체 처리.

---

## 8. 현재 ↔ 목표 갭 & 마이그레이션 로드맵

| 단계 | 작업 | 상태 |
|------|------|------|
| **P0** | Kafka 인프라 구축 (stream.prup.xyz, KRaft, 3 broker) | ✅ |
| **P0** | Django DB 모델 (`WebtoonCut`, `TextRegion`, `TextAnnotation`, `FaceRecord`, `Character`, `CharacterAppearance`, `WebtoonPipelineState`) | ✅ |
| **P0** | Step 1 Faust Agent: OCR+YOLO 병렬(`asyncio.gather`), S3 이미지 다운로드, DB 저장, face crop S3 업로드, `episode.phase1.complete` 발행 | ✅ |
| **P0** | Faust 앱 구조 (`webtoon-pipeline/src/`), CI/CD (GitHub Actions → registry.prup.xyz → k3s), k3s-super-worker-01 배포 | ✅ |
| **P0** | Step 2 Faust Agent: `{source}_{title_id}` 파티셔닝, 에피소드 순차, `processable_max_episode` 제어 | 🔲 |
| **P0** | Chroma `character_faces_{source}_{title_id}` collection 도입 + `query_similar` 연동 | 🔲 |
| **P0** | Step 2: Chroma `upsert` + 고정 doc_id 멱등성 | 🔲 |
| **P1** | Step 3 Faust Agent: GLM 통합 (활성 웹툰만), `name_discoveries` 출력 | 🔲 |
| **P1** | Step 3: 현재 컷 **OpenCV face overlay** 이미지 GLM 전달 | 🔲 |
| **P1** | `name_discoveries` 자동 반영 + webtoonmoa 검토 플래그 (§19) | 🔲 |
| **P1** | webtoonmoa 파이프라인 관리 UI — `processable_max_episode` 설정, 얼굴 검토, resume (§20) | 🔲 |
| **P2** | Step 4: EpisodeSummaryOperator (활성 웹툰만) | 🔲 |
| **P2** | ResNet50 → **애니 특화 임베딩** 교체 | 🔲 |
| **P1** | webtoonmoa 캐릭터 관리 페이지 (web_manager 대체) | 🔲 |
| **P2** | webtoonmoa 캐릭터 프로필 페이지 | 🔲 |
| **P2** | webtoonmoa 대사 검색 기능 (PostgreSQL 전문 검색) | 🔲 |
| **P2** | webtoonmoa AI 채팅 도우미 | 🔲 |

### Operator 재구성 (목표 `pipeline.py`)

```python
steps = [
    LocalExtractOperator,      # Step 1: PaddleOCR + YOLO (parallel)
    FaceIdentifyOperator,      # Step 2: Chroma search + assign
    SceneAnalysisOperator,     # Step 3: GLM unified (replaces OcrRefine + Scene)
    EpisodeSummaryOperator,    # Step 4: episode report
]
```

---

## 9. 병렬 처리 (Faust + Kafka)

> **규모 근거**: 30+종 웹툰, 누적 ~6,000 에피소드, ~60만 컷. 순차 처리 시 60만 × 3분 = 1,250일 소요 — 스트림 병렬 처리 필수.

### 스트림 처리 인프라

| 역할 | 기술 |
|------|------|
| 스트림 처리 | **Faust + Kafka** (§18 참조) |
| 상태 저장 | Faust RocksDB Table (sliding window context) |
| DB | PostgreSQL + Chroma |
| 컨테이너 | Docker Compose (PaddleOCR 재현성) |

### 병렬화 경계

```
[Step 1]  Kafka round-robin → OCR+YOLO workers (N개)
          에피소드 내 컷 순차, 에피소드 간 완전 병렬
          파티션 키: 없음 (라운드로빈)

[Step 2]  Kafka webtoon_id 파티션 → Chroma workers
          웹툰 내 에피소드 순차 (ep1→ep2→ep3...)
          파티션 키: webtoon_id
          ※ 웹툰 namespace 독립 → Race condition 없음

[Step 3]  Kafka webtoon_id 파티션 → GLM workers (활성 웹툰만)
          에피소드 내 컷 순차 (슬라이딩 윈도우 의존성)
          파티션 키: webtoon_id
```

### 처리량 추정

| Phase | worker 수 | 컷당 소요 | 60만 컷 소화 기간 |
|-------|-----------|-----------|------------------|
| Step 1 | 50 | ~15초 | **~50시간** (~2일) |
| Step 2 | 30 (웹툰 수) | ~5초 | **~28시간** (~1일) |
| Step 3 | z.ai rate limit 기준 | ~30초 | 활성 웹툰만 — 규모 제한적 |

### 체크포인팅
- Step 1: `WebtoonCut.processed_at` 존재 여부로 재시작 시 upsert (멱등성 보장)
- Step 2: `WebtoonPipelineState.phase2_last_completed_episode` — 재시작 시 다음 에피소드부터
- Step 3: `WebtoonCut.glm_analyzed_at` — 완료 컷 skip
- Human Checkpoint (§20): `processable_max_episode` 도달 시 idle → 검토 후 값 올려서 resume

---

## 10. 모델·외부 의존성

| 용도 | 모델 / URL | 비고 |
|------|-----------|------|
| OCR | PaddleOCR (korean) | 로컬 CPU/GPU |
| 얼굴 탐지 | [deepghs/anime_face_detection](https://huggingface.co/deepghs/anime_face_detection) | YOLO, `anime_face_detection.pt` |
| 얼굴 임베딩 | ResNet50 (torchvision) | P0 유지 → P2~P3 애니 특화 모델로 교체 |
| 멀티모달 분석 | GLM-4.6v (z.ai) | Step 3 통합 1회/컷 (refine 제거) |
| 벡터 DB | Chroma (persistent HTTP server) | OCI 인스턴스 / docker-compose, env var 주입 |
| 이미지 소스 | S3 (boto3 직접 다운로드) | env: `S3_HOST`, `S3_ACCESS_KEY`, `S3_BUCKET_NAME` |
| **스트림 처리** | **Faust** (faust-streaming) | Kafka consumer/producer, RocksDB state store |
| **메시지 브로커** | **Kafka** (`stream.prup.xyz`, KRaft, 3 broker, PLAINTEXT) | 토픽별 파티셔닝, 오프셋 기반 재시작 |

---

## 11. 리뷰 노트 (업그레이드 시 여기에 기록)

<!-- 리뷰할 때마다 날짜 + 변경/결정 사항을 아래에 추가 -->

| 날짜 | 결정 / 변경 | 비고 |
|------|------------|------|
| 2026-05-28 | PRD v2.0 — 리뷰 피드백 반영: MATCH_THRESHOLD P0 시작값 0.25 확정, Step 2 멱등성 가드 설계, OpenCV 오버레이 스타일 확정(흰 배경+검은글씨), is_name_auto_assigned 필드 추가, 404 vs 5xx 재시도 로직 코드 구현 | §3, §5, §7, §14, §18.3, §19.3, §20.3 |
| 2026-05-28 | PRD v1.9 — Step 1 구현 완료, DB 스키마 확정, S3 이미지 접근, Webtoon/WebtoonEpisode 통합 모델 반영, crop_path 제거, processable_max_episode 도입, CI/CD·k8s 배포 구성 완료 | §1, §6, §7, §8, §9, §10, §15, §18, §20 |
| 2026-05-27 | PRD v1.8 — Chroma 멀티테넌시, Cascade Short-circuit, RocksDB 생명주기, 404 retry, Kafka 배포 완료 | §5.1, §12.12, §15.6, §18.2, §18.4 |
| 2026-05-27 | PRD v1.7 — Faust/Kafka 스트림 아키텍처, Human Checkpoint, 이름 자동 추출, 규모 명확화 | §1, §3, §9, §12.9~12.11, §18, §19, §20 신규 |
| 2026-05-24 | PRD v1.6 — Chroma 운영 접속 정보 확정 (oci-croma.prup.xyz, 토큰 인증, docker-compose) | §5.6 |
| 2026-05-24 | PRD v1.5 — service/webtoonmoa 변경 사항 반영 (통합 API, source 필드, 미디어 경로) | §1, §15.2, §15.3, §15.5, §16.0 |
| 2026-05-23 | PRD v1.4 — 재처리 설계 수정 (즉각→일괄), 텍스트 제외 기능, cascade 범위 정정 | §12.8, §15.6, §7 TextRegion |
| 2026-05-23 | PRD v1.3 — 재처리 메커니즘 + CharacterAppearance 스키마 추가 | §7, §12.8, §15.6 신규 |
| 2026-05-23 | PRD v1.2 — service 통합 + webtoonmoa 기능 요구사항 추가 | §15, §16, §17 신규 |
| 2026-05-23 | PRD v1.1 — 아키텍처 리뷰 피드백 반영 | 아래 §13 참조 |
| | Step 2 순차+Lock, Redis Lock(중장기) | 신규 인물 레이스 방지 |
| | Step 3 OpenCV face overlay | 말풍선↔화자 매칭 정확도 |
| | §5.5 Edge Case 규칙 추가 | Identity Scope, Idempotency |
| | GLM refine 제거 → Step 3 통합 | §12.1 결정 |
| | 임베딩 모델 교체 = P2~P3 핵심 마일스톤 | ResNet50 한계 명시 |

---

## 12. 기술 결정 사항

### 12.1 Step 3 GLM 통합 — type 분류 품질 (결정)

**결정**: Step 1의 GLM refine **제거**, Step 3 멀티모달 1회 통합으로 진행.

**근거**: GLM-4.6v급 멀티모달 모델은 폰트 스타일, 말풍선 테두리, 내레이션 박스 등을 시각적으로 인지한다. Step 1 텍스트 전용 refine 없이 Step 3에서 type + corrected_text + speaker를 한 번에 처리해도 **품질 저하 없거나 문맥 반영으로 상승**할 가능성이 높다. (필요 시 소규모 A/B로 검증하되, 기본 방향은 통합.)

### 12.2 신규 인물 ID 스코프 (결정)

**결정**: `NEW_CHAR_{INCREMENT_ID}` — **웹툰 글로벌 스코프**. 에피소드 로컬 스코프 사용 안 함.

### 12.3 Chroma 재처리 정책 (결정)

**결정**: doc_id `{webtoon_id}_{episode}_{cut}_F{face_idx}` 고정 + **`upsert()`** 사용.

### 12.4 임베딩 모델 교체 타이밍 (결정)

**결정**: P0~P1은 ResNet50 유지. **P2~P3에서 애니메 특화 임베딩으로 교체** — 아키텍처 핵심 마일스톤.

**근거**: ResNet50(ImageNet)은 일반 사물 인식용. 만화 캐릭터의 헤어스타일·얼굴선 변화에 약함. 후보: ArcFace 만화 파인튜닝, `iart-ai/anime-character-embedding` 계열.

### 12.5 Step 4 요약 모델 (결정)

**결정**: GLM-4.6v 동일 모델 사용. 텍스트 전용 피딩이므로 비전 불필요. 추가 API 계정 없이 z.ai API 재사용.

### 12.6 Chroma 배포 방식 (결정)

**결정**: OCI 인스턴스에 Chroma HTTP server 구동. docker-compose에 `chromadb` 서비스 정의. `CHROMA_HOST` / `CHROMA_PORT` 환경 변수로 연결 주소 주입 — PostgreSQL 설정 방식과 동일.

### 12.7 Face 라벨링 UI (결정)

**결정**: 현재 Flask `web_manager.py` → **webtoonmoa 관리 페이지**로 대체. service API가 Chroma + PostgreSQL Character 테이블을 업데이트하는 엔드포인트 제공. Flask는 P1 관리 페이지 출시 전까지 임시 유지.

### 12.8 재처리 cascade 정책 (결정)

**결정**: Human 수정 후 해당 컷(N)부터 **회차 마지막 컷까지 순차 재분석**. 즉각 GLM 실행 없음 — 수정 완료 후 일괄 트리거.

**cascade 범위 근거**:
- 컷 N 수정 → N+1 재분석 (윈도우: N-1, N, N+1) → N+2 재분석 (윈도우: N, N+1, N+2) → N+3 재분석 ...
- 슬라이딩 윈도우 특성상 **N번 변경은 N+1부터 마지막 컷까지 전파**됨.
- 따라서 재분석 범위 = **수정된 컷 중 가장 이른 번호(min_dirty_cut) ~ 에피소드 마지막 컷**.

**실행 방식**: Human이 여러 수정을 마친 후 UI에서 "재분석 시작" 버튼 클릭 → `rerun_episode_from_cut(episode_id, from_cut=min_dirty_cut)` Celery 태스크 큐잉. 컷을 순서대로 처리 (병렬 불가 — 이전 컷 결과가 다음 컷 컨텍스트에 필요).

### 12.9 Step 3 name_discoveries 출력 (결정)

**결정**: GLM Step 3 출력에 `name_discoveries` 배열 추가. 대사·나레이션에서 이름-얼굴 매핑을 추출해 confidence 기반으로 자동/수동 처리.

**근거**: 직접 호칭("카락, 잠깐만"), 자기소개("나는 비요른 얀델이다"), 나레이션 캡션("[카락의 선택]") 등 작중 이름이 반복 등장한다. GLM이 이미 이미지+텍스트를 분석하므로 추가 API 호출 없이 동시 추출 가능. 주요 캐릭터는 수 화 내에 이름이 명확히 나와 사실상 자동화된다. 자세한 처리 규칙은 **§19**.

### 12.10 Step 3·4 활성 웹툰만 실행 (결정)

**결정**: GLM(Step 3)과 에피소드 요약(Step 4)은 `WebtoonPipelineState.phase3_enabled = True`인 웹툰만 실행.

**근거**: 60만 컷 전체 GLM 처리는 비용·시간 대비 효율이 낮다. Step 1·2는 백로그 전체를 소화해 face DB를 완성하고, Step 3·4는 실제 서비스 가치가 있는 활성 웹툰에 집중. 비활성 웹툰은 Step 1·2 결과를 보존하다가 활성화 시 Step 3부터 이어서 실행.

### 12.11 Step 3·4 Human Checkpoint 메커니즘 (결정)

**결정**: Step 2가 N 에피소드(default 10) 처리 후 자동 pause. 관리자 검토 후 수동 resume. §20 참조.

### 12.12 스트림 처리 기술 선택 — Faust + Kafka (결정)

**결정**: 파이프라인 스트림 처리 레이어로 **Faust + Kafka** 채택. (이전 §12.11 내용과 통합)

**근거**: 30+ 웹툰, 60만 컷 백로그. `webtoon_id` 파티셔닝으로 에피소드 순차 + 에피소드 간 병렬 인프라 레벨 보장. Kafka offset 기반 재시작·선형 확장.

### 12.13 Chroma 멀티테넌시 — 웹툰별 독립 컬렉션 (결정)

**결정**: 단일 `character_faces` 컬렉션 + metadata 필터링 방식 → **`character_faces_{webtoon_id}` 웹툰별 독립 컬렉션**으로 전환.

**근거**:
- 60만 건+ 규모에서 HNSW는 metadata 필터를 인덱스 레벨이 아닌 후처리로 적용 → 전체 스캔 후 필터링으로 성능 저하
- 특정 웹툰 재작업 시 컬렉션 `drop()` + 재생성이 가능 → 타 웹툰 데이터 영향 없음
- 웹툰 간 캐릭터 namespace 독립 → cross-collection 쿼리 불필요, 설계 단순화

### 12.14 Cascade Short-circuit (결정)

**결정**: 재처리 루프에서 GLM 결과가 기존과 동일하면 즉시 break + 이후 컷 `is_stale` 일괄 해제.

**근거**: 오타 하나 수정 시 100컷짜리 에피소드 전체 GLM 재호출은 z.ai API 비용 낭비. 슬라이딩 윈도우 컨텍스트가 변하지 않는 첫 시점부터 이후 컷은 재처리 불필요. 비교 대상: `corrected_text`, `speaker`, `action_summary` 3필드.

### 12.15 Faust RocksDB State 생명주기 (결정)

**결정**: Step 3 Agent에서 에피소드 마지막 컷 처리 완료 시 `scene_context_table[episode_id]` 명시적 삭제.

**근거**: 6,000 에피소드 전체 처리 시 슬라이딩 윈도우 컨텍스트가 RocksDB에 무한 누적. 에피소드 완료 후 해당 키 불필요 → 즉시 삭제.

### 12.16 Step 1 이미지 404 vs 네트워크 오류 구분 (결정)

**결정**: 404(ImageNotFoundError) → 즉시 에피소드 종료. Timeout·5xx → exponential backoff(2s→5s→15s) 3회 재시도 → 최종 실패 시 `episode.phase1.error` 토픽 알림.

**근거**: 네트워크 일시 오류를 404와 동일하게 처리하면 에피소드 중간에 완료 이벤트가 발행되어 이후 컷이 누락됨.

---

## 13. 아키텍처 리뷰 요약 (v1.1)

> 웹툰 **지식 그래프 빌더(Knowledge Graph Builder)** 로서 Region/Annotation 분리, 슬라이딩 윈도우, Chroma 실시간 인물 힌트 피딩 구조는 기존 비효율(5시간/화, GLM 중복)을 해소할 이상적 마일스톤.

| # | 피드백 | PRD 반영 위치 |
|---|--------|--------------|
| 1 | Step 2 신규 등록 레이스 컨디션 | §Step 2 동시성 제어, §5.5-4, §9 병렬화 경계 |
| 2 | Step 3 bbox 공간 매칭 → OpenCV overlay | §Step 3 Process, §5.5-3 |
| 3 | GLM refine 제거 가능 | §12.1 결정 |
| 4 | 임베딩 P2~P3 교체 | §12.4, §8 로드맵 P2 |

---

## 14. 미결정 사항 (TODO)

- [ ] Chroma MATCH_THRESHOLD 보정 — **P0 시작값 0.25** (0.45는 코사인 유사도 0.55로 ResNet50 ImageNet 기준 False Positive 폭발 위험. 미매칭 과다 시 0.05씩 완화. P2 애니 특화 모델 교체 후 재보정)
- [x] ~~Step 3 통합 시 type 분류 — GLM refine 제거~~ → **§12.1 통합 진행**
- [x] ~~`신규_인물_*` ID 스코프~~ → **§12.2 웹툰 글로벌 `NEW_CHAR_*`**
- [x] ~~동일 컷 재처리 Chroma 정책~~ → **§12.3 upsert + 고정 doc_id**
- [x] ~~Step 4 요약 모델~~ → **§12.5 GLM-4.6v 동일 모델**
- [x] ~~Chroma 배포 방식~~ → **§12.6 OCI + docker-compose + env var**
- [x] ~~Face 라벨링 UI~~ → **§12.7 webtoonmoa 관리 페이지**
- [ ] GLM API rate limit — Kafka GLM worker 수 상한 (z.ai rate limit 실측 후 동시 worker 수 결정)
- [x] ~~OpenCV overlay 스타일~~ → **확정: 흰색 filled rectangle 배경 + 검은 글씨(thickness=2). §3 참조** (GLM이 텍스트를 이미지로 인식하므로 배경 대비 필수)
- [ ] P2 임베딩 후보 모델 벤치마크 (ResNet50 vs anime-character-embedding)
- [ ] CharacterAppearance 자동 분리 기준 — 파이프라인이 "이 외형은 동일 캐릭터의 다른 외형"을 자동 감지할 수 있는지, 아니면 항상 human이 병합하는지
- [x] ~~재처리 cascade 범위~~ → **§12.8 수정 컷 N ~ 마지막 컷 전체, 일괄 재분석 방식**
- [x] ~~스트림 처리 기술 선택~~ → **§12.11 Faust + Kafka**
- [x] ~~Step 3 활성 웹툰 범위~~ → **§12.10 WebtoonPipelineState.phase3_enabled**
- [x] ~~이름 자동 추출 방식~~ → **§12.9 name_discoveries + confidence 임계값**
- [ ] Human Checkpoint 간격 — 웹툰 장르·캐릭터 수에 따라 default 10 조정 기준 정립
- [ ] Kafka 브로커 선택 — Kafka vs Redpanda (운영 부담 vs 호환성)
- [ ] Step 2 "활성 웹툰" 정의 기준 — 연재 중 기준인지, 구독자 수 기준인지

---

## 15. 통합 아키텍처 (bubble → service)

### 15.1 코드 구조

파이프라인은 독립 레포 (`data-pipeline`)에서 Faust 앱으로 운영. DB 모델은 `service` 레포 `apps/api/toon/models.py`에서 관리.

```
data-pipeline/
└── webtoon-pipeline/        # 파이프라인 1 (다른 파이프라인 추가 시 동일 구조)
    ├── Dockerfile
    ├── pyproject.toml
    ├── models/
    │   └── anime_face_detection.pt
    └── src/
        ├── worker.py         # Faust App
        ├── config/
        │   ├── settings.py   # env vars
        │   ├── db.py         # psycopg2 connection pool
        │   └── s3.py         # boto3 S3 접근
        ├── operators/
        │   ├── ocr.py        # PaddleOCR
        │   └── yolo.py       # YOLO 얼굴 탐지
        └── agents/
            └── local_extract.py  # Step 1 Agent ✅

service/backend/apps/api/toon/models.py
    # Webtoon, WebtoonEpisode (통합), WebtoonCut, TextRegion,
    # TextAnnotation, Character, CharacterAppearance, FaceRecord,
    # WebtoonPipelineState
```

### 15.2 이미지 접근 방식

S3에서 boto3로 직접 다운로드. S3 키 규칙은 service S3Storage `location` 설정과 동일.

```python
# S3 키 규칙
SOURCE_MEDIA_PATH = {"kakao": "kakao_webtoon", "naver": "webtoon"}

key = f"{S3_LOCATION}/{SOURCE_MEDIA_PATH[source]}/{title_id}/{episode_no}/{title_id}_{episode_no}_{cut}.jpg"
# 예: media/kakao_webtoon/808482/1/808482_1_1.jpg
```

face crop 저장:
```python
# face_record_id = FaceRecord PK
key = f"{S3_LOCATION}/{source}/{title_id}/face_crop/{face_record_id}.jpg"
# 예: media/kakao/808482/face_crop/1234.jpg
```

### 15.3 에피소드 다운로드 → 파이프라인 트리거

에피소드 다운로드 완료 후 Kafka `cut.phase1.start` 토픽에 메시지 발행.

```python
# apps/api/toon/tasks.py (다운로드 완료 훅)
episode = WebtoonEpisode.actives.get(webtoon__source=source, webtoon__title_id=title_id, no=no)
kafka_producer.send("cut.phase1.start", {
    "source": source,
    "title_id": title_id,
    "episode_no": no,
    "webtoon_episode_id": episode.id,
})
```

Faust Step 1 Agent가 메시지를 수신해 OCR+YOLO 처리 후 `episode.phase1.complete` 발행 → Step 2 트리거.

### 15.4 파이프라인 의존성

`data-pipeline/webtoon-pipeline/pyproject.toml`에서 관리. service 레포와 분리된 독립 Docker 이미지.

```
faust-streaming, psycopg2-binary, boto3
paddlepaddle, paddleocr
ultralytics, opencv-python-headless
pillow, numpy
```

### 15.5 WebtoonCut 모델 — Episode 연결

`Webtoon`/`WebtoonEpisode` 통합 모델 사용. source별 분리 FK 없음.

```python
class WebtoonCut(TimestampedModel):
    episode = models.ForeignKey(WebtoonEpisode, on_delete=models.CASCADE, related_name='cuts')
    cut_number = models.SmallIntegerField()

    source        = CharField(max_length=8)  # 'kakao' | 'naver' — 빠른 분기용
    cut_number    = SmallIntegerField()
    image_url     = TextField()              # 원본 URL (참고용)
    local_path    = TextField()              # MEDIA_ROOT 상대 경로
    processed_at  = DateTimeField(null=True)
    is_stale      = BooleanField(default=False)
    glm_analyzed_at   = DateTimeField(null=True)
    human_modified_at = DateTimeField(null=True)

    class Meta:
        constraints = [
            UniqueConstraint(fields=['kakao_episode', 'cut_number'],
                             condition=Q(kakao_episode__isnull=False), name='uniq_kakao_cut'),
            UniqueConstraint(fields=['naver_episode', 'cut_number'],
                             condition=Q(naver_episode__isnull=False), name='uniq_naver_cut'),
        ]

    @property
    def episode(self):
        """source에 관계없이 에피소드 객체 반환."""
        return self.kakao_episode or self.naver_episode
```

> **source 필드 목적**: FK traversal 없이 미디어 경로 분기 (`media_dir_for_source(source)`), API 응답 직렬화, 파이프라인 라우팅에 사용. webtoonmoa의 `imageBaseForSource(source)` 패턴과 동일.

### 15.6 재처리 메커니즘 (Human Correction → GLM Batch Re-run)

Human이 OCR 텍스트·얼굴 매칭·텍스트 제외를 수정한 뒤, **수동으로 일괄 재분석을 트리거**한다. 수정 즉시 GLM을 실행하지 않으므로 여러 수정을 모아 한 번에 처리할 수 있다.

#### WebtoonCut 상태 필드

```python
class WebtoonCut(TimestampedModel):
    ...
    glm_analyzed_at   = DateTimeField(null=True)   # 마지막 GLM 분석 완료 시각
    human_modified_at = DateTimeField(null=True)   # 마지막 human 수정 시각
    is_stale          = BooleanField(default=False)
    # is_stale=True: 이 컷의 GLM 결과가 현재 수정 상태를 반영하지 않음
```

#### Stale 전파 규칙

```
컷 N에 human 수정 발생 (텍스트·얼굴·제외 어느 것이든)
  → N.is_stale = True
  → N+1.is_stale = True   (윈도우에 N 포함)
  → N+2.is_stale = True   (윈도우에 N 포함)
  → N+3부터는 N+1·N+2가 stale이므로 전파 계속...
  → 결과: N ~ 마지막 컷 전체 is_stale = True

stale 전파 = DB 업데이트만 (GLM 실행 없음), 수정 저장 시 동기 처리.
```

#### 수정 유형별 처리

| 수정 유형 | 즉시 처리 | is_stale 전파 |
|----------|----------|--------------|
| 텍스트 수정 | `TextAnnotation(source='human')` 저장 | N ~ end |
| 텍스트 제외 | `TextRegion.is_excluded = True` 저장 | N ~ end |
| 얼굴 매칭 수정 | `FaceRecord.appearance_id` 변경 + Chroma upsert | N ~ end |

#### 일괄 재분석 Celery 태스크 (Short-circuit 포함)

```python
@app.task(queue="lopri")
def rerun_episode_from_cut(episode_id: int, from_cut_number: int) -> None:
    """is_stale인 컷을 from_cut_number부터 순서대로 GLM 재실행.
    Short-circuit: 재분석 결과가 기존과 동일하면 이후 컷은 건너뜀.
    """
    cuts = WebtoonCut.objects.filter(
        episode_id=episode_id,
        cut_number__gte=from_cut_number,
        is_stale=True,
    ).order_by('cut_number')

    for cut in cuts:
        # 1. 슬라이딩 윈도우: DB에서 이전 2컷 최신 결과 로드
        # 2. 입력 우선순위 적용
        #    텍스트: human annotation > paddle raw (is_excluded=True 제외)
        #    얼굴:  is_confirmed=True FaceRecord > 자동 매칭
        # 3. OpenCV face overlay 재생성
        # 4. GLM 호출
        new_result = run_glm(cut)

        # ── Short-circuit ──────────────────────────────────────────
        # 5. 기존 결과와 비교 — 완전히 동일하면 이후 컷은 재처리 불필요
        if results_identical(new_result, get_existing_result(cut)):
            # 이 컷부터 마지막까지 is_stale 일괄 해제
            WebtoonCut.objects.filter(
                episode_id=episode_id,
                cut_number__gte=cut.cut_number,
                is_stale=True,
            ).update(is_stale=False)
            break
        # ───────────────────────────────────────────────────────────

        # 6. TextAnnotation(source='glm') 기존 레코드 교체 + CutSceneMeta 갱신
        save_glm_result(cut, new_result)
        cut.is_stale = False
        cut.glm_analyzed_at = timezone.now()
        cut.save()
```

> **`results_identical()` 비교 대상**: `blocks[].corrected_text`, `blocks[].speaker`, `scene_meta.action_summary` 세 필드가 모두 일치하면 동일로 판단. 슬라이딩 윈도우 컨텍스트가 바뀌지 않았으므로 N+2부터는 재처리해도 결과가 달라지지 않는다.

> **순차 처리 이유**: 슬라이딩 윈도우가 이전 컷 GLM 결과를 참조하므로 병렬 불가. lopri 큐 단일 워커.

#### UI — 재분석 흐름

```
[webtoonmoa 관리 페이지]

수정 저장 → 해당 컷 + 이후 컷 배지: "🔄 재분석 필요" (is_stale=True)

"이 컷부터 재분석" 버튼 클릭
  → POST /api/pipeline/episodes/{episode_id}/rerun/?from_cut={N}
  → rerun_episode_from_cut.delay(episode_id, from_cut_number=N)
  → 컷별 is_stale 폴링으로 완료 표시 업데이트
```

#### GLM 결과 교체 정책

GLM 재실행 결과는 **기존 GLM TextAnnotation 삭제 후 재삽입** (source='glm'). paddle 원본은 보존. `model_version` 필드에 재실행 타임스탬프 기록.

---

## 16. webtoonmoa 기능 요구사항

### 16.0 webtoonmoa 현재 API 구조 (참고)

| 항목 | 내용 |
|------|------|
| 웹툰 목록 | `GET /v1/toon/webtoon/` → `[{title_id, title_name, source, latest_no, synopsis, ...}]` |
| 에피소드 목록 | `GET /v1/toon/webtoon/{title_id}/episode/` → `[{no, episode_name, source, ...}]` |
| 미디어 경로 | `imageBaseForSource(source)`: kakao=`/media/kakao_webtoon`, naver=`/media/webtoon` |
| 이미지 URL | `/{source_dir}/{title_id}/{no}/{title_id}_{no}_{seq}.jpg` |

파이프라인 결과 API도 동일한 `source` 필드 패턴을 따른다. 이후 추가될 엔드포인트 예시:
- `GET /v1/toon/webtoon/{title_id}/episode/{no}/analysis/` → 컷별 분석 결과
- `GET /v1/toon/webtoon/{title_id}/characters/` → 캐릭터 목록

### 16.1 기능 목록

| 기능 | 우선순위 | 데이터 소스 |
|------|---------|------------|
| 에피소드 요약 표시 | P1 | `CutSceneMeta.action_summary` 집계 → `EpisodeReport` |
| 전체 줄거리 요약 | P2 | `EpisodeReport` 전체 배열 |
| 캐릭터 프로필 페이지 | P1 | `Character` 테이블 (이름, 이명, 나이, 기술, 최초등장) |
| 캐릭터별 등장 회차·컷 목록 | P1 | `FaceRecord` ↔ `WebtoonCut` JOIN |
| 대사 검색 | P2 | `TextAnnotation` 전문 검색 (pg_trgm 또는 to_tsvector) |
| 텍스트 제외 관리 | P1 | `TextRegion.is_excluded` — 간판·UI 등 분석 불필요 영역 숨기기 |
| 수정 후 일괄 재분석 트리거 | P1 | `WebtoonCut.is_stale` 기반 "이 컷부터 재분석" 버튼 |
| 캐릭터 관리 페이지 (face 라벨링) | P1 | `FaceRecord`, `Character`, Chroma |
| AI 채팅 도우미 | P2 | 아래 §16.3 참조 |

### 16.2 캐릭터 프로필 — 표시 데이터

```
/webtoon/{titleId}/characters/{characterId}

표시 항목:
- 이름 / 이명 (aliases)
- 나이
- 기술·능력 목록
- 외형 탭: 현실 | 게임 아바타 | 변장 | ... (CharacterAppearance 목록)
  - 탭별 대표 얼굴 이미지 + 첫 등장 회차
- 전체 등장 회차: [1화, 3화, 7화, ...]  (모든 외형 통합, FaceRecord 집계)
- 외형별 등장 회차 (탭 선택 시)
- 대사 샘플 (최근 5개, 화자가 이 캐릭터인 TextAnnotation)
```

### 16.3 AI 채팅 도우미 — 데이터 요구사항

**위치**: 웹툰 뷰어 내 플로팅 버튼 → 채팅 패널 (UX는 P2에서 결정)

**지원 질문 유형**:
- "이 인물 언제 처음 나왔어?" → `Character.first_seen_episode`, `first_seen_cut`
- "카락이 몇 화에 등장해?" → `FaceRecord` 집계
- "이 대사 누가 했어?" → `TextAnnotation.speaker`
- "이번 화 요약해줘" → `EpisodeReport.episode_summary`
- "카락의 기술 알려줘" → `Character.skills`

**AI 구현 방식**:
- 사용자 질문 → service API에서 관련 meta 조회 → LLM에 컨텍스트 주입 → 답변
- 모델 미정 (GLM-4.6v / Claude Haiku 등 — P2 결정)
- RAG 불필요 (구조화 DB 쿼리로 충분한 경우 직접 조회 우선)

### 16.4 대사 검색 — 기술 스펙

```sql
-- TextAnnotation에 전문 검색 인덱스
CREATE INDEX ON text_annotation USING gin(to_tsvector('simple', text));

-- 검색 예시: "언제부턴가"라는 대사 찾기
SELECT ta.text, ta.speaker, wc.cut_number, ke.no AS episode_no
FROM text_annotation ta
JOIN text_region tr ON ta.region_id = tr.id
JOIN webtoon_cut wc ON tr.cut_id = wc.id
JOIN kakao_webtoon_episode ke ON wc.kakao_episode_id = ke.id
WHERE to_tsvector('simple', ta.text) @@ plainto_tsquery('simple', '검색어')
  AND ta.source IN ('glm', 'human')  -- paddle raw 제외
ORDER BY ke.no, wc.cut_number;
```

---

## 17. 전체 데이터 흐름 (통합 후)

```
[service] 에피소드 다운로드 완료
     │
     ▼ Celery 체이닝
[pipeline] run_pipeline_episode(source, title_id, no)
     │
     ├─ Step 1: PaddleOCR(로컬 파일) → TextRegion + TextAnnotation(paddle)
     │
     ├─ Step 2: YOLO(로컬 파일) → FaceRecord 생성
     │             └─ Chroma 검색 → CharacterAppearance 매칭 or NEW_CHAR 발급
     │                              (appearance → character 연결)
     │
     ├─ Step 3: GLM(3컷 슬라이딩 윈도우 + face overlay)
     │             → TextAnnotation(glm) 추가 + CutSceneMeta 저장
     │
     └─ Step 4: 회차 전체 완료 시 GLM → EpisodeReport 저장

[PostgreSQL] WebtoonCut(needs_glm_rerun) / TextRegion / TextAnnotation /
             Character / CharacterAppearance / FaceRecord /
             CutSceneMeta / EpisodeReport
[Chroma]     character_faces collection (appearance_id + character_id 메타 포함)

[webtoonmoa] — API base: /v1/toon/webtoon/ (source 필드로 Kakao/Naver 통합)
     ├─ 웹툰 뷰어: 컷 이미지 (imageBaseForSource(source))
     ├─ 에피소드 요약 패널: EpisodeReport
     ├─ 캐릭터 프로필: Character + CharacterAppearance 테이블
     ├─ 대사 검색: TextAnnotation 전문 검색
     ├─ 캐릭터 관리 페이지: FaceRecord + Character + CharacterAppearance (web_manager 대체)
     │   └─ 외형 병합: "이 클러스터 = 변장한 카락" 지정 가능
     ├─ 텍스트 제외 관리: TextRegion.is_excluded
     ├─ 재분석 트리거: "이 컷부터 재분석" → rerun_episode_from_cut
     ├─ **파이프라인 Human Checkpoint**: N 에피소드 검토 후 "계속 진행" (§20)
     ├─ **이름 확정 제안**: name_discoveries 검토 및 confirm (§19)
     └─ AI 채팅 도우미: meta 기반 QA (P2)
```

---

## 18. Faust 스트림 아키텍처

### 18.0 Kafka 배포 정보

| 항목 | 내용 |
|------|------|
| **배포 위치** | OCI 인스턴스 (`stream.prup.xyz`) |
| **이미지** | `apache/kafka:4.0.0` (KRaft 모드, Zookeeper 없음) |
| **Faust 실행 위치** | 홈서버 (외부에서 OCI Kafka에 접속) |
| **브로커 포트** | `9092` (kafka1), `9094` (kafka2), `9096` (kafka3) |
| **Kafka UI** | `stream.prup.xyz:8080` |

**환경 변수** (Chroma 설정과 동일 패턴):

```env
KAFKA_BROKERS=stream.prup.xyz:9092,stream.prup.xyz:9094,stream.prup.xyz:9096
```

**Faust 앱 연결 설정**:

```python
import faust

app = faust.App(
    'webtoon-pipeline',
    broker='kafka://stream.prup.xyz:9092;stream.prup.xyz:9094;stream.prup.xyz:9096',
)
```

> **ADVERTISED_LISTENERS 적용 완료**: `PLAINTEXT_HOST://stream.prup.xyz:{port}` 로 변경 후 재배포 완료.

```yaml
# 적용된 설정 (참고)
# kafka1
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:29092,PLAINTEXT_HOST://stream.prup.xyz:9092
# kafka2
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:29092,PLAINTEXT_HOST://stream.prup.xyz:9094
# kafka3
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka3:29092,PLAINTEXT_HOST://stream.prup.xyz:9096
```

> **OCI 방화벽**: Security List 또는 NSG에서 포트 `9092`, `9094`, `9096`, `8080` inbound 허용 확인.

---

### 18.1 Kafka 토픽 설계

파티션 키: `{source}_{title_id}` (Webtoon 단위 순서 보장)

| 토픽 | 파티션 키 | 생산자 | 소비자 |
|------|-----------|--------|--------|
| `cut.phase1.start` | 없음 (라운드로빈) | 다운로드 완료 훅 | Step 1 Agent ✅ |
| `episode.phase1.complete` | `{source}_{title_id}` | Step 1 Agent ✅ | Step 2 Agent |
| `episode.phase1.error` | `{source}_{title_id}` | Step 1 Agent ✅ | 모니터링·알림 (S3 재시도 소진 시) |
| `episode.phase2.complete` | `{source}_{title_id}` | Step 2 Agent | Step 2 Agent (다음 ep 트리거) |
| `cut.phase3.start` | `{source}_{title_id}` | Step 2 완료 핸들러 | Step 3 Agent |

### 18.2 Step 1 Agent ✅ (구현 완료)

```python
# webtoon-pipeline/src/agents/local_extract.py

class EpisodeStartMsg(faust.Record):
    source: str              # 'kakao' | 'naver'
    title_id: str
    episode_no: int
    webtoon_episode_id: int  # WebtoonEpisode DB PK

@app.agent(cut_phase1_start)
async def local_extract_agent(stream):
    loop = asyncio.get_running_loop()
    async for msg in stream:
        cut = 1
        while True:
            # S3에서 이미지 다운로드 (NoSuchKey → None → 에피소드 종료)
            image_bytes = await loop.run_in_executor(
                None, fetch_cut_image, msg.source, msg.title_id, msg.episode_no, cut
            )
            if image_bytes is None:
                break
            # OCR + YOLO 병렬 실행
            ocr_blocks, faces = await asyncio.gather(
                loop.run_in_executor(None, run_ocr, image_bytes),
                loop.run_in_executor(None, detect_faces, image_bytes),
            )
            # DB 저장 + face crop S3 업로드
            await loop.run_in_executor(None, _save_to_db, ...)
            cut += 1
        await episode_phase1_complete.send(key=f"{msg.source}_{msg.title_id}", ...)
```

### 18.3 Step 2 Agent (미구현)

```python
@app.agent(episode_phase1_complete)  # {source}_{title_id} 파티셔닝
async def phase2_agent(stream):
    async for msg in stream:
        state = get_pipeline_state(msg.webtoon_episode_id)

        # ── 에피소드 단위 멱등성 가드 ─────────────────────────────────────────
        # Resume API 중복 호출 또는 지연 이벤트 재도착 시 중복 처리 방지
        # FaceRecord.appearance_id 유무는 얼굴 없는 에피소드에서 오판 가능하므로
        # phase2_last_completed_episode의 episode.no 비교가 더 안전
        if (state.phase2_last_completed_episode_id and
                state.phase2_last_completed_episode.no >= msg.episode_no):
            continue  # 이미 처리 완료 — 조용히 skip
        # ─────────────────────────────────────────────────────────────────────

        # processable_max_episode 체크 (에피소드 번호 기준)
        if state.phase2_processable_max_episode is not None:
            if msg.episode_no > state.phase2_processable_max_episode:
                state.phase2_status = 'idle'
                state.save()
                continue  # 이벤트 안 보냄 → agent 대기

        # Chroma 인물 식별 (에피소드 내 순차)
        cuts = load_phase1_results(msg.webtoon_episode_id)
        for cut in cuts:
            process_chroma(cut)

        state.phase2_last_completed_episode_id = msg.webtoon_episode_id
        state.phase2_processed_count += 1
        state.save()

        next_ep = get_next_episode(msg.webtoon_episode_id)
        if next_ep:
            await episode_phase1_complete.send(key=..., value=...)  # 다음 ep 트리거

        if state.phase3_enabled:
            await cut_phase3_start.send(...)
```

### 18.4 Step 3 Agent

```python
@app.agent(cut_phase3_start_topic)  # webtoon_id 파티셔닝
async def phase3_agent(stream):
    async for msg in stream:  # EpisodeStartMsg(webtoon_id, episode_id)
        cuts = load_phase2_results(msg.episode_id)
        for cut in cuts:  # 슬라이딩 윈도우 의존성 → 순차
            prev_context = scene_context_table[msg.episode_id]
            analysis = run_glm(cut, prev_context)
            scene_context_table[msg.episode_id] = analysis  # Faust RocksDB Table
            save_analysis(cut, analysis)
            apply_name_discoveries(analysis.name_discoveries)  # §19

        # ── RocksDB State 정리 ─────────────────────────────────────
        # 에피소드 마지막 컷 처리 완료 후 해당 episode_id 상태 삭제
        # 누적 6,000 에피소드 처리 시 RocksDB 무한 비대화 방지
        del scene_context_table[msg.episode_id]
        # ───────────────────────────────────────────────────────────
```

> **RocksDB 생명주기**: `scene_context_table`은 슬라이딩 윈도우 컨텍스트(직전 컷 요약)를 저장한다. 에피소드 완료 시점에 해당 `episode_id` 키를 명시적으로 삭제하지 않으면 6,000 에피소드 × 슬라이딩 윈도우 데이터가 무한 누적된다. 에피소드가 완전히 끝난 뒤에는 해당 컨텍스트가 더 이상 필요 없으므로 즉시 삭제.

---

## 19. 인물 이름 자동 추출 (name_discoveries)

### 19.1 GLM이 추출하는 이름 케이스

| 케이스 | 예시 | 신뢰도 |
|--------|------|--------|
| 직접 호칭 | "야 카락, 이리 와" — 화자가 FACE_0 쪽 보며 | 높음 (0.9+) |
| 자기소개 | "나는 비요른 얀델이다" — 화자 = FACE_1 | 높음 (0.9+) |
| 나레이션·캡션 | `[카락의 선택]` — 현재 컷 주인공 얼굴 | 중간 (0.7~0.9) |
| 3인칭 언급 | "카락이 드디어 왔구나" — FACE_2가 FACE_0 쪽 보며 | 중간 (0.7~0.9) |
| 간접 추론 | 이전 컷 맥락으로만 식별 가능 | 낮음 (0.5~0.7) |

### 19.2 출력 스키마

```json
"name_discoveries": [
  {
    "name": "카락",
    "character_id": "NEW_CHAR_001",
    "confidence": 0.92,
    "evidence": "직접 호칭 — FACE_0에게 '카락, 잠깐만'"
  },
  {
    "name": "비요른 얀델",
    "character_id": "NEW_CHAR_003",
    "confidence": 0.73,
    "evidence": "자기소개 추정 — 화자 특정 불확실"
  }
]
```

### 19.3 처리 규칙

```python
def apply_name_discoveries(discoveries: list[dict]) -> None:
    for d in discoveries:
        char = Character.objects.get(character_id=d['character_id'])
        if d['confidence'] >= 0.85:
            # 자동 반영 — is_confirmed=False(미확정), is_name_auto_assigned=True(AI 지정)
            # UI에서 "AI 추천 이름 (검토 필요)" 배지 표시 → human이 최종 confirm
            # 회상·변장·사칭 등 고신뢰도 오인식 방지 목적
            char.name = d['name']
            char.is_confirmed = False
            char.is_name_auto_assigned = True
            char.save()
        else:
            # webtoonmoa 검토 큐에 추가
            NameDiscoverySuggestion.objects.create(
                character=char,
                suggested_name=d['name'],
                confidence=d['confidence'],
                evidence=d['evidence'],
            )
```

### 19.4 자동화 범위

| 캐릭터 유형 | 처리 |
|------------|------|
| 주요 캐릭터 (자주 호칭) | 수 화 내 자동 확정, human은 confirm만 |
| 조연 (이름 가끔 등장) | 낮은 confidence 제안 → human 확인 |
| 단역 (이름 없음) | NEW_CHAR_XXX 유지 |
| 변장·외형 변화 캐릭터 | CharacterAppearance 분리는 human |

---

## 20. Human Checkpoint 메커니즘

### 20.1 개요

`WebtoonPipelineState.phase2_processable_max_episode`로 Step 2 처리 범위를 **에피소드 번호 기준**으로 명시적 제어.

- `null` → 전체 처리
- `10` → `WebtoonEpisode.no <= 10`인 에피소드까지만 처리 후 idle

```
관리자: processable_max_episode = 10 설정 → Phase 2 시작
ep1(no=1) → ep2(no=2) → ... → ep10(no=10) 완료
                               ↓
           ep11(no=11) > 10 → idle (이벤트 안 보냄)
                               ↓
webtoonmoa: 얼굴 클러스터 검토, 이름 붙이기
                               ↓
관리자: processable_max_episode = 20 으로 업데이트
                               ↓
ep11 → ep12 → ... → ep20 완료 → idle
```

### 20.2 Pause 구현

agent가 `episode_no > processable_max_episode`를 감지하면 다음 이벤트를 보내지 않음 → Kafka 메시지 없음 → 자연 대기. 별도 consumer pause 불필요.

### 20.3 Resume API

```python
# PATCH /api/pipeline/webtoon/{webtoon_id}/
# body: {"phase2_processable_max_episode": 20}
def update_pipeline_state(webtoon_id: int, max_episode: int) -> None:
    state = WebtoonPipelineState.objects.get(webtoon_id=webtoon_id)
    state.phase2_processable_max_episode = max_episode
    state.phase2_status = 'running'
    state.save()
    # 대기 중인 다음 에피소드 트리거
    # ⚠ 이 API가 중복 호출되거나 지연 이벤트가 재도착하면 동일 에피소드에
    #   Phase 2가 두 번 실행될 수 있다. §18.3 에피소드 멱등성 가드가 1차 방어선.
    next_ep = get_next_episode_after(state.phase2_last_completed_episode)
    if next_ep and next_ep.no <= max_episode:
        kafka_producer.send("episode.phase1.complete", ...)
```

### 20.4 webtoonmoa UI 액션

```
[웹툰 A 파이프라인 관리]

상태: ○ idle  (10화까지 완료, 처리 한도 도달)
처리 현황: Step2 10/243 에피소드 완료

[얼굴 검토 하기]           → 클러스터 수정, 이름 붙이기
[20화까지 계속 처리]       → PATCH phase2_processable_max_episode=20
[전체 처리 (한도 해제)]    → PATCH phase2_processable_max_episode=null

──────────────────────────────────────────────
[웹툰 B 파이프라인 관리]

상태: ○ idle
[Phase 2 시작 (10화까지)] → processable_max_episode=10 설정 후 첫 이벤트 발행
```
