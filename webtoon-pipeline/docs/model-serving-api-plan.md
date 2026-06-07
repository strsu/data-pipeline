# FastAPI 모델 서빙 서비스 분리 계획

## 1. 배경 및 문제 정의

### 현재 문제: Faust 워커 OOM 재시작 시 컷 1번부터 재처리

현재 `webtoon-pipeline` Faust 앱은 OCR(PaddleOCR), YOLO, CLIP 임베딩 모델을 **단일 프로세스 내 싱글톤**으로 보유한다.

```
[단일 Faust 프로세스]
├── PaddleOCR 싱글톤  (~1.5GB)
├── YOLO 싱글톤        (~100MB)
├── CLIP ViT-L/14 싱글톤 (~3.5GB)
└── Faust 런타임       (~500MB)
```

PaddleOCR의 paddle C++ 백엔드는 Python GC 밖에서 동작하여, 추론 호출마다 내부 버퍼가 누적된다. 에피소드당 50~100컷을 루프로 처리하면 메모리가 지속 증가하다 리밋 초과로 OOM이 발생한다.

OOM으로 프로세스가 죽으면 Kafka 오프셋이 커밋되지 않아 동일 메시지가 재전달되고, `_process_episode`가 항상 `cut = 1`부터 시작하므로 에피소드 전체를 재처리한다.

### 두 가지 문제

| # | 문제 | 원인 |
|---|---|---|
| 1 | OOM 반복 | PaddleOCR paddle C++ 레이어 메모리 누수 (GC 불가) |
| 2 | 재시작 시 컷 1번부터 재처리 | Kafka 오프셋 미커밋 + 체크포인트 없음 |

---

## 2. 제안 아키텍처

### 핵심 아이디어

모델 추론(OCR/YOLO/CLIP)을 **별도 FastAPI 서비스**로 분리한다. Faust 워커는 모델을 직접 보유하지 않고 HTTP로 호출한다.

```
[Faust 워커] ──HTTP──▶ [FastAPI 모델 서빙]
  - 비즈니스 로직       ├── POST /ocr-yolo  (PaddleOCR + YOLO)
  - Kafka 메시지 처리   ├── POST /embed     (CLIP ViT-L/14)
  - DB / Chroma 접근    └── GET  /health
```

### 메모리 문제 해결 방식

FastAPI를 gunicorn으로 구동하고 `--max-requests` 옵션을 설정한다. worker가 N번 요청을 처리하면 자동으로 교체(fork)되면서 paddle C++ 힙을 포함한 **프로세스 메모리 전체가 OS로 반납**된다.

```
gunicorn --workers 2 --max-requests 50 --max-requests-jitter 10
```

- `--workers 2`: k3s-super-worker-01 노드 64GB RAM 기준 여유 있음 (아래 §5.1 메모리 산정 참조)
- `--max-requests 50`: 50회 처리 후 worker 교체 → paddle 메모리 반납
- `--max-requests-jitter 10`: 두 worker가 동시에 교체되지 않도록 분산

Faust 워커에는 모델이 없으므로 메모리가 안정적으로 유지된다.

---

## 3. FastAPI 서비스 상세 설계

### 3.1 엔드포인트

#### `GET /health`

서비스 준비 상태를 반환한다. 모델이 로딩 완료된 경우에만 200을 반환한다. Kubernetes readinessProbe가 이 엔드포인트를 사용한다.

**Response (ready)**
```json
{"status": "ok"}
```

**Response (not ready — 503)**
```json
{"status": "loading"}
```

#### `POST /ocr-yolo`

컷 이미지를 받아 OCR 텍스트 영역과 YOLO 얼굴 탐지 결과를 동시에 반환한다.

**Request**
```
Content-Type: multipart/form-data
file: <이미지 바이너리>
```

**Response**
```json
{
  "ocr": [
    {
      "text": "안녕하세요",
      "score": 0.9821,
      "bbox_2d": [10, 20, 150, 45]
    }
  ],
  "faces": [
    {
      "bbox": [100, 50, 200, 180],
      "conf": 0.943
    }
  ]
}
```

#### `POST /embed`

얼굴 크롭 이미지를 받아 CLIP ViT-L/14 기반 768-dim 임베딩 벡터를 반환한다.

**Request**
```
Content-Type: multipart/form-data
file: <얼굴 크롭 이미지 바이너리>
```

**Response**
```json
{
  "embedding": [0.021, -0.134, 0.887, ...]
}
```

> **임베딩 모델**: CLIP ViT-L/14 (`openai/clip-vit-large-patch14`, 768-dim, L2 정규화). 현재 `operators/embedding.py`와 동일. ResNet50은 사용하지 않는다.

### 3.2 모델 로딩 전략

- 두 모델 모두 **앱 시작 시 1회 로딩** (FastAPI lifespan 이벤트 활용)
- `/health`는 로딩 완료 후 200 반환 → Kubernetes readinessProbe로 준비 확인
- gunicorn worker 교체 시 각 worker가 독립적으로 모델 재로딩
- 모델 파일은 Docker 빌드 시점에 이미지 내부에 캐시 (기동 시 다운로드 없음)
  - PaddleOCR: `RUN python -c "from paddleocr import PaddleOCR; PaddleOCR(...)"` 빌드 시 실행
  - CLIP: `ENV HF_HOME=/project/hf_cache` + 빌드 시 `CLIPModel.from_pretrained(...)` 캐시

### 3.3 디렉토리 구조 (신규 코드)

```
webtoon-pipeline/
├── src/                         # 기존 Faust 코드
│   ├── agents/
│   └── operators/
│       ├── ocr_yolo_client.py   # 신규: HTTP 클라이언트 (기존 ocr.py + yolo.py 대체)
│       └── embedding.py         # 수정: HTTP 클라이언트로 교체
└── model-api/                   # 신규
    ├── Dockerfile
    ├── pyproject.toml
    └── src/
        ├── main.py              # FastAPI app, lifespan, /health
        ├── routers/
        │   ├── ocr_yolo.py      # POST /ocr-yolo
        │   └── embed.py         # POST /embed
        └── models/
            ├── ocr.py           # PaddleOCR 래퍼 (기존 operators/ocr.py 이식)
            ├── yolo.py          # YOLO 래퍼 (기존 operators/yolo.py 이식)
            └── embedding.py     # CLIP 래퍼 (기존 operators/embedding.py 이식)
```

---

## 4. Faust 에이전트 수정 계획

### 4.1 수정 대상 파일

| 파일 | 변경 내용 |
|---|---|
| `src/operators/ocr_yolo_client.py` | 신규 — PaddleOCR/YOLO 직접 호출 제거, `MODEL_API_URL/ocr-yolo` HTTP 호출 |
| `src/operators/ocr.py` | 삭제 (ocr_yolo_client.py로 통합) |
| `src/operators/yolo.py` | 삭제 (ocr_yolo_client.py로 통합) |
| `src/operators/embedding.py` | CLIP 직접 호출 제거 → `MODEL_API_URL/embed` HTTP 호출로 교체 |
| `src/config/settings.py` | `MODEL_API_URL` 환경변수 추가 |
| `webtoon-pipeline/Dockerfile` | PaddleOCR, CLIP, torch, transformers 의존성 전체 제거 → 이미지 경량화 (~8GB → ~200MB) |

### 4.2 operators 변경 방향

기존 `ocr.py`와 `yolo.py`는 항상 함께 호출된다. 두 결과를 하나의 HTTP 요청으로 받을 수 있도록 `ocr_yolo_client.py`로 통합한다.

```python
# src/operators/ocr_yolo_client.py
import httpx
from src.config.settings import MODEL_API_URL

def run_ocr_yolo(image_bytes: bytes) -> tuple[list[dict], list[dict]]:
    """HTTP로 모델 API 호출 → (ocr_blocks, faces) 반환."""
    response = httpx.post(
        f"{MODEL_API_URL}/ocr-yolo",
        files={"file": image_bytes},
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    return data["ocr"], data["faces"]


def extract_embedding(image_bytes: bytes) -> list[float]:
    """HTTP로 모델 API 호출 → CLIP 768-dim 임베딩 반환."""
    response = httpx.post(
        f"{MODEL_API_URL}/embed",
        files={"file": image_bytes},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["embedding"]
```

### 4.3 model-api 장애 시 Kafka 재큐

model-api가 일시적으로 불가한 경우(재시작 중 등) Faust 에이전트는 HTTP 예외를 잡아 메시지를 Kafka에 **재발행**한다. HTTP 수준 retry는 하지 않는다.

```python
# agents/ocr_yolo.py 에서
async for msg in stream:
    if msg.retry_count >= 5:
        # 재큐 상한 초과 → 에러 토픽으로 이동
        await episode_phase1_error.send(key=..., value=msg)
        continue
    try:
        ocr_blocks, faces = run_ocr_yolo(image_bytes)
    except httpx.HTTPError:
        # model-api 장애 → 재큐 (retry_count 증가)
        await cut_phase1_start.send(
            key=...,
            value=msg.evolve(retry_count=msg.retry_count + 1),
        )
        continue
```

- `retry_count` 필드를 `EpisodeStartMsg`에 추가 (default=0)
- 상한(5회) 초과 시 `episode.phase1.error` 토픽으로 라우팅하여 무한 루프 방지

### 4.4 에이전트 로직 변경 없음

`face_identify.py`, `face_chroma_sync.py`의 비즈니스 로직은 변경하지 않는다. operator 레이어만 교체되므로 에이전트 코드는 그대로 유지된다.

---

## 5. 배포 계획

모든 k8s 매니페스트는 **`pipeline_repo/`** 디렉토리에 추가한다. 기존 `webtoon-pipeline` ArgoCD 앱(business-apps Wave 2)이 해당 경로를 이미 관리하므로 **별도 ArgoCD 앱 추가 없이 자동 배포**된다.

### 5.1 신규 Kubernetes Deployment: `model-api`

**메모리 산정 (workers=2 기준)**

| 컴포넌트 | per-worker | workers=2 |
|---|---|---|
| PaddleOCR | ~1.5GB | ~3GB |
| CLIP ViT-L/14 | ~3.5GB | ~7GB |
| overhead | ~0.5GB | ~1GB |
| **합계** | ~5.5GB | **~11GB** |

k3s-super-worker-01 노드 64GB RAM 기준 11GB는 여유 있음.

```yaml
# pipeline_repo/model-api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: model-api
  namespace: beldori
  labels:
    app: model-api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: model-api
  template:
    metadata:
      labels:
        app: model-api
    spec:
      terminationGracePeriodSeconds: 60
      nodeSelector:
        kubernetes.io/hostname: k3s-super-worker-01
      containers:
        - name: model-api
          image: model-api  # 태그는 kustomization.yaml에서 관리
          imagePullPolicy: Always
          command:
            - gunicorn
            - src.main:app
            - --worker-class=uvicorn.workers.UvicornWorker
            - --workers=2
            - --max-requests=50
            - --max-requests-jitter=10
            - --timeout=120
            - --bind=0.0.0.0:8000
          ports:
            - containerPort: 8000
          readinessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 60   # CLIP 로딩 대기
            periodSeconds: 10
            failureThreshold: 6
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 120
            periodSeconds: 30
            failureThreshold: 3
          resources:
            requests:
              memory: "8Gi"
              cpu: "2000m"
            limits:
              memory: "14Gi"  # workers=2, 여유 마진 포함
              cpu: "4000m"
```

### 5.2 신규 Kubernetes Service: `model-api`

```yaml
# pipeline_repo/model-api-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: model-api
  namespace: beldori
spec:
  type: ClusterIP
  selector:
    app: model-api
  ports:
    - port: 8000
      targetPort: 8000
      protocol: TCP
```

### 5.3 기존 Faust Deployment 리소스 감소

모델 미보유로 메모리 사용량 대폭 감소 (`pipeline_repo/deployment.yaml` 수정):

```yaml
resources:
  requests:
    memory: "512Mi"   # 기존 4Gi → 512Mi
    cpu: "500m"       # 기존 2000m → 500m
  limits:
    memory: "1Gi"     # 기존 8Gi → 1Gi
    cpu: "1000m"
```

### 5.4 서비스 내부 통신

동일 namespace(`beldori`) 내 ClusterIP Service로 통신:
```
http://model-api.beldori.svc.cluster.local:8000
```

또는 같은 namespace이므로 단축 주소 사용 가능:
```
http://model-api:8000
```

### 5.5 pipeline_repo/ 최종 파일 구조

```
pipeline_repo/
├── kustomization.yaml          # 기존 + model-api 이미지 태그 추가
├── configmap.yaml              # MODEL_API_URL 추가
├── deployment.yaml             # Faust 워커 (리소스 축소)
├── model-api-deployment.yaml   # 신규
├── model-api-service.yaml      # 신규
└── infisical-secret.yaml       # 기존 유지
```

> **ArgoCD 배포 순서**: model-api와 Faust 워커는 동일 ArgoCD 앱 내에서 함께 배포된다. model-api readinessProbe가 통과하기 전까지 Faust가 먼저 뜨더라도 Kafka 재큐 로직(§4.3)이 처리하므로 별도 wave 분리는 불필요.

---

## 6. 재시작 체크포인트 (OOM 재처리 방지)

FastAPI 분리와 함께 **컷 재처리 방지** 로직도 추가한다.

`webtoon_cut.processed_at` 컬럼이 이미 존재하므로 이를 체크포인트로 활용:

```python
def _process_episode(msg):
    # 마지막으로 처리된 컷 번호 조회
    resume_from = _get_last_processed_cut(msg.webtoon_episode_id) + 1
    cut = resume_from  # 1 대신 resume_from부터 시작
    
    while True:
        # 이미 처리된 컷은 데이터 삭제 없이 스킵
        ...
```

재시작 시 이미 처리된 컷은 건너뛰어 중간부터 재개한다.

---

## 7. 작업 순서

```
Phase 1: FastAPI 모델 서빙 서비스 구현
  [ ] model-api/ 디렉토리 생성
  [ ] GET /health 엔드포인트 구현 (모델 로딩 완료 전 503, 완료 후 200)
  [ ] POST /ocr-yolo 엔드포인트 구현
  [ ] POST /embed 엔드포인트 구현
  [ ] Dockerfile 작성 (gunicorn, PaddleOCR + CLIP 빌드 시 캐시)
  [ ] 로컬 동작 검증

Phase 2: Faust operators HTTP 클라이언트로 교체
  [ ] operators/ocr_yolo_client.py 신규 작성 (ocr.py + yolo.py 통합)
  [ ] operators/embedding.py → HTTP 클라이언트로 교체
  [ ] operators/ocr.py, yolo.py 삭제
  [ ] EpisodeStartMsg에 retry_count 필드 추가
  [ ] 에이전트에 Kafka 재큐 에러 핸들링 추가 (retry_count 상한 5회)
  [ ] settings.py에 MODEL_API_URL 환경변수 추가
  [ ] webtoon-pipeline/Dockerfile에서 모델 의존성 전체 제거

Phase 3: 재시작 체크포인트 추가
  [ ] _process_episode에 resume_from 로직 추가
  [ ] _upsert_cut에서 이미 처리된 컷 스킵 처리

Phase 4: 배포
  [ ] pipeline_repo/model-api-deployment.yaml 작성
  [ ] pipeline_repo/model-api-service.yaml 작성
  [ ] pipeline_repo/configmap.yaml에 MODEL_API_URL 추가
  [ ] pipeline_repo/kustomization.yaml에 model-api 이미지 항목 추가
  [ ] pipeline_repo/deployment.yaml 리소스 축소 (4Gi→512Mi)
  [ ] GitHub Actions CI/CD에 model-api 이미지 빌드/푸시 추가
  [ ] 통합 테스트
```

---

## 8. 기대 효과

| 항목 | 변경 전 | 변경 후 |
|---|---|---|
| Faust 메모리 | 최대 8Gi (OOM 위험) | ~1Gi (안정) |
| OOM 시 영향 | Faust 전체 재시작 | model-api만 재시작, Faust 영향 없음 |
| 컷 재처리 | 에피소드 처음부터 | 마지막 처리 컷 다음부터 |
| 모델 메모리 관리 | GC 불가 (paddle C++ 누수) | gunicorn max-requests로 주기적 프로세스 교체 |
| Faust 이미지 크기 | ~8GB (모델 포함) | ~200MB (모델 없음) |
| model-api 이미지 | - | ~8GB (한 곳에서 관리) |
| ArgoCD 앱 추가 | - | 불필요 (pipeline_repo/ 기존 앱에 포함) |
