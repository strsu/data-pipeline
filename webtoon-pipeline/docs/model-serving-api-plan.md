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

PaddleOCR의 paddle C++ 백엔드는 Python GC 밖에서 동작하여, 추론 호출마다 내부 버퍼가 누적된다. 에피소드당 50~100컷을 루프로 처리하면 메모리가 지속 증가하다 8Gi 리밋 초과로 OOM이 발생한다.

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
  - Kafka 메시지 처리   └── POST /embed     (CLIP ViT-L/14)
  - DB / Chroma 접근
```

### 메모리 문제 해결 방식

FastAPI를 gunicorn으로 구동하고 `--max-requests` 옵션을 설정한다. worker가 N번 요청을 처리하면 자동으로 교체(fork)되면서 paddle C++ 힙을 포함한 **프로세스 메모리 전체가 OS로 반납**된다.

```
gunicorn --workers 1 --max-requests 30 --max-requests-jitter 5
```

Faust 워커에는 모델이 없으므로 메모리가 안정적으로 유지된다.

---

## 3. FastAPI 서비스 상세 설계

### 3.1 엔드포인트

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

얼굴 크롭 이미지를 받아 CLIP 768-dim 임베딩 벡터를 반환한다.

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

### 3.2 모델 로딩 전략

- 두 모델 모두 **앱 시작 시 1회 로딩** (FastAPI lifespan 이벤트 활용)
- 싱글톤 패턴 유지, worker 교체 시 자동 재로딩
- `max-requests` 교체 주기: OCR/YOLO worker는 30~50회, CLIP worker는 독립 조정 가능 (필요 시 분리)

### 3.3 디렉토리 구조 (신규)

```
webtoon-pipeline/
├── src/                         # 기존 Faust 코드
│   ├── agents/
│   └── operators/
│       ├── ocr.py               # 수정: 모델 직접 호출 → HTTP 클라이언트로 교체
│       ├── yolo.py              # 수정: 동일
│       └── embedding.py         # 수정: 동일
└── model-api/                   # 신규
    ├── Dockerfile
    ├── pyproject.toml
    └── src/
        ├── main.py              # FastAPI app, lifespan
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
| `src/operators/ocr.py` | PaddleOCR 직접 호출 제거 → `MODEL_API_URL/ocr-yolo` HTTP 호출로 교체 |
| `src/operators/yolo.py` | YOLO 직접 호출 제거 → ocr.py와 통합 (동일 엔드포인트 응답 파싱) |
| `src/operators/embedding.py` | CLIP 직접 호출 제거 → `MODEL_API_URL/embed` HTTP 호출로 교체 |
| `src/config/settings.py` | `MODEL_API_URL` 환경변수 추가 |
| `webtoon-pipeline/Dockerfile` | CLIP/PaddleOCR/YOLO 의존성 제거 (모델 없으므로 이미지 경량화) |

### 4.2 operators 변경 방향

기존 `ocr.py`와 `yolo.py`는 항상 함께 호출된다 (`_process_segment` 참조). 두 결과를 하나의 HTTP 요청으로 받을 수 있도록 `run_ocr_yolo(image_bytes)` 함수로 통합한다.

```python
# 변경 후 operators/ocr_yolo_client.py (가칭)
def run_ocr_yolo(image_bytes: bytes) -> tuple[list[dict], list[dict]]:
    """HTTP로 모델 API 호출 → (ocr_blocks, faces) 반환."""
    response = httpx.post(
        f"{MODEL_API_URL}/ocr-yolo",
        files={"file": image_bytes},
        timeout=60,
    )
    data = response.json()
    return data["ocr"], data["faces"]
```

### 4.3 에이전트 로직 변경 없음

`ocr_yolo.py`, `embedding_agent.py`, `face_identify.py`, `face_chroma_sync.py`의 **비즈니스 로직은 변경하지 않는다.** operator 레이어만 교체하므로 에이전트 코드는 그대로 유지된다.

---

## 5. 배포 계획

### 5.1 신규 Kubernetes Deployment: `model-api`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: model-api
  namespace: beldori
spec:
  replicas: 1
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: k3s-super-worker-01  # GPU/고메모리 노드
      containers:
        - name: model-api
          # gunicorn + uvicorn worker, max-requests로 메모리 관리
          command:
            - gunicorn
            - src.main:app
            - --worker-class=uvicorn.workers.UvicornWorker
            - --workers=1
            - --max-requests=30
            - --max-requests-jitter=5
            - --timeout=120
          resources:
            requests:
              memory: "6Gi"   # PaddleOCR + CLIP 상주
              cpu: "2000m"
            limits:
              memory: "10Gi"
              cpu: "4000m"
```

### 5.2 기존 Faust Deployment 리소스 감소

모델 미보유로 메모리 사용량 대폭 감소 예상:

```yaml
resources:
  requests:
    memory: "512Mi"   # 기존 4Gi → 512Mi
    cpu: "500m"       # 기존 2000m → 500m
  limits:
    memory: "1Gi"     # 기존 8Gi → 1Gi
    cpu: "1000m"
```

### 5.3 서비스 내부 통신

동일 namespace(`beldori`) 내 ClusterIP Service로 통신:
```
http://model-api.beldori.svc.cluster.local:8000
```

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
  [ ] POST /ocr-yolo 엔드포인트 구현
  [ ] POST /embed 엔드포인트 구현
  [ ] Dockerfile 작성 (gunicorn + max-requests)
  [ ] 로컬 동작 검증

Phase 2: Faust operators HTTP 클라이언트로 교체
  [ ] operators/ocr.py → HTTP 클라이언트
  [ ] operators/yolo.py → HTTP 클라이언트 (ocr과 통합)
  [ ] operators/embedding.py → HTTP 클라이언트
  [ ] settings.py에 MODEL_API_URL 추가
  [ ] Faust Dockerfile에서 모델 의존성 제거

Phase 3: 재시작 체크포인트 추가
  [ ] _process_episode에 resume_from 로직 추가
  [ ] _upsert_cut에서 이미 처리된 컷 스킵 처리

Phase 4: 배포
  [ ] model-api Kubernetes Deployment/Service 작성
  [ ] Faust Deployment 리소스 조정
  [ ] pipeline-config ConfigMap에 MODEL_API_URL 추가
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
| CLIP 중복 로딩 | 에이전트 3개에서 각각 | model-api 1곳에서 통합 서빙 |
