# PRD — 인물 연결(character linking) 재설계 : 익명 슬롯 + CCIP 클러스터 + reconcile 명명

작성 2026-07-24. **세션 이어가기용 자족 문서.** 세그먼트-단위 분석 작업 중 정체성(인물) 축이
근본 재설계로 이어진 경위·결정·현황·계획을 전부 기록. 관련 정본:
`docs/segment-unit-plan.md`(세그 구현), `remain-trouble.md`(남은 과제), `chamgyoyuk-manual-analysis-2026-07-17.md`(참교육 수동분석 대조군), `redesign-flow-first-2026-07-22.md`(흐름-first 원설계), `docs/session-handoff-2026-07-23.md`.

---

## 0. 왜 이 상황까지 왔나 (경위)

1. **세그먼트-단위 분석 구현**(2026-07-23~24): step3(Stage V)를 컷→세그먼트 단위로. 근거=컷(다운로드 타일)이 콘텐츠 밴드를 쪼개 truncation 컷~71% vs 세그~20%, 화자 커버리지 세그 1.7~2배. Phase A~D 구현·배포(커밋 23d90f6·b7b5d13, service 0040). 통제 E2E(참교육 ep2) 통과. → `segment-unit-plan.md`.
2. **화산귀환(17)을 segment_unit=true + flow_first=true로 실사용**하며 문제 3개 발견:
   - (a) **크래시**: 내가 Phase A(A2)에서 face INSERT `%s`를 15개(컬럼14)로 잘못 → 핫픽스 `7ae5297`.
   - (b) **서사 빵꾸**: 빈 세그 스킵 → 세그 index 불연속 → narrative beat 경계가 빈 index에 떨어짐. → A/B/C fix(`4c73091`): 빈 세그도 분석·전세그 컷범위·per-segment 로그. **검증됨**(주마등 컷62~77 커버 회복).
   - (c) **429 세그 드롭**: 동시성 과다 시 429로 세그 통째 드롭(call_llm_json이 4xx 재시도 안 함). → **429 재시도 수정 `e2c0cbc`**(429만 백오프 재시도). 배포됨.
3. **⭐ 근본 발견 — flow_first가 얼굴(인물)을 버린다**: `step2.py:714` flow_first ON이면 CCIP 정체결합 전면 스킵+purge. 화산귀환 ep1 face_identity=0, 인물=대사명명 클러스터만(승격0). 사용자: **"인물을 아예 버릴 순 없다. 인물분석을 개선해야지 안 하는 게 아니다."**
4. **flow_first true vs false 대조**(화산귀환 ep1, 둘 다 segment+A/B/C fix):

   | | flow_first=TRUE(익명+purge) | flow_first=FALSE(CCIP정체+표준resolve) |
   |---|---|---|
   | 얼굴 매칭 | **0**(purge) | **87** |
   | 인물 승격 | 전부 cluster | **character 승격**: 청명·왕초·천마·거지소년·화산장로 |
   | 화자배정 | 185 | **246** |
   | 요약·beats | 우수·연속 | 우수·연속(주마등 정위치) |

   → **얼굴 유지(false)가 인물을 제대로 만든다.** 단 false는 CCIP **이름**까지 주입(43% 오독 위험). true는 익명이라 오독은 없지만 인물을 버림.

---

## 1. 핵심 문제와 통찰

- **CCIP 명명은 나쁘다**(정체 추측 43% 정확 — 유령 카락·빙의 이한수 오귀속). → Stage V에 CCIP **이름**을 주입하면 화자·서사까지 오염.
- **CCIP 클러스터링은 좋다**(비슷한 얼굴 묶기는 잘함). "아예 못 하는 모델이 아니다."(사용자)
- **reconcile은 옳다**(참교육 박대석으로 증명 — 대사 증거로 오귀속 소급 강등). → 이름은 **대사(reconcile)**로 붙여야.

**결론**: **CCIP는 "누가 누구인지(A=A, 시각 클러스터)"를, LLM/reconcile은 "A는 무슨 이름인지"를** 담당. 둘을 분리·결합.

---

## 2. 설계 — 익명 슬롯 + CCIP 클러스터 유지 + reconcile 명명

**flow_first 토글 폐지. 아래를 영구 동작으로 고정:**

1. **step2: CCIP 클러스터링 항상 수행**(purge 제거). 얼굴→클러스터(appearance) = **시각 슬롯 a,b,c**. 이름은 아직 없음.
2. **Stage V: 익명 슬롯 주입**(CCIP 이름 X). ⭐**핵심 설계**: 유닛-로컬 F라벨이 아니라 **안정 클러스터 id(appearance_id)를 슬롯으로 주입** → LLM이 "슬롯 A가 seg1·seg5에서 말한다"를 회차 가로질러 추적. (현재 anonymize=True는 `{"id":"F0"}`만 = 유닛-로컬이라 추적 불가 — 이걸 클러스터 안정 id로 바꿔야 함.)
3. **resolve/reconcile: 슬롯에 이름 배정.** "슬롯 A = 청명"(대사 호명·자칭 증거), 대사 모순 시 소급 교정.

= **CCIP 클러스터(a,b,c) → reconcile이 이름 얹기.** 이름 없는 익명 분석이라 CCIP 오독 명명 오염 없음 + 얼굴(인물) 유지.

### 열린 설계 질문 (다음 세션서 정할 것)
- **Q1**: "클러스터 id를 슬롯으로 주입" — appearance_id를 그대로? 아니면 회차-로컬 슬롯번호(A/B/C)로 재라벨? (프롬프트 가독성 vs 안정성)
- **Q2**: reconcile을 **언제** 돌려 이름을 얹나 — 회차별 R 스테이지 내부? 전 회차 후 웹툰-전역 1회? (persona 링커=대사·화법 회차간 연결, 원설계 `redesign-flow-first`에 있으나 미배포.)
- **Q3**: 교차회차 정체 — 같은 인물이 회차마다 다른 클러스터로 잡힘. 클러스터를 회차 넘어 어떻게 잇나(CCIP 갤러리 매칭? persona? 이름?).
- **Q4**: 세그-네이티브 저장(remain-trouble D2)과의 순서 — 독립이라 병행 가능.

---

## 3. 현재 상태 (2026-07-24 세션 끝 시점)

### 배포 완료
- 세그먼트-단위 분석(A~D): 커밋 `b7b5d13`(+A `23d90f6`), service 마이그 `0040`(strip_y·segment_scene_meta·segment_unit_enabled). **토글 기본 OFF**.
- 크래시 핫픽스 `7ae5297`, A/B/C 서사빵꾸 fix `4c73091`, **429 재시도 `e2c0cbc`**(방금 배포).
- 문서 커밋: `2972dbe`·`e44a632`(remain-trouble 결정), 이 PRD.

### 미착수 (다음)
- **flow_first 제거 + 하이브리드**(이 PRD §2) — 미착수. 순서: ①step2 purge제거 ②Stage V 클러스터슬롯 주입 ③resolve/reconcile 명명 ④flow_first 컬럼·게이트 제거.
- 세그-네이티브 저장(remain-trouble D2, 프론트 대공사).
- LLM 주도 lookback(remain-trouble D4, 프로덕션 미적용).

### ⚠️ 화산귀환(17) 현재 데이터 상태 (정리 필요)
- config: `flow_first_enabled=true`, `segment_unit_enabled=true`(내가 실험 중 false로 바꿨다 **true로 복원함**).
- ep1(episode_id 1255): **flow_first=false로 분석돼 있음**(monkeypatch 실험 결과, 얼굴87·화자246·character 승격). config는 true인데 데이터는 false 산출 = **불일치**. 하이브리드 배포 후 재분석 권장.
- ep2~10: step1,2 완료. ep2(1334)는 버그코드 both-true 분석(초삼→구칠 오명명·마지막 회상 오판 — remain-trouble §B/ep2). ep3~10 step3 미완/부분(체인 종료됨).
- 실행 중인 백그라운드 워크플로/프로세스: 없음(다 종료). orphan vision run 몇 개 'running' 표시(무해).

---

## 4. 인프라·운영 노트 (재개 시 필수)

- **prod DB**: `prod.env`(gitignore), psycopg2, POSTGRES_PORT=5459(direct). 읽기전용 원칙. litellm 원본=dbname=litellm(모델명 `openai/glm-4.6v`, startTime=UTC). ⚠️ psycopg2 LIKE는 `%%` 이스케이프.
- **LLM**: 비전=glm-4.6v(config default), 텍스트=glm-5.2. pod는 `vllm.prup.xyz`(Cloudflare 터널) 경유 — **동시성6에서 429**. 로컬 대량 실험은 **litellm 직결 `http://192.168.1.15:4000`**(터널502 회피)+**CONC 낮게**(PASS1_WORKERS=1~2, 429회피). 방금 429 재시도 수정돼 드롭은 줄지만 동시성은 여전히 낮게.
- **CCIP/embed**: `embed-ccip-api`는 ClusterIP(내부전용, localhost:8000) — **로컬 실행 불가**, step2는 pod 안에서만. 얼굴 crop→CCIP→Chroma. GPU_SERVER=gpgpu.prup.xyz(OCR/YOLO).
- **클러스터**: `sshpass -p 123123 ssh root@192.168.1.36 "kubectl ... -n beldori"`. pod=webtoon-pipeline-*. 배포=data-pipeline push→CI→ghcr→ArgoCD(proxmox 태그 자동커밋). docs push도 재빌드 유발(pod 재롤아웃, 무해).
- **spark**: `sshpass -p '12qw!@QW' ssh jj@192.168.1.15`(litellm:4000·OCR/YOLO)·`.16`(qwen). qwen thinking-OFF=chat_template_kwargs.enable_thinking=false.
- **워크플로 트리거**(pod 안): `python -m src.temporal.starter naver 769209 <start> <steps> <max_ep>` (steps=step1,step2,step3). 또는 EpisodeChainWorkflow.run을 custom id로 start(예 컷 조각별). 정지=`get_workflow_handle(id).terminate()`.
- **config 토글**: service 경유 `docker exec z_docker-backend-1 python manage.py shell -c "from apps.api.toon.models import WebtoonPipelineState; ps=WebtoonPipelineState.actives.filter(webtoon_id=17).first(); ...; ps.save()"`. 직접 UPDATE 금지 원칙.
- **실험 monkeypatch**(로컬 step3): `step3._flow_first_enabled`·`step3_segment._flow_first_enabled`·`step3_segment.segment_unit_enabled`를 lambda로 덮어씀(DB 미변경). step2는 pod에서만 가능(CCIP 내부).

---

## 5. 파이프라인 단계 요약 (참고)

- **step1**(OCR+YOLO, `step1.py`): 세그먼트(콘텐츠밴드, 300~2400px, 슬리버흡수) 단위로 OCR/YOLO → text_region/annotation(paddle)/face_detection. 검출은 세그에서, **저장은 컷-네이티브**(cut_id+컷로컬bbox+strip_y, 세그provenance). ← 세그-네이티브로 갈지가 D2.
- **step2**(CCIP, `step2.py`): 얼굴 crop→CCIP임베딩(Chroma)→유사도 매칭→face_identity(클러스터/character). **flow_first면 purge(제거 예정).**
- **step3**(LLM, `step3.py`+`step3_segment.py`): 3a Stage V(비전 1콜/유닛, blocks·scene·화자후보·name_evidence) → 3b roster→R(resolve)→N(narrate) [flow_first면 consolidate→N] → 3c apply(결정론 커밋+reconcile 소급강등). 세그모드는 `extract_episode_segment`+세그 region_map+beat remap(A/B/C fix).

---

## 6. 다음 세션 시작 체크리스트
1. 이 PRD + `remain-trouble.md`(우선순위 D3✅→D1→D2→D4) 읽기.
2. §2 설계의 **Q1~Q3 확정**(클러스터 슬롯 주입 방식, reconcile 명명 시점, 교차회차 연결).
3. 구현 순서: step2 purge제거 → Stage V 클러스터슬롯 → resolve/reconcile 명명 → flow_first 컬럼/게이트 제거.
4. 화산귀환 데이터 정리(하이브리드로 재분석) — config/데이터 불일치 해소.
5. 배포는 push→CI→ArgoCD. 대량 LLM은 직결+낮은 동시성.
