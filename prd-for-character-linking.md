# PRD — 인물 연결(character linking) 재설계 : 익명 슬롯 + CCIP 클러스터 + reconcile 명명

작성 2026-07-24. **세션 이어가기용 자족 문서.** 세그먼트-단위 분석 작업 중 정체성(인물) 축이
근본 재설계로 이어진 경위·결정·현황·계획을 전부 기록. 관련 정본:
`docs/segment-unit-plan.md`(세그 구현), `remain-trouble.md`(남은 과제 D5~D8), `chamgyoyuk-manual-analysis-2026-07-17.md`(참교육 수동분석 대조군), `redesign-flow-first-2026-07-22.md`(흐름-first 원설계), `docs/session-handoff-2026-07-23.md`.

---

## ★★ 최종 설계 v2 (2026-07-25) — cluster-first 대사흐름 정체 + 멀티모달 (야간 R&D 검증)

아래 §0~6은 경위(Path A→reconcile). 이 절이 **현행 도달 결론**이다. 상세·실험로그 = `remain-trouble.md` D5~D8 + `$CLAUDE_JOB_DIR/tmp/RESEARCH_LOG.md`·`exp_*.py`.

**핵심 원칙 (사용자 통찰)**: ①정체는 **대사가 이끈다**(얼굴 아님 — 웹툰 얼굴은 작화변이·측면·SD·가림·옷으로 약함). ②**Character 1:n Appearance(외형: 젊/늙/옷) 1:n Name(별호/직함/전생명)** — 스키마 이미 지원(`analysis_character` 1:n `analysis_character_appearance`). ③**이름은 최후에만** 커밋(cluster-first) — 조기 명명이 flip·오염의 근원.

**파이프라인**:
1. **Pass1 = 별칭-인식 coref** (회차 전체 트랜스크립트 단일 텍스트콜, glm-5.2). 소설처럼 이어 읽으며 캐릭터 파악: 한 캐릭터=여러 이름(별호·직함·전생명·OCR변이)을 **네이티브 그룹핑**, 서로 대화(conversant)하는 자는 다른 캐릭터. **5장르 검증**(무협·학교·바바리안·무속·회귀 — 전부 클린). 이미지 불필요. ⭐윈도우/carry-forward보다 압도적(순차 드리프트 붕괴 회피).
2. **경계선 안정화**: 유사인물쌍(청명/구칠, 환생 거지)은 단일 Pass1이 ~40% 병합(비결정). → **conversant 하드제약**(대화하는 자는 반드시 다른 캐릭터 — 청명↔구칠 대화 검출 신뢰) + **앙상블 투표** + **face tiebreak**(청명 appearance 4610 ≠ 구칠 4614).
3. **별칭 vs 다른사람 판별 = conversant(공기)**: 서로 대화하면 다른 사람, **절대 공존 안 함(회상·전생만)이면 별호**(청명/매화검존 = 한 캐릭터). *기계적 "같은 컷 등장" 공기는 실패*(자기 다중명명이 거짓공기) → LLM 대화관계로.
4. **얼굴 = 종속·확증** (정체 주 아님): 말풍선 꼬리로 얼굴→캐릭터 부착. 얼굴클러스터는 tiebreak/확증·교차회차 링크용. CCIP=러프 prior.
5. **교차회차 글로벌 정체** = **이름링크**(양쪽 명명 인물, flip 없음) + **얼굴클러스터 링크**(한쪽만 명명된 무명 인물 — appearance가 회차 span, 예 구칠 4614 ep1·ep2) + persona. 완전 링크 = 이름 ∪ 얼굴.
6. **cluster-first 구현**: `_cluster_first_enabled` 게이트(config, 기본OFF·안전폴백). ON이면 apply가 **eager 명명·승격·자동귀속 정지**(D6 오염 벡터=자동귀속 차단), 이름은 suggestion만, 클러스터 익명 유지. **전역 명명 최후 패스는 미구현**(별칭coref + 얼굴/공기 링크 → 이름).

**미검증/미착수**: Hungarian per-화 매칭·그래프 커뮤니티(전역 클러스터링), 앙상블·conversant제약 통합, 얼굴부착 정밀도(청명/구칠 스왑 잔존), 전역 명명 최후 패스, 프로덕션 배선. **⚠️ prod: D5(a47259d) 자동귀속 라이브 → 신규분석 오염 지속. cluster-first ON 배포로 정지 필요(코드 준비됨, 미배포).**

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

### ✅ 설계 결정 (2026-07-24 확정)
- **Q1 = F라벨 + 회차-로컬 문자슬롯 병기.** Stage V에 `{"id":"F0","slot":"A"}` — F는 오버레이 grounding(이미지에 그린 숫자와 일치), slot은 `appearance_id`를 회차 내 결정론적 순서로 A/B/C 재라벨한 **안정 축**(raw int 아님 — LLM 정수-크기 헛패턴·가독성 회피). 매핑 `appearance_id↔slot`은 결정론적이라 resolve/reconcile이 되짚음. **부가정제**: `source='human'`(확정) 얼굴은 CCIP 추측이 아니라 ground truth라 `name`까지 주입(불가침) — 익명화를 "CCIP step2 추측에만" 좁힘.
- **Q2 = 회차별 명명 먼저(즉시 출하) + 웹툰-전역 나중.** 회차 내 대사증거로 슬롯 명명(박대석 소급강등 = 이미 검증). 전역 명명전파는 Q3 링킹 이후 후속.
- **Q3 = 이름-우선 링킹 + 미명명 클러스터에만 CCIP 보조.** 대사-이름이 정체를 몲(ep1-slotA=ep5-slotB=청명이면 병합), CCIP는 이름없는 클러스터에만 보조(이름 절대 안 덮음). persona 링커는 3순위. = 옛 43% 시각자석 실패의 반전.
- **⭐ 아키텍처 = Path A (시각슬롯 단일화).** 이중구조를 "시각 슬롯 단일 우주"로 구현: Stage V에 익명 CCIP 시각슬롯 주입 → 화자배정도 **세그 비전이 face_label 참조로 수행**(기존 resolve 경로에서 CCIP 이름만 벗김) → reconcile이 시각슬롯에 명명. **텍스트-only consolidate 경로 폐기.** 근거: 흐름-first가 텍스트-only로 우회한 이유(컷단위 비전 화자배정 과분할)를 **세그먼트 단위 전환이 이미 제거**(화자 커버리지 1.7~2배). 세그 단위 비전이면 익명 시각슬롯 화자배정이 grounding까지 얻으며 한 우주로 끝남. ⚠️ **세그 비전 화자배정이 흐름-first 0.95에 근접하는지는 화산귀환 A/B 소규모 대조로 확인 후 토글 제거.**
- **Q4**: 세그-네이티브 저장(remain-trouble D2)과 독립 — 병행 가능.

> ⚠️ **핵심 발견**: 코드에 "슬롯"이 두 우주다 — ①시각 슬롯(`appearance_id`, step2 CCIP) ②페르소나 슬롯(`consolidate_episode`가 대사만으로 발명, `_commit_slots`가 슬롯당 새 cluster 생성, **얼굴과 연결 0**). flow_first purge만 빼면 둘이 각자 놀아 "얼굴에 이름"이 저절로 안 됨. Path A는 ②(consolidate)를 버리고 ①(시각)로 단일화 + resolve 경로에서 CCIP 이름만 벗기는 방식.

---

## 3. 현재 상태 (2026-07-24 세션 끝 시점)

### 배포 완료
- 세그먼트-단위 분석(A~D): 커밋 `b7b5d13`(+A `23d90f6`), service 마이그 `0040`(strip_y·segment_scene_meta·segment_unit_enabled). **토글 기본 OFF**.
- 크래시 핫픽스 `7ae5297`, A/B/C 서사빵꾸 fix `4c73091`, **429 재시도 `e2c0cbc`**(방금 배포).
- 문서 커밋: `2972dbe`·`e44a632`(remain-trouble 결정), 이 PRD.

### Path A 하이브리드 구현 (2026-07-24, ✅ 코드완료·미배포·미커밋)
§2 Path A를 `flow_first_enabled=true` 뒤에 구현(토글은 검증까지 유지 — true=Path A, false=옛 CCIP이름). 변경(로컬, 미커밋):
- **step2.py**(익명 클러스터링): purge 조기반환 삭제 → `anonymous_only` 플래그로 정상 클러스터링 진입. `_get_excluded_appearance_ids(webtoon_id, anonymous_only)`에 `OR c.kind='character'`(명명인물 매칭후보 제외=자석 차단, load_ccip_anchors+cosine $nin 양쪽 반영되는 단일지점). `_seed_confirmed_faces`는 `AND c.kind='cluster'`로 명명인물 앵커 시딩 안 함. 신규는 이미 kind='cluster',name=''.
- **step3.py**: `_slot_label`(엑셀식 A/B/…AA)+`_episode_slot_map(ep_id)`(appearance_id→슬롯, 첫등장순, human>step2 우선 일치) 신설. `build_pass1_input(...,slot_map=)` 익명경로 재작성: `{"id":F,"slot":A}` + **human 확정만 `confirmed`+`name`**(불가침). `extract_cut`/`extract_episode`에 slot_map 배선(회차1회). Stage V 시스템프롬프트에 slot 설명 추가. **resolve_and_narrate의 consolidate 분기 삭제** → flow_first여도 표준 resolve(roster→R→N). 화자배정은 face_label→appearance→cid(이름 없이 작동), 명명은 name_evidence(대사).
- **step3_segment.py**: `extract_segment`/`extract_episode_segment`에 slot_map 배선.
- 검증: py_compile OK, step3 회귀테스트 13통과(1실패는 사전존재·무관 segment-share 속성테스트).
- **consolidate_and_commit_episode/_commit_slots 등 옛 텍스트-only 경로는 코드에 남김**(dead, 검증 후 §④에서 제거).

### ✅ 배포·검증 완료 (2026-07-24)
- **배포**: 이미지 `0545bd5` 라이브(자석수정 `name<>''` 포함). CI가 docs 커밋을 tip으로 보면 파이프라인 빌드 스킵하는 함정 겪음(HEAD~1..HEAD diff) → 파이프라인 파일 커밋으로 재빌드 강제.
- **웹툰17 전체 리셋**: reset_webtoon.py에 최신 테이블 3개(name_edge·llm_sample·segment_scene_meta) 누락 보강 후 실행(analysis 전부·cut 1114·R2 1004·Chroma 3컬렉션 삭제).
- **화산귀환 ep1 재분석 검증 = Path A 통과**:
  - step2: 89 얼굴 → **29 익명 클러스터, 명명 인물 0**(자석 소멸. 옛 false는 5 char+11 named cluster).
  - 명명(대사 기반): **청명(main,11얼굴)·동료걸인소년(sup,7)·천마(sup,0얼굴=외형없는이름 B3 정확)** 3명 + 익명 26. **misid=[]**(유령/빙의 오귀속 0).
  - 화자배정 168(청명40·걸인31·천마6·익명클러스터91).
  - ⭐**정밀도>재현율**: 옛 5명(CCIP추측 왕초·화산장로 포함)→3명(대사확증만). 적게 명명하되 안 틀림. 왕초 등은 익명 클러스터로 생존, Q3 교차회차 이름링킹이 나중에 명명.

### 미착수 (다음)
- **§④ 토글 제거**(검증 후): flow_first 컬럼·게이트·consolidate 코드 삭제 → Path A 무조건화. + resolve pass2 페이로드에 slot 라벨 노출(cid 대신 가독성, 교차유닛 추적 실질 이득).
- **Q3 웹툰-전역 명명링킹**(이름-우선+미명명만 CCIP) — 회차별 명명 안정화 후.
- 세그-네이티브 저장(remain-trouble D2, 프론트 대공사). LLM 주도 lookback(D4).

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
