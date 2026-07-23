# 세션 핸드오프 — 2026-07-22~23 (웹툰 분석 파이프라인 개선)

세션 초기화 후 이어서 작업하기 위한 상태 정본. 날짜 절대값 기준.

## 0. 한 줄 요약
바바리안 정체성 되돌리기(자석 수정) + min-merge(세그먼트 하한) + 장르 교차 재분석 +
teacher 입출력 수집 구현 + 정리 패스/제안검토(adjudicate) 전면 폐기. 전부 배포됨.

## 0.5 최신 작업 (2026-07-23 오후) — reconcile 구현 + 세그먼트-단위 검증 ⭐

### A. 정체 조정(reconcile) — CCIP 오귀속이 서사를 오도하는 근본 문제 해결 (배포됨)
**문제(박대석 케이스)**: 참교육 ep2 요약이 "박대석이 살아서 교실에 있다"고 오판 — 실제론 대사가
"자살/투신/죽었다/안 보이네요"(사망·부재)인데 **CCIP가 죽은 인물에 저점수 얼굴(0.06~0.11)을 오귀속**해
유령 등장을 만듦. 카락(바바리안)·천마(화산귀환)도 같은 패턴. **점수 컷팅은 답 아님**(진짜/오귀속 점수 다
낮아 겹침 — 나화진 0.03~0.11) → **의미(대사) 판단**이 답.

**해결(커밋 b448802 → 007bcf3, 배포됨)**: 로스터 스테이지를 reconcile 권위로 강화 —
- `_ROSTER_SYSTEM_PROMPT`: "faces=CCIP 추측, 대사>얼굴, 서사 모순 얼굴=오귀속 → present_now=false + status에 사망/부재 명시" + character_id 출력.
- `_apply_reconcile`(step3c 신규): 로스터가 **present_now=false + 사망/부재** 판정한 인물의 **이 회차 step2 얼굴정체를 소급 무효화**(human 불가침) + 사망은 narrative_state='dead'. 강등은 요약도 구동하는 present_now에서 파생(별도 misid 리스트는 모델 변동에 취약해 폐기).
- **요약은 로스터가 R/N보다 먼저 돌아 자동 교정**(N이 교정된 로스터 소비).
- 이게 "설계엔 있었지만(§4.8 소급 재라벨, R/N present_now 계약) **소급 적용(§17.8)이 미구현**"이던 걸 완성.

**검증(전부 통과)**: 조정실험(glm-5.2 참교육/qwen 화산귀환·바바리안 — 2모델 3장르 오귀속 강등, 과강등0),
로스터실험(프로덕션 입력→박대석 present=false+사망), 배선테스트(박대석 얼굴 7→0·narrative_state=dead,
나화진 present 21 유지). w43 ep2는 이미 교정 완료(요약 "자살한 박대석").
**하네스**: `anon-roster/exp_reconcile.py`·`exp_roster_reconcile.py`(읽기전용, 재사용 가능).

### B. 세그먼트-단위 분석 — 6웹툰 검증 완료(2026-07-23), 미구현
**발견**: 컷(webtoon_cut=다운로드 타일 ~1600px)이 콘텐츠 밴드(analysis_episode_segment ~948px)를 쪼갬 —
w43 ep2 **65/116 세그(56%)가 컷 경계를 넘음**. → 세그먼트-단위 분석이 실측으로 더 나음.

**6웹툰 에피소드 전체 비전 대조(qwen-vl-fp8, 2026-07-23, err=0)** — 검증된 min/max 세그먼트만 사용:
| 웹툰 | 잔존<300px | 세그 trunc | 컷 trunc |
|---|---|---|---|
| 참교육 ep2 | 0 | 12% | 58% |
| kakao-53607472 ep2 | 0 | 10% | 70% |
| 화산귀환 ep2 | 1 | 12% | 72% |
| 당골 ep1 | 3 | 34% | 80% |
| naver-808482 ep2 | 6 | 25% | 73% |
| naver-820097 ep3 | 18 | 28% | 74% |
| **평균** | | **~20%** | **~71%(3.5×)** |

- **결론**: 6웹툰·6장르 전부 세그가 컷 대비 truncation 3.5배(범위 2.4~7×) 낮음, 예외 없음. 컷은 과반(58~80%)이 콘텐츠 절단. 대사총은 컷이 종종 더 많음=경계 파편 중복카운트.
- **잔존 파편(<300px)이 세그 truncation을 끌어올림**: 잔존0~1=10~12%(최저), 잔존6~18=25~28%. 즉 잘린 세그=대부분 min-merge 잔존 파편.
- **잔존 파편 정체(코드 확인)**: `merge_short_intervals`가 못 병합한 <300px 세그. 두 구멍 — ①**max_h=2400 상한**(양이웃 병합시 >2400이면 못 붙음, 밀집 회차에서 발생; 820097 idx111=268px), ②**윈도우 경계 carry**(min-merge는 스트리밍 윈도우별 `terminated`에만 적용, 자랄 수 있는 carry-over=last 제외 → 이음매 조각이 이웃 만나기 전 방출; 820097 18개 중 ~7개 gap>350px). §4 "윈도우간 carry 후순위"가 이것.
  → **extract_segment 설계 시 이 슬리버를 어떻게 다룰지가 관건**(작은 조각을 이웃 세그에 흡수해 분석단위로 쓸지, min-merge cross-window carry를 먼저 고칠지).

**Phase 2/3 프로토타입 — 세그-단위가 화자 커버리지 개선 실증(2026-07-23, glm-4.6v)**: 하네스 `anon-roster/exp_segment_pipeline.py`가 같은 리전을 **컷-단위 Pass-1(extract_cut) vs 세그-단위 Pass-1**(동일 build_pass1_input+프롬프트, 이미지만 세그)로 각각 돌려 region_id로 정렬 대조(세그멘테이션 효과만 격리). 슬리버는 **런타임 병합**(저장세그 불변, 분석직전 <300px를 이웃에 흡수 — 참교육 병합0·820097 18흡수·808482 6흡수, 매핑 정합 리전/얼굴 100%·bbox이탈0 검증).

| 회차 | 리전 | 일치 | 불일치 | 세그만배정 | 컷만배정 |
|---|---|---|---|---|---|
| 참교육 ep2 | 416 | 147 | 45 | **49** | 29 |
| 820097 ep3 | 411 | 47 | 4 | **42** | 20 |

- **세그가 화자 커버리지 ~1.7~2배**(세그만 49·42 > 컷만 29·20). 세그전용 케이스 = **컷경계 넘는 연속 발화**(대사+꼬리+얼굴이 다른 컷에 분리→컷은 못붙임, 세그는 온전밴드에서 정확배정, 대부분 tail 0.9 고신뢰). 컷전용은 conf 0.3~0.6 저신뢰 추측 위주.
- **불일치 판정은 glm-4.6v 한글 명명 약점에 오염**(대개 "이름 vs 얼굴만(?)" 형태, seg>cut도 cut>seg도 아님) → 세그멘테이션 이득 정밀판정하려면 강한 namer(gemma4 26B/qwen 27B)로 재실행 필요. 결과전문=`anon-roster/segment_pipeline_compare.md`.
- ⚠️ **인프라 교훈**: qwen-vl-fp8(spark2)은 대량병렬에 524(과포화). glm은 Cloudflare터널(vllm.prup.xyz) 스트림 절단으로 502/incomplete → **litellm 직결 `http://192.168.1.15:4000`으로 우회**(0.018s 도달, 502/429/incomplete 0). GLM 동시성은 2가 안전(터널경유 6은 429). litellm SpendLogs 모델명=`openai/glm-4.6v`, startTime=UTC.

**✅ 구현·배포 완료(2026-07-24)** — 정본 계획·현황 `docs/segment-unit-plan.md`. 정석(세그 1급)으로 Phase A~D 전부 커밋·main push·배포(pod b7b5d13). 토글 `config_webtoon_pipeline_state.segment_unit_enabled` 기본 OFF=휴면(A2 strip_y저장·A4 슬리버흡수만 활성=순개선). 신규 `src/core/segment_loader.py`·`step3_segment.py`. 통제 E2E(참교육 ep2 세그모드 V→R→N→apply) 통과: 화자250·segScene115·beats컷범위·무효speaker0. **남은 것: 웹툰 토글 ON+재분석해 실사용·스케일 검증**(계획 doc "남은 것" 참조).
**하네스**: `anon-roster/exp_segment_episode.py`(6웹툰 대상·결과 파일 저장·500 재시도, 결과=`segment_episode_results.md`)·`exp_segment_vision.py`.
⚠️ 세그먼트 이미지 재구성엔 `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python` 필요(chromadb/protobuf 충돌).
⚠️ spark2 qwen 500은 콜당 재시도로 무해화됨(6웹툰 err=0). GLM 불필요.
⚠️ **세그 대상은 반드시 min-merge 재추출본만**(deploy 2026-07-22 12:28 UTC 이후 `updated_at`, max_h≤2400 확인) — 구 로직 세그(예 69500409 ep2 max_h=7543, 07-07)는 무효.

**다음 갈래(사용자와)**: ①세그먼트-단위 분석 구현(extract_segment) ②misid 죽은코드 정리.

## 1. 배포된 변경 (커밋)
### data-pipeline (main)
- `90b5299` min-merge 최종flush 대칭 / `feeeece` 재실행 FK 정리 / `f793f9d` step1 human정리(라벨 disposable)+llm_client qwen thinking-OFF 자동주입
- `b60dad7` teacher 입출력 수집 / `de1cfa3` hangul(임시) / `d52fdd6` flow-first Phase4 링커 죽은코드 제거
- `25bd241` 정리 패스 auto-hook 폐기 / `3614b0d` consolidate/adjudicate 죽은코드 삭제(adjudicate.py·service_bus.py 등) / `5950aec` RegenerateBatchWorkflow 제거
- `b448802` reconcile(정체 조정) 구현 / `007bcf3` reconcile 강등을 present_now 파생으로(§0.5 A)
### proxmox-configuration (main)
- `7a8bc8c` LLM_MAX_CONCURRENCY 2→6 (GLM 동시성)
### service (main)
- `8741472` 정리 패스 API 제거 + teacher 스키마(마이그레이션 0038 collect_io/LLMSample, 0039 CONSOLIDATE enum 제거). **마이그레이션 0038·0039 prod 적용 완료.**
### webtoonmoa (main)
- `4254dbd` 정리 패스/심판배지 UI 제거

## 2. 진행 중 (Temporal durable, 자동)
- **재분석 체인**: `relseg_*`(EpisodeChainWorkflow) — **2026-07-23 05:30 UTC 전부 Completed**(Running 0). 6웹툰 31회차 재분석 완료(53607472·758037·769209·808482·820097·838215).
  - ⚠️ **reconcile 배포 타이밍 갭(2026-07-23 검증)**: reconcile는 pod 롤아웃 기준 **b448802=03:36 UTC / 007bcf3(현행)=04:40 UTC**부터 배포됨. 체인이 07-22 저녁부터 돌아, **31개 중 27개 회차의 resolve가 03:36 UTC 이전에 끝나 reconcile 미적용**(v1 2개·v2 2개만 적용). **"재분석분은 오귀속 소급 강등됨"은 사실 아님** — 아래 §4 참조. (단 27개가 다 오귀속인 건 아님; 참교육 ep2는 미적용인데도 DB 정상=재분석 로스터가 자연히 교정. reconcile는 오귀속 실발생 회차에서만 효과.)
- **teacher 수집**: `analysis_llm_sample`에 vision/roster/resolve/narrative 원문 적재 중(glm-4.6v·glm-5.2만, `collect_io=true`).

## 3. 핵심 성과 (검증됨)
- **자석 근본수정**: flow_first_enabled=False(되돌림) + step2 시딩 쿼리 `is_match_excluded` 필터로 죽은 카락(2818)·빙의 이한수(2786) 앵커 제외. w23 얼굴 정상 귀속.
- **min-merge**: MIN_SEGMENT_PX=300, `cut_merger.merge_short_intervals`. 5장르 거대세그(~8000px)→≤2400, 파편 대폭 감소. (잔존 파편=윈도우 경계 격리, 소수.)
- **teacher 수집**: `config_llm_model.collect_io` + `analysis_llm_sample`(system_prompt·user_text·image_refs[cut참조]·raw_output·finish_reason·repaired + ep/cut/stage/run). `step3._record_llm_sample`가 4콜사이트 배선. 학습 땐 stage/finish/repaired 큐레이션 후 JSONL export, 이미지는 cut_id로 R2 페치.
- **정리 패스 폐기**: 프론트(버튼·심판배지)+API(뷰/URL/celery/adjudicate/admin/temporal)+파이프라인(ConsolidateWebtoonWorkflow·RegenerateBatchWorkflow·adjudicate.py·service_bus.py) 3레포 제거. **유지**: AI 제안 표시·human 수락기각(apply_suggestion_status)·개별 regen(RegenerateCharacterWorkflow).

## 4. 남은 것 / 열린 항목
- [다음 후보] **세그먼트-단위 분석 구현**(§0.5 B — 검증됨, extract_cut→extract_segment). 세그 테스트 더 많은 웹툰(spark2 500 회복 or GLM 사용).
- [정정됨] ~~relseg 체인 나머지 회차 완료 대기(재분석분은 오귀속 소급 강등됨)~~ → 체인은 07-23 05:30 UTC 전부 완료. **하지만 27/31 회차가 reconcile 배포(03:36 UTC) 전에 resolve가 끝나 미적용**(§2 갭 참조). reconcile 적용하려면 그 27개를 007bcf3 pod에서 resolve 재실행 필요. **사용자 결정(07-23): 지금은 미조치**(재분석 재트리거 시 자동 재적용되고, 오귀속 실발생 회차만 문제라 일괄 재실행은 보류). 필요 회차 목록은 아래 주석.
  <!-- reconcile 미적용 27회차(title_id ep): 53607472 e1-6, 758037 e1-4, 769209 e1-4, 808482 e1,2,21,22, 820097 e1-6, 838215 e1-3 -->
- [결정] teacher 데이터로 실제 SFT/증류 — 언제 얼마나 모아서. (지금 계속 쌓임)
- [정리] misid 죽은코드(ResolveResult.misid_character_ids·_sanitize_misid·extract_roster misid 반환 — reconcile을 present_now 파생으로 바꿔 미사용, 무해). `apply_suggestion_status`의 `hook_collector`(항상 None).
- [후순위] min-merge 잔존 파편(윈도우간 carry).
- [인지] 동시성6 + 롤링배포 겹침 → 순간 2pod×6=12로 GLM 429 버스트(self-heal). replicas=1 전제. 잦으면 maxSurge=0.
- [인지] spark2 qwen-vl-fp8이 병렬 대량 호출에 500 에러 잦음(단발은 OK).

## 5. 인프라 / 접속 (메모리 [[spark-servers-infra-2026-07-22]] 상세)
- **prod DB**: `data-pipeline/prod.env`(gitignore). psycopg2, **POSTGRES_PORT=5459**(direct). 읽기전용 원칙, 쓰기는 파이프라인 raw SQL/service 마이그레이션. litellm 원본은 dbname=litellm.
- **spark1=192.168.1.15**: OCR/YOLO(GPU_SERVER=gpgpu.prup.xyz)·litellm:4000(VLLM_API_HOST=vllm.prup.xyz)·comfyui. **spark2=192.168.1.16**: qwen-vl-fp8:8002(27B)·qwen-base:8001. SSH `sshpass -p '12qw!@QW' ssh jj@<ip>`. qwen thinking-OFF=chat_template_kwargs.enable_thinking=false.
- **클러스터**: `sshpass -p 123123 ssh root@192.168.1.36 "kubectl ... -n beldori"`. 파이프라인 pod=webtoon-pipeline-*, celery/backend도 beldori ns. 배포=data-pipeline push→CI→ghcr→ArgoCD(proxmox 태그 자동커밋, 수동수정 금지).
- **GLM**: api.z.ai(실제 클라우드, 다운 없음). 동시성 6이 정당(configmap LLM_MAX_CONCURRENCY).

## 6. 상태 확인 명령 (재개 시)
```bash
cd /Users/jj/github/data-pipeline/webtoon-pipeline && set -a && source ../prod.env && set +a
# 재분석 완료 회차
.venv/bin/python -c "import psycopg2;c=psycopg2.connect(host='$POSTGRES_HOST',port='$POSTGRES_PORT',dbname='$POSTGRES_DB',user='$POSTGRES_USER',password='$POSTGRES_PASSWORD');cur=c.cursor();cur.execute(\"SELECT w.id,count(DISTINCT e.no) FROM analysis_run r JOIN webtoon_episode e ON e.id=r.episode_id JOIN webtoon w ON w.id=r.webtoon_id WHERE r.kind='resolve' AND r.status='succeeded' AND r.started_at>now()-interval '1 day' GROUP BY w.id\");print(cur.fetchall())"
# teacher 샘플
# SELECT stage,count(*) FROM analysis_llm_sample GROUP BY stage;
# 러닝 워크플로: 파이프라인 pod에서 temporalio client list_workflows('ExecutionStatus="Running"')
```

## 7. 관련 정본 문서
- `prd-for-improve.md`(웹툰 분석 염원·재현절차), `prd.md`(설계·계약), 이 세션 산출 scratchpad: OVERNIGHT_PLAN.md·MORNING_REPORT.md·overnight_results.md(세션 격리라 참고만).
- 메모리 인덱스 `~/.claude/projects/-Users-jj-github-data-pipeline/memory/MEMORY.md`.
