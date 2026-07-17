# 얼굴 분류(캐릭터 식별) — CCIP vs qwen 실측, 바바리안 (2026-07-16)

## 배경

- 바바리안 1~10화는 사람이 **얼굴 라벨링 + 클러스터링을 수동 확정**(analysis_face_identity source=human).
- 11화부터는 사람 개입 없이 **CCIP(임베딩 유사도) + LLM 판단만** → 얼굴 분류가 "엉망"(과분할·오귀속).
  서사 분석(로스터/비트/요약)은 대체로 잘 맞는데 **얼굴 정체 귀속**이 약함.
- 질문: 얼굴 분류를 **VLM(qwen)이 시각적으로** 하면 CCIP보다 나은가?

## 방법 (`webtoon-pipeline/tools/face_classify.py`, 읽기 전용)

1. **갤러리**: human 확정 얼굴(1~N화)로 인물별 **번호행 몽타주** 구성. 이번엔 12명 × 6장
   (비요른·에르웬·한스·이한수·가드위버·아이나르·말멀른·헤이나·구드눌프·라이린·오름·한스아울록).
2. **쿼리**: 대상 회차의 사용 얼굴 크롭(R2 `fetch_face_crop`).
3. **qwen 콜**: [갤러리 몽타주][쿼리 얼굴 1장] → "몇 번 인물? (없으면 -1) + confidence" JSON.
4. **대조**: human 정답(검증 회차)·CCIP(step2) 대비. 산출 `datasets/face_classify_w23_e{no}_{mode}_{model}.html`.
- `--mode validate`(정답 있는 회차 정확도) / `--mode apply`(정답 없는 11+ CCIP vs qwen).
- ⚠️ qwen-vl은 reasoning 모델이라 **thinking OFF 필수**(ON이면 얼굴당 ~3분). `chat_template_kwargs.
  enable_thinking=false`를 게이트웨이에 직접 실어야 함(call_llm_json은 이 제어가 없어 `_call_direct` 추가).

## 검증 결과 (ep10, human 확정 정답 53개)

| 방식 | 정확도 | 비고 |
|---|---|---|
| **CCIP (현행, step2 임베딩)** | **43.4%** (23/53) | 사람이 손대기 전 raw 정체 |
| qwen-base 9B (Stage V와 동일 모델) | 34.0% (18/53) | **CCIP보다 나쁨** |
| **qwen-vl 27B (thinking OFF)** | **94.3%** (50/53) | CCIP의 2배+ |

### 오답 분석
- **qwen-base(9B)** 오답 35개 중 34개가 2개 혼동에 집중: 비요른→이한수 **23**(같은 인물 다른 몸),
  에르웬→아이나르 **11**(동성). 즉 9B는 유사 범주 미세 구분 실패.
- **qwen-vl(27B)** 오답 3개뿐: 비요른→이한수 2, 비요른→가드위버 1. 비요른/이한수(빙의)는 근본적으로 애매.

## 핵심 결론

1. **얼굴 분류는 qwen으로 가능하다 — 단 27B(qwen-vl)에서.** 9B(qwen-base)는 CCIP보다도 못하다(모델 크기 결정적).
2. **thinking OFF 필수** — 정확도는 유지되고 속도만 3분→17s/얼굴로 회복.
3. **유일 약점 = 속도 17s/얼굴**(27B on DGX GB10 + 큰 몽타주). ep11-15 383얼굴 ≈ 2시간. 정확도(94%)는
   증명됐으니 이건 최적화 문제.

## 프로덕션 함의 (미결정)

- 전량 VLM 대체는 속도상 무겁다 → **하이브리드** 유력: CCIP로 후보 top-k 좁힌 뒤 qwen-vl로 확정(콜당
  이미지·후보 축소로 가속). 또는 몽타주 해상도/인물 수 축소.
- 신규 인물(갤러리에 없는): qwen이 row=-1(unknown) 반환 → 신규 클러스터 신호로 활용 가능.

## 상태

- 검증(ep10) 완료: qwen-vl 94% 확인. HTML `datasets/face_classify_w23_e10_validate_{qwen-vl,qwen-base}.html`.
- 적용(ep11~15, 정답 없음) qwen-vl로 진행 중 → 회차별 CCIP vs qwen 대조 HTML. ep11·12 완료, 13~15 진행.
- 정본 메모리: `qwen-vl-face-classify-2026-07-16`. 관련 CCIP 과분할 이슈는 prd §18.5 / 트랙C(ccip-bleed).
