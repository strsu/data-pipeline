# cluster-first 인물연결 — 구현·배포·검증·미결정 종합 (2026-07-25~27)

> **목적**: 이 세션에서 한 작업 전부 + 검증에서 드러난 문제 + **남은 선택지**를 사용자가 검토/결정할 수 있게 기록. 자족적 문서.
> **정본 설계**: `prd-for-character-linking.md` "최종 설계 v2" + `remain-trouble.md` D8. **하네스/산출물**: `anon-roster/identity-redesign/`.

---

## 0. 현재 상태 한 줄
A+B+C(cluster-first 전역 명명) **구현·3회 리뷰·배포·검증 완료** → 검증에서 **주인공이 남의 얼굴 대량 흡수(co-presence 오염)** 발견 → **step3d 자동병합 임시 OFF(`_GLOBAL_NAMING_ENABLED=False`) 배포로 오염 정지** → 자동 얼굴병합이 이 웹툰에서 신뢰불가로 판명(몽타주 검증) → **방향 미결정(사용자 검토 중)**.

---

## 1. 무엇을 만들었나 (배포됨, 라이브)

**아키텍처(D8)**: 정체=대사가 이끈다. 얼굴(CCIP) 약함. per-episode는 익명 클러스터+이름 suggestion만, 웹툰 전역 최후패스가 명명·병합.

| 파트 | 내용 | 파일 |
|---|---|---|
| **B** | `name_clusters_by_dialogue` = conversant-제약 **slot-coref 2콜**(①대화 슬롯쌍=다른인물 ②그 하드제약 하 슬롯→인물 그룹핑+명명). 라인단위 아닌 **slot 단위**(O(슬롯수), 긴 회차 타임아웃 회피). 별호=aliases. | `src/core/step3.py` |
| **C** | `resolve_global_identities(webtoon_id)` = pending name suggestion→클러스터별 정본→**name-link union-find**(같은 이름 병합)→대표 승격+나머지 `_attach_cluster_to_character` 흡수. `step3d_global_identities` activity로 apply 후 배선. | `step3.py`, `temporal/activities.py`·`workflows.py`·`worker.py` |
| **A** | cluster-first 전역 ON 게이트(`_cluster_first_enabled`, no-row→True) + service 마이그 0041(`config_webtoon_pipeline_state.cluster_first_enabled`, db_default true) | `step3.py` / service `models.py`+`0041` |
| **E** | dead code 제거 — 옛 consolidate/flow-first 클러스터(382줄) + D7 `step3_reconcile.py`. `_flow_first_enabled`(Path A 게이트)는 live 보존. | `step3.py` |

**코드리뷰 3회 반영**: M1(클러스터별 자기신뢰 게이트=저신뢰 편승흡수 차단) · M2(`_GENERIC_NAME_STOPLIST` 호칭 오병합 차단) · M3(흡수 전 소스 재검사=HITL 클로버 방지) · m5(그룹당 단일 트랜잭션) · 별칭오염 수정(정본과 함께 투표된 별호만) · best_conf→canon_conf.

### 커밋 (data-pipeline main, 전부 push·배포됨)
- `b0bf6cf` A+B+C
- `2da618b` 별칭 오염 수정
- `d7c5111` 문서
- `4a3a086`+`7a0ddfe` E dead code 제거
- `267bd9d` **step3d 자동병합 임시 OFF(안전 정지)** ← 현재 유효 상태
- service: `c55491e`(마이그 0041, 적용됨)

---

## 2. 검증 (웹툰17 = naver 769209, 리셋→ep1~3 재분석)

### ✅ 잘 된 것
- **별칭 오염 수정 확인**: 청명 aliases=[매화검존] clean(회차간 정체 flip에도 천마/운암 오유입 0).
- **명명 구조**: 청명·천마·운암·구칠(→"거지 소년"으로 명명됨) 등 **별도 named character로 존재**.
- cluster-first + step3d 파이프라인 동작(명명 5 / 익명 29).

### 🐛 치명 문제 — 얼굴 대량 오염 (사용자 발견)
- **청명(cid4716)이 26 appearance / 194 얼굴 흡수.** 반면 천마 0얼굴·구칠(거지소년) 2얼굴.
- **결정적 증거**: 한 컷에 "청명" 얼굴이 2개인 컷 27개, 3개인 컷 3개 — 한 사람이 한 컷에 2~3 얼굴은 불가능 → 남의 얼굴이 청명으로 잘못 배정.
- **근본 원인 = co-presence 노이즈**:
  1. B가 청명 대사를 **얼굴 클러스터에 귀속**할 때, 여러 얼굴 있는 컷에서 화자 옆 얼굴까지 크레딧.
  2. → 천마·구칠 등 얼굴 클러스터가 **고신뢰 "청명" 이름표**를 받음.
  3. → C의 name-link가 "청명" 투표한 26개를 **전부 병합** → 청명이 남 얼굴 흡수.
  4. M1 임계(0.85) 무력 — 투표 신뢰는 "이름이 청명"에 대한 확신이지 "이 얼굴이 청명인가"가 아님.
- 야간 R&D가 경고했던 "**얼굴=확증 필요**(co-presence 노이즈)"(E11)를 프로덕션 B/C가 누락. (내 초기 검증이 appearance 개수·별칭만 보고 얼굴배정을 안 봐서 놓침.)

---

## 3. 시도한 수정 = co-occurrence 게이트 (그리고 왜 안 됐나)

**아이디어**: "같은 컷에 공존하는 두 얼굴 클러스터 = 다른 사람" → name-link가 공존 클러스터는 병합 금지. 대사의 conversant 원리를 얼굴에 적용. **임베딩 불필요, 순수 Postgres.** (임베딩은 Postgres 아닌 Chroma에 있음 — `analysis_face_embedding`엔 chroma_doc_id만.)

**데모**(`anon-roster/identity-redesign/cooccur_demo.py`, 재분석 없이 현재 데이터): 청명 26 appearance → 유지 14(77얼굴) / 분리 12(117얼굴).

**몽타주 시각검증**(`montage.py` → `cheongmyeong_montage.jpg`) 결과 = **게이트 신뢰불가**:
- "유지 14"에도 오염: `app4718`(수염 노인=청명 아님)·`app4703`(핏빛/마귀=천마 or 회상).
- "분리 12"에 진짜 청명 대거: `app4731`(27얼굴)·`app4723`·`app4750`·`app4714`·`app4702`가 젊은 청명 얼굴인데 분리됨.
- **왜 틀리나**:
  1. **"컷"이 멀티패널**(타일에 여러 칸) → 청명이 한 컷에 2번 등장 가능 → co-occurrence가 "다른 사람"으로 오판 → 진짜 청명을 쪼갬.
  2. **닮은 얼굴 다수** — 청명·구칠·초삼·운암이 비슷한 화풍의 젊은 무사 → 얼굴만으론 사람도 구분 어려움.

**결론**: name-link도 co-occurrence 게이트도 이 웹툰(약한 얼굴+닮은 인물+멀티패널 컷)에선 **자동 얼굴 확정 불가**.

---

## 4. 남은 선택지 (사용자 결정 대기)

현재 = step3d 자동병합 OFF(오염 정지, 이름은 suggestion만). 여기서:

### (C) 자동병합 포기 + 사람 검토 — *세션 종료 시점 추천*
- step3d 자동병합 **계속 OFF**. 이름은 제안(suggestion)으로만.
- 정체 확정 = **사람이 몽타주 보고 승인**(§3 같은 뷰). "AI 제안 → 사람 승인".
- 할 일: 검토 UI/도구(appearance별 얼굴 몽타주 + 제안 이름 + 승인/분리 버튼). webtoonmoa 또는 admin.
- 장점: 오염 없음, 사람이 최종 판단. 단점: 수작업 필요, 완전 자동 아님.

### (D) 정체=대사만, 얼굴은 참고용
- 얼굴 자동병합 **완전 제거**. 화자 정체는 **대사(slot-coref)만으로** 추적, 얼굴은 표시/힌트용으로만(자동 귀속 안 함).
- 장점: co-presence 오염 원천 제거(얼굴을 정체 앵커로 안 씀). 단점: "이 얼굴이 누구" 질문엔 약함, 얼굴 기반 조회 기능 축소.

### (E) 얼굴 게이트 개선 후 재시도 (실험적)
- co-occurrence를 **패널 단위**로(멀티패널 컷 문제 해결) + CCIP 임베딩 유사도 확증(Chroma 연동) 병행. + 순수 꼬리(말풍선 꼬리→화자 얼굴)로 co-presence 노이즈 감소(E11 미구현분).
- 장점: 자동화 유지 가능성. 단점: 복잡·불확실(약한 얼굴은 근본 한계), 재검증 필요. **"실험 더" 성격.**

### (F) 부분 자동 — 고확신만
- 얼굴이 **명백히 일치**(단독 등장 컷·CCIP 고유사)하는 것만 자동 병합, 나머지는 suggestion. 보수적.

**참고**: 이전에 논의된 별건 follow-up(별개로 남음) — ①청명 significance=minor_functional(step3d 대표 클러스터가 minor 라벨 물어옴 → 병합그룹 최대 significance 투영으로 수정) ②구칠→"거지 소년" 서술명명(coref 프롬프트 "실명 우선") ③regen(reresolve) 경로 step3d 미배선 ④skip된 pending suggestion 잔류.

---

## 5. 재개 방법 (다음 세션)

- **읽기**: 이 문서 + `prd-for-character-linking.md` v2 + `remain-trouble.md` D8.
- **재활성 스위치**: `src/core/step3.py`의 `_GLOBAL_NAMING_ENABLED`(현재 False). 게이트 로직 넣고 True로.
- **DB 조회**(prod, 읽기전용): `webtoon-pipeline/.venv` psycopg2, `set -a && source ../prod.env`. 웹툰17=id 17, naver 769209. 청명=cid4716(현재 오염상태).
- **재분석**: 워커 파드에서 `python -m src.temporal.starter naver 769209 1 step1,step2,step3 <max>`. 리셋=`eval-reset/reset_webtoon.py --source naver --title-id 769209 --execute`. **glm-5.2 콜당 ~270s**(litellm p50 268s=정상, 회차당 ~1시간).
- **하네스/데모**: `anon-roster/identity-redesign/`(cooccur_demo.py·montage.py·cheongmyeong_montage.jpg·test_thinkoff.py·exp_*.py).
- **LLM 직결**: `VLLM_API_HOST=http://192.168.1.15:4000`(prod.env는 터널값이라 하드셋 필요). thinking-off는 glm엔 무효.
- **인프라**: kubectl은 `sshpass -p 123123 ssh root@192.168.1.36`, namespace beldori.

## 6. 교훈
- 검증은 **개수·별칭이 아니라 실제 얼굴 배정**을 봐야 함(몽타주). 초기 "오염0" 판정이 틀렸던 이유.
- 약한 얼굴 웹툰에서 **자동 얼굴병합은 근본적으로 위험** — 사용자 지적("너도 완벽하지 않으니 먼저 보여줘")이 정확.
- co-presence(공존)는 강한 "다른 사람" 신호지만 **멀티패널 컷에서 깨짐**.
