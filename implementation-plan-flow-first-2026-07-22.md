# 구현 계획 — 흐름-first 정체성/명명 (스키마 마이그레이션 포함, 2026-07-22)

정본 근거: `redesign-flow-first-2026-07-22.md`(§0~17 실측), `prd-for-improve.md`(원칙·계약),
`ep1-fresh-agent-analysis-2026-07-22.md`(무오염 삼각검증). 이 문서는 **그 실측을 프로덕션에 옮기는 계획**.

> ⚠️ **레포 경계(CLAUDE.md·메모리):** DB 쓰기·스키마 정본은 **service(Django)**. data-pipeline은 prod를
> **읽기만**. 따라서 마이그레이션은 `service/backend/apps/api/toon/models.py`에 작성하고, 운영 액션은
> service 함수+celery+admin 3단(관리 command 금지). data-pipeline은 **분석 로직/콜**만 바꾼다.
> 프론트 검증은 사용자 docker 워치, service 실행은 `docker exec z_docker-backend-1`.

---

## 0. 설계 원칙 (실측이 정한 것)

| 원칙 | 근거(실측) |
|---|---|
| 익명 슬롯 + 대사 흐름 = 척추, 얼굴 = 강등된 투표권 | E1 0.95 / M2 자석 100% CCIP |
| 이름 = confidence+source 붙은 **수정가능 엣지**, 기본 NIL | E2E 교정 / M5 이름 이미 깨끗 |
| persona(화법 지문) = 교차회차 링킹 키, **영속·누적** | 링커 v3: 드리프트는 재도출이 원인 |
| alias/canonical + 포함 dedup | v2/v3: 로트밀러/브라운로트밀러·엘리사/엘리사베헨크 중복 |
| 이름 증거 3역할(self/vocative/reference), 죽음=파생 상태 | v3 3인칭 분리 / 사용자 카락-회상 지적 |
| belief-state(증거+confidence+출처) 영속 | ep1 에이전트: 확정 미룸+소급 재라벨이 성공 요인 |
| 전사·시각=glm-4.6v / 통합·화자·명명=glm-5.2 | E1: 9B는 태스크 못 따름 |

---

## 1. 스키마 마이그레이션 (service repo)

**전략: 가산적(additive) + dual-write.** 기존 컬럼 유지(롤백), 신규 병행. 배포 순서=service 먼저.

### 1.1 appearance = 회차 스코프 익명 슬롯 (기존 `analysis_character_appearance` 승격)
현재 거의 안 쓰이는 이 테이블을 1급 슬롯으로:
```
analysis_character_appearance 에 추가:
  episode_id        FK  (슬롯은 회차 스코프 = §C2)
  local_id          char(A/B/…, 회차 내 익명 기호)
  persona           jsonb  (화법 지문: {registers:[~냥,~당], honorific, relations:[{to,mode}], role})
  prominence        enum(main|minor|extra)         (§C7 승격 게이트)
  timeline_context  enum(present|flashback|dream)   (§C8 회상 프레임)
  representative_detection_id  FK nullable          (대표 crop 1장)
  link_method       enum(persona|name|face|narrative|human)  (character 결합 근거)
  link_confidence   float
character_id 는 유지(=링크). nullable 허용(미링크=신규/보류).
```
- **1 character : N appearance** 그대로. 청명 4형태·비요른=이한수가 이 그릇에 들어감.

### 1.2 이름 = 엣지 (신규 `analysis_name_edge`)
`analysis_character.name` 을 단일값에서 엣지 집합으로:
```
analysis_name_edge (신규)
  id
  character_id     FK
  appearance_id    FK nullable   (이 이름을 준 회차 슬롯; reference/mentioned면 null)
  surface          text          (표면형: '에르완' 등 오독 포함)
  canonical_name   text          (정규화 후 정본)
  is_canonical     bool          (character의 정본 이름 1개)
  role             enum(self|vocative|reference|narration_subject|card|human|ccip)
  confidence       float
  source           enum(llm|human|ccip)
  run_id           FK
  status           enum(active|superseded|rejected)
```
- **alias/canonical**(§C8.1): 한 character에 여러 edge, `is_canonical` 1개. 비요른/이한수·청명/초삼.
- **정규화(§8②)**: 자모 병합 + **포함(substring) dedup**('브라운 로트밀러' ⊇ '로트밀러' → 같은 엔티티,
  정본=긴 쪽). ⚠️ 정규화는 **발화행위 게이트 통과분에만**, 죽은 이름 방향은 veto 아님(파생 상태로 판단).
- **reference role**: 3인칭 언급은 appearance_id=null 로 엔티티 존재만 기록, 현재 슬롯 미결합.

### 1.3 character 상태·persona (기존 `analysis_character` 확장)
```
analysis_character 에 추가:
  narrative_state   enum(alive|dead|absent|unknown)   (요약에서 파생)
  first_seen_ep / last_seen_ep  int
  persona           jsonb   (누적 화법 지문 — 회차마다 재도출 X, 증거로 갱신 = 드리프트 해법)
  is_adopted_name   bool    (비요른처럼 차용된 정체 표시)
kind=cluster|character 유지(승격은 증거가 요구, §C7).
```

### 1.4 텍스트 type 확장 (§C10, ep1 레지스터)
```
analysis_text_annotation.type enum 확장:
  기존: speech|monologue|narration|system|other
  추가: sfx | name_card | diegetic_prop | meta(title/credit/staff) | disclaimer
analysis_text_annotation 에 추가:
  speaker_applicable  bool   (sfx·system·meta·narration(3인칭)은 false)
  is_diegetic         bool
speaker_id → appearance_id 로 재해석(슬롯에 귀속, §4.6 흐름 배정).
```

### 1.5 얼굴 강등
- `analysis_face_identity` 의 전수 귀속 **중단**(신규 회차부터). 대신 appearance 당 대표 crop 1장만
  (`representative_detection_id`). CCIP는 `analysis_name_edge`/appearance link의 **투표 source**로만 잔존.
- 기존 face_identity 는 보존(롤백·과거 조회), 신규 파이프라인이 참조 안 함.

### 1.6 제안 큐 폐기
- adjudicator 경로 제거. 저신뢰 `name_edge`/appearance link(status=active·confidence<τ)를 **human 라벨
  UI에 직접** 노출. `analysis_suggestion` 은 신규 생성 중단(과거분 보존).

---

## 2. 파이프라인 (data-pipeline)

### Phase A — 컷별 익명 추출 (기존 Stage V 유지·정체성 주입 제거)
- glm-4.6v, 컷당 1콜. **identified_faces 주입 제거**(이게 오염원). 산출: 익명 인물 + 대사블록 + type +
  말풍선 스타일. SFX는 type=sfx로 분류(배제는 정리단계에서).
- 실측: 전사·빈컷 판정 견고(E4), 원칙1 회수(E1c).

### Phase B — 회차 정리 (신규, step3b roster 대체) ⭐
회차 전역 1콜(glm-5.2, 이미지 없이 텍스트):
1. **슬롯 통합** — 컷별 익명을 회차 슬롯으로(어미 지문 클러스터링). 실측: 17→2/3 수렴(E1b).
2. **화자 배정** — 흐름(교대·화법·호칭·POV) → 슬롯. narration POV 포함. 실측 0.91~0.95(E1).
3. **이름 증거 수집** — 3역할 분류(self/vocative/reference/narration_subject) + 발화행위 게이트.
   reference는 mentioned로. → `name_edge` 후보.
4. 산출: appearance(persona 포함) + speaker-attributed 대사 + name_edge 후보 + mentioned.

### Phase C — 교차회차 링커 (신규)
- prior character 로스터(누적 persona)에 회차 appearance를 **persona로 매칭**(+name+face 투표).
- 가드: 발화행위 게이트, 자모+포함 dedup, alias/canonical, **persona 누적 갱신**(드리프트 해법),
  파생 상태 참조(죽은 인물 현재-결합은 flashback 프레임 시만).
- 미링크+자칭앵커 → 신규 character. 미링크+앵커없음 → 익명 cluster 유지(NIL).

### Phase D — 서사 상태 파생 + 명명 확정
- 요약/beat에서 `narrative_state`(사망/이탈) 파생 → character 갱신.
- name_edge argmax + self-verify → `is_canonical` 확정. 억지 명명 안 함(NIL 허용).

### Phase E — 얼굴 대표 crop / 제안 폐기 / SFX 배제
- appearance 당 대표 crop 1장 선택. CCIP 투표만. adjudicator off. type=sfx 배제(name/speaker 미사용).

---

## 3. 롤아웃·검증

| 단계 | 내용 | 검증 |
|---|---|---|
| M1 | service 마이그레이션(가산)+admin | 스키마 배포, 기존 읽기 무영향 |
| M2 | Phase A 정체성주입 제거 | ep10/11 재분석, 오귀속 감소 |
| M3 | Phase B 정리 신설 | E1b 재현(로스터 2/3, 화자 0.9+) |
| M4 | Phase C 링커 + persona 누적 | ep37~44 재연쇄: 드리프트·중복 v3 대비 감소 |
| M5 | Phase D~E + 얼굴 강등 | **웹툰23 backfill: 카락 자석·6인=5인·이한수 흡수 소멸 확인** |
| M6 | 제안 폐기 + human UI 엣지 노출 | webtoonmoa(사용자 docker) 검증 |

- **핵심 수용 기준**: 웹툰23 재분석 시 (a) 카락에 브라운 로트밀러 얼굴 안 쌓임 (b) 파티 5인 (c) 이한수가
  비요른 얼굴 안 훔침 (d) narration 화자 커버리지 0%→대폭↑ (e) 제안 노이즈 소멸.
- 롤백: 구 컬럼·face_identity·suggestion 보존이라 파이프라인만 되돌리면 복구.

---

## 4. 남은 열린 질문 (구현 중 결정)
- persona 지문의 구체 표현(어미 n-gram? 관계 그래프?)과 매칭 임계.
- 포함 dedup의 정본 선택 규칙(항상 긴 쪽? 자칭>나레이션 우선?).
- 파생 상태의 신뢰도(요약 오류 시 오판 방지 — human override).
- 세계 규칙/지속 설정(§ep1 "외부인=악령")을 담을 서사 엔티티(별도 backlog).
- 회상 프레임 자동 검출(§C8 주마등 마커) — 별도 실측 필요.
