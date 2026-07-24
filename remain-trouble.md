# 남은 해결과제 (remain-trouble) — 2026-07-24

세그먼트-단위 분석 + flow_first 작업 중 드러난 미해결 과제 종합. 관련 정본:
`docs/segment-unit-plan.md`(세그 구현), `chamgyoyuk-manual-analysis-2026-07-17.md`(참교육 수동분석),
`redesign-flow-first-2026-07-22.md`(흐름-first 원설계).

---

## A. 세그+flow_first 조합 (둘 다 ON) — 미완성·논의 필요

화산귀환(17)을 segment_unit=true + flow_first=true로 돌리며 발견. 이 조합은 **E2E 미검증이었음**
(내 세그 E2E는 flow_first OFF였음).

### A1. ⭐ 얼굴↔인물 매칭 소멸 (flow_first purge) — **논의 대기(D)**
- `step2.py:714` — flow_first ON이면 step2가 **CCIP 정체결합 전면 스킵 + 기존 정체 purge**(`flow_first_purge`).
  → ep1 face_embedding=0·face_identity=0. 인물=대사흐름 명명 클러스터만(kind=character 승격 0, 얼굴 매칭 0).
- 사용자 지적: "분석(a,b,c) 다 하고 **나중에 얼굴 매칭**을 해야 하는 것 아닌가" — 지금은 아예 버림.
- **결정 필요 방향**: (a)회차별 step3 후 얼굴매칭 후속단계 / (b)전회차 후 웹툰전역 1회 매칭 /
  (c)flow_first를 얼굴매칭과 공존 재설계(익명분석+사후 얼굴 클러스터링·persona 링커로 이름전파).
  추천=(c) — 원설계의 persona 링커(대사·화법 회차간 연결) + 얼굴 클러스터 사후병합.

### A2. consolidate 경로 미검증
- 세그+flow_first는 flow-first **consolidate** 단계를 탐(내 Phase C resolve/apply 아님). 화자배정을
  consolidate가 함(슬롯5·배정361·speakers180) → **apply.speakers_resolved=0**(내 세그 region_map은 이 조합서 미사용).
- 즉 내 Phase C(세그 region_map)는 flow_first OFF일 때만 활성. flow_first ON+세그 조합의 정합성은 별도 검증 필요.

### A3. 화산귀환 재분석 검증 진행 중
- A/B/C fix(아래) 후 step3 재실행(`naver_769209_s3`) 중. ep1 서사 빵꾸 해소 검증 대기.

---

## B. 참교육 수동분석(chamgyoyuk-2026-07-17)의 미해결 발견

`chamgyoyuk-manual-analysis-2026-07-17.md` 참조. **스킵 컷 문제(세그 커버리지)는 07-22 재분석+strip_y로
해결됨**(ep1 c3→13리전·c5→10리전 검증). 아래는 아직 유효:

### B1. 나레이션 주인이 회차 안에서 바뀐다 (§4.8① 깨짐)
- 참교육 ep1: 다큐3인칭(c1~11) → 학생1인칭(c17~52) → 냉소적 성인화자(c60~). 회차 내 2번 전환.
- **`basis=pov`("나레이션=주인공")를 기본값으로 두면 안 됨.** 회차 내 판정 변수. → 화산귀환 서사 품질에도 영향 가능.

### B2. type enum이 못 담는 텍스트 (신규계약 C10 후보)
- 면책고지·법조문·인포그래픽/차트·뉴스자막·시간경과자막·문서프롭·스태프크레딧·편집자주.
- 전부 **"화자 없는 텍스트"** — 화자 슬롯 무의미. 현행 `speech|monologue|narration|system|other` 5종 부족.

### B3. 언급만 되고 화면에 없는 인물 (로스터↔이름 1:1 아님)
- 참교육 '대식'(자살, 화면 미등장, 이름만). 익명 로스터(외형)엔 안 잡히고 텍스트에만 존재.
- **외형 없는 이름 / 이름 없는 외형 둘 다 정상** — 로스터·명명 대응을 1:1로 강제하면 안 됨.

### B4. 명명 경로 ③ 문서/소품(prop)
- 나화진 이름=화면 속 공무원증 인쇄(ep1 c124). diegetic screen/document text(뉴스자막·상태창도 동류).
- OCR 취약(기울어진 원근·작은 글씨). 이름은 살고 직함은 깨짐(교권보호국→'교권보호구').

---

## C. 세그먼트-단위 구현 잔여 (기능은 배포됨, 개선 여지)

### C1. apply/reresolve가 region_map 위해 strip 재조립 (비효율)
- 세그 모드 apply가 `episode_region_map_and_ranges`로 컷 전량 재페치+vstack. fresh flow서 extract가 이미 함.
- **최적화**: ExtractResult에 region_map 부착해 apply가 재사용(재조립 회피). reapply(ep=int)만 재조립.

### C2. 뷰어(webtoonmoa) 세그모드 미대응
- 세그모드는 scene을 `segment_scene_meta`에 씀, `cut_scene_meta`는 빔. 뷰어가 옛 cut_scene_meta를
  읽으면 컷별 scene 안 보임. → 프론트가 segment_scene_meta 읽도록 수정 필요(webtoonmoa).

### C3. 재분석 필수
- strip_y·세그모드는 step1부터 재적재해야 반영. 기존 회차는 구 데이터. 대상 웹툰 재분석 체인 필요.

---

## D. 인프라·기타

- **YOLO 서버(gpgpu.prup.xyz) 간헐 502** — step1 YOLO 콜, 재시도로 self-heal(무해).
- **flow_first persona 링커 미배포** — 교차회차 정체(대사·화법)를 얼굴 없이 잇는 실험코드
  (`anon-roster/exp_linker*.py`). A1(c) 되살릴 때 활용.
- **litellm 인프라**: 로컬 대량 실험은 직결 `http://192.168.1.15:4000`(터널 502 회피)+동시성 낮게(429 회피).
  pod는 클러스터 내부라 무관.

---

## 우선순위 제안
1. **A1(D) 얼굴 매칭 후속화 설계** — 사용자와 방향 결정 후 구현 (가장 큰 미결).
2. **A3 화산귀환 재분석 검증** — 서사 빵꾸 해소 확인(진행 중).
3. **B1 나레이션 화자 전환** — 서사 품질 직결, 비교적 국소 수정.
4. **C1 apply 재조립 최적화** / **C2 뷰어 세그 대응**.
5. **B2 type enum 확장(C10)** / **B3·B4** — 계약·설계 논의.
