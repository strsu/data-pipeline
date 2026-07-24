# 세그먼트-단위 분석 정석 구현 계획 (2026-07-23)

## ✅ 구현 완료·배포 (2026-07-24)
**Phase A/B/C/D 전부 구현·커밋·main push·배포됨.** 토글 `segment_unit_enabled` 기본 OFF(전 웹툰) → **휴면**(A2/A4만 활성=순개선).
- 커밋: data-pipeline `23d90f6`(A: step1 strip_y+슬리버흡수) `b7b5d13`(B+C+D: 세그 Stage V+다운스트림 세그키잉) / service `385f729`(마이그 0040).
- 신규 모듈: `src/core/segment_loader.py`(로더) `src/core/step3_segment.py`(extract_segment·extract_episode_segment·region_map·reresolve·beats remap). 게이팅: step3.apply_resolution/reresolve_episode + temporal/activities(순환회피 지연import).
- **통제 E2E(참교육 ep2, V→R→N→apply 세그모드)**: resolve_error=null, segScene=115·cutScene=0, 화자배정=250(나화진84·교장33·김학재39…)·무효speaker=0, beats cut_end=99≤100컷(remap✅), region_map 불변식 416키 일치. → 통과.
- **남은 것**: → `remain-trouble.md`로 이관·종합.

## 작업 로그 (2026-07-24, 시간순)
1. **Phase A~D 구현·배포** (커밋 `23d90f6` A / `b7b5d13` B+C+D / service `385f729` 마이그 0040). 신규 `segment_loader.py`·`step3_segment.py`. 통제 E2E(참교육 ep2, flow_first OFF+세그) 통과.
2. **크래시 핫픽스** (`7ae5297`): Phase A(A2)에서 `face_detection` INSERT의 `%s`를 15개(컬럼 14)로 잘못 넣어 `IndexError: tuple index out of range` — 화산귀환 step1 세그 처리 실패. 14개로 수정. (text_region INSERT는 14=14로 정상이었음.)
3. **화산귀환(17) 파이프라인** — 사용자가 segment_unit+flow_first **둘 다 ON**. 체인-A(step1,2 1~10)→ep1 완료→체인-B(step3 1~10). ep1 결과 요약·티저 우수(청명 명명 정확).
4. **A/B/C fix** (`4c73091`): 세그+flow_first 화산귀환 ep1 **서사 빵꾸**(주마등 컷62~77 미커버) 수정.
   - 근본원인: 빈 세그(리전·얼굴0) 스킵 → 세그 index 불연속 → narrative LLM이 beat 경계를 빈 index(78·79)에 찍음 → remap 실패.
   - **A**: extract_segment이 빈 세그도 분석(스킵 제거) — "모든 컷 분석" 합의 준수 + index 연속성.
   - **B**: 로더가 strip_y 겹침으로 전 세그 컷범위 계산(`_cuts_overlapping`) — beat remap 전세그 커버.
   - **C**: extract_segment per-segment 로그 추가.
   - fix 후 step3 재실행 중.
5. **참교육 수동분석 교차확인** (`chamgyoyuk-manual-analysis-2026-07-17.md`): 그 md가 지목한 스킵 컷(세그 커버리지) 문제는 07-22 재분석+strip_y로 **해결됨** 검증(c3→13리전). 미해결 발견들은 `remain-trouble.md` §B로.

---


Stage V(step3)를 컷-단위 → **세그먼트-단위(1급 시민)**로 전환. MVP(재투영) 아니라 정석.
근거: 6웹툰 truncation 컷~71% vs 세그~20%, 화자 커버리지 세그 1.7~2배(핸드오프 §0.5 B).

## 핵심 아키텍처 발견
**step1이 이미 세그먼트로 OCR/YOLO를 돌린다** (`_iter_episode_segments` → `seg.image_bytes` +
`_ensure_segment` → `_process_segment_ocr/_yolo(seg, segment_id)`). 즉:
- 세그 이미지 조립·`split_tall_interval`·세그↔컷 귀속(`state.bounds`) **이미 존재**.
- region/face 생성 시점에 **segment_id를 이미 알고 있음** — 지금은 episode_segment 공유용으로만 쓰고 row엔 저장 안 함.
- **Stage V(step3)만 컷으로 재페치** — 여기만 세그로 바꾸면 됨.

→ step3에서 재조립(프로토타입 방식) 대신 **step1이 이미 만든 세그 산출물을 저장·활용**하는 게 정석.

## 좌표 사실
- text_region/face_detection bbox = **컷-로컬 좌표**(cut_id FK). step1이 세그에서 탐지→컷 귀속하며 변환 저장.
- Stage V 오버레이엔 **세그-로컬 bbox** 필요 → step1이 세그-로컬(또는 전역 strip) bbox도 저장하거나, step3가 변환.

## resolve/apply 식별자 (다운스트림 영향)
- `_build_pass2_user_payload`: `rec.cut_number` + `b.index`로 블록 식별.
- speaker_resolution 출력: `(cut, block_index)`. `apply._episode_region_map`: **(cut_number, tr.index)→region_id**.
- 정석 = 이 식별을 **(segment_index, index)로 전환** → payload/region_map/reresolve 재구성 모두 세그 기준.

---

## ⚡ Phase A 재산정 (2026-07-23 코드 확인) — 대부분 이미 존재
**step1이 세그-단위를 미리 대비**: `TextRegion.segment`·`FaceDetection.segment` FK **이미 있음**(모델+마이그, prod 100% 채움 86843·17213). step1이 `segment_id` **이미 기록**(step1.py:825·909). `gy1/gy2`(전역 strip y)도 **이미 계산 중**(step1.py:806) — 저장만 안 함.
- ~~segment_id FK~~ ✅ 존재 / ~~step1 segment_id 기록~~ ✅ 존재
- **A1 [service 마이그레이션]**: ①region/face에 `strip_y1/strip_y2` 컬럼(step1이 이미 계산하는 gy 저장; x는 컷=세그 동일이라 불필요) ②`analysis_segment_scene_meta(segment_id, action_summary, key_objects, run_id)` ③`config_webtoon_pipeline_state.segment_unit_enabled`(flow_first 패턴).
- **A2 [step1]**: region/face INSERT에 `strip_y1/y2`(=gy1/gy2) 추가 저장. 소규모.
- **A3 [세그 이미지]**: **step3 재조립**(결정) — strip vstack 후 `episode_segment.strip_y1/y2`로 크롭. bbox는 저장 strip_y로 배치(컷offset 재계산 불필요). R2 저장 안 함.
- **A4 [step1]**: 슬리버 방출단 흡수 — `_iter_episode_segments`/`merge_short_intervals`의 cross-window carry 수정.

## Phase B — step3 Stage V 세그-단위
- **B1**: 세그 로더 — episode_segment 순회 + `WHERE segment_id=%s`로 region/face 로드 + R2 세그 이미지 페치.
- **B2**: `extract_segment` — build_pass1_input(세그 이미지 + 세그-로컬 오버레이 + ocr_blocks) + 프롬프트 + _sanitize_pass1. (build_pass1_input 재사용, 이미지만 세그.)
- **B3**: `extract_episode_segment` — 세그 순회 병렬 → belief 누적(세그 읽기순) → ExtractResult(records=세그 레코드).
- **B4**: 세그 scene_meta 영속(`analysis_segment_scene_meta`).

## Phase C — 다운스트림 세그-키잉
- **C1**: `_build_pass2_user_payload` — 컷별 → 세그별 그룹(segment_summary/blocks).
- **C2**: `_episode_region_map` (segment_index, index)→region_id + speaker_resolution ref (segment, block_index) + `_commit_speaker_resolution`.
- **C3**: reresolve/reapply DB 재구성(`_build_pass1_records`/`_load_provisional_blocks`)을 컷→세그 기준으로.
- **C4**: beats(cut_start/cut_end)·리포트 — 컷범위 유지(세그→컷범위 매핑) or 세그 전환. **정석 최소침습=컷범위 유지**.
- **C5**: prepare/cleanup(`prepare_episode_scene`) 세그 scene_meta 반영.

## Phase D — 설정·배선·롤아웃
- **D1 [결정]**: 토글 — `config_webtoon_pipeline_state.segment_unit_enabled`(service 마이그레이션) or env. **정석=DB 플래그**(per-webtoon 롤아웃/롤백).
- **D2**: `activities.py:381` 게이트 — 플래그시 extract_episode_segment.
- **D3**: 백호환 — 기존 컷-분석 회차는 segment_id NULL. 세그 경로 켜면 재분석 필요(step1부터 재적재해 segment_id 채움).

## Phase E — 검증 & 코드리뷰
- **E1**: 테스트 회차 E2E(플래그 ON) — resolve/apply/reconcile/report 정상, 화자매핑 무파손.
- **E2**: 세그 vs 컷 산출 대조(하네스 재사용) — 회귀 없음 + 커버리지 개선 확인.
- **E3**: 코드리뷰(/code-review 또는 self) — 각 Phase 커밋 단위.

## 잠근 결정 (2026-07-23, 코딩 전 확정) ✅
1. **슬리버(A4)**: **방출단 흡수** — step1 min-merge를 cross-window까지 고쳐 <300px 세그 소멸. 저장세그=분석단위 일치.
2. **bbox 저장(A1)**: **전역 strip-y 컬럼 추가** — region/face에 strip_y1/y2. 컷-런·세그-런 양쪽 파생 가능(step1이 이미 전역 y 앎).
3. **다운스트림 식별(C)**: **세그-키잉 전환**(정석) — (segment_index, index) 기준.
4. **토글(D1)**: **DB 플래그** `config_webtoon_pipeline_state.segment_unit_enabled` (flow_first_enabled 패턴).
5. **beats(C4)**: **컷범위 유지**(cut_start/cut_end) — 세그→컷범위 매핑, 최소침습.

## 규모 요약
- **3 레포**: data-pipeline(step1·step3), service(마이그레이션 2~3), (proxmox 무관).
- **마이그레이션**: text_region/face_detection segment_id+bbox, segment_scene_meta, pipeline_state 플래그.
- **핵심 리스크**: Phase C(resolve/apply/reresolve 세그-키잉) — 다운스트림 전체 경로. 여기가 대부분의 작업·리뷰 무게.
- **재분석 필수**: segment_id 채우려면 step1부터. 배포 후 대상 웹툰 재분석 체인.
