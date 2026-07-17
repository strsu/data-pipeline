# qwen-base(Qwen3.5 9B Q4_K_M) 비전 Stage V 성능 평가 (2026-07-10~11)

> **목적**: 로컬 소형 비전 모델 qwen-base가 Stage V(Pass-1 컷 비전 추출)에서 glm-4.6v 수준 추론이 되는지 실측.
> **결론 요약**: 장면 이해·화자 추론은 **대등~우세**(2건은 glm이 틀리고 qwen이 정답), 속도 3~6배, 로컬 무료.
> 단 **OCR 블록 1:1 바인딩 병합 위반**(6컷 중 3컷)이 프롬프트·샘플링 어느 것으로도 안 고쳐짐(greedy에서도 발생 = 모달 출력).
> **그대로 투입 불가, 병합 감지+방어 로직(아래 §6)이 선결 조건.** 이후 fallback/배치용으로 유망.

## 1. 비교 대상 모델

| | qwen-base | glm-4.6v |
|---|---|---|
| 정체 | **Qwen3.5 9B, Q4_K_M 양자화**(로컬 서빙) | Stage V 프로덕션 기본 모델(DB `config_llm_model` vision 슬롯) |
| 접근 | `https://vllm.prup.xyz/v1/chat/completions`, model_id=`qwen-base` | 동일 게이트웨이, model_id=`glm-4.6v` |
| 비용/속도 | 로컬 사실상 무료, 컷당 8~18s, ~2k tok | 컷당 30~94s, 3~6k tok(reasoning 오버헤드) |
| params | 테스트에선 `{"temperature": 0.2}` 기준 | DB params `{"temperature": 0.7→0.2로 클램프, "context_window": 32768}` |

주의: 프로덕션 `llm_client._call_model_with_retries`는 **temperature만 전송**(top_p/top_k 미전송). 샘플링 스윕은 body 직접 구성으로 수행(§4).

## 2. 테스트 설계 (모든 실험 공통)

- **경로**: 프로덕션 코드 그대로 — `step3.extract_cut(EP_ID, cut_number, ctx=<모델별>, persist=False, webtoon_id=23)`. 동일 오버레이 이미지(F라벨)·동일 `_PASS1_SYSTEM_PROMPT`·동일 identified_faces/ocr_blocks 페이로드. persist=False라 DB 무변경. 샘플링 스윕만 `_stream_llm_once`에 body 직접 구성(동일 페이로드 재현).
- **대상**: webtoon 23(게임 속 바바리안으로 살아남기, naver/808482) **ep13(episode_id=3832)**, 컷 6개:
  - cut65: 대사만 6블록·얼굴 0 (비요른 전략 독백)
  - cut82: 4블록·얼굴 2 (에르웬 대사 — prd §20.5 근거대조 정답 있음)
  - cut84: 5블록·얼굴 1 (에르웬 3 + 상대 응답 "그/그래-")
  - cut91: 2블록·얼굴 1 (**오프패널 비요른**의 "옷을 벗겨라/에르웬" — 호명 함정)
  - cut93: 5블록·얼굴 2 ("아니 너 말고/저 모험가들 시체"=에르웬, "아"=비요른, system 2)
  - cut98: 1블록·얼굴 0 (OCR 오탈자 '포달이에요!' — 정답 '포탈이에요!' 에르웬)
- **참조(정답)**: ① prod 저장 glm 산출(analysis_text_annotation llm행 + cut_scene_meta) ② prd §20.5의 rerun_extract=True 근거대조 확정 화자.
- **채점 휴리스틱(병합 감지)** — 후속 방어 로직의 씨앗이므로 영속:
  ```python
  # corrected_text가 "다른 블록의 OCR 원문(공백 제거, 2자+)"을 포함하고
  # 자기 블록 OCR보다 유의미하게 길면 병합으로 판정
  own = ocr[idx].replace(" ", "")
  text = block["corrected_text"].replace(" ", "")
  merged = any(t in text for i, t in ocr.items() if i != idx and len(t) >= 2) \
           and len(text) > len(own) + 1
  ```

## 3. 실험 1 — 기본 비교 (glm-4.6v 라이브 vs qwen-base, 컷 6개 × 2모델)

전 콜 JSON 유효·블록 개수 정확(6/6 컷 모두 N/N). 위반은 "개수"가 아니라 "내용 배치".

| 컷 | glm-4.6v | qwen-base | 승자 |
|---|---|---|---|
| 65 | 블록 4·5를 `other`로 오분류. summary가 대사 이어붙이기 수준 | 6블록 전부 speech(참조와 일치). summary 묘사 풍부 | **qwen** |
| 82 | 완벽 — 4블록 1:1, 전부 F0/에르웬 conf 0.9 tail | **병합**: #0에 0+1, #1에 2+3 텍스트 합침, #2/#3 type=None 잔해 | **glm** |
| 84 | 5블록 전부 에르웬 conf 0.9 — "그/그래-"까지 **오귀속**(정답은 상대 응답) | #0~2 에르웬(face), #3/#4는 **화자 미정 conf 0.5로 남김** + summary에 "상대가 대답" — 정답 해석 | **qwen** |
| 91 | #0 F0 conf 0.5(낮은 확신, 무난), #1 '에르웬' 호명을 other 처리 | #0을 F0/에르웬 conf 0.8 **오귀속**(오프패널 화자 함정에 빠짐) + #1에 효과음 '퍼엉!' **날조** | **glm** |
| 93 | #0/#1을 비요른, #2를 에르웬으로 — **정답과 반대** | 화자 배정은 정답(에르웬/비요른) 그러나 **병합**+#2에 '쿠루' 날조+#4 type=None | 이해력 qwen / 형식 glm |
| 98 | '포달이에요!' 그대로(교정 없음) | **'포탈이에요!'로 교정** ✓ | **qwen** |

- 속도: qwen 9.5~17.8s vs glm 29.8~94.1s. 토큰: qwen 1.8~2.2k vs glm 3.0~6.1k.

## 4. 실험 2 — 프롬프트 강화 probe (1:1 addendum, 위반 3컷 재실행)

addendum 원문(영속 — 재실험 시 이걸 `_PASS1_SYSTEM_PROMPT`에 append):
```
[STRICT 1:1 BINDING — 최우선 규칙]
- blocks 배열 길이는 입력 ocr_blocks 길이와 정확히 같아야 한다. index도 입력과 동일.
- 여러 ocr_blocks가 한 문장이어도 절대 병합하지 말 것 — 각 index의 corrected_text는 그 index의 OCR 텍스트만 교정한 것이어야 한다(다른 블록 텍스트/효과음을 옮겨 넣지 말 것).
- 존재하지 않는 텍스트를 만들어 넣으면 안 된다. 문장 연결/의미 통합은 이후 단계가 한다.
```

| 컷 | 결과 |
|---|---|
| 93 | **완전 교정** — 5블록 1:1 + 화자 3/3 정답(conf 0.95) |
| 91 | 부분 개선(오귀속 소멸 — 화자 미정 conf 0.5) — 병합·'퍼엉!' 날조는 여전 |
| 82 | 변화 없음(병합 그대로) |

## 5. 실험 3·4 — 샘플링 스윕 + 분산 측정 (addendum 프롬프트 기준)

6조합 × 3컷(82/91/93). 채점: 병합/날조(type=None)/화자 오답·정답.

| 설정 | cut82 병합 | cut91 병합 | cut93 병합 | 화자 오답 | 비고 |
|---|---|---|---|---|---|
| t0.2 (baseline) | 2 | 1 | 0 | 2 (cut91) | |
| **t0.0 (greedy)** | 2 | 1 | 0 | **0** | 화자 최안전 |
| t0.2 + top_p0.8 | 2 | 0 | 0 | 0 | cut91에서 **16,384 토큰 폭주(447s)** 1회, json-repair 구제 |
| t0.2 + top_k20 | 2 | 1 | 0 | 0 | |
| t0.7/p0.8/k20 (Qwen 권장) | 2 | 1 | 0 | 0 | |
| t0.6/p0.95/k20 | **0** | 1 | **2** | 0 | cut82 고침·cut93 망침 → 분산 측정으로 검증 |

**분산 측정(반복 시행)** — t0.6/p0.95/k20의 cut82 성공이 재현되는지:

| 시행 | 결과(병합 수 per trial) |
|---|---|
| cut82 @ t0.6/p0.95/k20 × 5 | [2,2,2,2,2] → **clean 0/5, 스윕의 성공은 요행** |
| cut93 @ t0.6/p0.95/k20 × 5 | [2,2,0,0,0] → clean 3/5 — 고온은 멀쩡한 컷도 40% 오염 |
| cut82 @ greedy × 3 | [2,2,2] → **greedy에서도 항상 병합 = 모달 출력** |

### 확정 결론
1. **병합은 temperature/top_p/top_k 어떤 조합으로도 체계적으로 못 고침** — greedy에서도 발생하므로 샘플링 노이즈가 아니라 모델(9B Q4)의 최빈 출력.
2. 쓴다면 **t0.0(greedy) 고정**: 화자 오귀속 0, 결정론, 병합률 동일. 고온은 분산만 증가. top_p 0.8은 토큰 폭주 리스크.
3. JSON 유효율 100%(총 49콜, json-repair 구제 2회), 블록 개수는 항상 정확 — 붕괴가 아니라 "내용 재배치" 문제라 **기계 감지 가능**(§2 휴리스틱).

## 6. 후속 계획 (미착수 — 재개 anchor)

1. **병합 방어 로직**을 `extract_cut`(또는 `_sanitize_pass1`)에: §2 휴리스틱으로 감지 → ① 1회 재요청(2-shot, 로컬이라 비용 0) → ② 여전히 위반이면 해당 블록 corrected_text를 OCR 원문으로 복원(type/화자 판단은 유지).
2. 방어 탑재 후 **glm-4.6v의 fallback으로 등록**(config_llm_model self-FK) 또는 저부하 배치/재시도용. 방어 없이 등록 금지(region 오염).
3. 확장 실험 여지: 에피소드 단위(50+컷) 위반률 측정, 화자 정확도 정량화(재해소 정답셋 대비), Qwen3.5 9B 비양자화/상위 체급과 비교.

## 7. 재현 방법

- 하니스(세션 scratchpad, **휘발**): `qwen_base_pass1_test.py`(기본 비교) / `qwen_base_probe2.py`(프롬프트 probe) / `qwen_sampling_sweep.py`(스윕) / `qwen_variance_probe.py`(분산). 결과 json 동일 디렉토리. 핵심 로직은 이 문서(§2 채점, §4 addendum)에 영속돼 있어 재작성 가능.
- 실행 틀: `cd webtoon-pipeline && set -a && source ../prod.env && set +a && PYTHONPATH=. .venv/bin/python <하니스>` — `extract_cut(…, ctx={"provider":"vllm","model_id":"qwen-base","params":{"temperature":0.2},"supports_vision":True}, persist=False)`. top_p/top_k 실험은 `_stream_llm_once(endpoint, body, headers)`로 body 직접 구성(프로덕션 클라이언트는 temperature만 전송함에 유의).
