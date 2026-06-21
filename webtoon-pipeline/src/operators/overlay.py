"""현재 컷(N)에 얼굴 bbox + ID 라벨 오버레이 (Step3 멀티모달 보조).

GLM이 말풍선 꼬리 ↔ 얼굴을 시각적으로 매칭하도록, 현재 컷에 bbox와 'F{idx}' 라벨을
박은 이미지를 추가 전달한다. 라벨은 ASCII(F0/F1…)로 — CJK 폰트 의존 회피.
'F0=카락' 같은 이름 매핑은 프롬프트 텍스트로 따로 전달한다.
PIL만 사용(코어가 opencv 비의존).
"""
from __future__ import annotations

from io import BytesIO

from PIL import Image, ImageDraw, ImageFont

_BOX_COLOR = (255, 0, 0)       # 빨강 bbox
_LABEL_BG = (255, 255, 255)    # 흰 배경
_LABEL_FG = (0, 0, 0)          # 검은 글씨


def _font():
    try:
        return ImageFont.load_default()
    except Exception:
        return None


def overlay_faces(image_bytes: bytes, faces: list[dict]) -> bytes:
    """faces: [{"id": "F0", "bbox": [x1,y1,x2,y2]}]. 반환: 오버레이된 JPEG bytes."""
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    draw = ImageDraw.Draw(img)
    font = _font()

    for face in faces:
        x1, y1, x2, y2 = [int(v) for v in face["bbox"]]
        label = str(face.get("id", ""))
        draw.rectangle([x1, y1, x2, y2], outline=_BOX_COLOR, width=3)
        if not label:
            continue
        # 라벨 배경 박스 크기 계산
        try:
            tb = draw.textbbox((0, 0), label, font=font)
            tw, th = tb[2] - tb[0], tb[3] - tb[1]
        except Exception:
            tw, th = 7 * len(label), 11
        ly2 = max(0, y1)
        ly1 = max(0, ly2 - th - 4)
        draw.rectangle([x1, ly1, x1 + tw + 6, ly1 + th + 4], fill=_LABEL_BG)
        draw.text((x1 + 3, ly1 + 2), label, fill=_LABEL_FG, font=font)

    buf = BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue()
