"""ResNet50 얼굴 임베딩 추출 (P0 — P2~P3에서 애니 특화 모델로 교체 예정, §12.4)."""
from __future__ import annotations

from io import BytesIO

import torch
import torchvision.models as models
import torchvision.transforms as transforms
from PIL import Image

_model: models.ResNet | None = None
_transform: transforms.Compose | None = None


def _get_model() -> tuple[models.ResNet, transforms.Compose]:
    global _model, _transform
    if _model is None:
        m = models.resnet50(weights=models.ResNet50_Weights.IMAGENET1K_V1)
        m.fc = torch.nn.Identity()  # 2048-dim 임베딩 직접 출력
        m.eval()
        _model = m
        _transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ])
    return _model, _transform


def extract_embedding(image_bytes: bytes) -> list[float]:
    """이미지 바이트 → ResNet50 임베딩 (2048-dim float list)."""
    model, transform = _get_model()
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    tensor = transform(img).unsqueeze(0)
    with torch.no_grad():
        embedding = model(tensor).squeeze(0).numpy()
    return embedding.tolist()
