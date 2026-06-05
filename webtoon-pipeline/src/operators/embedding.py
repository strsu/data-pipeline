"""CLIP 이미지 인코더 기반 얼굴 임베딩 추출 (openai/clip-vit-large-patch14, 768-dim)."""
from __future__ import annotations

from io import BytesIO

import torch
from PIL import Image
from transformers import CLIPModel, CLIPProcessor

_MODEL_ID = "openai/clip-vit-large-patch14"
_model: CLIPModel | None = None
_processor: CLIPProcessor | None = None


def _get_model() -> tuple[CLIPModel, CLIPProcessor]:
    global _model, _processor
    if _model is None:
        _model = CLIPModel.from_pretrained(_MODEL_ID)
        _model.eval()
        _processor = CLIPProcessor.from_pretrained(_MODEL_ID)
    return _model, _processor


def extract_embedding(image_bytes: bytes) -> list[float]:
    """이미지 바이트 → CLIP 이미지 임베딩 (768-dim, L2 정규화된 float list)."""
    model, processor = _get_model()
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    inputs = processor(images=img, return_tensors="pt")
    with torch.no_grad():
        features = model.get_image_features(**inputs)
        # transformers 5.x: get_image_features → BaseModelOutputWithPooling
        if not isinstance(features, torch.Tensor):
            features = features.pooler_output
        # L2 정규화 — cosine distance = 1 - cosine_similarity
        features = features / features.norm(dim=-1, keepdim=True)
    return features.squeeze(0).numpy().tolist()
