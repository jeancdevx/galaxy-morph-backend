"""Image preprocessing transforms for inference.

Uses the same transforms as validation/test in galaxy-morph-ml:
  Resize(256) → CenterCrop(224) → ToTensor → Normalize(ImageNet)
"""

import torch
from torchvision.transforms import v2 as transforms


def get_inference_transforms(input_size: int = 224) -> transforms.Compose:
    """Get preprocessing transforms for inference (same as val transforms).

    Args:
        input_size: Target size for ResNet (224).

    Returns:
        Composed transforms pipeline.
    """
    return transforms.Compose(
        [
            transforms.Resize(input_size + 32),       # 256
            transforms.CenterCrop(input_size),          # 224
            transforms.ToImage(),
            transforms.ToDtype(torch.float32, scale=True),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225],
            ),
        ]
    )
