"""Image preprocessing transforms for inference.

Uses the same transforms as validation/test in galaxy-morph-ml:
  CenterCrop(320) → Resize(224) → ToTensor → Normalize(ImageNet)

Original images are 424×424 px. CenterCrop(320) removes the ~52 px
black border per side before scaling to 224 px.
"""

import torch
from torchvision.transforms import v2 as transforms

_CROP_SIZE = 320  # matches training: removes black border from 424×424 images


def get_inference_transforms(input_size: int = 224) -> transforms.Compose:
    """Get preprocessing transforms for inference (same as val/test transforms).

    Args:
        input_size: Target size for the model input (224).

    Returns:
        Composed transforms pipeline.
    """
    return transforms.Compose(
        [
            transforms.CenterCrop(_CROP_SIZE),          # 424×424 → 320×320
            transforms.Resize(input_size),               # 320×320 → 224×224
            transforms.ToImage(),
            transforms.ToDtype(torch.float32, scale=True),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225],
            ),
        ]
    )
