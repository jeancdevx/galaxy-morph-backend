"""MaxViT-T model architecture for galaxy morphology classification.

Uses torchvision's MaxViT-T implementation, matching the checkpoint
trained in galaxy-morph-ml.
"""

import torch.nn as nn
from torchvision.models import maxvit_t


def GalaxyMorphMaxViT(num_classes: int = 6):
    """Return a MaxViT-T model configured for galaxy morphology classification.

    Architecture (MaxViT-T):
    - Stem: Conv3×3(stride 2) + Conv3×3
    - 4 stages of MaxViT blocks (MBConv + Window Att. + Grid Att.)
    - AdaptiveAvgPool2d → Flatten → LayerNorm → Linear(512→512) → Tanh → Linear(512→num_classes)

    Args:
        num_classes: Number of output classes (default 6).

    Returns:
        torchvision MaxVit nn.Module instance.
    """
    model = maxvit_t(num_classes=num_classes)
    # torchvision 0.19.x builds classifier[5] with bias=False, but the
    # checkpoint was trained with bias=True — patch before loading weights.
    in_features = model.classifier[3].out_features
    model.classifier[5] = nn.Linear(in_features, num_classes, bias=True)
    return model
