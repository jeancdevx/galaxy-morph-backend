"""ResNet50 model architecture for galaxy morphology classification.

This is a copy of the model from galaxy-morph-ml/src/models/resnet.py.
Spark workers need their own copy since they don't have access to the
ML project's source tree.
"""

import torch
import torch.nn as nn
from torchvision.models import resnet50, ResNet50_Weights


class GalaxyMorphResNet50(nn.Module):
    """ResNet50 backbone with custom head for 5-class galaxy morphology.

    Architecture:
    - ResNet50 pretrained backbone
    - Global Average Pooling
    - Dropout(0.5) → Linear(2048, num_classes)
    """

    def __init__(
        self,
        num_classes: int = 5,
        pretrained: bool = False,
        dropout: float = 0.5,
    ):
        super().__init__()

        self.num_classes = num_classes

        # Load ResNet50 backbone
        if pretrained:
            weights = ResNet50_Weights.IMAGENET1K_V2
            self.backbone = resnet50(weights=weights)
        else:
            self.backbone = resnet50(weights=None)

        # Get number of features from backbone
        num_features = self.backbone.fc.in_features

        # Replace final layer
        self.backbone.fc = nn.Identity()

        # Custom classification head
        self.head = nn.Sequential(
            nn.Dropout(dropout),
            nn.Linear(num_features, num_classes),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        backbone_out = self.backbone(x)
        logits = self.head(backbone_out)
        return logits
