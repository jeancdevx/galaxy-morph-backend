"""Galaxy morphology classifier for Spark inference.

Loads the trained GalaxyMorphResNet50 checkpoint and runs inference
on galaxy images downloaded from R2.
"""

from __future__ import annotations

import io
from typing import Dict

import torch
import torch.nn.functional as F
from PIL import Image

from inference.model import GalaxyMorphMaxViT
from inference.transforms import get_inference_transforms

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_config import CLASS_NAMES, INPUT_SIZE, NUM_CLASSES


class GalaxyClassifier:
    """Loads a trained model and performs galaxy morphology classification.

    This class is designed to be instantiated ONCE per Spark partition
    via mapPartitions, avoiding repeated model loading.
    """

    def __init__(self, model_path: str):
        """Initialize classifier with trained model checkpoint.

        Args:
            model_path: Path to best_model.pt checkpoint file.
        """
        self.device = torch.device("cpu")
        self.class_names = CLASS_NAMES
        self.transform = get_inference_transforms(INPUT_SIZE)

        # Create model architecture (same as training)
        self.model = GalaxyMorphMaxViT(num_classes=NUM_CLASSES)

        # Load checkpoint — supports both full training state and bare state_dict
        checkpoint = torch.load(model_path, map_location=self.device, weights_only=False)
        if isinstance(checkpoint, dict) and "model_state_dict" in checkpoint:
            state_dict = checkpoint["model_state_dict"]
        else:
            state_dict = checkpoint

        cleaned_state_dict = {}
        for key, value in state_dict.items():
            new_key = key.replace("module.", "", 1) if key.startswith("module.") else key
            cleaned_state_dict[new_key] = value

        self.model.load_state_dict(cleaned_state_dict)
        self.model.to(self.device)
        self.model.eval()

        print(f"✓ GalaxyClassifier loaded from {model_path}")

    def predict(self, image_bytes: bytes) -> Dict:
        """Run inference on a single image.

        Args:
            image_bytes: Raw image file bytes (JPEG/PNG).

        Returns:
            Dictionary with:
                - predictedClass: str (e.g., 'Spiral')
                - confidence: float (0-1)
                - probabilities: dict mapping class name → probability
        """
        # Load and preprocess image
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        tensor = self.transform(image).unsqueeze(0).to(self.device)

        # Inference
        with torch.no_grad():
            logits = self.model(tensor)
            probabilities = F.softmax(logits, dim=1).squeeze(0)

        # Extract results
        confidence, predicted_idx = probabilities.max(0)
        predicted_class = self.class_names[predicted_idx.item()]

        probs_dict = {
            name: round(probabilities[i].item(), 6)
            for i, name in enumerate(self.class_names)
        }

        return {
            "predictedClass": predicted_class,
            "confidence": round(confidence.item(), 6),
            "probabilities": probs_dict,
        }
