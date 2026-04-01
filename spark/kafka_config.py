"""Kafka and Spark configuration for galaxy-morph streaming."""

import os

# Kafka config
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
KAFKA_INGESTION_TOPIC = os.environ.get("KAFKA_INGESTION_TOPIC", "galaxy.ingestion")
KAFKA_RESULTS_TOPIC = os.environ.get("KAFKA_RESULTS_TOPIC", "galaxy.results")

# R2 / S3-compatible config
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "galaxy-morph")

# Model config
MODEL_PATH = os.environ.get("MODEL_PATH", "/opt/spark/models/best_model.pt")
NUM_CLASSES = 5
CLASS_NAMES = ["Elliptical", "Spiral", "Barred_Spiral", "Edge_on", "Irregular_Merger"]
INPUT_SIZE = 224
