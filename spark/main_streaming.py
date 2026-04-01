"""Galaxy-Morph Spark Streaming — Main entry point.

Reads messages from galaxy.ingestion Kafka topic,
runs ResNet50 inference on each image (downloaded from R2),
and publishes results to galaxy.results topic.

Pattern: foreachBatch + mapPartitions
  → Model loaded ONCE per partition (not per row)
  → Efficient for ~100MB PyTorch model
"""

from __future__ import annotations

import json
import traceback

import boto3
from kafka import KafkaProducer

from spark_session import create_spark_session
from kafka_config import (
    KAFKA_BROKERS,
    KAFKA_INGESTION_TOPIC,
    KAFKA_RESULTS_TOPIC,
    R2_ENDPOINT,
    R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME,
    MODEL_PATH,
)


def create_r2_client():
    """Create S3 client configured for Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def download_image_from_r2(s3_client, image_key: str) -> bytes:
    """Download image bytes from R2.

    Args:
        s3_client: Boto3 S3 client.
        image_key: R2 object key (e.g., 'galaxies/uuid/image.jpg').

    Returns:
        Raw image bytes.
    """
    response = s3_client.get_object(Bucket=R2_BUCKET_NAME, Key=image_key)
    return response["Body"].read()


def process_partition(rows):
    """Process all rows in a Spark partition.

    Loads the model ONCE, then classifies each image sequentially.
    This is the mapPartitions callback.
    """
    from inference.classifier import GalaxyClassifier

    # Load model once for this partition
    classifier = GalaxyClassifier(MODEL_PATH)
    s3_client = create_r2_client()

    # Create Kafka producer for results
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for row in rows:
        try:
            # Parse ingestion payload from Kafka message value
            payload = json.loads(row["value"])
            job_id = payload["jobId"]
            image_key = payload["imageKey"]
            client_id = payload["clientId"]

            print(f"Processing: job={job_id}, image={image_key}")

            # Download image from R2
            image_bytes = download_image_from_r2(s3_client, image_key)

            # Run inference
            result = classifier.predict(image_bytes)

            # Build result payload
            result_payload = {
                "jobId": job_id,
                "clientId": client_id,
                "imageKey": image_key,
                "status": "SUCCESS",
                "classification": result,
            }

        except Exception as e:
            print(f"Error processing image {payload.get('imageKey', '?')}: {e}")
            traceback.print_exc()

            result_payload = {
                "jobId": payload.get("jobId", "unknown"),
                "clientId": payload.get("clientId", "unknown"),
                "imageKey": payload.get("imageKey", "unknown"),
                "status": "ERROR",
                "error": str(e),
            }

        # Publish result to Kafka
        producer.send(KAFKA_RESULTS_TOPIC, value=result_payload)

    # Flush pending messages
    producer.flush()
    producer.close()

    return iter([])  # mapPartitions expects an iterator return


def process_batch(batch_df, batch_id):
    """Process a micro-batch from Spark Structured Streaming.

    Uses mapPartitions so the model is loaded once per partition,
    not once per row.
    """
    if batch_df.isEmpty():
        return

    count = batch_df.count()
    print(f"\n{'='*50}")
    print(f"Batch {batch_id}: {count} message(s)")
    print(f"{'='*50}")

    # mapPartitions: each partition loads model once
    batch_df.rdd.mapPartitions(process_partition).collect()


def main():
    """Start the Spark Structured Streaming pipeline."""
    print("\n" + "=" * 60)
    print("  Galaxy-Morph Spark Streaming Pipeline")
    print("=" * 60)
    print(f"  Kafka brokers:    {KAFKA_BROKERS}")
    print(f"  Ingestion topic:  {KAFKA_INGESTION_TOPIC}")
    print(f"  Results topic:    {KAFKA_RESULTS_TOPIC}")
    print(f"  Model path:       {MODEL_PATH}")
    print(f"  R2 bucket:        {R2_BUCKET_NAME}")
    print("=" * 60 + "\n")

    # Create Spark session
    spark = create_spark_session()

    # Read stream from Kafka
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_INGESTION_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as value")
    )

    # Process with foreachBatch
    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/tmp/galaxy-morph-checkpoint")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("✓ Streaming query started — waiting for messages...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
