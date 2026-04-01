"""SparkSession factory for galaxy-morph streaming."""

from pyspark.sql import SparkSession

from kafka_config import KAFKA_BROKERS


def create_spark_session() -> SparkSession:
    """Create SparkSession with Kafka streaming support."""

    spark = (
        SparkSession.builder
        .appName("GalaxyMorph-Streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ SparkSession created (brokers: {KAFKA_BROKERS})")

    return spark
