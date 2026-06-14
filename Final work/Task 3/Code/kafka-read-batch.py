#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-read-batch-app").getOrCreate()

    KAFKA_BOOTSTRAP_SERVERS = "rc1d-5d4v2qhsl5je7sra.mdb.yandexcloud.net:9091"
    KAFKA_TOPIC = "dataproc-kafka-topic"
    KAFKA_USERNAME = "user1"
    KAFKA_PASSWORD = "password1"
    OUTPUT_PATH = "s3a://dz1/Output"

    json_schema = StructType([
        StructField("application_id", StringType(), True),
        StructField("customer", StructType([
            StructField("customer_id", StringType(), True),
            StructField("region", StringType(), True)
        ]), True),
        StructField("loan", StructType([
            StructField("amount", IntegerType(), True),
            StructField("term_months", IntegerType(), True)
        ]), True),
        StructField("scoring", StructType([
            StructField("score", IntegerType(), True),
            StructField("risk_level", StringType(), True)
        ]), True),
        StructField("decision_status", StringType(), True),
        StructField("submitted_at", StringType(), True)
    ])

    df_raw = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                f"username='{KAFKA_USERNAME}' "
                f"password='{KAFKA_PASSWORD}';") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("cast(value as string) as raw_value") \
        .where(col("raw_value").isNotNull())

    df_parsed = df_raw.withColumn("data", from_json(col("raw_value"), json_schema))

    df_flatten = df_parsed.select(
        col("data.application_id"),
        col("data.customer.customer_id"),
        col("data.customer.region"),
        col("data.loan.amount"),
        col("data.loan.term_months"),
        col("data.scoring.score"),
        col("data.scoring.risk_level"),
        col("data.decision_status"),
        col("data.submitted_at")
    )

    df_flatten.write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)
    spark.stop()


if __name__ == "__main__":
    main()