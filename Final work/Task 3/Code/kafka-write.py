#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

    KAFKA_BOOTSTRAP_SERVERS = "rc1d-5d4v2qhsl5je7sra.mdb.yandexcloud.net:9091"
    KAFKA_TOPIC = "dataproc-kafka-topic"
    KAFKA_USERNAME = "user1"
    KAFKA_PASSWORD = "password1"
    json_file_path = "s3a://dz1/Datasets/Dataset.json"

    df = spark.read.option("multiline", "false").json(json_file_path)
    df_kafka = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))
    
    df_kafka.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                f"username='{KAFKA_USERNAME}' "
                f"password='{KAFKA_PASSWORD}';") \
        .save()
    
    spark.stop()

if __name__ == "__main__":
    main()