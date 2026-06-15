#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

json_schema = StructType([
    StructField("application_id", StringType()),
    StructField("customer", StructType([
        StructField("customer_id", StringType()),
        StructField("region", StringType())
    ])),
    StructField("loan", StructType([
        StructField("amount", IntegerType()),
        StructField("term_months", IntegerType())
    ])),
    StructField("scoring", StructType([
        StructField("score", IntegerType()),
        StructField("risk_level", StringType())
    ])),
    StructField("documents", ArrayType(StructType([
        StructField("type", StringType()),
        StructField("status", StringType())
    ]))),
    StructField("decision_status", StringType()),
    StructField("submitted_at", StringType())
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "rc1a-heo36tqq2j04nng4.mdb.yandexcloud.net:9091") \
    .option("subscribe", "json-topic") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.scram.ScramLoginModule required '
            'username="producer" '
            'password="password";') \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_raw.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select(
    col("data.application_id"),
    col("data.customer.customer_id"),
    col("data.customer.region"),
    col("data.loan.amount").alias("loan_amount"),
    col("data.loan.term_months").alias("loan_term_months"),
    col("data.scoring.score").alias("credit_score"),
    col("data.scoring.risk_level"),
    col("data.documents")[0]["type"].alias("document_type"),
    col("data.documents")[0]["status"].alias("document_status"),
    col("data.decision_status"),
    to_timestamp(col("data.submitted_at")).alias("submitted_at")
)

query = df_parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "s3a://airflow-bucket-1/kafka_output_csv/") \
    .option("checkpointLocation", "s3a://airflow-bucket-1/checkpoints/") \
    .option("header", "true") \
    .trigger(once=True) \
    .start()

query.awaitTermination()

print("Data saved to CSV")