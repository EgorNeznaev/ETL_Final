#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col
import random

spark = SparkSession.builder.appName("KafkaWriteJSON").getOrCreate()

data = []
for i in range(100000):
    data.append({
        "application_id": f"loan_{i}",
        "customer": {
            "customer_id": f"cust_{random.randint(1, 50000)}",
            "region": random.choice(["DE-HE", "FR-PAR", "UK-LON"])
        },
        "loan": {
            "amount": random.randint(1000, 50000),
            "term_months": random.choice([12, 24, 36])
        },
        "scoring": {
            "score": random.randint(300, 850),
            "risk_level": random.choice(["low", "medium", "high"])
        },
        "documents": [{"type": "passport", "status": "verified"}],
        "decision_status": random.choice(["approved", "rejected", "manual_review"]),
        "submitted_at": "2026-06-15T10:00:00Z"
    })

df = spark.createDataFrame(data)
df = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))

df.write.format("kafka") \
    .option("kafka.bootstrap.servers", "rc1a-heo36tqq2j04nng4.mdb.yandexcloud.net:9091") \
    .option("topic", "json-topic") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.scram.ScramLoginModule required '
            'username="producer" '
            'password="password";') \
    .save()

print("100 000 messages sent to Kafka")