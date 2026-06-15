from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.appName("Fraud Analysis Task").getOrCreate()

columns = [
    "transaction_id", "customer_id", "transaction_date", "transaction_time",
    "hour_of_day", "is_weekend", "is_night_transaction", "country", "city",
    "merchant_category", "payment_method", "device_type", "customer_age",
    "credit_score", "account_age_years", "account_balance", "transaction_amount",
    "num_prev_transactions", "transaction_freq_monthly", "distance_from_home_km",
    "time_since_last_txn_hrs", "is_international", "failed_attempts",
    "pin_changed_recently", "is_fraud", "fraud_type"
]
print("Чтение данных")
df = spark.read \
    .option("header", "false") \
    .option("delimiter", ",") \
    .csv("s3a://finaalobject/2026/06/14/fraud_transactions/*.csv")
for i, col_name in enumerate(columns):
    df = df.withColumnRenamed(f"_c{i}", col_name)
df = df.withColumn("customer_age", col("customer_age").cast(IntegerType()))
df = df.withColumn("credit_score", col("credit_score").cast(IntegerType()))
df = df.withColumn("account_balance", col("account_balance").cast(DoubleType()))
df = df.withColumn("transaction_amount", col("transaction_amount").cast(DoubleType()))
df = df.withColumn("is_fraud", col("is_fraud").cast(IntegerType()))
df = df.withColumn("hour_of_day", col("hour_of_day").cast(IntegerType()))
total_count = df.count()
print(f"Всего транзакций: {total_count}")
fraud_by_country = df.filter(col("is_fraud") == 1) \
    .groupBy("country") \
    .agg(
        count("transaction_id").alias("fraud_count"),
        avg("transaction_amount").alias("avg_fraud_amount")
    ) \
    .orderBy(col("fraud_count").desc())
print("Топ стран по мошенничеству:")
fraud_by_country.show(10)
fraud_by_country.write.mode("overwrite").parquet("s3a://output-bucket-1/fraud_by_country")
spark.stop()