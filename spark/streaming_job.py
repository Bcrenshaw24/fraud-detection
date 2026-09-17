import os 
from pyspark.sql import SparkSession 
import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql.functions import ( 
    col, from_json, window, count, sum as _sum, avg, 
    countDistinct, last, to_timestamp
)
from pyspark.sql.types import ( 
    StructType, StructField, StringType, DoubleType
)

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
KAFKA_TOPIC = "financial_transactions"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ml_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ml_password")
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"

spark = (SparkSession.builder
        .appName("FraudDetectionFeatureEnginer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
        .getOrCreate() 
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([ 
    StructField("transaction_id", StringType(), True), 
    StructField("user_id", StringType(), True), 
    StructField("amount", DoubleType(), True), 
    StructField("currency", StringType(), True), 
    StructField("merchant_category", StringType(), True), 
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

raw_stream = (spark.readStream 
              .format("kafka") 
              .option("kafka.bootstrap.servers", KAFKA_SERVER)
              .option("subscribe", KAFKA_TOPIC) 
              .option("startingOffsets", "latest")
              .load())
parsed_stream = (raw_stream 
                 .selectExpr("CAST(value AS STRING) as json_payload") 
                 .select(from_json(col("json_payload"), schema).alias("data"))
                 .select("data.*")
                 .withColumn("event_time", to_timestamp(col("timestamp")))
                 .withWatermark("event_time", "10 minutes"))

features_df = (parsed_stream
               .groupBy( 
                   window(col("event_time"), "5 minutes", "1 minute"),
                   col("user_id")
               )
               .agg( 
                   count("transaction_id").alias("tx_count_5m"), 
                   _sum("amount").alias("total_amount_5m"), 
                   avg("amount").alias("avg_amount_24h"), 
                   countDistinct("merchant_category").alias("unique_categories_1h"), 
                   last("latitude").alias("last_latitude"), 
                   last("longitude").alias("last_longitude"), 
                   last("event_time").alias("last_tx_timestamp")
               ))
def process_batch(batch_df, batch_id): 
    if batch_df.isEmpty(): 
        return 

    rows = batch_df.collect()

    conn = psycopg2.connect( 
        host=POSTGRES_HOST, 
        database=POSTGRES_DB, 
        user=POSTGRES_USER, 
        password=POSTGRES_PASSWORD, 
        port=5432
    )
    cursor = conn.cursor() 

    query = """
    INSERT INTO feature_store.user_features ( 
        user_id, tx_count_5m, total_amount_5m, avg_amount_24h, 
        unique_categories_1h, last_latitude, last_longitude, last_tx_timestamp
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE SET 
    tx_count_5m = EXCLUDED.tx_count_5m,
    total_amount_5m = EXCLUDED.total_amount_5m,
    avg_amount_24h = EXCLUDED.avg_amount_24h,
    unique_categories_1h = EXCLUDED.unique_categories_1h,
    last_latitude = EXCLUDED.last_latitude,
    last_longitude = EXCLUDED.last_longitude,
    last_tx_timestamp = EXCLUDED.last_tx_timestamp;
    """

    data_to_insert = [
        (
            r["user_id"],
            int(r["tx_count_5m"]),
            float(r["total_amount_5m"] or 0.0),
            float(r["avg_amount_24h"] or 0.0),
            int(r["unique_categories_1h"]),
            float(r["last_latitude"]) if r["last_latitude"] else None,
            float(r["last_longitude"]) if r["last_longitude"] else None,
            r["last_tx_timestamp"]
        )
        for r in rows
    ]
    try: 
        execute_batch(cursor, query, data_to_insert)
        conn.commit()
        print(f"Batch {batch_id}: Successfully upserted {len(data_to_insert)} user features.")
    except Exception as e: 
        conn.rollback()
        print(f"Batch {batch_id} failed with error: {e}")
    finally: 
        cursor.close() 
        conn.close()

query = (features_df.writeStream
         .outputMode("update")
         .foreachBatch(process_batch)
         .option("checkpointLocation", "/tmp/spark_checkpoints_fraud")
         .start())
query.awaitTermination()

