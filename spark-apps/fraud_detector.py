from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, current_timestamp, to_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, IntegerType
)
from pyspark.sql.functions import window, count, sum as spark_sum, avg

# ============================================
# SPARK SESSION CONFIGURATION
# ============================================
def create_spark_session():
    """Initialize Spark with Kafka and Postgres connectors"""
    
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/spark-data/checkpoints") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ============================================
# SCHEMA DEFINITION
# ============================================
transaction_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("is_fraudulent", BooleanType(), True),
    StructField("fraud_label", StringType(), True)
])

# ============================================
# FRAUD SCORING LOGIC
# ============================================
@udf(returnType=IntegerType())
def calculate_fraud_score(amount, merchant_category, country, fraud_label):
    """Calculate fraud risk score (0-100)"""
    score = 0
    
    if amount > 5000:
        score += 40
    elif amount > 2000:
        score += 20
    elif amount > 1000:
        score += 10
    
    suspicious_categories = ["wire_transfer", "atm_withdrawal", "jewelry", "luxury", "electronics"]
    if merchant_category in suspicious_categories:
        score += 30
    
    high_risk_countries = ["RU", "CN", "NG", "BR"]
    if country in high_risk_countries:
        score += 20
    
    fraud_patterns = ["velocity_attack", "amount_anomaly", "geographic_impossible", 
                      "round_amount", "late_night"]
    if fraud_label in fraud_patterns:
        score += 50
    
    return min(score, 100)

@udf(returnType=StringType())
def classify_risk_level(fraud_score):
    """Classify transaction into risk categories"""
    if fraud_score >= 70:
        return "HIGH"
    elif fraud_score >= 40:
        return "MEDIUM"
    else:
        return "LOW"

# ============================================
# KAFKA CONSUMER
# ============================================
def read_from_kafka(spark):
    """Read streaming data from Kafka"""
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "txn_raw") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), transaction_schema).alias("data")) \
        .select("data.*")
    
    return parsed_df

# ============================================
# FRAUD SCORING PIPELINE
# ============================================
def apply_fraud_scoring(df):
    """Apply fraud detection logic to transactions"""
    from pyspark.sql.functions import to_timestamp
    
    scored_df = df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("fraud_score", 
                   calculate_fraud_score(
                       col("amount"), 
                       col("merchant_category"),
                       col("country"),
                       col("fraud_label")
                   )) \
        .withColumn("risk_level", classify_risk_level(col("fraud_score"))) \
        .withColumn("processed_at", current_timestamp())
    
    return scored_df

def detect_velocity_attacks(scored_df):
    """
    Detect velocity attacks using sliding windows
    - 5-minute sliding windows
    - 10-minute watermark for late data
    """
    
    # Add watermark to handle late-arriving data (10 minutes)
    windowed_df = scored_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),  # 5-min window, 1-min slide
            col("user_id")
        ) \
        .agg(
            count("*").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
            avg("fraud_score").alias("avg_fraud_score"),
            spark_sum(col("is_fraudulent").cast("int")).alias("fraud_count")
        )
    
    # Flag velocity attacks (>10 transactions in 5 minutes)
    velocity_alerts = windowed_df \
        .filter(col("txn_count") > 10) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("user_id"),
            col("txn_count"),
            col("total_amount"),
            col("avg_fraud_score"),
            col("fraud_count"),
            current_timestamp().alias("alert_time")
        )
    
    return velocity_alerts

# ============================================
# POSTGRES WRITER
# ============================================
def write_to_postgres(batch_df, batch_id):
    """
    Write batch to Postgres with comprehensive error handling
    - Try/catch at batch level
    - Dead letter queue for malformed records
    - Detailed error logging
    """
    
    if batch_df.isEmpty():
        print(f"⊘ Batch {batch_id}: No data to write")
        return
    
    try:
        # Validate data before writing
        valid_df = batch_df.filter(
            col("txn_id").isNotNull() &
            col("user_id").isNotNull() &
            col("amount").isNotNull() &
            col("timestamp").isNotNull()
        )
        
        invalid_df = batch_df.subtract(valid_df)
        
        # Send invalid records to DLQ
        if not invalid_df.isEmpty():
            send_to_dlq(invalid_df, batch_id, "validation_failed")
            print(f"⚠️  Batch {batch_id}: Sent {invalid_df.count()} invalid records to DLQ")
        
        # Attempt to write valid records
        if not valid_df.isEmpty():
            postgres_df = valid_df.select(
                "txn_id",
                "user_id",
                "amount",
                "currency",
                "merchant",
                "merchant_category",
                to_timestamp(col("timestamp")).alias("timestamp"),
                "country",
                "device_id",
                "ip_address",
                "fraud_score",
                "risk_level",
                "is_fraudulent",
                "fraud_label"
            )
            
            postgres_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/appdb") \
                .option("dbtable", "scored_transactions") \
                .option("user", "admin") \
                .option("password", "admin") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            print(f"✓ Batch {batch_id}: Wrote {postgres_df.count()} transactions to Postgres")
    
    except Exception as e:
        # Batch-level error handling
        error_msg = str(e)
        print(f"❌ Batch {batch_id}: ERROR - {error_msg}")
        
        # Send entire batch to DLQ for manual investigation
        send_to_dlq(batch_df, batch_id, f"batch_error: {error_msg}")
        
        # Log to error table
        log_batch_error(batch_id, error_msg, batch_df.count())
        
        # Don't raise - continue processing next batch
        print(f"⚠️  Batch {batch_id}: Sent to DLQ, continuing with next batch")

# ============================================
# MAIN STREAMING APPLICATION
# ============================================
def main():
    spark = create_spark_session()
    
    # Read from Kafka
    raw_df = read_from_kafka(spark)
    
    # Apply fraud scoring
    scored_df = apply_fraud_scoring(raw_df)
    
    # Detect velocity attacks
    velocity_alerts = detect_velocity_attacks(scored_df)
    
    # Write scored transactions to Postgres
    query1 = scored_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Write velocity alerts to Postgres
    query2 = velocity_alerts.writeStream \
        .foreachBatch(write_velocity_alerts_to_postgres) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    query1.awaitTermination()
    query2.awaitTermination()

def write_velocity_alerts_to_postgres(batch_df, batch_id):
    """Write velocity alerts to Postgres"""
    if batch_df.isEmpty():
        return
    
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/appdb") \
        .option("dbtable", "velocity_alerts") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    print(f"✓ Batch {batch_id}: Wrote {batch_df.count()} velocity alerts")

def send_to_dlq(df, batch_id, error_reason):
    """Send failed records to Dead Letter Queue (Kafka topic)"""
    from pyspark.sql.functions import to_json, struct, lit
    
    dlq_df = df.select(
        to_json(struct("*")).alias("value")
    ).withColumn("batch_id", lit(batch_id)) \
     .withColumn("error_reason", lit(error_reason)) \
     .withColumn("dlq_timestamp", current_timestamp())
    
    try:
        dlq_df.select("value").write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", "txn_dlq") \
            .save()
    except Exception as e:
        print(f"Failed to send to DLQ: {e}")

def log_batch_error(batch_id, error_msg, record_count):
    """Log batch errors to Postgres for monitoring"""
    from pyspark.sql import SparkSession
    from datetime import datetime
    
    spark = SparkSession.builder.getOrCreate()
    
    error_data = [(batch_id, error_msg, record_count, datetime.now())]
    error_df = spark.createDataFrame(
        error_data,
        ["batch_id", "error_message", "record_count", "error_time"]
    )
    
    try:
        error_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/appdb") \
            .option("dbtable", "batch_errors") \
            .option("user", "admin") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Failed to log error: {e}")

if __name__ == "__main__":
    main()