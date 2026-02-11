import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, max, min, first, last, 
    stddev, count, to_json, struct, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, LongType, BooleanType, TimestampType
)

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "raw_crypto_trades"
OUTPUT_TOPIC = "aggregated_crypto_stats"
CHECKPOINT_DIR = "/tmp/checkpoint/crypto_stats"

# Logging Setup
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

def main():
    # Initialize Spark Session
    # FIX 1: Changed package to 3.5.1 and Scala 2.12 to match stable PySpark
    spark = SparkSession.builder \
       .appName("CryptoAggregator_2026") \
       .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
       .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
       .config("spark.sql.shuffle.partitions", "2") \
       .getOrCreate()

    # Reduce log verbosity
    spark.sparkContext.setLogLevel("WARN")

    # FIX 2: Defined the actual schema matching your producer
    schema = StructType([
        StructField("trade_id", LongType()),
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", DoubleType()),
        StructField("timestamp", LongType()),
        StructField("is_buyer_maker", BooleanType())
    ])

    # 2. Read Stream from Kafka
    raw_stream = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
       .option("subscribe", INPUT_TOPIC) \
       .option("startingOffsets", "latest") \
       .load()

    # 3. Parse JSON & Extract Event Time
    # FIX 3: Correctly cast the input 'timestamp' (ms) to timestamp type
    trades = raw_stream.selectExpr("CAST(value AS STRING)") \
       .select(from_json(col("value"), schema).alias("data")) \
       .select("data.*") \
       .withColumn("event_time", (col("timestamp") / 1000).cast(TimestampType()))

    # 4. Windowed Aggregation (1 Minute Tumbling Window)
    # Watermark handles late data up to 1 minute
    windowed_aggs = trades \
       .withWatermark("event_time", "1 minute") \
       .groupBy(
            window(col("event_time"), "1 minute"),
            col("symbol")
        ) \
       .agg(
            first("price").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            last("price").alias("close"),
            # stddev can return null if only 1 trade exists, so we coalesce to 0.0
            expr("coalesce(stddev(price), 0.0)").alias("volatility"),
            count("trade_id").alias("trade_count")
        ) \
       .select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            col("symbol"),
            col("open"), col("high"), col("low"), col("close"),
            col("volatility"), col("trade_count")
        )

    # 5. Output Sink (Kafka)
    # Mode: "update" ensures we emit intermediate results as the window builds
    query = windowed_aggs \
       .selectExpr("to_json(struct(*)) AS value") \
       .writeStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
       .option("topic", OUTPUT_TOPIC) \
       .outputMode("update") \
       .trigger(processingTime="5 seconds") \
       .start()

    logger.warning("Spark Streaming Job Started...")
    query.awaitTermination()

if __name__ == "__main__":
    main()