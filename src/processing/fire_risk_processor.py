from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType

# 1. CONFIGURACIÓN DE LA SESIÓN SPARK
# ------------------------------------------------
spark = SparkSession.builder \
    .appName("EcoPulse-FireRisk") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🚀 Spark Session Iniciada correctamente")

# 2. DEFINICIÓN DE ESQUEMAS
# ------------------------------------------------
fire_schema = StructType([
    StructField("source", StringType()),
    StructField("region", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("temp_k", DoubleType()),
    StructField("confidence", StringType()),
    StructField("timestamp", DoubleType())
])

weather_schema = StructType([
    StructField("source", StringType()),
    StructField("location_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_deg", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("timestamp", DoubleType())
])

# 3. LECTURA DE STREAMS (KAFKA LOCALHOST)
# ------------------------------------------------
def read_kafka_stream(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

print("📡 Conectando a Kafka Streams (Localhost)...")
fire_df = read_kafka_stream("fire-events", fire_schema)
weather_df = read_kafka_stream("weather-events", weather_schema)

# 4. PROCESAMIENTO
# ------------------------------------------------
fire_processed = fire_df.withColumn("processed_at", current_timestamp())
weather_processed = weather_df.withColumn("processed_at", current_timestamp())

# 5. ESCRITURA EN DELTA LAKE (SINK)
# ------------------------------------------------
print(" Configurando escrituras en MinIO...")

# Query para Incendios
query_fire = fire_processed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/fire") \
    .option("path", "s3a://lakehouse/silver/fire_events") \
    .start()

# Query para Clima
query_weather = weather_processed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/weather") \
    .option("path", "s3a://lakehouse/silver/weather_events") \
    .start()

print("✅ Streams iniciados. Escribiendo en MinIO... (No cierres esta terminal)")
spark.streams.awaitAnyTermination()