import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sqrt, row_number
from pyspark.sql.window import Window

# 1. CONFIGURACIÓN SPARK (Se inicia una sola vez)
print("🚀 Iniciando Spark Session para Gold Layer Service...")
spark = SparkSession.builder \
    .appName("EcoPulse-GoldRisk-Service") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def run_gold_processing():
    """Lógica de procesamiento encapsulada"""
    try:
        print(f"\n🔄 Ejecutando ciclo de procesamiento: {time.strftime('%H:%M:%S')}")
        
        # LEER SILVER
        fire_df = spark.read.format("delta").load("s3a://lakehouse/silver/fire_events")
        weather_df = spark.read.format("delta").load("s3a://lakehouse/silver/weather_events")

        # LOGICA DE NEGOCIO (Igual que antes)
        window_spec = Window.partitionBy("location_id").orderBy(col("timestamp").desc())

        latest_weather = weather_df.withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") == 1) \
            .select(
                col("location_id").alias("weather_station"),
                col("lat").alias("station_lat"),
                col("lon").alias("station_lon"),
                col("wind_speed"),
                col("temperature"),
                col("humidity")
            )

        active_fires = fire_df.select(
            col("lat").alias("fire_lat"),
            col("lon").alias("fire_lon"),
            col("confidence"),
            col("timestamp")
        )

        joined_df = active_fires.crossJoin(latest_weather)

        distance_expr = sqrt(
            (col("fire_lat") - col("station_lat")) ** 2 +
            (col("fire_lon") - col("station_lon")) ** 2
        )

        # Filtro de distancia (Ajustado para demo)
        nearby_risks = joined_df.withColumn("distance_deg", distance_expr) \
            .filter(col("distance_deg") < 20.0)

        gold_risk_df = nearby_risks.withColumn(
            "risk_level",
            when(
                (col("confidence") == "h") & (col("wind_speed") >= 30) & (col("temperature") >= 303.15) & (col("humidity") <= 30),
                "EXTREME"
            ).when(
                (col("confidence") == "h") & (col("wind_speed") >= 30), "VERY_HIGH"
            ).when(
                (col("confidence") == "h") & (col("wind_speed") >= 20), "HIGH"
            ).when(
                (col("confidence") == "h"), "MODERATE"
            ).otherwise("LOW")
        ).select(
            "timestamp", "fire_lat", "fire_lon", "weather_station", 
            "wind_speed", "temperature", "humidity", "risk_level", "distance_deg"
        )

        # GUARDAR
        count = gold_risk_df.count()
        if count > 0:
            gold_risk_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save("s3a://lakehouse/gold/fire_risk_alerts")
            print(f"✅ Alertas actualizadas: {count}")
        else:
            print("⚠️ No se encontraron riesgos en este ciclo.")

    except Exception as e:
        print(f"❌ Error en el ciclo: {e}")

# BUCLE INFINITO (Simulando un Scheduler como Airflow)
if __name__ == "__main__":
    try:
        while True:
            run_gold_processing()
            print("💤 Durmiendo 60 segundos...")
            time.sleep(60)
    except KeyboardInterrupt:
        print("🛑 Servicio detenido.")
        spark.stop()