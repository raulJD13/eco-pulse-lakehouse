from pyspark.sql import SparkSession

# Usamos la misma configuración que en el processor para conectar a MinIO local
spark = SparkSession.builder \
    .appName("EcoPulse-Verifier") \
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

print("\n INSPECCIONANDO CAPA SILVER (INCENDIOS)...")
try:
    # Leemos en formato DELTA (¡Viajamos en el tiempo!)
    fire_df = spark.read.format("delta").load("s3a://lakehouse/silver/fire_events")
    
    count = fire_df.count()
    print(f" Total de registros de incendio encontrados: {count}")
    
    if count > 0:
        print("Muestra de los últimos datos:")
        fire_df.orderBy("timestamp", ascending=False).show(5, truncate=False)
        
except Exception as e:
    print(f" Error leyendo incendios (quizás aún no hay datos escritos): {e}")

print("\n🔍 INSPECCIONANDO CAPA SILVER (CLIMA)...")
try:
    weather_df = spark.read.format("delta").load("s3a://lakehouse/silver/weather_events")
    
    count = weather_df.count()
    print(f" Total de registros de clima encontrados: {count}")
    
    if count > 0:
        print("Muestra de los últimos datos:")
        weather_df.orderBy("timestamp", ascending=False).show(5, truncate=False)

except Exception as e:
    print(f"Error leyendo clima: {e}")

print("\n✅ Verificación completada.")