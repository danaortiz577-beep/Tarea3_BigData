# -*- coding: utf-8 -*-
"""
Consumidor de Spark Structured Streaming

Lee los mensajes del topic "temperatura_min" de Kafka y calcula, por ventana
de 1 minuto y por departamento: numero de lecturas, temperatura promedio,
temperatura minima y numero de lecturas de helada (<= 0 C).

Ejecucion (con el productor ya corriendo):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        spark_streaming_tmin.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, TimestampType)

spark = (SparkSession.builder.appName("StreamingTemperaturaMin")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Esquema de los mensajes JSON enviados por el productor
schema = StructType([
    StructField("codigo_estacion", StringType()),
    StructField("estacion", StringType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType()),
    StructField("temperatura", DoubleType()),
    StructField("fecha_original", StringType()),
    StructField("timestamp", TimestampType())
])

# Lectura en streaming desde Kafka
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "temperatura_min")
      .load())

# Parseo del JSON
datos = (df.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
           .select("d.*"))

# Estadisticas por ventana de 1 minuto y departamento
stats = (datos.groupBy(F.window("timestamp", "1 minute"), "departamento")
         .agg(F.count("*").alias("lecturas"),
              F.round(F.avg("temperatura"), 2).alias("temp_promedio"),
              F.round(F.min("temperatura"), 1).alias("temp_min"),
              F.sum(F.when(F.col("temperatura") <= 0, 1).otherwise(0))
              .alias("heladas"))
         .select(F.col("window.start").alias("minuto"), "departamento",
                 "lecturas", "temp_promedio", "temp_min", "heladas")
         .orderBy(F.col("minuto").desc(), F.col("lecturas").desc()))

# Salida en consola (modo complete: se reimprime la tabla en cada batch)
query = (stats.writeStream.outputMode("complete").format("console")
         .option("truncate", "false").option("numRows", 15).start())

query.awaitTermination()
