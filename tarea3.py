# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data 
Analisis batch con Apache Spark: temperatura minima del aire (IDEAM).

Fuente de datos: datos.gov.co - "Cantidad de datos temperatura minima".
El CSV se lee desde HDFS. Ajusta RUTA_HDFS si tu archivo tiene otro nombre.

Ejecucion:
    python3 tarea3.py
"""
import csv
from pyspark.sql import SparkSession, functions as F

# ------------------------------------------------------------
# Parametros
# ------------------------------------------------------------
RUTA_HDFS = "hdfs://localhost:9000/Tarea3/query.csv.1"
LIM_MIN, LIM_MAX = -20, 40   # rango fisico razonable de temperatura minima (C)

# ------------------------------------------------------------
# 0. SESION DE SPARK
# ------------------------------------------------------------
# 8 particiones de shuffle: mas rapido en una VM pequena
spark = (SparkSession.builder.appName("Tarea3")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# ------------------------------------------------------------
# 1. CARGA DESDE HDFS
# ------------------------------------------------------------
raw = (spark.read.format("csv")
       .option("header", "true")
       .load(RUTA_HDFS))

print("\n=== 1. ESQUEMA Y MUESTRA ORIGINAL ===")
raw.printSchema()
raw.select("CodigoEstacion", "FechaObservacion", "ValorObservado",
           "Departamento", "Municipio").show(5, truncate=False)
print("Total de registros cargados:", raw.count())

# ------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACION
# ------------------------------------------------------------
v_txt = F.col("ValorObservado").cast("string")
valor = F.regexp_replace(v_txt, ",", ".").cast("double")
f = F.col("FechaObservacion")
fecha = F.coalesce(
    F.to_timestamp(f, "yyyy MMM dd hh:mm:ss a"),
    F.to_timestamp(f, "yyyy MMM dd HH:mm:ss"),
    F.to_timestamp(f, "MM/dd/yyyy hh:mm:ss a"),
    F.to_timestamp(f, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
    F.to_timestamp(f, "yyyy-MM-dd HH:mm:ss"))

df = raw.select("CodigoEstacion", "NombreEstacion",
                "Departamento", "Municipio",
                valor.alias("valor"), fecha.alias("fecha"),
                f.alias("fecha_texto"))

total = df.count()
sin_fecha = df.filter(F.col("fecha").isNull()).count()
sin_valor = df.filter(F.col("valor").isNull()).count()
print("\n=== 2. LIMPIEZA ===")
print("Sin fecha valida:", sin_fecha, "| sin valor:", sin_valor)

if sin_fecha == total:
    print("ATENCION: no se pudo leer ninguna fecha. Ejemplos:")
    df.select("fecha_texto").show(5, truncate=False)
    spark.stop()
    raise SystemExit

print("Estadisticas ANTES de limpiar atipicos:")
df.select("valor").summary().show()

limpio = (df.dropDuplicates()
          .dropna(subset=["valor", "fecha", "Departamento"])
          .filter(F.col("valor").between(LIM_MIN, LIM_MAX))
          .withColumn("anio", F.year("fecha"))
          .withColumn("mes", F.month("fecha"))
          .cache())

n_limpio = limpio.count()
print("Registros validos tras limpiar:", n_limpio)
print("Registros descartados:", total - n_limpio)

# ------------------------------------------------------------
# 3. ANALISIS EXPLORATORIO (EDA) CON DATAFRAMES
# ------------------------------------------------------------
print("\n=== 3.1 ESTADISTICAS DESCRIPTIVAS (datos limpios) ===")
limpio.select("valor").summary().show()

print("=== 3.2 RESUMEN POR DEPARTAMENTO ===")
por_dep = (limpio.groupBy("Departamento")
           .agg(F.count("*").alias("registros"),
                F.round(F.avg("valor"), 2).alias("promedio"),
                F.round(F.min("valor"), 1).alias("minimo"),
                F.round(F.max("valor"), 1).alias("maximo")))
por_dep.orderBy(F.desc("registros")).show(15, truncate=False)

print("=== 3.3 PROMEDIO POR ANIO ===")
por_anio = (limpio.groupBy("anio")
            .agg(F.round(F.avg("valor"), 2).alias("promedio"),
                 F.count("*").alias("registros"))
            .orderBy("anio"))
por_anio.show(50, truncate=False)

print("=== 3.4 PROMEDIO POR MES ===")
por_mes = (limpio.groupBy("mes")
           .agg(F.round(F.avg("valor"), 2).alias("promedio"))
           .orderBy("mes"))
por_mes.show(12, truncate=False)

print("=== 3.5 DIEZ ESTACIONES MAS FRIAS (min. 100 registros) ===")
(limpio.groupBy("NombreEstacion", "Departamento")
 .agg(F.count("*").alias("registros"),
      F.round(F.avg("valor"), 2).alias("promedio"))
 .filter(F.col("registros") >= 100)
 .orderBy("promedio")
 .show(10, truncate=False))

print("=== 3.6 HELADAS (temperatura minima <= 0 C) ===")
heladas = limpio.filter(F.col("valor") <= 0)
print("Total de registros con helada:", heladas.count())
heladas_dep = (heladas.groupBy("Departamento")
               .agg(F.count("*").alias("heladas"))
               .orderBy(F.desc("heladas")))
heladas_dep.show(10, truncate=False)

# ------------------------------------------------------------
# 4. EJEMPLO CON RDD (map + reduceByKey + take)
# ------------------------------------------------------------
print("\n=== 4. CONTEO POR DEPARTAMENTO CON RDD ===")
conteo_rdd = (limpio.select("Departamento").rdd
              .map(lambda r: (r[0], 1))
              .reduceByKey(lambda a, b: a + b)
              .sortBy(lambda x: -x[1]))
print(conteo_rdd.take(5))


# ------------------------------------------------------------
# 5. EXPORTAR RESULTADOS PARA GRAFICAR EN EXCEL
#    (separador ; y coma decimal para que Excel en espanol los abra bien)
# ------------------------------------------------------------
def guardar(tabla, nombre):
    with open(nombre, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(tabla.columns)
        for r in tabla.collect():
            w.writerow([str(x).replace(".", ",") if isinstance(x, float) else x
                        for x in r])


guardar(por_anio, "resumen_anio.csv")
guardar(por_mes, "resumen_mes.csv")
guardar(por_dep.orderBy("promedio"), "resumen_departamento.csv")
guardar(heladas_dep, "resumen_heladas.csv")

print("\n=== 5. ARCHIVOS GENERADOS ===")
print("resumen_anio.csv, resumen_mes.csv,")
print("resumen_departamento.csv y resumen_heladas.csv")
spark.stop()
