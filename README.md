# Tarea 3 – Procesamiento de datos con Apache Spark

Análisis de la **temperatura mínima del aire en Colombia** (estaciones del IDEAM) con Apache Spark en modo **batch** y en **tiempo real** con **Apache Kafka + Spark Structured Streaming**.

| | |
|---|---|
| **Autor** | Dana Vanessa Fajardo Ortiz |
| **Curso** | Big Data (código 202016911) – Universidad Nacional Abierta y a Distancia (UNAD) |

---

## 1. Problema

¿Cómo varía la temperatura mínima del aire por **departamento, mes y año**, y con qué frecuencia se registran **temperaturas de helada (≤ 0 °C)**?

Es una pregunta útil para la gestión del riesgo climático y agrícola, y se resuelve con agregaciones sobre un conjunto de datos de gran tamaño.

## 2. Dataset

- **Fuente:** Portal de Datos Abiertos de Colombia – *Cantidad de datos temperatura mínima* (IDEAM):
  <https://www.datos.gov.co/Ambiente-y-Desarrollo-Sostenible/Cantidad-de-datos-temperatura-m-nima/xiya-sibu>
- **Contenido:** lecturas de temperatura mínima del aire a 2 m de estaciones automáticas, con código y nombre de la estación, departamento, municipio, zona hidrográfica, latitud, longitud y fecha de observación.
- **Tamaño usado:** extracción de **150.351 registros** (~38 MB) de los ~873 mil que publica el portal, con datos de 2001 a 2023.
- **Cómo obtenerlo:** en la página del dataset, *Exportar → CSV*. El archivo **no se incluye en este repositorio** por su tamaño (ver `.gitignore`).
- Son datos **crudos, sin validar** por el IDEAM, por lo que el script incluye una etapa de limpieza.

## 3. Estructura del repositorio

```
bigdata-tarea3-spark/
├── README.md
├── requirements.txt
├── .gitignore
├── batch/
│   └── tarea3.py                      # análisis batch con PySpark
└── streaming/
    ├── kafka_producer_tmin.py         # productor: envía registros reales a Kafka
    └── spark_streaming_tmin.py        # consumidor: Spark Structured Streaming
```

## 4. Entorno utilizado

Máquina virtual en VirtualBox con Ubuntu 22.04 y:

- Java 11
- Hadoop 3.3.x (HDFS en `localhost:9000`)
- Apache Spark 3.5.x y PySpark
- Python 3.10
- Apache Kafka 3.9.2 (con ZooKeeper) y `kafka-python`

```bash
pip install -r requirements.txt
```

---

## 5. Procesamiento en batch

### 5.1 Cargar el dataset en HDFS

Con el usuario `hadoop`:

```bash
start-all.sh                      # inicia HDFS y YARN
jps                               # debe listar NameNode y DataNode
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put /home/hadoop/query.csv.1 /Tarea3
hdfs dfs -ls /Tarea3
```

### 5.2 Ejecutar el análisis

Con el usuario `vboxuser`:

```bash
python3 batch/tarea3.py
```

Si tu archivo en HDFS tiene otro nombre, cambia la constante `RUTA_HDFS` al inicio del script.

### 5.3 Qué hace el script

| Etapa | Descripción |
|---|---|
| 1. Carga | Lee el CSV desde HDFS con `spark.read` |
| 2. Limpieza | Convierte el valor a número, interpreta la fecha (varios formatos), elimina duplicados y nulos y descarta lecturas fuera del rango −20 a 40 °C |
| 3. EDA (DataFrames) | Estadísticas descriptivas; promedios por departamento, año y mes; estaciones más frías; conteo de heladas |
| 4. RDD | Conteo de registros por departamento con `map` + `reduceByKey` + `take` |
| 5. Exportación | Genera `resumen_anio.csv`, `resumen_mes.csv`, `resumen_departamento.csv` y `resumen_heladas.csv` (separador `;` y coma decimal, listos para abrir en Excel y graficar) |

### 5.4 Resultados principales

| Indicador | Valor |
|---|---|
| Registros cargados | 150.351 |
| Registros válidos tras la limpieza | 150.266 (85 descartados) |
| Temperatura mínima promedio | 18,9 °C (mín. −15,0 °C; máx. 39,9 °C) |
| Departamentos con más registros | Cundinamarca (17.103), Boyacá (12.517), Antioquia (12.001) |
| Departamento con menor promedio* | Caldas (≈ 8,5 °C) |
| Estaciones más frías | Conejeras2 – AUT (0,85 °C) y Nevado Santa Isabel – AUT (0,86 °C), ambas en Caldas |
| Registros de helada (≤ 0 °C) | 1.560 (Caldas 475, Meta 220, Boyacá 154) |
| Variación por mes | Casi nula (18,6–19,1 °C), esperable en clima tropical |

\* Entre los departamentos con más registros.

### 5.5 Limitaciones

- El archivo es una **extracción parcial** del dataset: no hay registros de 2020 y 2021, y 2022–2023 tienen muy pocos.
- Hay lecturas de exactamente **0,0 °C en departamentos cálidos** (Magdalena, Cesar, Meta), que probablemente son errores de sensor y no heladas reales; el conteo de heladas podría estar sobrestimado.
- Los datos no están validados por el IDEAM.

---

## 6. Procesamiento en tiempo real (Kafka + Spark Streaming)

### 6.1 Arquitectura

```
CSV del dataset ──► Productor (Python) ──► Kafka: topic "temperatura_min" ──► Spark Structured Streaming ──► Consola
                    (1 registro real                                          (ventana de 1 minuto por
                     cada 0,2 s)                                               departamento)
```

El productor toma registros reales del dataset y los publica con la **hora actual** como marca de tiempo para simular la llegada de datos en vivo. El consumidor calcula, por ventana de 1 minuto y departamento, el número de lecturas, la temperatura promedio y mínima, y cuántas lecturas son de helada.

### 6.2 Pasos

1. Copiar el CSV a una carpeta local (el productor no usa HDFS) y ajustar `ARCHIVO` en `kafka_producer_tmin.py`:

   ```bash
   sudo cp /home/hadoop/query.csv.1 /home/vboxuser/tmin.csv
   sudo chown vboxuser:vboxuser /home/vboxuser/tmin.csv
   ```

2. Iniciar ZooKeeper y Kafka (en segundo plano):

   ```bash
   sudo -v
   sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
   sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
   ```

3. Crear el topic:

   ```bash
   /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
       --replication-factor 1 --partitions 1 --topic temperatura_min
   ```

4. En una terminal, ejecutar el **productor**:

   ```bash
   python3 streaming/kafka_producer_tmin.py
   ```

5. En otra terminal, ejecutar el **consumidor**:

   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
       streaming/spark_streaming_tmin.py
   ```

6. Abrir la interfaz de Spark en `http://IP_DE_LA_VM:4040` (pestañas *Jobs*, *Structured Streaming* y *Executors*). Para terminar, `Ctrl+C` en ambas terminales.

---

## 7. Referencias

- Instituto de Hidrología, Meteorología y Estudios Ambientales (IDEAM). *Cantidad de datos temperatura mínima* [Conjunto de datos]. Portal de Datos Abiertos de Colombia. https://www.datos.gov.co
- Apache Software Foundation. *Apache Spark Documentation*. https://spark.apache.org/docs/latest/
- Apache Software Foundation. *Apache Kafka Documentation*. https://kafka.apache.org/documentation/
- Universidad Nacional Abierta y a Distancia. (2026). *Guía de aprendizaje – Tarea 3: Procesamiento de datos con Apache Spark* y *Anexo 3: Instructivo Spark Streaming y Kafka*. Curso Big Data.
