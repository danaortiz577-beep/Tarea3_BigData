# -*- coding: utf-8 -*-
"""
Productor de Kafka (generador de datos) 

Lee el CSV del dataset de temperatura minima del IDEAM, toma registros reales
al azar y los publica en el topic de Kafka simulando la llegada de datos en
tiempo real. Cada mensaje lleva la hora actual como "timestamp" para que las
ventanas de 1 minuto de Spark Streaming tengan sentido (las fechas originales
son de 2001 a 2023 y se conservan en "fecha_original").

Ejecucion:
    python3 kafka_producer_tmin.py
"""
import csv
import json
import random
import time

ARCHIVO = "/home/vboxuser/tmin.csv"   # CSV local (no HDFS)
TOPIC = "temperatura_min"
MENSAJES_POR_SEGUNDO = 5


def leer_registros(ruta):
    """Devuelve la lista de registros validos del CSV."""
    registros = []
    with open(ruta, newline="", encoding="utf-8-sig") as fh:
        for fila in csv.DictReader(fh):
            fila = {k.strip().lower(): v for k, v in fila.items()}
            try:
                valor = float(str(fila["valorobservado"]).replace(",", "."))
            except (KeyError, ValueError):
                continue
            if not -20 <= valor <= 40 or not fila.get("departamento"):
                continue
            registros.append({
                "codigo_estacion": fila.get("codigoestacion", ""),
                "estacion": fila.get("nombreestacion", ""),
                "departamento": fila["departamento"],
                "municipio": fila.get("municipio", ""),
                "temperatura": valor,
                "fecha_original": fila.get("fechaobservacion", ""),
            })
    return registros


def main():
    # Se importa aqui para poder probar leer_registros sin tener Kafka
    from kafka import KafkaProducer

    registros = leer_registros(ARCHIVO)
    print("Registros validos cargados:", len(registros))

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"))

    while True:
        r = random.choice(registros).copy()
        r["timestamp"] = int(time.time())
        producer.send(TOPIC, value=r)
        print("Enviado ->", r["departamento"], "|", r["estacion"], "|",
              r["temperatura"], "C")
        time.sleep(1.0 / MENSAJES_POR_SEGUNDO)


if __name__ == "__main__":
    main()
