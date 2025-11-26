# Sistema de Clasificación de Frutas con Big Data & AI

Este proyecto implementa una arquitectura **Lambda (Batch + Streaming)** para la clasificación automática de calidad de frutas en entornos agroindustriales. Utiliza **Apache Spark** para el entrenamiento masivo y **Apache Kafka** para la inferencia en tiempo real.

## Tecnologías

* **Inteligencia Artificial:** TensorFlow, Keras (EfficientNetB0).
* **Big Data:** Apache Spark (PySpark).
* **Streaming:** Apache Kafka, Zookeeper.
* **Infraestructura:** Docker, Docker Compose.
* **MLOps:** MLflow.

## Estructura del Proyecto

* `job.py`: Script de entrenamiento distribuido (ETL + Training) para ejecutar en Spark.
* `app.py`: Microservicio consumidor que realiza la inferencia en tiempo real.
* `camara.py`: Script productor que simula un dispositivo IoT enviando video.
* `docker-compose.yml`: Orquestación de contenedores para Kafka y Zookeeper.
* `modelo_final.h5`: Pesos del modelo de Deep Learning entrenado.

## ⚙️ Instalación y Uso

### 1. Requisitos Previos
Tener instalado Docker Desktop y Python 3.9+.

### 2. Levantar Infraestructura
```bash
docker-compose up -d