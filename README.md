# Projet BigData — Détection d'Anomalies Temps Réel

Pipeline Kafka + Spark Structured Streaming + PostgreSQL

## Stack
- Apache Kafka 3.7.0
- Apache Spark / PySpark 4.1.2
- PostgreSQL 16
- Python 3
- Docker Compose

## Démarrage rapide

### 1. Lancer l'infrastructure
```bash
docker compose up -d
```

### 2. Créer le topic Kafka
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic security-events \
  --partitions 1 --replication-factor 1
```

### 3. Lancer le producteur
```bash
cd producer
pip install kafka-python
python producer.py
```

### 4. Lancer Spark
```bash
cd spark
pip install pyspark psycopg2-binary
JAVA_HOME=~/java17 PATH=~/java17/bin:$PATH spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.postgresql:postgresql:42.7.3 \
  stream_processor.py
```

## Phases
- ✅ Phase 0 : Infrastructure Docker
- ✅ Phase 1 : Ingestion NSL-KDD → Kafka
- ✅ Phase 2 : Traitement Spark → PostgreSQL
- 🔲 Phase 3 : Dashboard Grafana
- 🔲 Phase 4 : Compte-rendu & Slides
