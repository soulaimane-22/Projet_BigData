# 🛡️ Projet BigData — Détection d'Anomalies de Sécurité en Temps Réel

---

## 📌 Stack technique

| Composant | Version | Rôle |
|-----------|---------|------|
| Apache Kafka | 3.7.0 | Ingestion des événements (broker pub/sub) |
| Apache Spark (PySpark) | 3.5.1 | Traitement temps réel (Structured Streaming) |
| PostgreSQL | 16 | Stockage des alertes détectées |
| Grafana | 10.4.2 | Dashboard temps réel |
| Python | 3 | Producteur de données |
| Docker Compose | — | Orchestration de tous les services |

---

## 🏗️ Architecture — Pipeline Kappa

```
Producteur Python        Apache Kafka          Apache Spark              PostgreSQL        Grafana
(NSL-KDD dataset)  →→→  (security-events) →→→  (Structured Streaming) →→→  (alerts)  →→→  (Dashboard)
~20 événements/s         topic Kafka            Détection anomalies         Stockage        5 panels
```

L'architecture retenue est **Kappa** : toutes les données transitent par un flux unique (pas de couche batch séparée), ce qui minimise la latence pour la détection d'intrusion.

---

## 📁 Structure du projet

```
Projet_BigData/
├── docker-compose.yml              ← Lance tout en une commande
├── data/
│   └── init.sql                    ← Création automatique de la table alerts
├── producer/
│   ├── producer.py                 ← Lit NSL-KDD et publie dans Kafka
│   └── KDDTrain+.txt               ← Dataset (125 000 connexions labelisées)
├── spark/
│   └── stream_processor.py         ← Consomme Kafka, détecte, écrit PostgreSQL
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── postgres.yml        ← Connexion PostgreSQL (auto)
    │   └── dashboards/
    │       └── dashboard.yml       ← Provider JSON (auto)
    └── dashboards/
        └── security_dashboard.json ← 5 panels temps réel
```

---

## 🚀 Lancement complet depuis zéro

### 1. Cloner le repo
```bash
git clone https://github.com/soulaimane-22/Projet_BigData.git
cd Projet_BigData
```

### 2. Télécharger le dataset (non inclus dans Git)
```bash
cd producer
wget https://raw.githubusercontent.com/defcom17/NSL_KDD/master/KDDTrain+.txt
cd ..
```

### 3. Installer les dépendances Python
```bash
pip install kafka-python pyspark psycopg2-binary
```

### 4. Lancer l'infrastructure (Kafka + Spark + PostgreSQL + Grafana)
```bash
docker compose up -d
docker ps   # vérifier que les 4 conteneurs sont Up
```

### 5. Créer le topic Kafka
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic security-events \
  --partitions 1 --replication-factor 1
```

### 6. Lancer le producteur — Terminal 1
```bash
cd producer
python producer.py
```

### 7. Lancer Spark Structured Streaming — Terminal 2
```bash
cd spark
JAVA_HOME=~/java17 PATH=~/java17/bin:$PATH spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.postgresql:postgresql:42.7.3 \
  stream_processor.py
```

### 8. Ouvrir le dashboard Grafana
```
http://localhost:3000
Login : admin / admin
```

---

## ✅ Phases du projet

| Phase | Description | Statut |
|-------|-------------|--------|
| Phase 0 | Infrastructure Docker (Kafka + Spark + PostgreSQL) | ✅ Done |
| Phase 1 | Ingestion NSL-KDD → Kafka (~20 evt/s) | ✅ Done |
| Phase 2 | Traitement Spark → Détection → PostgreSQL | ✅ Done |
| Phase 3 | Dashboard Grafana (5 panels temps réel) | ✅ Done |

---

## 📊 Résultats — Anomalies détectées (Phase 2)

| Attaque | Total | Catégorie |
|---------|-------|-----------|
| neptune | 218 | DoS (SYN flood) |
| ipsweep | 24 | Probe (scan IP) |
| portsweep | 21 | Probe (scan ports) |
| warezclient | 9 | R2L |
| satan | 9 | Probe |
| smurf | 8 | DoS |
| teardrop | 7 | DoS |
| nmap | 4 | Probe |
| back | 4 | DoS |
| pod | 1 | DoS |

---

## 🖥️ Dashboard Grafana — 5 panels

| Panel | Type | Contenu |
|-------|------|---------|
| 1 | Bar chart | Alertes par type d'attaque (top 15) |
| 2 | Time series | Timeline des alertes par minute |
| 3 | Pie chart | Top 10 services attaqués |
| 4 | Donut chart | Répartition protocoles tcp/udp/icmp |
| 5 | Stat | Compteurs globaux (total, types, services) |

Rafraîchissement automatique toutes les **5 secondes**.

---

## 🔧 Dépannage

| Problème | Solution |
|----------|----------|
| Erreur checkpoint Spark | `rm -rf /tmp/bigdata/` puis relancer |
| Grafana affiche "No data" | Vérifier que Spark tourne et que la table alerts contient des données |
| Port 3000 occupé | Changer en `"3001:3000"` dans docker-compose.yml |
| Spark ne trouve pas Java | `java -version` doit retourner Java 17 (pas 21+) |

---

