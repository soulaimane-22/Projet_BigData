# 🛡️ Projet BigData — Détection d'Anomalies de Sécurité en Temps Réel

---

## 📌 Stack technique

| Composant | Version | Rôle |
|-----------|---------|------|
| Apache Kafka | 3.7.0 | Ingestion des événements (broker pub/sub) |
| Apache Spark (PySpark) | 3.5.1 | Traitement temps réel (Structured Streaming) |
| PostgreSQL | 16 | Stockage des alertes détectées |
| Grafana | 10.4.2 | Dashboard temps réel |
| Python | 3.11 | Producteur de données |
| Docker Compose | — | Orchestration de tous les services |

---

## 🏗️ Architecture — Pipeline Kappa
Producteur Python        Apache Kafka          Apache Spark              PostgreSQL        Grafana

(NSL-KDD dataset)  →→→  (security-events) →→→  (Structured Streaming) →→→  (alerts)  →→→  (Dashboard)

~20 événements/s         topic Kafka            Détection anomalies         Stockage        5 panels

L'architecture retenue est **Kappa** : toutes les données transitent par un flux unique (pas de couche batch séparée), ce qui minimise la latence pour la détection d'intrusion.

---

## 📁 Structure du projet
Projet_BigData/

├── docker-compose.yml              ← Lance tout en une commande

├── data/

│   └── init.sql                    ← Création automatique de la table alerts

├── producer/

│   ├── producer.py                 ← Lit NSL-KDD et publie dans Kafka en boucle

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

---

## 🚀 Lancement complet depuis zéro

### 1. Cloner le repo
```bash
git clone https://github.com/soulaimane-22/Projet_BigData.git
cd Projet_BigData
```

### 2. Dataset
Le dataset `KDDTrain+.txt` est déjà inclus dans le repo — aucune action nécessaire.

### 3. Lancer toute l'infrastructure en une commande
```bash
docker compose up -d
docker ps   # vérifier que les 5 conteneurs sont Up
```

Cela lance automatiquement : Kafka, PostgreSQL, Spark (stream_processor.py), Producteur Python, Grafana.

### 4. Créer le topic Kafka (première fois uniquement)
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic security-events \
  --partitions 1 --replication-factor 1
```

### 5. Ouvrir le dashboard Grafana
http://localhost:3000

Login : admin / admin

Le dashboard **"Détection d'Anomalies — Cybersécurité NSL-KDD"** se charge automatiquement.

---

## ✅ Phases du projet

| Phase | Description | Statut |
|-------|-------------|--------|
| Phase 0 | Infrastructure Docker (Kafka + Spark + PostgreSQL + Grafana) | ✅ Done |
| Phase 1 | Ingestion NSL-KDD → Kafka (~20 evt/s, boucle infinie) | ✅ Done |
| Phase 2 | Traitement Spark → Détection → PostgreSQL | ✅ Done |
| Phase 3 | Dashboard Grafana (5 panels temps réel) | ✅ Done |

---

## 📊 Résultats — Anomalies détectées

| Attaque | Catégorie | Description |
|---------|-----------|-------------|
| neptune | DoS | SYN flood — connexions TCP sans réponse |
| ipsweep | Probe | Scan d'adresses IP |
| satan | Probe | Scan de vulnérabilités |
| portsweep | Probe | Scan de ports |
| smurf | DoS | Amplification ICMP |
| nmap | Probe | Reconnaissance réseau |
| teardrop | DoS | Fragmentation IP malformée |
| back | DoS | Attaque Apache |
| warezclient | R2L | Téléchargement illégal |
| pod | DoS | Ping of Death |

---

## 🖥️ Dashboard Grafana — 5 panels

| Panel | Type | Contenu |
|-------|------|---------|
| 1 | Bar chart | Alertes par type d'attaque (top 15) |
| 2 | Time series | Timeline des alertes par minute |
| 3 | Bar chart | Top 10 services attaqués |
| 4 | Bar chart | Répartition protocoles tcp/udp/icmp |
| 5 | Stat | Compteurs globaux (total, types, services) |

Rafraîchissement automatique toutes les **5 secondes**.

---

## 🔧 Dépannage

| Problème | Solution |
|----------|----------|
| Erreur checkpoint Spark | `rm -rf /tmp/bigdata/` puis `docker compose restart spark` |
| Grafana affiche "No data" | Attendre ~2 min que Spark télécharge les JARs et commence à écrire |
| Port 3000 occupé | Changer en `"3001:3000"` dans docker-compose.yml |
| Spark ne trouve pas Java | Java 17 requis — installer via SDKMAN : `sdk install java 17.0.11-tem` |
| Permission Docker | Exécuter `newgrp docker` dans le terminal courant |

---

## ⚠️ Note Java

Spark 3.5.1 requiert **Java 17** (pas Java 21+). Sur Fedora 44 (qui ne fournit pas Java 17 via dnf) :
```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 17.0.11-tem
```
