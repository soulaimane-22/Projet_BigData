import json
import time
import csv
from kafka import KafkaProducer

COLUMNS = [
    "duration", "protocol_type", "service", "flag",
    "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
    "hot", "num_failed_logins", "logged_in", "num_compromised",
    "root_shell", "su_attempted", "num_root", "num_file_creations",
    "num_shells", "num_access_files", "num_outbound_cmds",
    "is_host_login", "is_guest_login", "count", "srv_count",
    "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
    "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
    "dst_host_count", "dst_host_srv_count", "dst_host_same_srv_rate",
    "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
    "dst_host_srv_diff_host_rate", "dst_host_serror_rate",
    "dst_host_srv_serror_rate", "dst_host_rerror_rate",
    "dst_host_srv_rerror_rate", "label", "difficulty"
]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Producteur démarré — envoi vers Kafka topic: security-events")

with open('KDDTrain+.txt', 'r') as f:
    reader = csv.reader(f)
    count = 0
    for row in reader:
        if len(row) != len(COLUMNS):
            continue
        event = dict(zip(COLUMNS, row))
        producer.send('security-events', event)
        count += 1
        if count % 100 == 0:
            print(f"{count} événements envoyés — dernier label: {event['label']}")
        time.sleep(0.05)  # ~20 evt/s

producer.flush()
print("Terminé.")
