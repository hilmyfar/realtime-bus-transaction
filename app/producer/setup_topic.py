"""
producer/setup_topic.py
========================
Membuat Kafka topic dan mendaftarkan Avro schema ke Schema Registry.
Jalankan SEKALI sebelum producer/consumer dijalankan.
"""

import os
import json
import requests
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))  # e.g. app/spark/
APP_DIR     = os.path.dirname(BASE_DIR)                   # app/
ROOT_DIR    = os.path.dirname(APP_DIR)                    # project root

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, "config", ".env"))

BOOTSTRAP_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME           = os.getenv("KAFKA_TOPIC",             "bus-transactions")
SCHEMA_REGISTRY_URL  = os.getenv("SCHEMA_REGISTRY_URL",    "http://localhost:8081")
SCHEMA_FILE          = os.path.join(ROOT_DIR, "schema", "bus_transaction.avsc")

# ============================================================
# 1. BUAT KAFKA TOPIC
# ============================================================
def create_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    topic = NewTopic(
        topic          = TOPIC_NAME,
        num_partitions = 7,       
        replication_factor = 1,  
        config = {
            "retention.ms":      str(7 * 24 * 60 * 60 * 1000),  # 7 hari
            "cleanup.policy":    "delete",
            "compression.type":  "snappy",
            "min.insync.replicas": "1",
        }
    )

    result = admin.create_topics([topic])
    for topic_name, future in result.items():
        try:
            future.result()
            print(f"[TOPIC] Topic '{topic_name}' berhasil dibuat.")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"[TOPIC] Topic '{topic_name}' sudah ada, skip.")
            else:
                raise e


# ============================================================
# 2. DAFTARKAN SCHEMA KE SCHEMA REGISTRY
# ============================================================
def register_schema():
    with open(SCHEMA_FILE, "r") as f:
        schema_str = f.read()

    subject  = f"{TOPIC_NAME}-value"
    url      = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    headers  = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload  = json.dumps({"schema": schema_str})

    response = requests.post(url, data=payload, headers=headers)

    if response.status_code in (200, 201):
        schema_id = response.json().get("id")
        print(f"[SCHEMA] Schema berhasil didaftarkan. ID: {schema_id}")
    else:
        print(f"[SCHEMA] Gagal mendaftarkan schema: {response.status_code} — {response.text}")


# ============================================================
# 3. VERIFIKASI TOPIC
# ============================================================
def verify_topic():
    admin   = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    topics  = admin.list_topics(timeout=10).topics
    if TOPIC_NAME in topics:
        meta = topics[TOPIC_NAME]
        print(f"[VERIFY] Topic '{TOPIC_NAME}': {len(meta.partitions)} partisi")
    else:
        print(f"[VERIFY] Topic '{TOPIC_NAME}' TIDAK ditemukan!")


if __name__ == "__main__":
    print("=" * 60)
    print("KAFKA TOPIC & SCHEMA REGISTRY SETUP")
    print("=" * 60)
    create_topic()
    register_schema()
    verify_topic()
    print("\nSetup selesai. Siap menjalankan producer.")
