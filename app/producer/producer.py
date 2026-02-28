"""
producer/producer.py
=====================
Kafka Producer: mengambil event dari simulator dan mempublish
ke topic Kafka dengan serialisasi Avro via Schema Registry.

Key partitioning: bus_id → event dari bus yang sama masuk partisi yang sama.
"""

import os
import sys
import json
# Tambahkan app/ ke path agar bisa import simulator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

from simulator.simulate import stream_transactions, BusTransaction

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))  # e.g. app/spark/
APP_DIR     = os.path.dirname(BASE_DIR)                   # app/
ROOT_DIR    = os.path.dirname(APP_DIR)                    # project root

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, "config", ".env"))

BOOTSTRAP_SERVERS   = "localhost:9092"
TOPIC_NAME          = os.getenv("KAFKA_TOPIC",             "bus-cardreader-event")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL",    "http://localhost:8081")
PRODUCE_INTERVAL_MS = int(os.getenv("PRODUCE_INTERVAL_MS", "500"))
SCHEMA_FILE          = os.path.join(ROOT_DIR, "schema", "bus_transaction.avsc")


# ============================================================
# LOAD AVRO SCHEMA
# ============================================================
def load_schema() -> str:
    with open(SCHEMA_FILE, "r") as f:
        return f.read()


# ============================================================
# SERIALIZER: BusTransaction → dict untuk Avro
# ============================================================
def transaction_to_dict(tx: BusTransaction, ctx) -> dict:
    return {
        "transaction_id": tx.transaction_id,
        "card_id":        tx.card_id,
        "bus_id":         tx.bus_id,
        "event_type":     tx.event_type,
        "timestamp":      tx.timestamp,
        "latitude":       tx.latitude,
        "longitude":      tx.longitude,
    }


# ============================================================
# CALLBACK: delivery report
# ============================================================
def delivery_report(err, msg):
    if err:
        print(f"[PRODUCER] Gagal kirim: {err}")
    else:
        print(f"[PRODUCER] {msg.topic()} | partition={msg.partition()} "
              f"| offset={msg.offset()} | key={msg.key()}")


# ============================================================
# SETUP PRODUCER
# ============================================================
def create_producer() -> SerializingProducer:
    schema_str     = load_schema()
    sr_client      = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client = sr_client,
        schema_str             = schema_str,
        to_dict                = transaction_to_dict,
    )

    producer_config = {
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "key.serializer":     StringSerializer("utf_8"),
        "value.serializer":   avro_serializer,
        "acks":               "all",
        "retries":            3,
        "retry.backoff.ms":   300,
        "linger.ms":          10,
        "batch.size":         16384,
        "compression.type":   "snappy",
    }

    return SerializingProducer(producer_config)


# ============================================================
# MAIN: Produce semua event
# ============================================================
def main():
    print("=" * 60)
    print("BUS SMART CARD KAFKA PRODUCER")
    print("=" * 60)
    print(f"Bootstrap servers : {BOOTSTRAP_SERVERS}")
    print(f"Topic             : {TOPIC_NAME}")
    print(f"Schema Registry   : {SCHEMA_REGISTRY_URL}")
    print(f"Interval          : {PRODUCE_INTERVAL_MS}ms per event")
    print()

    producer = create_producer()
    count    = 0
    try:
        for tx in stream_transactions(interval_ms=PRODUCE_INTERVAL_MS):
            producer.produce(
                topic    = TOPIC_NAME,
                key      = tx.bus_id,
                value    = tx,
                on_delivery = delivery_report,
            )
            producer.poll(0)
            count += 1

            if count % 50 == 0:
                producer.flush()
                print(f"[PRODUCER] {count} event telah dikirim...")

    except KeyboardInterrupt:
        print("\n[PRODUCER] Dihentikan oleh user.")
    finally:
        producer.flush()
        print(f"\n[PRODUCER] Total event terkirim: {count}")


if __name__ == "__main__":
    main()
