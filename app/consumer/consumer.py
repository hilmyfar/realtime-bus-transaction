"""
consumer/consumer.py
=====================
Kafka Consumer: membaca event dari topic Kafka, deserialisasi Avro,
lalu menyimpan raw events ke Neon PostgreSQL.

Consumer ini berjalan independen dari PySpark (untuk raw sink).
PySpark memiliki consumer/reader-nya sendiri via spark.readStream.
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import signal
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from dotenv import load_dotenv

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))  # e.g. app/spark/
APP_DIR     = os.path.dirname(BASE_DIR)                   # app/
ROOT_DIR    = os.path.dirname(APP_DIR)                    # project root

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, "config", ".env"))

BOOTSTRAP_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME          = os.getenv("KAFKA_TOPIC",             "bus-transactions")
GROUP_ID            = os.getenv("KAFKA_GROUP_ID",          "bus-consumer-group")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL",    "http://localhost:8081")
DATABASE_URL        = os.getenv("DATABASE_URL")
SCHEMA_FILE          = os.path.join(ROOT_DIR, "schema", "bus_transaction.avsc")

# Buffer untuk batch insert
BATCH_SIZE = 20
running    = True


# ============================================================
# SIGNAL HANDLER: graceful shutdown
# ============================================================
def handle_signal(sig, frame):
    global running
    print("\n[CONSUMER] Graceful shutdown dimulai...")
    running = False

signal.signal(signal.SIGINT,  handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# ============================================================
# DATABASE
# ============================================================
def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def insert_raw_events(conn, events: list):
    if not events:
        return
    sql = """
        INSERT INTO raw_events
            (transaction_id, card_id, bus_id, event_type, event_time, latitude, longitude, geom)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        ON CONFLICT (transaction_id) DO NOTHING
    """
    rows = [
        (
            e["transaction_id"],
            e["card_id"],
            e["bus_id"],
            e["event_type"],
            datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc),
            e["latitude"],
            e["longitude"],
            e["longitude"],  # MakePoint(lon, lat)
            e["latitude"],
        )
        for e in events
    ]
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows)
    conn.commit()
    print(f"[CONSUMER] ✅ {len(rows)} event disimpan ke raw_events")


# ============================================================
# SETUP CONSUMER
# ============================================================
def create_consumer() -> DeserializingConsumer:
    with open(SCHEMA_FILE, "r") as f:
        schema_str = f.read()

    sr_client        = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(
        schema_registry_client = sr_client,
        schema_str             = schema_str,
    )

    consumer_config = {
        "bootstrap.servers":        BOOTSTRAP_SERVERS,
        "key.deserializer":         StringDeserializer("utf_8"),
        "value.deserializer":       avro_deserializer,
        "group.id":                 GROUP_ID,
        "auto.offset.reset":        "earliest",
        "enable.auto.commit":       False,    # Manual commit setelah DB insert
        "session.timeout.ms":       30000,
        "max.poll.interval.ms":     300000,
        "fetch.min.bytes":          1,
        "fetch.wait.max.ms":        500,
    }

    consumer = DeserializingConsumer(consumer_config)
    consumer.subscribe([TOPIC_NAME])
    return consumer


# ============================================================
# MAIN: Consume dan simpan ke DB
# ============================================================
def main():
    print("=" * 60)
    print("BUS SMART CARD KAFKA CONSUMER")
    print("=" * 60)
    print(f"Bootstrap servers : {BOOTSTRAP_SERVERS}")
    print(f"Topic             : {TOPIC_NAME}")
    print(f"Group ID          : {GROUP_ID}")
    print(f"Batch size        : {BATCH_SIZE}")
    print()

    consumer = create_consumer()
    conn     = get_db_connection()
    buffer   = []
    total    = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush buffer jika ada sisa
                if buffer:
                    insert_raw_events(conn, buffer)
                    consumer.commit()
                    total  += len(buffer)
                    buffer  = []
                continue

            if msg.error():
                print(f"[CONSUMER] ❌ Error: {msg.error()}")
                continue

            event = msg.value()
            if event:
                buffer.append(event)

            if len(buffer) >= BATCH_SIZE:
                insert_raw_events(conn, buffer)
                consumer.commit()
                total  += len(buffer)
                buffer  = []
                print(f"[CONSUMER] Total event diproses: {total}")

    finally:
        # Flush sisa buffer
        if buffer:
            insert_raw_events(conn, buffer)
            consumer.commit()
            total += len(buffer)

        consumer.close()
        conn.close()
        print(f"\n[CONSUMER] Selesai. Total event: {total}")


if __name__ == "__main__":
    main()
