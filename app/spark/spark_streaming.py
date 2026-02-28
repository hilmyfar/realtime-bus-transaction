"""
spark/spark_streaming.py
Pipeline streaming— jalan di Docker.
Submit via: bash docker/submit_spark.sh
"""

import os
import uuid
import pytz
from datetime import datetime
from typing import Iterator, Tuple

import pandas as pd
import requests
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType, IntegerType
)
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.avro.functions import from_avro
from dotenv import load_dotenv

# ============================================================
# TIMEZONE & HELPERS
# ============================================================
LOCAL_TZ   = "Asia/Jakarta"
JAKARTA_TZ = pytz.timezone(LOCAL_TZ)


def to_epoch_us(ts) -> int:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")          # naive dari Spark = UTC
    else:
        t = t.tz_convert("UTC")
    return int(t.value // 1000)

def from_epoch_us(us: int):
    return (
        pd.Timestamp(int(us) * 1000, unit="ns", tz="UTC")
        .tz_convert(LOCAL_TZ)
        .to_pydatetime()
    )

def normalize_to_utc(ts) -> datetime:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")          # naive dari Spark = UTC
    else:
        t = t.tz_convert("UTC")
    return t.to_pydatetime()

def to_naive_utc(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")          # naive dari Spark = UTC
    else:
        t = t.tz_convert("UTC")
    return t.tz_localize(None)            # strip tz → naive UTC

# Pakai naive datetime64[ns] — hindari bug pandas 2.0 tz-aware concat
# Spark TimestampType = UTC by default, konversi ke WIB di sisi PostgreSQL:
#   SELECT tap_in_time AT TIME ZONE 'Asia/Jakarta' FROM trips;
EMPTY_TRIPS = pd.DataFrame({
    "trip_id":      pd.Series([], dtype="object"),
    "card_id":      pd.Series([], dtype="object"),
    "bus_id":       pd.Series([], dtype="object"),
    "tap_in_time":  pd.Series([], dtype="datetime64[ns]"),
    "tap_out_time": pd.Series([], dtype="datetime64[ns]"),
    "tap_in_lat":   pd.Series([], dtype="float64"),
    "tap_in_lon":   pd.Series([], dtype="float64"),
    "tap_out_lat":  pd.Series([], dtype="float64"),
    "tap_out_lon":  pd.Series([], dtype="float64"),
})

EMPTY_FLAGS = pd.DataFrame({
    "bus_id":        pd.Series([], dtype="object"),
    "pending_count": pd.Series([], dtype="int32"),
    "flagged_at":    pd.Series([], dtype="datetime64[ns]"),
    "segment":       pd.Series([], dtype="object"),        # ← tambah ini
    "message":       pd.Series([], dtype="object"),
})

# ============================================================
# SCHEMA REGISTRY
# ============================================================
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

def get_avro_schema(subject: str) -> str:
    url  = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()["schema"]

# ============================================================
# KONFIGURASI
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(os.path.dirname(BASE_DIR))
load_dotenv(dotenv_path=os.path.join(ROOT_DIR, "config", ".env"))

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_NAME        = os.getenv("KAFKA_TOPIC", "bus-cardreader-event")
NEON_HOST         = os.getenv("NEON_HOST")
NEON_PORT         = os.getenv("NEON_PORT", "5432")
NEON_DATABASE     = os.getenv("NEON_DATABASE")
NEON_USER         = os.getenv("NEON_USER")
NEON_PASSWORD     = os.getenv("NEON_PASSWORD")
HDFS_NAMENODE     = os.getenv("HDFS_NAMENODE", "hdfs://namenode:8020")

CROWD_THRESHOLD = 15
WATERMARK_DELAY = "5 minutes"
SCHEMA_VERSION  = os.getenv("SCHEMA_VERSION", "v12_20260228")
CHECKPOINT_BASE = f"{HDFS_NAMENODE}/spark-checkpoints/bus-smartcard/{SCHEMA_VERSION}"
DLQ_BASE        = f"{HDFS_NAMENODE}/bus-smartcard-dlq/{SCHEMA_VERSION}"

# ============================================================
# SPARK SCHEMAS
# ============================================================
TRIP_OUTPUT_SCHEMA = StructType([
    StructField("trip_id",      StringType(),    False),
    StructField("card_id",      StringType(),    False),
    StructField("bus_id",       StringType(),    False),
    StructField("tap_in_time",  TimestampType(), False),
    StructField("tap_out_time", TimestampType(), False),
    StructField("tap_in_lat",   DoubleType(),    False),
    StructField("tap_in_lon",   DoubleType(),    False),
    StructField("tap_out_lat",  DoubleType(),    False),
    StructField("tap_out_lon",  DoubleType(),    False),
])

STATE_SCHEMA = StructType([
    StructField("tap_in_tx_id", StringType(), True),
    StructField("bus_id",       StringType(), True),
    StructField("tap_in_ts_us", LongType(),   True),
    StructField("tap_in_lat",   DoubleType(), True),
    StructField("tap_in_lon",   DoubleType(), True),
])

CROWD_FLAG_SCHEMA = StructType([
    StructField("bus_id",        StringType(),    False),
    StructField("pending_count", IntegerType(),   False),
    StructField("flagged_at",    TimestampType(), False),
    StructField("segment",       StringType(),    True),   # ← halte terdekat saat flag
    StructField("message",       StringType(),    True),
])

STATE_CROWD_SCHEMA = StructType([
    StructField("tap_in_count",  IntegerType(), True),
    StructField("tap_out_count", IntegerType(), True),
])

# Koordinat halte untuk lookup segmen — mirror dari HALTE di simulator
_HALTE_COORDS = [
    ("H-A", -6.1375, 106.8135),
    ("H-B", -6.1482, 106.8178),
    ("H-C", -6.1589, 106.8221),
    ("H-D", -6.1696, 106.8264),
    ("H-E", -6.1750, 106.8272),
    ("H-F", -6.1804, 106.8280),
    ("H-G", -6.1934, 106.8230),
    ("H-H", -6.2012, 106.8230),
    ("H-I", -6.2134, 106.8198),
    ("H-J", -6.2256, 106.8162),
]
def current_segment(lat: float, lon: float) -> str:
    """Cari dua halte terdekat → bentuk segmen 'H-X -> H-Y' urut utara ke selatan."""
    # Hitung jarak ke semua halte
    distances = [
        (h[0], (h[1] - lat) ** 2 + (h[2] - lon) ** 2)
        for h in _HALTE_COORDS
    ]
    distances.sort(key=lambda x: x[1])

    # Dua halte terdekat
    halte_a = distances[0][0]
    halte_b = distances[1][0]

    # Urutkan berdasarkan index di _HALTE_COORDS (utara → selatan)
    idx_a = next(i for i, h in enumerate(_HALTE_COORDS) if h[0] == halte_a)
    idx_b = next(i for i, h in enumerate(_HALTE_COORDS) if h[0] == halte_b)

    if idx_a < idx_b:
        return f"{halte_a} -> {halte_b}"
    else:
        return f"{halte_b} -> {halte_a}"
    
# ============================================================
# SPARK SESSION
# ============================================================
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("BusCardStreaming")
        .master("spark://spark-master:7077")
        .config("spark.cores.max", "2")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.network.timeout", "120s")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.kafka.consumer.cache.enabled", "false")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.streaming.blockInterval", "500ms")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    log4j = spark._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    log4j.LogManager.getLogger("org").setLevel(log4j.Level.ERROR)
    log4j.LogManager.getLogger("akka").setLevel(log4j.Level.ERROR)
    log4j.LogManager.getLogger("kafka").setLevel(log4j.Level.ERROR)

    return spark

# ============================================================
# 1. INGESTION + VALIDASI + DEDUP
# ============================================================
def read_and_validate_stream(spark: SparkSession):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)
        .option("kafka.session.timeout.ms", "120000")
        .option("kafka.request.timeout.ms", "120000")
        .option("kafka.connections.max.idle.ms", "540000")
        .option("kafka.fetch.min.bytes", "1")
        .option("kafka.fetch.max.wait.ms", "500")
        .load()
    )

    avro_schema = get_avro_schema(f"{TOPIC_NAME}-value")

    parsed_df = (
        raw_df
        .select(
            F.col("value"),
            F.col("value").cast("string").alias("value_raw"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "data",
            from_avro(
                F.expr("substring(value, 6, length(value) - 5)"),
                avro_schema,
            )
        )
        .select(
            "value_raw", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
            "data.*"
        )
        .withColumn("event_time", F.col("timestamp"))
        .filter(F.col("transaction_id").rlike("^[0-9a-fA-F-]{36}$"))
        .filter(F.col("card_id").rlike("^[A-Za-z0-9]+$"))
        .filter(F.col("bus_id").rlike("^BUS-[0-9]+$"))
        .filter(F.col("event_type").isin("tap_in", "tap_out"))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    valid_event_type  = F.col("event_type").isin("tap_in", "tap_out")
    valid_lat         = F.col("latitude").isNotNull() & F.col("latitude").between(-90, 90)
    valid_lon         = F.col("longitude").isNotNull() & F.col("longitude").between(-180, 180)
    required_not_null = (
        F.col("transaction_id").isNotNull()
        & F.col("card_id").isNotNull()
        & F.col("bus_id").isNotNull()
        & F.col("event_type").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
    )
    good_cond = required_not_null & valid_event_type & valid_lat & valid_lon

    bad_reason = (
        F.when(F.col("transaction_id").isNull(), F.lit("missing_transaction_id"))
         .when(F.col("card_id").isNull(),        F.lit("missing_card_id"))
         .when(F.col("bus_id").isNull(),         F.lit("missing_bus_id"))
         .when(~valid_event_type,                F.lit("invalid_event_type"))
         .when(F.col("timestamp").isNull(),      F.lit("missing_timestamp"))
         .when(F.col("event_time").isNull(),     F.lit("invalid_timestamp_parse"))
         .when(~valid_lat,                       F.lit("invalid_latitude"))
         .when(~valid_lon,                       F.lit("invalid_longitude"))
         .otherwise(F.lit("unknown_parse_error"))
    )

    good_df = (
        parsed_df
        .where(good_cond)
        .dropDuplicates(["transaction_id"])
        .select("transaction_id", "card_id", "bus_id", "event_type", "event_time", "latitude", "longitude")
    )

    bad_df = (
        parsed_df
        .where(~good_cond)
        .select(
            "value_raw",
            bad_reason.alias("error_reason"),
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp"
        )
    )

    return good_df, bad_df

# ============================================================
# 2. STATEFUL PAIRING: tap_in -> tap_out per card_id
# ============================================================
def pair_tap_events(parsed_df: DataFrame) -> DataFrame:

    def pair_func(
        key: Tuple,
        pdf_iter: Iterator[pd.DataFrame],
        state: "GroupState"
    ):
        card_id = key[0]

        pending = None
        if state.exists:
            s = state.get
            if isinstance(s, pd.DataFrame) and not s.empty:
                r = s.iloc[0]
                pending = {
                    "tap_in_tx_id": str(r["tap_in_tx_id"]),
                    "bus_id":       str(r["bus_id"]),
                    "tap_in_ts":    from_epoch_us(int(r["tap_in_ts_us"])),
                    "tap_in_lat":   float(r["tap_in_lat"]),
                    "tap_in_lon":   float(r["tap_in_lon"]),
                }

        results = []
        
        for pdf in pdf_iter:
            if not pd.api.types.is_datetime64_any_dtype(pdf["event_time"]):
                pdf["event_time"] = pd.to_datetime(pdf["event_time"], utc=True)
            pdf = pdf.sort_values("event_time").reset_index(drop=True)
            for _, row in pdf.iterrows():
                event_dt = normalize_to_utc(row["event_time"])

                if row["event_type"] == "tap_in":
                    pending = {
                        "tap_in_tx_id": str(row["transaction_id"]),
                        "bus_id":       str(row["bus_id"]),
                        "tap_in_ts":    event_dt,
                        "tap_in_lat":   float(row["latitude"]),
                        "tap_in_lon":   float(row["longitude"]),
                    }
                    state.update(pd.DataFrame([{
                        "tap_in_tx_id": pending["tap_in_tx_id"],
                        "bus_id":       pending["bus_id"],
                        "tap_in_ts_us": to_epoch_us(pending["tap_in_ts"]),
                        "tap_in_lat":   pending["tap_in_lat"],
                        "tap_in_lon":   pending["tap_in_lon"],
                    }]))

                elif row["event_type"] == "tap_out" and pending is not None:
                    results.append({
                        "trip_id":      str(uuid.uuid4()),
                        "card_id":      card_id,
                        "bus_id":       pending["bus_id"],
                        "tap_in_time":  to_naive_utc(pending["tap_in_ts"]),
                        "tap_out_time": to_naive_utc(event_dt),
                        "tap_in_lat":   pending["tap_in_lat"],
                        "tap_in_lon":   pending["tap_in_lon"],
                        "tap_out_lat":  float(row["latitude"]),
                        "tap_out_lon":  float(row["longitude"]),
                    })
                    pending = None
                    state.remove()

        if pending:
            state.update(pd.DataFrame([{
                "tap_in_tx_id": pending["tap_in_tx_id"],
                "bus_id":       pending["bus_id"],
                "tap_in_ts_us": to_epoch_us(pending["tap_in_ts"]),
                "tap_in_lat":   pending["tap_in_lat"],
                "tap_in_lon":   pending["tap_in_lon"],
            }]))
        elif state.exists:
            state.remove()

        state.setTimeoutDuration(2 * 60 * 60 * 1000)

        if results:
            out = pd.DataFrame(results)
            out["tap_in_time"]  = pd.to_datetime(out["tap_in_time"]).astype("datetime64[ns]")
            out["tap_out_time"] = pd.to_datetime(out["tap_out_time"]).astype("datetime64[ns]")
            out["tap_in_lat"]   = out["tap_in_lat"].astype("float64")
            out["tap_in_lon"]   = out["tap_in_lon"].astype("float64")
            out["tap_out_lat"]  = out["tap_out_lat"].astype("float64")
            out["tap_out_lon"]  = out["tap_out_lon"].astype("float64")
            yield out
        else:
            yield EMPTY_TRIPS

    return (
        parsed_df
        .groupBy("card_id")
        .applyInPandasWithState(
            func             = pair_func,
            outputStructType = TRIP_OUTPUT_SCHEMA,
            stateStructType  = STATE_SCHEMA,
            outputMode       = "append",
            timeoutConf      = GroupStateTimeout.ProcessingTimeTimeout,
        )
    )

# ============================================================
# 3. CROWD FLAGGING: deteksi bus penuh
# ============================================================
def detect_crowd(parsed_df: DataFrame) -> DataFrame:

    def crowd_func(
        key: Tuple,
        pdf_iter: Iterator[pd.DataFrame],
        state: "GroupState"
    ):
        bus_id = key[0]

        tap_in_count  = 0
        tap_out_count = 0
       
        if state.exists:
            s = state.get
            print(f"[DEBUG] state type: {type(s)}, value: {s}")
            if isinstance(s, pd.DataFrame) and not s.empty:
                r = s.iloc[0]
                tap_in_count  = int(r.get("tap_in_count", 0))
                tap_out_count = int(r.get("tap_out_count", 0))

        flags = []

        last_pdf = None
        for pdf in pdf_iter:
            last_pdf = pdf
            for _, row in pdf.iterrows():
                if row["event_type"] == "tap_in":
                    tap_in_count += 1
                elif row["event_type"] == "tap_out":
                    tap_out_count += 1

        pending = max(0, tap_in_count - tap_out_count)

        if pending > CROWD_THRESHOLD and last_pdf is not None:
            flagged_at_ts = pd.Timestamp.utcnow().to_pydatetime().replace(tzinfo=None)
            last_lat  = float(last_pdf.iloc[-1]["latitude"])
            last_lon  = float(last_pdf.iloc[-1]["longitude"])
            segment   = current_segment(last_lat, last_lon)
            msg = (
                f"[CROWD FLAG] {flagged_at_ts} ALERT: Bus {bus_id} @ {segment} — "
                f"{pending} penumpang aktif (threshold: {CROWD_THRESHOLD})"
            )
            print(f"[CROWD FLAG] {msg}")
            flags.append({
                "bus_id":        bus_id,
                "pending_count": int(pending),
                "flagged_at":    flagged_at_ts,
                "segment":       segment,
                "message":       msg,
            })

        state.update(pd.DataFrame([{
            "tap_in_count":  int(tap_in_count),
            "tap_out_count": int(tap_out_count),
        }]))
        state.setTimeoutDuration(60 * 60 * 1000)

        if flags:
            out = pd.DataFrame(flags)
            out["flagged_at"]    = pd.to_datetime(out["flagged_at"]).astype("datetime64[ns]")
            out["pending_count"] = out["pending_count"].astype("int32")
            yield out
        else:
            yield EMPTY_FLAGS

    return (
        parsed_df
        .groupBy("bus_id")
        .applyInPandasWithState(
            func             = crowd_func,
            outputStructType = CROWD_FLAG_SCHEMA,
            stateStructType  = STATE_CROWD_SCHEMA,
            outputMode       = "append",
            timeoutConf      = GroupStateTimeout.ProcessingTimeTimeout,
        )
    )

# ============================================================
# 4. SINK
# ============================================================
def get_jdbc_url() -> str:
    return f"jdbc:postgresql://{NEON_HOST}:{NEON_PORT}/{NEON_DATABASE}?sslmode=require"

def get_jdbc_props() -> dict:
    return {
        "user":           NEON_USER,
        "password":       NEON_PASSWORD,
        "driver":         "org.postgresql.Driver",
        "batchsize":      "500",
        "isolationLevel": "READ_COMMITTED",
        "stringtype":    "unspecified",
        "options":        "-c timezone=UTC"
    }

def write_raw_to_db(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return
    (
        batch_df
        .withColumn("event_time", F.col("event_time").cast(TimestampType()))
        .select("transaction_id", "card_id", "bus_id", "event_type", "event_time", "latitude", "longitude")
        .write.jdbc(url=get_jdbc_url(), table="raw_events", mode="append", properties=get_jdbc_props())
    )
    print(f"[SINK] Batch {batch_id}: {batch_df.count()} raw events disimpan")

def write_trips_to_db(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return
    (
        batch_df
        .withColumn("tap_in_time",  F.col("tap_in_time").cast(TimestampType()))
        .withColumn("tap_out_time", F.col("tap_out_time").cast(TimestampType()))
        .select(                          # ← hapus tap_in_geom & tap_out_geom
            "trip_id", "card_id", "bus_id",
            "tap_in_time", "tap_out_time",
            "tap_in_lat", "tap_in_lon",
            "tap_out_lat", "tap_out_lon",
        )
        .write.jdbc(url=get_jdbc_url(), table="trips", mode="append", properties=get_jdbc_props())
    )
    print(f"[SINK] Batch {batch_id}: {batch_df.count()} trips disimpan")

def write_flags_to_db(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return
    (
        batch_df
        .withColumn("flagged_at", F.col("flagged_at").cast(TimestampType()))
        .select("bus_id", "flagged_at", "pending_count", "segment", "message")   # ← tambah segment
        .write.jdbc(url=get_jdbc_url(), table="crowd_flags", mode="append", properties=get_jdbc_props())
    )
    print(f"[SINK] Batch {batch_id}: {batch_df.count()} flags disimpan")

def write_dlq_to_hdfs(bad_df: DataFrame, batch_id: int):
    if bad_df.isEmpty():
        return
    path = f"{DLQ_BASE}/raw_bad/batch_id={batch_id}"
    bad_df.repartition(1).write.mode("append").parquet(path)
    print(f"[DLQ] Batch {batch_id}: {bad_df.count()} bad records -> {path}")

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("BUS SMART CARD — PYSPARK STREAMING JOB")
    print("=" * 60)

    spark = create_spark_session()

    good_df, bad_df = read_and_validate_stream(spark)
    good_df.printSchema()
    trips_df        = pair_tap_events(good_df)
    flags_df        = detect_crowd(good_df)

    raw_query = (
        good_df.writeStream
        .foreachBatch(write_raw_to_db)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw")
        .trigger(processingTime="10 seconds")
        .start()
    )

    trips_query = (
        trips_df.writeStream
        .foreachBatch(write_trips_to_db)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/trips")
        .trigger(processingTime="15 seconds")
        .start()
    )

    flags_query = (
        flags_df.writeStream
        .foreachBatch(write_flags_to_db)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/flags")
        .trigger(processingTime="10 seconds")
        .start()
    )

    dlq_query = (
        bad_df.writeStream
        .foreachBatch(write_dlq_to_hdfs)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/dlq")
        .trigger(processingTime="15 seconds")
        .start()
    )

    print("[SPARK] Semua query jalan. Ctrl+C untuk stop.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[SPARK] Stopping...")
        for q in [raw_query, trips_query, flags_query, dlq_query]:
            q.stop()
    finally:
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        print("[SPARK] Selesai.")

if __name__ == "__main__":
    main()