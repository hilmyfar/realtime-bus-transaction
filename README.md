# REAL‑TIME BUS USAGE MONITORING

## Struktur Workspace

```
realtime-bus-transactions/
│
├── docker/                          ← SEMUA konfigurasi Docker
│   ├── docker-compose.yml           ← Konfigurasi services docker
│   ├── submit_spark.ps1             ← Submit PySpark job ke container
│   └── hdfs/                        ← Data HDFS (auto-generated) [Ga gw masukin github sorry]
│       ├── namenode/
│       └── datanode/
│
├── db/                              ← SQL untuk database Neon
│   ├── schema.sql                   ← DDL: tabel + materialized views (jalankan pertama)
│   └── geofencing.sql               ← Snap koordinat ke halte terdekat
│
├── schema/
│   └── bus_transaction.avsc         ← Avro schema untuk Kafka message
│
├── app/                             ← Semua kode Python (di-mount ke container Spark)
│   ├── simulator/
│   │   └── simulate.py              ← Generate data transaksi simulasi
│   ├── producer/
│   │   ├── setup_topic.py           ← Buat Kafka topic + register Avro schema 
│   │   └── producer.py              ← Kirim event ke Kafka
│   └── spark/
│       └── spark_streaming.py       ← PySpark job (dijalankan VIA Docker, bukan submit_spark)
│
├── config/
│   └── .env                         ← Ga gw masukin github sorry
│
└── README.md
```
