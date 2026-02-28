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
│   ├── schema.sql                   ← DDL: tabel + materialized views (Ga sempet masukin sini, ada di PPT)
│   └── trigger.sql                  ← Ga sempet masukin sini, ada di PPT
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
│       └── spark_streaming.py       ← PySpark job (dijalankan VIA Docker, pakai submit_spark)
│
├── config/
│   └── .env                         ← Ga gw masukin github sorry
│
└── README.md
```
## Link dokumentasi tambahan dan penjelasan singkat projek
https://docs.google.com/presentation/d/1H7mT8G3gNuvcaZcc9fQzJ--ERehL-bBzgTsPzm760Tk/edit?usp=sharing

## Langkah
1. docker compose-up //jalanin docker
2. python app/producer/setup_topic.py //buat topic
3. python app/producer/producer.py //prodcer jalan
4. powershell -ExecutionPolicy Bypass -File .\docker\submit_spark.ps1 //consume jalan
