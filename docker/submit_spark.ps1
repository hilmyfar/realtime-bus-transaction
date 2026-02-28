# submit_spark.ps1
# Jalankan dari folder manapun di project:
#   .\docker\submit_spark.ps1

docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-client:3.3.4,org.apache.hadoop:hadoop-hdfs-client:3.3.4,org.apache.spark:spark-avro_2.12:3.5.1 `
  --conf spark.sql.shuffle.partitions=6 `
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 `
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true `
  --conf "spark.driver.extraJavaOptions=-Divy.home=/root/.ivy2" `
  --conf "spark.executor.extraJavaOptions=-Divy.home=/root/.ivy2" `
  /app/spark/spark_streaming.py