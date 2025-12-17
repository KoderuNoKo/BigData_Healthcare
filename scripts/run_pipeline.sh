#!/bin/bash
# define all the commands to run the full pipeline
#!run each part INDIVIDUALLY, they all infinite jobs, running this script as a whole will cause lock
set -euo pipefail

# part 1 - produce data + stream to kafka topic (background)
docker exec -d skibidi bash -c "
  source ./.venv/bin/activate &&
  python3 -m KafkaProducer.main
"

sleep 10


# part 2 - spark jobs (-d to run in backgroud)
docker exec spark bash -c "
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.driver.extraJavaOptions='-Dlog4j.configurationFile=/app/conf/log4j2.properties' \
    --py-files modules.zip \
    job_stream_bronze.py
"

sleep 10

# part 3 - monitor job (foreground, print to console)
docker exec spark bash -c "
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.driver.extraJavaOptions='-Dlog4j.configurationFile=/app/conf/log4j2.properties' \
    --py-files modules.zip \
    stream_monitor.py
"
