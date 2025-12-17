KAFKA_CONTAINER="cp-kafka"   # name of container in docker-compose (change if different)
BOOTSTRAP=localhost:9092

spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.driver.extraJavaOptions="-Dlog4j.configurationFile=/app/conf/log4j2.properties" \
    --py-files modules.zip job_read_process.py 