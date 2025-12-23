zip -r modules.zip ./modules -x "*/__pycache__/*";

spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.driver.extraJavaOptions="-Dlog4j.configurationFile=/app/conf/log4j2.properties" \
    --py-files modules.zip job4_load_fact_wh.py;