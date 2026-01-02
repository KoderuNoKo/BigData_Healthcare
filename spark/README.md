# Spark Jobs

- This directory contains all the spark jobs to be used in this project

## Directory Structure

```
├───conf
├───modules
    └───__pycache__
└───# spark jobs
```
Where:
- `conf` contains the `.properties` file defining settings for spark jobs
- `modules` common dependencies of spark jobs

## How to run

- By default, this directory is mounted to `spark` container
- To submit a job, run this command in `spark` container
```bash
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.driver.extraJavaOptions='-Dlog4j.configurationFile=/app/conf/log4j2.properties' \
    --py-files modules.zip \
    <spark_job>.py
```
- Note that `./modules` must be compressed to `.zip` every time changes is made. It is the zipped file that is submit to spark, not the folder