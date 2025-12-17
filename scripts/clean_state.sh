#!/bin/bash
# this script is used during development between test runs
# to clean the data + state of all components in the pipeline, including
# kafka broker message queue, minio data lake, postgresql data warehouse

# print executed commands
set -x

# kill all running spark jobs (not runnable, 4some reason, idk)
#? run 'pkill -f spark' manually from inside the spark container works
#? do that instead if necessary
# docker exec spark bash -c 'pkill -f spark'

# reset kafka topics
docker exec cp-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group spark-icu-consumer \
  --reset-offsets \
  --to-earliest \
  --execute \
  --topic icu_chartevents

# clean minio data lake
docker exec minio bash -c 'rm -rf /data/mimic-bronze/*'
docker exec minio bash -c 'rm -rf /data/mimic-silver/*'

# reset postgresql data warehouse
docker exec -it postgres psql -U dev -d mimic_dw -c \
  "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"