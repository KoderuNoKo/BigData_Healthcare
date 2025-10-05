#!/usr/bin/env bash
KAFKA_CONTAINER="cp-kafka"   # name of container in docker-compose (change if different)
BOOTSTRAP=localhost:9092

docker exec -i ${KAFKA_CONTAINER} kafka-topics --create \
  --bootstrap-server ${BOOTSTRAP} \
  --replication-factor 1 --partitions 6 --topic icu_vitals

docker exec -i ${KAFKA_CONTAINER} kafka-topics --create \
  --bootstrap-server ${BOOTSTRAP} \
  --replication-factor 1 --partitions 6 --topic ed_vitals

docker exec -i ${KAFKA_CONTAINER} kafka-topics --create \
  --bootstrap-server ${BOOTSTRAP} \
  --replication-factor 1 --partitions 3 --topic lab_results

docker exec -i ${KAFKA_CONTAINER} kafka-topics --create \
  --bootstrap-server ${BOOTSTRAP} \
  --replication-factor 1 --partitions 3 --topic diagnoses