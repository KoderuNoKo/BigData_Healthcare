# Project Setup Guide

## Prerequisites

- Docker and Docker Compose installed
- Python 3.x installed
- Bash shell access

## Getting Started

### 1. Start Docker Services

Build and start all three Docker containers:

```bash
docker compose up -d --build
```

### 2. Set Up Kafka Producer

Open a terminal and run one of the following Kafka producer scripts:

```bash
python3 ./kafka_producer/kafka_producer.py
```

**OR**

```bash
python3 ./kafka_producer/kafka_loop.py
```

### 3. Set Up Spark

Open a new terminal and execute the following commands:

#### Make the setup script executable:
```bash
chmod +x spark_setup.sh
```

#### Run the Spark-Kafka setup script:
```bash
bash setup_spark_kafka.sh
```

#### Enter the Spark container:
```bash
docker exec -it skibidi /bin/bash
```

#### Submit the Spark job:
```bash
spark-submit spark.py
```

## Notes

- Ensure Kafka is running before submitting the Spark job
- Keep the Kafka producer terminal running while processing data
- The Spark container is named `skibidi`