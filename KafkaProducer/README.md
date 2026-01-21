# Producer Module

This module implements a **Apache Kafka producer** that publishes streaming data to a Kafka broker.

## Overview

The producer application connects to a Kafka broker and repeatedly sends messages to a specified topic.

## Features

- A multi-process Kafka producer, which read the data from a data source, and stream those data to a Kafka 
- For each data source configured, it spawn a producer process to stream that source to its designated topic. Data from source is read in batch, sends in stream.
- Support graceful shutdown

## Module Structure

```bash
├───config
├───producers
├───_coordinator
└───_utils
```
Whereas
- `config` module configuration
- `producers` base + concrete producer for each data source
- `_coordinator` managing processes
- `utils` utilities components
## Usage
### Run producers
- From the repository's root directory, run
	```bash
	python3 -m KafkaProducer.main
	```
	- This will run all enabled producers, check `main.py` for other usage
### Configuration
- In `./config/kafka_config.py`, contains configurations to create Kafka client for producers
- In `./config/dataset_config.py`, contains information about the data source: enable status, source file repository, target topics on broker, etc...