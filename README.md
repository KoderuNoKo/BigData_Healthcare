# Project: Build an application with Big Data Analytics in Healthcare

## Topic details:
Big data analytics for IoT-based on 
- A tool generating data streaming like many electricity/water meters
- Data collection: using Kafka
- Data analytics: (1) filtering, sampling, integration, (2) your analytics goals – using Spark
- Visualization: using Tableau/Kibana
- Functions and performance evaluation

## Dataset
- MIMIC-IV data set, more details in `./docs/Dataset_Notes.md`

# Project Structure

```sh
.
|-- configs
|-- data
|   |-- mimic-iv-3.1
|   |   |-- hosp
|   |   `-- icu
|   `-- mimic-iv-ed-2.2
|       `-- ed
|-- docker
|   |-- base
|   |-- kafka
|   `-- spark
|-- docs
|-- kafka
|-- KafkaProducer
|-- misc
|-- scripts
|-- spark
`-- Tableau
```

# Guide for Developers

## Dev environment

- It is recommended to run and develop the project within an Ubuntu environment using Docker. The current setup already covers the initialization of a Docker Ubuntu container that will serve as the primary dev environment. Below is a simple guide to set up the dev environment.

### 1. Build environment (containers)

For the first run, in the main project repo, run:

```shell
docker compose up -d --build
```

### 2. Access the container

```shell
docker exec -it <container_name> /bin/bash
```

- Replace `<container_name>` with the actual container name.
- You can change the container name in `./docker-compose.yml`.
- By default, it is `skibidi`.

### 3. Python virtual environment & dependencies

- Python 3.12.x is available in the container (`python3`, `python3-venv`).
- Create a virtual environment:

```bash
python3 -m venv .venv
```

- Activate it:
```shell
source ./.venv/bin/activate
```

- Install dependencies:
```shell
pip install -r ./docker/base/requirements.txt
```

- Update the requirements file when needed:
```shell
pip freeze > ./docker/base/requirements.txt
```

## Data source

- The data used in this project is MIMIC-IV v3.1, specifically the modules: `hosp`, `icu`, and `ed`. The data itself require credentialed access.
- Once you get access to the data. There is 2 ways to feed data to the project

### 1. Local data storage (not recommended)

The first and simplest way is to download and extract the dataset into your machine. Placing them under the `./data` directory. However, the MIMIC-IV dataset is huge. (Fully extracted size can reach up to ~100 GBs)

### 2. Google BigQuery