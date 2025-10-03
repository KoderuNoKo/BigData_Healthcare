# Project: Build an application with Big Data Analytics in Healthcare

## Topic details:
Big data analytics for IoT-based electricity/water meters
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
|-- misc
|-- scripts
`-- spark_jobs
```

# Guide for Developers

- It is recommended to run and develop the project within an Ubuntu environment using Docker. The current setup already covers the initialization of a Docker Ubuntu container. Below is a simple guide to set up the dev environment.

### 1. Build container
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

```shell
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

### 4. Running Jupyter notebooks in VS Code

- Install the **Jupyter** and **Python** and **Dev Containers** extensions in VS Code.
- Make sure your container is running. Then connect VS Code to the container with the **Remote Explorer** extension.
- Open any `.ipynb` file.
- In the top-right kernel selector, pick the interpreter inside your `.venv` (usually `.venv/bin/python`).
- Run notebook cells as usual inside VS Code.