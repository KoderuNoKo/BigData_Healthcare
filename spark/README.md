# Spark Jobs

- This directory contains all the spark jobs to be used in this project

## Directory Structure

```
├───conf
├───data
├───modules
    └───__pycache__
└───# spark jobs
```
Where:
- `conf` contains the `.properties` file defining settings for spark jobs
- `data` contains the csv data source file, used during development 
- `modules` common dependencies of spark jobs

## How to run

- By default, this directory is mounted to `spark` container
- To submit a job, adjust the script file `./run_spark_job.sh` to run a spark job