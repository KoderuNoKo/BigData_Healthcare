# Overview

- The core idea of the project is to study the handling, processing and analysis of Big Data. Study + Simulate the solutions to address Big Data problems (The Vs of Big Data), specifically, in this project, we aim to address *Volume* and *Velocity* :
	- Volume: the project is built upon the MIMIC-IV dataset, a really large dataset whose size of up to ~100 GBs
	- Velocity: we will simulate the velocity of data by building a streaming pipeline with Apache Kafka to process the streaming of the above dataset into the system. 

## Technologies
- This project will primarily revolves around the following technologies and tools:
	- Apache Kafka: Handle the simulated stream of data into the system
	- Apache Spark: Subscribe to the data stream returned by Kafka, perform processing and analytic works, prepared to be showed.
	- Tableau: Analytic module, read the processed data from Spark. Perform analytic + visualization works

# Problem Statement

- Build a system with the ability to handle a large and high velocity data stream. Implement a pipeline from Kafka -> Spark -> Tableau 
- Specifically, with the MIMIC-IV dataset. We aim to create a robust and comprehensive  dashboard showing summary and visualization of patient status. While also provides abnormality detection.

# Feature Selection

- MIMIC-IV is a huge dataset, we will begin by first selecting a subset of features in the table 

- Modules 
	- Hosp
	- ICU
	- ED

- Patient vitals life signs & diagnostic 
	- `ICU.chartevents` charted items occurring during the ICU stay. Contains the majority of information documented in the ICU.
	- `Hosp.labevents`  stores the results of all laboratory measurements made for a single patient. These include hematology measurements, blood gases, chemistry panels, and less common tests such as genetic assays.
	- `ED.vitalsign` Patients admitted to the emergency department have routine vital signs taken ever 1-4 hours. These vital signs are stored in this table.
		- Processing with this table can be challenging with a great number of missing values.
	- (Extension) `ICU.inputevents` Information documented regarding continuous infusions or intermittent administrations.
	- (Extension) `ICU.outputevents` Information regarding patient outputs including urine, drainage, and so on.

# Simulation & Processing

### **System Workflow Idea**

- **Batch layer**: Preprocess static tables into **dimension tables** (patients, admissions, diagnosis) stored in a database (Postgres or Parquet).
    
- **Stream layer**: Spark reads Kafka topics → applies transformations/aggregations (e.g., rolling average of HR, detecting abnormal labs).
    
- **Visualization layer**: Tableau connects to Spark/Postgres for both **historical baselines** and **simulated streaming dashboards**.

### 1. **Batch Processing (static, no `charttime`)**

These are datasets where values are fixed per patient or admission, not continuously measured over time. They make excellent **contextual features** (baseline) for any diagnostic/predictive model.

### 2. **Streaming Simulation (time-series with `charttime`)**

This is where Kafka comes in. You can treat these tables as “IoT sensor data” from patients, publishing them row by row according to their timestamp.

# 