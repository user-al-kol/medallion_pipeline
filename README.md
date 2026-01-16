# Medallion Pipeline

## Purpose

This project is intended for:
- Demonstrating a medallion architecture implementation
- Practicing data engineering concepts
- Showcasing end-to-end batch data processing
- Serving as a foundation for analytical and BI workloads

## Project Structure

The project is organized into:
- Dedicated Dockerfiles per pipeline stage
- Isolated Python code per layer
- Shared volumes for data exchange
- Centralized logging



## Overview

This project is a data pipeline that follows the **medallion architecture** for data processing. 
It ingests **CSV files** as input and ultimately produces a **relational database consisting of fact and dimensional tables**, 
ready to be consumed by a **data analyst** for building a **semantic model** and generating final analytical reports.

The pipeline is designed to simulate a real-world **data engineering lifecycle**, focusing on data ingestion, transformation, and modeling using batch processing.

---

## Technologies Used

- **Docker** – Containerization and orchestration  
- **Docker Compose** – Multi-container pipeline orchestration  
- **Python** – Pipeline logic and orchestration scripts  
- **PySpark** – Distributed data processing and transformations  
- **SQLite** – Analytical database for the gold layer  

---

## Architecture Overview

The pipeline follows the **medallion architecture pattern**:


Each layer is implemented as an **independent Docker container**, orchestrated via a `docker-compose.yml` file. 
Although all containers run on the same Docker network, they communicate through **shared volumes** for simplicity and transparency.

The pipeline operates in **daily batch mode**, processing only the data corresponding to the current execution date.

---

## Pipeline Stages

### Raw Layer (Raw Container)

The raw layer simulates the **data acquisition (data generation) process**.  
In this project, there is no script that collects data from multiple sources, as the emphasis was placed on the subsequent stages of the data engineering lifecycle.

A Python script runs inside the raw container and continuously **monitors the `raw/` directory**. Whenever new CSV files appear, 
they are automatically forwarded to the next stage of the pipeline.

---

### Landing Layer (Landing Container)

The landing layer is responsible for **data ingestion**.  
At this stage, the data enters the system in **raw CSV format**.

A **partition-based incremental ingestion strategy** is applied:
- The container processes all new files from the `raw/` directory.
- Data is **partitioned by date**.
- Each execution ingests only the data corresponding to the current day.

This approach enables efficient incremental processing and prepares the data for downstream transformations.

---

### Bronze Layer (Bronze Container)

The bronze layer represents the first structured persistence of the data.

At this stage:
- Data is already **partitioned by date**.
- Only the **current day’s data** is processed.
- **Duplicate records are removed**.
- Data is written into a database table using **upsert logic**.

In this layer, the **data structure is enforced**:
- The number and meaning of columns are defined.
- Key fields are identified to support **data quality guarantees**.

However, the schema remains **loosely typed**:
- No strict schema constraints are applied.
- All columns are stored as **text (string type)** to maintain flexibility.

---

### Silver Layer (Silver Container)

The silver layer applies **data cleansing, normalization, and business-level transformations**.

Key responsibilities of this stage include:
- Applying business rules.
- Standardizing and normalizing data.
- Renaming and restructuring columns where necessary.
- Improving overall **data quality and consistency**.

The output of this layer consists of **clean, conformed tables** that represent core business entities and are suitable for analytical workloads.

---

### Gold Layer (Gold Container)

The gold layer is the **analytics-ready layer** of the pipeline.

At this stage:
- **Fact and dimension tables** are created.
- Business aggregations and calculations are applied.
- The data model is optimized for **analytical queries** and **business intelligence** use cases.

The final output is a **SQLite database** containing fact and dimensional tables, ready to be consumed by a **data analyst** for building a semantic model and final reports.

---

## Data Flow & Execution Model

- Data flows sequentially through each layer via shared volumes.
- Each stage processes data incrementally on a **daily basis**.
- Logs from all containers are stored centrally for monitoring and debugging.


## Deployment

- Update the `docker-compose.yml` file by setting the paths that point to your local **home directory**.
- The pipeline can be executed on any environment where **Docker** is supported. In this project, a **Linux remote server** is used.
- Start the pipeline using Docker Compose.
- Once all containers are up and running, copy the input CSV files from the `data/myfiles/` directory into the `data/raw/` directory to trigger the pipeline.

## Pipeline Architecture Diagram

```mermaid
flowchart LR
    A[Raw Layer] --> B[Landing Layer]
    B --> C[Bronze Layer]
    C --> D[Silver Layer]
    D --> E[Gold Layer]

    subgraph Data Sources
        F[CSV Files in data/myfiles/]
    end
    F --> A

    subgraph Output
        G[SQLite Database - Fact & Dimension Tables]
    end
    E --> G

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#bbf,stroke:#333,stroke-width:2px
    style C fill:#bfb,stroke:#333,stroke-width:2px
    style D fill:#ffb,stroke:#333,stroke-width:2px
    style E fill:#fbb,stroke:#333,stroke-width:2px
