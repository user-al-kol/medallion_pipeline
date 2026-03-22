# Medallion Pipeline — LMS Data Engineering Project

A fully containerized, end-to-end batch data pipeline that transforms raw Learning Management System (LMS) CSV exports into an analytics-ready Kimball-style data mart — built entirely with open-source tools and runnable locally via Docker.

---

## Why This Project

Learning Management Systems generate rich behavioral data — course completions, assessment scores, learner activity — but it typically lives in flat CSV exports with no structure suitable for analysis. This project simulates the full data engineering lifecycle: ingesting that raw data, cleaning and conforming it layer by layer, and delivering a star schema data mart ready for a BI tool or semantic model.

The goal was to build something architecturally honest — no managed cloud services, no shortcuts — just Docker, Python, PySpark, and SQL doing real work.

---

## Architecture Overview

The pipeline follows the **medallion architecture** pattern (Raw → Landing → Bronze → Silver → Gold), where each layer is an independent Docker container. Containers communicate through shared volumes, making the data flow explicit and inspectable at every stage.

```
CSV Files (LMS exports)
       │
       ▼
┌─────────────┐
│  Raw Layer  │  File watcher — detects new CSV files and forwards them downstream
└──────┬──────┘
       │
       ▼
┌──────────────────┐
│  Landing Layer   │  Ingestion — partitions data by execution date (incremental)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Bronze Layer    │  Persistence — deduplication, upsert logic, loose typing
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Silver Layer    │  Cleansing — business rules, normalization, column conforming
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   Gold Layer     │  Kimball model — fact & dimension tables in SQLite
└──────────────────┘
         │
         ▼
  Analytics / BI Layer (semantic model, reports)
```

Each container is independently defined in a dedicated Dockerfile and orchestrated via `docker-compose.yml`. This design keeps concerns cleanly separated and makes individual stages easy to debug, replace, or scale independently.

---

## Tech Stack

| Tool | Role |
|---|---|
| **Docker + Docker Compose** | Container orchestration and pipeline execution |
| **Python** | Pipeline logic, orchestration scripts, file watching |
| **PySpark** | Distributed data transformation (Bronze → Silver) |
| **SQLite** | Analytical database for the Gold layer |
| **Git** | Version control |

No cloud account required. Runs on any machine with Docker installed.

---

## Key Engineering Decisions

### Incremental Ingestion by Date Partition
Rather than reprocessing all data on every run, the Landing layer partitions incoming files by execution date and processes only the current day's batch. This simulates production-grade incremental loading and keeps the pipeline efficient as data volume grows.

### Loose Typing in Bronze, Strict Typing in Silver
The Bronze layer stores all columns as strings — a deliberate choice to avoid ingestion failures from upstream data quality issues. Type casting and constraint enforcement happen in the Silver layer, once the data has been validated and understood. This mirrors how real pipelines handle schema instability from source systems.

### Kimball Star Schema in Gold
The Gold layer produces a classic star schema: fact tables capturing LMS events (completions, assessments, logins) joined to dimension tables (learners, courses, dates). This model is optimized for analytical queries and is directly consumable by a BI tool or semantic layer.

### Container-per-Layer Design
Each pipeline stage runs in its own container with its own Dockerfile. This isn't just for cleanliness — it means each stage can be developed, tested, and redeployed independently without touching the rest of the pipeline.

---

## Pipeline Stages in Detail

### Raw Layer
Monitors the `raw/` directory for new CSV files. When new files arrive, they are forwarded to the Landing stage. Simulates a data acquisition / landing zone pattern common in production data lake architectures.

### Landing Layer
Ingests CSV files with a partition-based incremental strategy. Each run processes only files corresponding to the current execution date, writing partitioned output ready for the Bronze stage.

### Bronze Layer
First structured persistence of the data. Applies deduplication via upsert logic, enforces column definitions, and writes to a persistent store. Data remains loosely typed (all columns as strings) to prioritize ingestion robustness over early constraint enforcement.

### Silver Layer
Applies business-level transformations: standardization, normalization, renaming, and restructuring. Outputs clean, conformed tables representing core LMS entities (learners, courses, activities) that are suitable for analytical workloads.

### Gold Layer
Produces the final Kimball-style data mart in SQLite. Fact and dimension tables are built with proper surrogate keys, grain definitions, and business aggregations. This layer is the handoff point to a data analyst or BI engineer.

---

## How to Run

**Prerequisites:** Docker and Docker Compose installed on your machine.

```bash
# 1. Clone the repository
git clone https://github.com/user-al-kol/medallion_pipeline.git
cd medallion_pipeline

# 2. Update volume paths in docker-compose.yml
#    Set the paths to point to your local home directory

# 3. Start the pipeline
docker-compose up

# 4. Trigger the pipeline
#    Copy the sample CSV files from data/myfiles/ into data/raw/
#    The Raw layer will detect them and kick off the full pipeline automatically
```

The pipeline runs in **daily batch mode**. Each execution processes the data corresponding to the current date. Logs from all containers are written to a centralized location for monitoring and debugging.

---

## Project Structure

```
medallion_pipeline/
├── Dockerfiles/          # One Dockerfile per pipeline stage
├── src/                  # Python source code per layer
├── data/
│   ├── myfiles/          # Sample input CSV files
│   └── raw/              # Drop files here to trigger the pipeline
├── docker-compose.yml    # Multi-container orchestration
└── README.md
```

---

## What I Would Add Next

- **Airflow or Prefect** for proper pipeline orchestration and scheduling (currently triggered manually)
- **Great Expectations** for data quality checks between layers
- **A Metabase or Evidence.dev dashboard** connected to the Gold SQLite database to close the full analytics loop
- **Unit tests** for the transformation logic in the Silver layer

---

## About

This project was built to demonstrate a production-style data engineering workflow using only local, open-source tooling — specifically to sidestep the dependency on cloud platforms while still implementing real architectural patterns used in industry.

It reflects hands-on experience with containerized pipeline design, incremental batch processing, and dimensional modeling.
