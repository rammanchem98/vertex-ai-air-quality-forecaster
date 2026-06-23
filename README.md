# Multi-City Air Quality Forecasting Engine (MLOps on GCP)

An end-to-end, asynchronous, and production-grade MLOps system built on Google Cloud Platform (GCP). The architecture decouples the live streaming data ingestion layer, the automated offline machine learning training loops, and the daily batch inference execution cycles using Kubeflow Pipelines (KFP) V2 and Vertex AI.

---

## 🏗️ System Architecture & Data Flow


The project is structured into three decoupled layers interacting via managed storage boundaries:

1. **Live Data Ingestion (Cloud Run Jobs & BigQuery)**
   * A Python worker script fetches real-time atmospheric and pollution metrics from the OpenWeather API for 10 global cities.
   * Features include ambient temperature, pressure, humidity, precipitation, and absolute $PM_{2.5}$ density measurements.
   * Target classes (`low`, `medium`, `high`, `hazardous`) are calculated dynamically before appending records to `world_weather_dataset.world_air_quality`.

2. **Training Pipeline (`training_pipeline_v1.py`)**
   * Processes archival datasets extracted from Cloud Storage.
   * Performs data engineering, splits resources into training, validation, and testing partitions, and normalizes inputs with Scikit-Learn's `StandardScaler`.
   * Fits a multinomial Logistic Regression classifier and registers it to the Vertex Model Registry.
   * **The Artifact Promotion Pattern:** Promotes the production model and normalization structures to static, permanent aliases inside GCS (`.../production/latest_model.pkl` and `latest_scaler.pkl`), completely decoupling training dependencies from prediction runs.

3. **Batch Prediction & Monitoring Pipeline (`batch_prediction.py`)**
   * Triggered automatically on a cron cadence by a managed Vertex AI Pipeline Schedule.
   * Utilizes `dsl.importer` nodes to dynamically reference the static production artifacts.
   * Extracts historical inference input parameters via BigQuery queries, runs classifications, drops timezone configurations, and pushes forecasts into `world_weather_dataset.predictions_output`.
   * Evaluates system state synchronization via an inner join check against real-world observations, logging active tracking values directly onto the Vertex AI canvas interface.

---

## 📂 Repository Layout

```text
├── .gcloudignore                # Prevents massive local caches/virtual envs from building
├── .gitignore                   # Keeps auxiliary model artifacts and pipeline json maps local
├── Dockerfile                   # Continuous Integration base layer containing structural setups
├── cloudbuild.yaml              # Core Cloud Build multi-stage image deployment pipeline
├── config-dev.yaml              # Unified environment variable configurations profile
├── requirements.txt             # Strict, explicit analytical processing dependencies
├── training_pipeline_v1.py      # Production-hardened KFP training graph
├── batch_prediction.py          # Scheduled production inference and evaluation KFP graph
├── schedule_prediction.py       # Orchestration script registering Vertex Cron Schedules
├── ingest_data.py               # API polling engine designed for Cloud Run Job deployment
└── src/
    └── preprocessing.py         # Shared cleaning and tracking transformation formulas