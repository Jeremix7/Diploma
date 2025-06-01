# Diploma - Credit Scoring Data Pipeline

This project is a prototype of an automated data processing pipeline for credit scoring in the banking sector. It is built using modern open-source tools and is designed to be scalable, modular, and easy to deploy in both local and cloud environments.

## Architecture

![Pipeline Architecture](https://github.com/Jeremix7/Diploma/blob/master/img/Architecture.png?raw=true)

The system consists of the following components:

- **PostgreSQL** – stores input customer data and model prediction results.
- **Spark** – performs batch processing, data preprocessing, and model inference using Spark MLlib.
- **Airflow** – orchestrates the pipeline with DAGs that automate data extraction, preprocessing, model inference, and writing results to the database.
- **Kafka** – buffers and decouples incoming customer application data to ensure reliable real-time ingestion and scalability.
- **Superset** – visualizes model prediction metrics, customer characteristics, and other key KPIs.
- **Docker** – containers for all system components for simplified deployment and scaling.

## Pipeline Process

1. New customer loan applications are ingested through Kafka.
2. Airflow triggers a Spark job every 5 minutes to:
   - Extract data from PostgreSQL
   - Preprocess and normalize the data
   - Load a pre-trained model from disk
   - Classify customers as creditworthy or not
   - Write results back to PostgreSQL
3. Materialized views are updated to allow fast Superset visualizations.
4. Superset dashboards reflect real-time model predictions and analytics.

## Technologies Used

- `PostgreSQL`
- `Spark` + `Spark MLlib`
- `Airflow`
- `Kafka`
- `Superset`
- `Docker`
- `Python` (pandas, numpy, SQLAlchemy, kafka-python, pyspark)

## Deployment

To run the entire system locally:

```bash
docker-compose up --build
```

This will start:
- Airflow (scheduler, webserver, worker)
- PostgreSQL
- Kafka
- Superset

## Monitoring

![Dashboards](https://github.com/Jeremix7/Diploma/blob/master/img/Dashboard.jpg?raw=true)

- Airflow Web UI shows DAG execution status, logs, task dependencies.
- Superset provides dashboards and charts for tracking loan application scoring statistics.

## Future Enhancements

- Automatic model retraining DAGs on new data
- Model monitoring (AUC, precision/recall tracking)
- Segment-wise models for different customer groups
- Alerts for anomalies or system failures
- Cloud migration to platforms like AWS or Yandex.Cloud
