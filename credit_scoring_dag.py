import datetime
import logging
import pendulum
import importlib
import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

from datetime import datetime, timedelta
from airflow.decorators import dag, task


default_args = {
    "owner": "iaisupov",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 4, 23, tz="UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}


@dag(
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
    tags=["credit", "model"],
    max_active_runs=1,
)
def credit_scoring_pipeline():

    @task
    def produce_to_kafka():
        logger.info("Producing requests to Kafka.")
        from src.kafka.producer import send_loan_requests

        send_loan_requests()
        logger.info("Kafka messages sent.")

    @task
    def read_data_from_kafka():
        logger.info("Reading IDs from Kafka.")
        from src.kafka.consumer import get_kafka_data

        ids = get_kafka_data()
        logger.info(f"Read {len(ids)} IDs from Kafka.")
        return ids

    @task
    def create_spark():
        logger.info("Creating Spark session.")
        from src.functions import create_spark_client

        spark = create_spark_client()
        logger.info("Spark session successfully created.")
        return spark

    @task
    def extract_data(ids_data):
        logger.info("Getting data from the database.")
        from src.functions import get_data_from_db
        from src.table_models import LoanData

        df = get_data_from_db(LoanData, ids_data)
        logger.info(f"Data successfully extracted from DB.")
        return df

    @task
    def transform_data(spark, df):
        logger.info("Transforming data for model.")
        from src.functions import transform_data_for_model

        transformed_df = transform_data_for_model(spark, df)
        logger.info("Data transformation complete.")
        return transformed_df

    @task
    def load_model_and_predict(transformed_data):
        logger.info("Loading model and generating predictions.")
        from pyspark.ml.classification import LogisticRegressionModel

        model_save_path = "/opt/airflow/models/logistic_regression_model"
        loaded_model = LogisticRegressionModel.load(model_save_path)
        predictions = loaded_model.transform(transformed_data)
        pandas_df = predictions.select("id", "prediction").toPandas()
        logger.info(f"Predictions complete. {len(pandas_df)} rows predicted.")
        return pandas_df

    @task
    def save_results(predictions_df):
        logger.info("Saving predictions to DB.")
        from src.functions import save_data_to_db
        from src.table_models import PredLoanData

        save_data_to_db(PredLoanData, predictions_df)
        logger.info("Predictions successfully saved.")

    @task
    def close_spark(spark):
        spark.stop()
        logger.info("Spark session stopped.")

    @task
    def update_materialized_view():
        logger.info("Refreshing materialized view.")
        from src.functions import refresh_materialized_view

        refresh_materialized_view("loan_data_with_predictions_mv")
        logger.info("Materialized view successfully refreshed.")

    produce = produce_to_kafka()
    ids_data = read_data_from_kafka()
    spark = create_spark()
    df = extract_data(ids_data)
    transformed_df = transform_data(spark, df)
    predictions_df = load_model_and_predict(transformed_df)
    save_task = save_results(predictions_df)
    refresh_mv = update_materialized_view()
    stop_spark = close_spark(spark)

    (
        produce
        >> ids_data
        >> spark
        >> df
        >> transformed_df
        >> predictions_df
        >> save_task
        >> refresh_mv
        >> stop_spark
    )


credit_scoring_dag = credit_scoring_pipeline()
