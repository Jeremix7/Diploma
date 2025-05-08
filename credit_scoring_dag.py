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
    def create_spark():
        from src.functions import create_spark_client

        return create_spark_client()

    @task
    def extract_data(spark):
        from src.functions import get_data_from_db
        from src.table_models import LoanData

        return get_data_from_db(LoanData)

    @task
    def transform_data(spark, df):
        from src.functions import transform_data_for_model

        return transform_data_for_model(spark, df)

    @task
    def load_model_and_predict(transformed_data):
        from pyspark.ml.classification import LogisticRegressionModel

        model_save_path = "/opt/airflow/models/logistic_regression_model"
        loaded_model = LogisticRegressionModel.load(model_save_path)
        predictions = loaded_model.transform(transformed_data)
        return predictions.select("id", "prediction").toPandas()

    @task
    def save_results(predictions_df):
        from src.functions import save_data_to_db
        from src.table_models import PredLoanData

        save_data_to_db(PredLoanData, predictions_df)

    @task
    def close_spark(spark):
        spark.stop()

    # Поток выполнения задач
    spark_session = create_spark()
    data_df = extract_data(spark_session)
    transformed_df = transform_data(spark_session, data_df)
    predictions_df = load_model_and_predict(transformed_df)
    save_task = save_results(predictions_df)
    stop_spark = close_spark(spark_session)

    # Устанавливаем зависимости
    (
        spark_session
        >> data_df
        >> transformed_df
        >> predictions_df
        >> save_task
        >> stop_spark
    )


credit_scoring_dag = credit_scoring_pipeline()
