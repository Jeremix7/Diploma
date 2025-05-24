from configs import settings
from datetime import datetime
from database_client import database_client
from sqlalchemy import insert, text
from pyspark.sql import SparkSession
from table_models import LoanData, PredLoanData
import random
import pandas as pd
import numpy as np

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegressionModel


def create_spark_client():
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.jars", "/path/to/jdbc_driver.jar")
        .getOrCreate()
    )
    return spark


def spark_df_to_pandas_df(spark_df):
    return spark_df.toPandas()


def insert_values_into_table(df):
    with database_client.engine.connect() as connection:
        records = df.to_dict("records")

        connection.execute(insert(LoanData), records)


def get_data_from_db(table_model):
    with database_client.engine.connect() as connection:
        query = text(f"SELECT * FROM {table_model.__tablename__}")
        result = connection.execute(query)

        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pd.DataFrame(columns=columns)

        num_rows = random.randint(40, 200)

        random_rows = random.sample(rows, num_rows)

        return pd.DataFrame(random_rows, columns=columns)


def save_data_to_db(table_model, df):
    if isinstance(df, pd.DataFrame):
        dict_to_db = df.replace({np.nan: None}).to_dict(orient="records")
    elif isinstance(df, list):
        dict_to_db = df
    df = df.rename(columns={"id": "loan_data_id"})
    df["created_at"] = datetime.now()

    with database_client.engine.connect() as connection:
        table_name = table_model.__tablename__

        columns = df.columns.tolist()
        print("\n", columns, "\n")
        columns_str = ", ".join(columns)

        query = text(
            f"INSERT INTO {table_name} ({columns_str}) VALUES "
            f"({', '.join([f':{col}' for col in columns])})"
        )

        data = df.to_dict(orient="records")

        connection.execute(query, data)


def transform_data_for_model(spark, df):
    spark_df = spark.createDataFrame(df)
    target_column = "loan_describe_int"

    categorical_columns = ["term"]

    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
        for col in categorical_columns
    ]

    encoders = [
        OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_encoded")
        for col in categorical_columns
    ]

    pipeline = Pipeline(stages=indexers + encoders)

    df_encoded = pipeline.fit(spark_df).transform(spark_df)

    numeric_columns = [
        "last_fico_range_high",
        "last_fico_range_low",
        "int_rate",
        "fico_range_low",
        "acc_open_past_24mths",
        "dti",
        "num_tl_op_past_12m",
        "loan_amnt",
        "mort_acc",
        "avg_cur_bal",
        "bc_open_to_buy",
        "num_actv_rev_tl",
    ]

    encoded_columns = [f"{col}_encoded" for col in categorical_columns]
    final_feature_columns = numeric_columns + encoded_columns
    assembler = VectorAssembler(inputCols=final_feature_columns, outputCol="features")
    final_data = assembler.transform(df_encoded).select("id", "features", target_column)

    return final_data


def load_model(model_save_path, model):
    return model.load(model_save_path)


if __name__ == "__main__":
    spark = create_spark_client()
    df = get_data_from_db(LoanData)
    transformed_df = transform_data_for_model(spark=spark, df=df)

    model_save_path = (
        "/home/diploma/Diploma/credit_scoring_pipeline/src/logistic_regression_model"
    )
    loaded_model = LogisticRegressionModel.load(model_save_path)

    predictions = loaded_model.transform(transformed_df)
    result = predictions.select("id", "prediction").toPandas()
    save_data_to_db(table_model=PredLoanData, df=result)
    spark.stop()
