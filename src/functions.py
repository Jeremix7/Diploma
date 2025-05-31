from src.configs import settings
from datetime import datetime
from src.database_client import database_client
from sqlalchemy import insert, text
from pyspark.sql import SparkSession
from src.table_models import LoanData, PredLoanData
from typing import List, Type, Union, Any
import random
import pandas as pd
import numpy as np

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql.session import SparkSession as SparkSessionType
from pyspark.sql import DataFrame as SparkDataFrame


def create_spark_client() -> SparkSessionType:
    """
    Create local Spark session.
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.jars", "/path/to/jdbc_driver.jar")
        .getOrCreate()
    )
    return spark


def spark_df_to_pandas_df(spark_df: SparkDataFrame) -> pd.DataFrame:
    """
    Convert spark DataFrame to a pandas DataFrame.
    """
    return spark_df.toPandas()


def insert_values_into_table(df: pd.DataFrame) -> None:
    """
    Insert records from pandas DataFrame into the LoanData table.
    """
    with database_client.engine.connect() as connection:
        records = df.to_dict("records")

        connection.execute(insert(LoanData), records)


def get_data_from_db(table_model: Type, id_list: List[int]) -> pd.DataFrame:
    """
    Retrieve records from the specified table using a list of IDs.
    """
    consumer_data = ", ".join([str(id) for id in id_list])
    query = f"SELECT * FROM {table_model.__tablename__} WHERE id IN ({consumer_data})"

    with database_client.engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()

        return pd.DataFrame(rows, columns=columns)


def save_data_to_db(table_model: Type, df: Union[pd.DataFrame, list]) -> None:
    """
    Save prediction results into the database table.
    """
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


def transform_data_for_model(spark: SparkSession, df: pd.DataFrame) -> SparkDataFrame:
    """
    Transform raw input data into a format suitable for ml model.
    """
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


def load_model(model_save_path: str, model: type) -> Any:
    """
    Load a machine learning model from the specified path.
    """
    return model.load(model_save_path)


def refresh_materialized_view(view_name: str) -> None:
    """
    Refresh the specified materialized view in the database.
    """
    with database_client.engine.connect() as connection:
        query = text(f"REFRESH MATERIALIZED VIEW {view_name}")
        connection.execute(query)
