from configs import settings
from database_client import database_client
from sqlalchemy import insert, text
from pyspark.sql import SparkSession
from table_models import LoanData
import pandas as pd

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegressionModel



def create_spark_client():
    spark = SparkSession.builder\
                    .master("local[*]") \
                    .config("spark.executor.memory", "8g") \
                    .config("spark.driver.memory", "4g") \
                    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
                    .config("spark.jars", "/path/to/jdbc_driver.jar") \
                    .getOrCreate()
    return spark

def spark_df_to_pandas_df(spark_df):
    return spark_df.toPandas()

def insert_values_into_table(df):
    with database_client.engine.connect() as connection:
        # Преобразуем DataFrame в список словарей
        records = df.to_dict('records')
        
        # Выполняем вставку (id будет генерироваться автоматически)
        connection.execute(
            insert(LoanData),
            records
        )
        
def get_data_from_db(table_model, date, num_rows=100):
    with database_client.engine.connect() as connection:
        query = text(f"SELECT * FROM {table_model.__tablename__} WHERE issue_d = :date LIMIT :limit")
        
        # Выполнение запроса с параметрами
        result = connection.execute(
            query, 
            {'date': date, 'limit': num_rows}
        )
        
        rows = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(rows, columns=columns)

def transform_data_for_model(spark, df):
    spark_df = spark.createDataFrame(df)
    all_columns = spark_df.columns
    target_column = "loan_describe_int"

    categorical_columns = ["term"]

    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") for col in categorical_columns]

    encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_encoded") for col in categorical_columns]

    pipeline = Pipeline(stages=indexers + encoders)

    df_encoded = pipeline.fit(spark_df).transform(spark_df)

    numeric_columns = [
        "last_fico_range_high", "last_fico_range_low", "int_rate", "fico_range_low",
        "acc_open_past_24mths", "dti", "num_tl_op_past_12m", "loan_amnt",
        "mort_acc", "avg_cur_bal", "bc_open_to_buy", "num_actv_rev_tl"
    ]

    encoded_columns = [f"{col}_encoded" for col in categorical_columns]
    final_feature_columns = numeric_columns + encoded_columns
    assembler = VectorAssembler(inputCols=final_feature_columns, outputCol="features")
    final_data = assembler.transform(df_encoded).select("id", "features", target_column)
    return final_data
    

# spark = create_spark_client()
# data = spark.read.parquet('/home/diploma/Diploma/cleaned_df_after_2018.parquet')
# df = spark_df_to_pandas_df(data)


def load_model(model_save_path, model):
    return model.load(model_save_path)

if __name__ == "__main__":
    spark = create_spark_client()
    df = get_data_from_db(table_model=LoanData, date='2018-01-01')
    transformed_df = transform_data_for_model(spark=spark, df=df)
    from pyspark.ml.classification import LogisticRegressionModel
    model_save_path = "/home/diploma/Diploma/credit_scoring_pipeline/src/logistic_regression_model"
    loaded_model = LogisticRegressionModel.load(model_save_path)
    print('\n\n\n\n')
    print(loaded_model)
    print('\n\n\n\n')
    
    predictions = loaded_model.transform(transformed_df)
    predictions.select('id', 'loan_describe_int', 'prediction').show(50)