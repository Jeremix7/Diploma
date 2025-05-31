from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.jars", "/path/to/jdbc_driver.jar")
    .getOrCreate()
)

model_save_path = "logistic_regression_model"
loaded_model = LogisticRegressionModel.load(model_save_path)
