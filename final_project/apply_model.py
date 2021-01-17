from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, FloatType
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import functions as F


model_path = "als_trained"

model = ALSModel.load(model_path)

own_purchases = cass_baseline \
    .filter(F.col("name") == "random_5") \
    .select("list") \
    .rdd.flatMap(lambda x: x[0]) \
    .collect()