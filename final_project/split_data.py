# coding=utf-8
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, ArrayType, FloatType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

spark = SparkSession.builder.appName("shadrin_final_spark").getOrCreate()

######################################################################
# Записываем часть даных на
######################################################################
schema_purchases = StructType() \
    .add("user_id", IntegerType()) \
    .add("item_id", IntegerType()) \
    .add("quantity", IntegerType()) \
    .add("sales_value", FloatType())

purchases = spark \
    .read \
    .format("csv") \
    .schema(schema_purchases) \
    .options(header=True) \
    .load('data/purchases.csv')

user_id,item_id,quantity,sales_value
schema_purchases = StructType() \
    .add("product_id", IntegerType())

top_5 = spark \
    .read \
    .format("csv") \
    .schema(schema_top_5) \
    .options(header=True) \
    .load('data/top_5_by_category.csv')

top_5.write \
    .format("parquet") \
    .save("historical_purchases/train.parquet")