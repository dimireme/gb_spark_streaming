# coding=utf-8
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, ArrayType, FloatType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

import pyspark.ml.recommendation as foo

spark = SparkSession.builder.appName("shadrin_final_spark").getOrCreate()

######################################################################
# записываем файл топ 5 товаров из top_5_by_category.csv в Кассандру
######################################################################

schema_top_5 = StructType().add("product_id", IntegerType())

top_5 = spark \
    .read \
    .format("csv") \
    .schema(schema_top_5) \
    .options(header=True) \
    .load('data/top_5_by_category.csv')


top_5_list = top_5.select(F.collect_list('product_id')).first()[0]
# [995242, 1082185, 1127831, 5569230, 951590]

# Записываем массив топ 5 популярных товаров в таблицу shadrin_final:top_5 по ключу "top".
spark.createDataFrame(
    [('top', top_5_list)],
    ['name', 'list']
).write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="top_5", keyspace="shadrin_final") \
    .mode("append") \
    .save()

######################################################################
# записываем в Кассандру собственные покупки пользователей
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

