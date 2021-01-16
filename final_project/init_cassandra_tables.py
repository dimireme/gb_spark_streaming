# coding=utf-8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("shadrin_final_cass_spark").getOrCreate()

######################################################################
# записываем в Кассандру фичи товаров
######################################################################
schema_item_features = StructType() \
    .add("product_id", IntegerType()) \
    .add("department", StringType()) \
    .add("category", StringType())

item_features = spark \
    .read \
    .format("csv") \
    .schema(schema_item_features) \
    .options(header=True) \
    .load('data/item_features.csv')

item_features.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="item_features", keyspace="shadrin_final") \
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

own_purchases = purchases \
    .groupBy(F.col("user_id")) \
    .agg(F.array_distinct(F.collect_list("item_id")).alias("item_id_list"))

own_purchases.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="own_purchases", keyspace="shadrin_final") \
    .mode("append") \
    .save()

######################################################################
# записываем файл топ 5 товаров из top_5_by_category.csv в Кассандру
######################################################################
# TODO: move it to model re-fit batch

schema_top_5 = StructType() \
    .add("product_id", IntegerType())

top_5_df = spark \
    .read \
    .format("csv") \
    .schema(schema_top_5) \
    .options(header=True) \
    .load('data/top_5_by_category.csv')

# [995242, 1082185, 1127831, 5569230, 951590]
top_5_list = top_5_df \
    .select('product_id') \
    .rdd.flatMap(lambda x: x) \
    .collect()

# [1185870, 1492723, 8019418, 894845, 12814320]
random_5_list = purchases.select("item_id") \
    .distinct() \
    .orderBy(F.rand(seed=42)) \
    .limit(5) \
    .rdd.flatMap(lambda x: x) \
    .collect()


# Записываем бэйзлайны в таблицу shadrin_final:baseline.
spark.createDataFrame(
    [
        ('top_5', top_5_list),
        ('random_5', random_5_list),
    ],
    ['name', 'list']
).write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="baseline", keyspace="shadrin_final") \
    .mode("append") \
    .save()




