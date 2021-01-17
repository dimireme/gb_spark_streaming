# coding=utf-8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, FloatType
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("shadrin_final_model_spark").getOrCreate()

######################################################################
# Разбиваем датафрейм на тренировочный и валидационный 80/20
######################################################################
schema_purchases = StructType() \
    .add("user_id", IntegerType()) \
    .add("item_id", IntegerType()) \
    .add("quantity", IntegerType()) \
    .add("sales_value", FloatType())

data = spark \
    .read \
    .format("parquet") \
    .schema(schema_purchases) \
    .load('historical_purchases')

train, valid = data.randomSplit([0.8, 0.2])

# Собственные покупки пользователя
own_purchases = data \
    .groupBy(F.col("user_id")) \
    .agg(F.array_distinct(F.collect_list("item_id")).alias("own"))


# Метрика precision@k показывает какую долю рекомендованных товаров покупал пользователь.
def precision_at_k (recs, own):
    flags = [1 if i in own else 0 for i in recs]
    return sum(flags) / float(len(recs))


precision_at_k_udf = F.udf(precision_at_k)

######################################################################
# Обучаем модель
######################################################################
als = ALS(maxIter=10,
          regParam=0.1,
          userCol="user_id",
          itemCol="item_id",
          ratingCol="quantity",
          coldStartStrategy="drop")

model = als.fit(train)

k = 5

users_train = train.select(als.getUserCol()).distinct()
users_valid = valid.select(als.getUserCol()).distinct()

train_result = model.recommendForUserSubset(users_train, k) \
    .selectExpr("user_id", "recommendations.item_id as recs") \
    .join(own_purchases, on=['user_id'], how="left") \
    .withColumn("precision_at_k", precision_at_k_udf(F.col("recs"), F.col("own")))  \
    .agg({"precision_at_k": "avg"})
train_result.show()

valid_result = model.recommendForUserSubset(users_valid, k) \
    .selectExpr("user_id", "recommendations.item_id as recs") \
    .join(own_purchases, on=['user_id'], how="left") \
    .withColumn("precision_at_k", precision_at_k_udf(F.col("recs"), F.col("own")))  \
    .agg({"precision_at_k": "avg"})
valid_result.show()

# Сохраняем модель
model.save("als_trained")
