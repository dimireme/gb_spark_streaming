# coding=utf-8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, FloatType
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F
import time


spark = SparkSession.builder.appName("shadrin_final_foreach_spark").getOrCreate()

# указываем одну из нод с кафкой
kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

# загружаем модель
model_path = "als_trained"
model = ALSModel.load(model_path)
######################################################################
# Загружаем модель и сопутствующие датафреймы.
######################################################################


# Метрика precision@k показывает какую долю рекомендованных товаров покупал пользователь.
def precision_at_k(recs, own):
    flags = [1 if i in own else 0 for i in recs]
    return sum(flags) / float(len(recs))


precision_at_k_udf = F.udf(precision_at_k)

# Количество рекомендаций
k = 5

# Датафрейм с собственными покупками, для измерения качества рекомендаций
cass_own_purchases = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="own_purchases", keyspace="shadrin_final") \
    .load()

# Датафрейм с описанием товаров
cass_item_features = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="item_features", keyspace="shadrin_final") \
    .load()


def foreach_batch_sink(df, freq):
    return df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq) \
        .start()


def foreach_batch_function(df, epoch_id):
    start_time = time.time()
    # df
    user_ids = df.select("user_id").distinct()
    # python list
    user_ids_list = user_ids.select('user_id') \
        .rdd.flatMap(lambda x: x) \
        .collect()
    #
    own_purchases = cass_own_purchases \
        .filter(F.col("user_id").isin(user_ids_list)) \
        .withColumnRenamed("item_id_list", "own")
    #
    train_result = model.recommendForUserSubset(user_ids, k) \
        .selectExpr("user_id", "recommendations.item_id as recs") \
        .join(own_purchases, on=['user_id'], how="left") \
        .withColumn("precision_at_k", precision_at_k_udf(F.col("recs"), F.col("own"))) \
    #
    train_result.persist()
    #
    item_ids_list = df.select("item_id") \
        .distinct() \
        .rdd.flatMap(lambda x: x) \
        .collect()
    #
    item_features = cass_item_features \
        .filter(F.col("product_id").isin(item_ids_list)) \
        .withColumnRenamed("product_id", "item_id")
    #
    extended_df = df \
        .join(train_result.select("user_id", "recs", "precision_at_k"), on=['user_id'], how="left") \
        .join(item_features, on=['item_id'], how="left")
    extended_df.show()
    #
    avg_precision_at_k = train_result \
        .agg({"precision_at_k": "avg"}) \
        .rdd.flatMap(lambda x: x) \
        .collect()[0]
    print "avg_precision_at_k: ", avg_precision_at_k
    # сохраним батч на HDFS
    extended_df.write.mode("append").parquet("new_purchases")
    # выведем время выполнения батча
    duration = time.time() - start_time
    print "duration: ", duration
    train_result.unpersist()


schema_purchases = StructType() \
    .add("user_id", IntegerType()) \
    .add("item_id", IntegerType()) \
    .add("quantity", IntegerType()) \
    .add("sales_value", FloatType())

raw_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "shadrin_purchases"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "100"). \
    load()

parsed_data = raw_data \
    .select(F.from_json(F.col("value").cast("String"), schema_purchases).alias("value"), "offset") \
    .select("value.*", "offset")


s = foreach_batch_sink(parsed_data, 60)
s.stop()


