# coding=utf-8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, FloatType
from pyspark.sql import functions as F
import subprocess

spark = SparkSession.builder.appName("shadrin_final_topic_spark").getOrCreate()

######################################################################
# Разбиваем датафрейм на тренировочный и тестовый 50/50
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

shuffled = purchases.orderBy(F.rand())
train, test = shuffled.randomSplit([0.5, 0.5])
train = train.orderBy(F.rand())
test = test.orderBy(F.rand())


######################################################################
# Сохраняем train и test на HDFS в разные папки
######################################################################

# на всякий случай
subprocess.call(["hdfs", "dfs", "-rm", "-r", "for_topic"])
subprocess.call(["hdfs", "dfs", "-rm", "-r", "for_train"])

test.repartition(1).write.csv("for_topic")
train.repartition(1).write.csv("for_train")

train.count()
# 1188916

test.count()
# 1190432

######################################################################
# Записываем тестовые данные в топик shadrin_purchases
######################################################################

# читаем файлы в стриме
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema_purchases) \
    .options(path="for_topic", header=False) \
    .load()

# указываем одну из нод с кафкой
kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

# пишем стрим в Кафку
def kafka_sink(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "shadrin_purchases") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "shadrin_purchases_kafka_checkpoint") \
        .start()


stream = kafka_sink(raw_files, 30)

# На этом этапе нужно посмотреть в соседней консоли как пишется стрим.

# По завершении останавливаю стрим.
stream.stop()

######################################################################
# Проверяем, что записалось в топик
######################################################################
raw_purchases = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "shadrin_purchases"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "10"). \
    load()

parsed_purchase = raw_purchases \
    .select(F.from_json(F.col("value").cast("String"), schema_purchases).alias("value"), "offset") \
    .select("value.*", "offset")


def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .options(truncate=False) \
        .start()


stream = console_output(parsed_purchase, 5)
stream.stop()
