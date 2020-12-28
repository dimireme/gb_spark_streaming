from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime

spark = SparkSession.builder.appName("shadrin_spark").getOrCreate()

schema = StructType() \
    .add("President", StringType()) \
    .add("Took_office", StringType()) \
    .add("Left_office", StringType())

raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()

def file_sink(df, freq):
    return df.writeStream.foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("checkpointLocation", "checkpionts/my_parquet_checkpoint") \
        .start()

def foreach_batch_function(df, epoch_id):
    load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    print("START BATCH LOADING. TIME = " + load_time)
    df.withColumn("p_date", F.lit("load_time")) \
        .write \
        .mode("append") \
        .parquet("my_submit_parquet_files/p_date=" + str(load_time))
    print("FINISHED BATCH LOADING. TIME = " + load_time)

stream = file_sink(raw_files, 10)

while(True):
    print("I'M STILL ALIVE")
    stream.awaitTermination(9)

# unreachable
spark.stop()