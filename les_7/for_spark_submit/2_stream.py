from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime

spark = SparkSession.builder.appName("shadrin_spark").getOrCreate()
schema = StructType() \
    .add("product_category_name", StringType()) \
    .add("product_category_name_english", StringType())

raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()

load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path","my_submit_parquet_files/p_date=" + str(load_time)) \
        .option("checkpointLocation", "checkpionts/my_parquet_checkpoint") \
        .start()

timed_files = raw_files.withColumn("p_date", F.lit("load_time"))

stream = file_sink(timed_files,10)

#will always spark.stop() at the end
