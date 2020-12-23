## Урок 4. Spark Streaming. Sinks.

##### Задание 1. Задание как обычно осознать что было на уроке. Повторить со своими данными.


1\.1\. Подключаемся к серверу

```bash
ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```


1\.2\. Запускаем `pyspark`. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ export SPARK_KAFKA_VERSION=0.10
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --master local[1]
```

Здесь указываем локально усановленный Spark, версия которого 2.4 выше версии установленной на кластере 2.3. Это нужно для использования синка `ForeachBatch`, который появился только в версии Spark 2.4. 
 
В параметрах запуска `--packages` указывает пакет Кафки, который будем использовать. Параметр `--driver-memory` ограничивает объем оперативной памяти, которую может занять наше приложение.



1\.3\. Прочитаем топик `shadrin_iris`, вспомним что лежит в Кафке.

Стандартные импорты и константы.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType
kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"
```

Создаем стрим, читаем из Кафки.

```python
raw_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "shadrin_iris"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()
```

Задаём структуру данных, котора содержится в топике.

```python
schema = StructType() \
    .add("sepalLength", FloatType()) \
    .add("sepalWidth", FloatType()) \
    .add("petalLength", FloatType()) \
    .add("petalWidth", FloatType()) \
    .add("species", StringType())
```
   
Преобразуем данные в соответствии со схемой.
   
```python
parsed_iris = raw_data \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset")
```

Задаём метод для вывода стрима на консоль.

```python
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()
```

Проверяем.

```python
out = console_output(parsed_iris, 5)
```


    -------------------------------------------                                     
    Batch: 0
    -------------------------------------------
    +-----------+----------+-----------+----------+-------+------+
    |sepalLength|sepalWidth|petalLength|petalWidth|species|offset|
    +-----------+----------+-----------+----------+-------+------+
    |5.1        |3.5       |1.4        |0.2       |setosa |0     |
    |4.9        |3.0       |1.4        |0.2       |setosa |1     |
    |4.7        |3.2       |1.3        |0.2       |setosa |2     |
    |4.6        |3.1       |1.5        |0.2       |setosa |3     |
    |5.0        |3.6       |1.4        |0.2       |setosa |4     |
    +-----------+----------+-----------+----------+-------+------+

Всё работает, данные в топике прежние.

```python
out.stop()
```

1\.4\. MEMORY SINK

Метод для записи стрима в оперативную память. Обязательный параметр `queryName` задаёт название таблицы в памяти Спарка, в которой будут храниться прочитанные даные. Эта таблица существует только в рамках сессии и есть только в памяти драйвера. Из-за этого этот синк не параллелится и используется только для тестов. 

```python
def memory_sink(df, freq):
    return df.writeStream.format("memory") \
        .queryName("my_memory_sink_table") \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()
```

Запускаем стрим.

```python
stream = memory_sink(parsed_iris, 5)
```

Проверим что лежит в таблице `my_memory_sink_table`.

```python
spark.sql("select * from my_memory_sink_table").show()
```

    +-----------+----------+-----------+----------+-------+------+
    |sepalLength|sepalWidth|petalLength|petalWidth|species|offset|
    +-----------+----------+-----------+----------+-------+------+
    |        5.1|       3.5|        1.4|       0.2| setosa|     0|
    |        4.9|       3.0|        1.4|       0.2| setosa|     1|
    |        4.7|       3.2|        1.3|       0.2| setosa|     2|
    |        4.6|       3.1|        1.5|       0.2| setosa|     3|
    |        5.0|       3.6|        1.4|       0.2| setosa|     4|
    |        5.4|       3.9|        1.7|       0.4| setosa|     5|
    |        4.6|       3.4|        1.4|       0.3| setosa|     6|
    |        5.0|       3.4|        1.5|       0.2| setosa|     7|
    |        4.4|       2.9|        1.4|       0.2| setosa|     8|
    |        4.9|       3.1|        1.5|       0.1| setosa|     9|
    |        5.4|       3.7|        1.5|       0.2| setosa|    10|
    |        4.8|       3.4|        1.6|       0.2| setosa|    11|
    |        4.8|       3.0|        1.4|       0.1| setosa|    12|
    |        4.3|       3.0|        1.1|       0.1| setosa|    13|
    |        5.8|       4.0|        1.2|       0.2| setosa|    14|
    |        5.7|       4.4|        1.5|       0.4| setosa|    15|
    |        5.4|       3.9|        1.3|       0.4| setosa|    16|
    |        5.1|       3.5|        1.4|       0.3| setosa|    17|
    |        5.7|       3.8|        1.7|       0.3| setosa|    18|
    |        5.1|       3.8|        1.5|       0.3| setosa|    19|
    +-----------+----------+-----------+----------+-------+------+
    only showing top 20 rows

Посмотрим сколько записей в табличке. Это число постоянно увеличивается, так как стрим читает из Кафки по 5 сообщениий за раз. 

```python
spark.sql("select count(*) from my_memory_sink_table").show()
```

    +--------+
    |count(1)|
    +--------+
    |      70|
    +--------+

Останавливаем стрим. Статическая табличка с данными в памяти драйвера остаётся, но число записей в ней не изменяется.

```python
stream.stop()
```

1\.5\. FILE SINK (only with checkpoint)

Метод для записи стрима в файлы. Обязательная опция `path` указывает путь на HDFS, по которому будут сохраняться файлы. Обязательная опция `checkpointLocation` указывает на папку с чекпойнтами (аналогично как на занятии 3).

```python
def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path","my_parquet_sink") \
        .option("checkpointLocation", "shadrin_iris_file_checkpoint") \
        .start()
```

Запускаем стрим на некоторое время.

```python
stream = file_sink(parsed_iris, 5)
stream.stop()
```

Проверим как создались директории чекпойнтов и файлов синка.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -ls
Found 5 items
drwx------   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-15 00:00 .Trash
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-14 12:50 .sparkStaging
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-21 00:31 my_parquet_sink
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-15 00:48 shadrin_iris_console_checkpoint
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-21 00:31 shadrin_iris_file_checkpoint
```

Директории успешно созданы. В директории `my_parquet_sink` видим несколько файлов, которые успели записаться за время работы стрима. В каждом файле находится один микробатч (при создании стрима указывали 5 записей в микробатче). Как видим, каждый файл весит примерно 2кБ. Оптимальным будет размер файла равный размеру блока на HDFS.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -ls my_parquet_sink
Found 6 items
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-21 01:58 my_parquet_sink/_spark_metadata
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1732 2020-12-21 01:58 my_parquet_sink/part-00000-0b22e524-6a99-44dc-8bf8-eca4425415ff-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1726 2020-12-21 01:58 my_parquet_sink/part-00000-438bfaed-9ed1-42b5-ad16-51587627eaba-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1739 2020-12-21 01:58 my_parquet_sink/part-00000-47a47ad0-e7c4-4a19-a524-705a4c28989c-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1721 2020-12-21 01:58 my_parquet_sink/part-00000-6b87a662-cbbc-461a-9fe1-1dd5eaaef290-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1726 2020-12-21 01:58 my_parquet_sink/part-00000-fb5eeebe-8b53-4469-b121-6f18f4e7529b-c000.snappy.parquet
```

Так как на каждый файл в HDFS создаётся запись в name-ноде о месте расположения файла и его реплик, то большое количество файлов маленького размера могут тормозить работу с HDFS. Оптимальным будет писать файлы в директорию с партиционированием по дате (.option("path","my_parquet_sink/p_date=20201215")), а на следующий день собирать все файлы из директории предыдущего дня и склеивать их в несколько файлов большого размера. Это возможно, так как при таком подходе в директорию прошлых дней не будут добавлятсья файлы. В задании 2 написана функция для сжатия файлов.

1\.6\. KAFKA SINK

В отдельном окне терминала создадим топик `shadrin_iris_sink`, в который будем записывать сообщения (как на втором занятии).

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic shadrin_iris_sink --zookeeper bigdataanalytics-worker-0.novalocal:2181 --partitions 3 --replication-factor 2 --config retention.ms=-1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "shadrin_iris_sink".
```

Создаём консьюмер, который вычитыват сообщения из топика и пишет их в консоль.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-consumer.sh --topic shadrin_iris_sink --bootstrap-server bigdataanalytics-worker-0.novalocal:6667
```

В первом терминале pyspark определяем метод для записи в созданный топик. Здесь преобразуем наши данные из структуры в строку. В отличии от настройки чтения из Кафки, топик указывается не в опции `subscribe`, а в опции `topic`. 


```python
def kafka_sink(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(struct(*) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "shadrin_iris_sink") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "shadrin_iris_kafka_checkpoint") \
        .start()
```

На некоторое время запускаем стрим.

```python
stream = kafka_sink(parsed_iris, 5)
stream.stop()
```

Во втором терминале наблюдаем что в топик `shadrin_iris_sink` пишутся данные:

```bash
[4.9, 3.0, 1.4, 0.2, setosa, 1]
[5.0, 3.6, 1.4, 0.2, setosa, 4]
[4.7, 3.2, 1.3, 0.2, setosa, 2]
[5.1, 3.5, 1.4, 0.2, setosa, 0]
[4.6, 3.1, 1.5, 0.2, setosa, 3]
[4.6, 3.4, 1.4, 0.3, setosa, 6]
[4.9, 3.1, 1.5, 0.1, setosa, 9]
[5.0, 3.4, 1.5, 0.2, setosa, 7]
[5.4, 3.9, 1.7, 0.4, setosa, 5]
[4.4, 2.9, 1.4, 0.2, setosa, 8]
[4.8, 3.0, 1.4, 0.1, setosa, 12]
[5.4, 3.7, 1.5, 0.2, setosa, 10]
[4.3, 3.0, 1.1, 0.1, setosa, 13]
[4.8, 3.4, 1.6, 0.2, setosa, 11]
[5.8, 4.0, 1.2, 0.2, setosa, 14]
[5.7, 4.4, 1.5, 0.4, setosa, 15]
[5.7, 3.8, 1.7, 0.3, setosa, 18]
[5.4, 3.9, 1.3, 0.4, setosa, 16]
[5.1, 3.8, 1.5, 0.3, setosa, 19]
[5.1, 3.5, 1.4, 0.3, setosa, 17]
```

Если прочитать топик `shadrin_iris_sink` ещё раз с начала, то данные будут уже более упорядоченны. Видно явное разделение сообщений по трём партициям. Не понятно, почему в режиме реального времени сообщения перемешиваются.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-consumer.sh --topic shadrin_iris_sink --from-beginning --bootstrap-server bigdataanalytics-worker-0.novalocal:6667
[4.7, 3.2, 1.3, 0.2, setosa, 2]
[5.4, 3.9, 1.7, 0.4, setosa, 5]
[4.4, 2.9, 1.4, 0.2, setosa, 8]
[4.8, 3.4, 1.6, 0.2, setosa, 11]
[5.8, 4.0, 1.2, 0.2, setosa, 14]
[5.1, 3.5, 1.4, 0.3, setosa, 17]
[4.9, 3.0, 1.4, 0.2, setosa, 1]
[5.0, 3.6, 1.4, 0.2, setosa, 4]
[5.0, 3.4, 1.5, 0.2, setosa, 7]
[5.4, 3.7, 1.5, 0.2, setosa, 10]
[4.3, 3.0, 1.1, 0.1, setosa, 13]
[5.4, 3.9, 1.3, 0.4, setosa, 16]
[5.1, 3.8, 1.5, 0.3, setosa, 19]
[5.1, 3.5, 1.4, 0.2, setosa, 0]
[4.6, 3.1, 1.5, 0.2, setosa, 3]
[4.6, 3.4, 1.4, 0.3, setosa, 6]
[4.9, 3.1, 1.5, 0.1, setosa, 9]
[4.8, 3.0, 1.4, 0.1, setosa, 12]
[5.7, 4.4, 1.5, 0.4, setosa, 15]
[5.7, 3.8, 1.7, 0.3, setosa, 18]
```

Так как мы делали преобразование структуры в строку, то информация о названгии ключей и типах данных потерялась. Для лучшей читаемости данных последующими консьюмрами, лучше преобразовывать данные в JSON-строку.

```python
def kafka_sink_json(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(to_json(struct(*)) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "shadrin_iris_sink") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "shadrin_iris_kafka_checkpoint") \
        .start()
```

Запустим на время стрим. 
 
 ```python
stream = kafka_sink_json(parsed_iris, 5)
stream.stop()
```

В соседнем терминале наблюдаем пришедшие данные.

```bash
{"sepalLength":5.1,"sepalWidth":3.7,"petalLength":1.5,"petalWidth":0.4,"species":"setosa","offset":21}
{"sepalLength":4.6,"sepalWidth":3.6,"petalLength":1.0,"petalWidth":0.2,"species":"setosa","offset":22}
{"sepalLength":5.4,"sepalWidth":3.4,"petalLength":1.7,"petalWidth":0.2,"species":"setosa","offset":20}
{"sepalLength":5.1,"sepalWidth":3.3,"petalLength":1.7,"petalWidth":0.5,"species":"setosa","offset":23}
{"sepalLength":4.8,"sepalWidth":3.4,"petalLength":1.9,"petalWidth":0.2,"species":"setosa","offset":24}
{"sepalLength":5.0,"sepalWidth":3.0,"petalLength":1.6,"petalWidth":0.2,"species":"setosa","offset":25}
{"sepalLength":5.0,"sepalWidth":3.4,"petalLength":1.6,"petalWidth":0.4,"species":"setosa","offset":26}
{"sepalLength":5.2,"sepalWidth":3.5,"petalLength":1.5,"petalWidth":0.2,"species":"setosa","offset":27}
{"sepalLength":5.2,"sepalWidth":3.4,"petalLength":1.4,"petalWidth":0.2,"species":"setosa","offset":28}
{"sepalLength":4.7,"sepalWidth":3.2,"petalLength":1.6,"petalWidth":0.2,"species":"setosa","offset":29}
{"sepalLength":4.8,"sepalWidth":3.1,"petalLength":1.6,"petalWidth":0.2,"species":"setosa","offset":30}
{"sepalLength":5.5,"sepalWidth":4.2,"petalLength":1.4,"petalWidth":0.2,"species":"setosa","offset":33}
```

Запись в Кафку сериализованного JSON-объекта (`"CAST(to_json(struct(*)) AS STRING) as value"`) предпочтительнее чем запись только значений сериализованных в строку (`CAST(struct(*) AS STRING) as value`) .

В конце удалим топик `shadrin_iris_sink`.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --zookeeper bigdataanalytics-worker-0.novalocal:2181 --delete --topic shadrin_iris_sink
Topic shadrin_iris_sink is marked for deletion.
Note: This will have no impact if delete.topic.enable is not set to true.
```

1\.7\. FOREACH BATCH SINK

Добавим к датасету `parsed_iris` метку времени, когда обрабатывался микробатч.

```python
extended_iris = parsed_iris.withColumn("my_current_time", F.current_timestamp())
```

В синке `foreach batch` вместо формата указывается функция, которая будет получать микробатч и его порядковый номер. Внутри функции с микробатчем можно работать как со статическим датафреймом.

```python
def foreach_batch_sink(df, freq):
    return  df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()
```

Функция, которая будет обрабатывать микробатч:

```python
def foreach_batch_function(df, epoch_id):
    print("starting epoch " + str(epoch_id) )
    print("average values for batch:")
    df.groupBy("species").avg().show()
    print("finishing epoch " + str(epoch_id))
```

Запустим стрим.

```python
stream = foreach_batch_sink(extended_iris, 5)
```

В консоли наблюдаем преобразованные микробатчи. Не наблюдаем колонку `my_current_time`, видимо по ней не отрабатывает аггрегационная функция.

    starting epoch 9
    average values for batch:
    +-------+-----------------+---------------+------------------+------------------+-----------+
    |species| avg(sepalLength)|avg(sepalWidth)|  avg(petalLength)|   avg(petalWidth)|avg(offset)|
    +-------+-----------------+---------------+------------------+------------------+-----------+
    | setosa|4.960000038146973|            3.4|1.4599999904632568|0.2200000047683716|       47.0|
    +-------+-----------------+---------------+------------------+------------------+-----------+
    
    finishing epoch 9
    starting epoch 10
    average values for batch:
    +----------+-----------------+------------------+-----------------+------------------+-----------+
    |   species| avg(sepalLength)|   avg(sepalWidth)| avg(petalLength)|   avg(petalWidth)|avg(offset)|
    +----------+-----------------+------------------+-----------------+------------------+-----------+
    |versicolor|6.460000038146973|2.9199999809265136|4.539999961853027|1.4399999856948853|       52.0|
    +----------+-----------------+------------------+-----------------+------------------+-----------+
    
    finishing epoch 10

Остановим стрим.

```python
stream.stop()
```

Внутри функции `foreach_batch_function` с микробатчем можно работать как со статическим датафреймом. Можно по фильтру разбивать данные на разные датафреймы, обогащать их и записывать в файлы по разным директориям.

##### Задание 2. Для DE написать стабильную функцию compaction.
 
```python
import subprocess 

def compact_directory(path):
    df_to_compact = spark.read.parquet(path + "/*.parquet")
    tmp_path = path + "_tmp"
    df_to_compact.write.mode("overwrite").parquet(tmp_path)
    df_to_compact = spark.read.parquet(tmp_path + "/*.parquet")
    df_to_compact.write.mode("overwrite").parquet(path)
    subprocess.call(["hdfs", "dfs", "-rm", "-r", tmp_path])
```

Тестируем.

```python
compact_directory("my_parquet_sink")
```

До упаковки:

```bash 
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -ls my_parquet_sink
Found 6 items
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-21 01:58 my_parquet_sink/_spark_metadata
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1732 2020-12-21 01:58 my_parquet_sink/part-00000-0b22e524-6a99-44dc-8bf8-eca4425415ff-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1726 2020-12-21 01:58 my_parquet_sink/part-00000-438bfaed-9ed1-42b5-ad16-51587627eaba-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1739 2020-12-21 01:58 my_parquet_sink/part-00000-47a47ad0-e7c4-4a19-a524-705a4c28989c-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1721 2020-12-21 01:58 my_parquet_sink/part-00000-6b87a662-cbbc-461a-9fe1-1dd5eaaef290-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1726 2020-12-21 01:58 my_parquet_sink/part-00000-fb5eeebe-8b53-4469-b121-6f18f4e7529b-c000.snappy.parquet
```

После упаковки:

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -ls my_parquet_sink
Found 2 items
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin          0 2020-12-21 01:59 my_parquet_sink/_SUCCESS
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       2062 2020-12-21 01:59 my_parquet_sink/part-00000-2d437995-1c62-48f4-a687-7a15cf2cd5cd-c000.snappy.parquet
```

Проверил, чекполйнт не сломался, стрим можно перезапустить, он будет добавлять в директорию `my_parquet_sink` новые файлы:

```
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -ls my_parquet_sink
Found 5 items
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin          0 2020-12-21 01:59 my_parquet_sink/_SUCCESS
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-21 02:01 my_parquet_sink/_spark_metadata
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       2062 2020-12-21 01:59 my_parquet_sink/part-00000-2d437995-1c62-48f4-a687-7a15cf2cd5cd-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1744 2020-12-21 02:01 my_parquet_sink/part-00000-3c1203ca-7a67-4244-b2d1-5929b76918a2-c000.snappy.parquet
-rw-r--r--   3 BD_274_ashadrin BD_274_ashadrin       1757 2020-12-21 02:01 my_parquet_sink/part-00000-f693807c-aa56-44f3-91d1-ba72666ddc01-c000.snappy.parquet
```

Проверил данные в файлах, в "упакованом файле" записи с офсетами 0-24, в остальных записи с офсетами 25-34. Только не уверен что при таком подходе после упаковки данные большого размера автоматически побьются на партиции по размерам блока на HDFS.
