## Урок 5. Spark Streaming. Stateful streams.

##### Задание 1. Запустить агрегацию по временному окну в разных режимах.

1\.1\. Подключаемся к серверу

```bash
ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```


1\.2\. Запускаем `pyspark`. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ export SPARK_KAFKA_VERSION=0.10
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --master local[1]
```

1\.3\. Прочитаем топик `shadrin_iris`, вспомним что лежит в Кафке.

Далее делаем стандартные импорты и определяем константы, схему данных и сам стрим, который читает топик `shadrin_iris` из Кафки.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

raw_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "shadrin_iris"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "6"). \
    load()

schema = StructType() \
    .add("sepalLength", FloatType()) \
    .add("sepalWidth", FloatType()) \
    .add("petalLength", FloatType()) \
    .add("petalWidth", FloatType()) \
    .add("species", StringType())
```
   
1\.4\. WATERMARK и дубликаты внутри одного батча.

Waterwark - одна из настроек стрима для сохранения состояния этого стрима внутри чекпойнта. Добавим колонку с временем обработки микробатча. Она понадобится для настройки чекпойнта и waterwark.
   
```python
extended_iris = raw_data \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset") \
    .withColumn("receive_time", F.current_timestamp())

extended_iris.printSchema()
```

    root
     |-- sepalLength: float (nullable = true)
     |-- sepalWidth: float (nullable = true)
     |-- petalLength: float (nullable = true)
     |-- petalWidth: float (nullable = true)
     |-- species: string (nullable = true)
     |-- offset: long (nullable = true)
     |-- receive_time: timestamp (nullable = false)


В методе `console_output` обязательно указываем папку, в которой будет храниться чекпойнт. 

```python
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("checkpointLocation", "checkpoints/duplicates_console_chk") \
        .options(truncate=False) \
        .start()
```

Запускаем стрим и смотрим как растёт наш чекпойнт (команда `hdfs dfs -du -h checkpoints/duplicates_console_chk`).

```python
stream = console_output(extended_iris , 5)
stream.stop()
```

Задаём waterwark, которая должна очищать чекпоинт. Первый параметр - назване колонки, на которую смотрит waterwark, второй параметр - гарантированное время жизни информации о сообщении в чекпойнте. Именно для этого мы добавляли столбец `receive_time`.

```python
waterwarked_iris = extended_iris.withWatermark("receive_time", "30 seconds")
waterwarked_iris.printSchema()
```

Схема не поменялась. Waterwark только следит за чекпойнтом, но никак не аффектит наши данные. 

Теперь данные можно проверить на наличие дубликатов. Дубли проверяем по двум колонкам: `species` и `receive_time`. Таким образом будут отсеиваться дубли по полю `species` внутри одного микробатча, так как столбец `receive_time` для всех записей внутри этого микробатча одинаковый. 

```python
deduplicated_iris = waterwarked_iris.drop_duplicates(["species", "receive_time"])
```

Чтобы всё заработало, нужно предварительно очистить чекпойнт (команда `hdfs dfs -rm -r checkpoints/duplicates_console_chk`).

```python
stream = console_output(deduplicated_iris , 20)
```
    -------------------------------------------                                     
    Batch: 7
    -------------------------------------------
    +-----------+----------+-----------+----------+-------+------+-----------------------+
    |sepalLength|sepalWidth|petalLength|petalWidth|species|offset|receive_time           |
    +-----------+----------+-----------+----------+-------+------+-----------------------+
    |4.4        |3.2       |1.3        |0.2       |setosa |42    |2020-12-23 22:28:00.003|
    +-----------+----------+-----------+----------+-------+------+-----------------------+
    
    -------------------------------------------                                     
    Batch: 8
    -------------------------------------------
    +-----------+----------+-----------+----------+----------+------+-----------------------+
    |sepalLength|sepalWidth|petalLength|petalWidth|species   |offset|receive_time           |
    +-----------+----------+-----------+----------+----------+------+-----------------------+
    |7.0        |3.2       |4.7        |1.4       |versicolor|50    |2020-12-23 22:28:20.003|
    |5.3        |3.7       |1.5        |0.2       |setosa    |48    |2020-12-23 22:28:20.003|
    +-----------+----------+-----------+----------+----------+------+-----------------------+
    
    -------------------------------------------                                     
    Batch: 9
    -------------------------------------------
    +-----------+----------+-----------+----------+----------+------+-----------------------+
    |sepalLength|sepalWidth|petalLength|petalWidth|species   |offset|receive_time           |
    +-----------+----------+-----------+----------+----------+------+-----------------------+
    |6.5        |2.8       |4.6        |1.5       |versicolor|54    |2020-12-23 22:28:40.005|
    +-----------+----------+-----------+----------+----------+------+-----------------------+


В каждом микробатче только записи с уникальным значением по колонке `species`.

```python
stream.stop()
```

Без указания в `drop_duplicates` столбца с временнОй меткой, дубликаты отсеивались бы по всем данным. И все эти данные хранились бы в чекпойнте, даже не смотря на то что у нас настроен waterwark. Чтобы не допустить раздувания чекпойнта, указывается столбец с временнОй меткой. Waterwark ограничивает "глубину" данных не только для поиска дубликатов, но и для любой группирующей функции. 

1\.5\. WINDOW и дубликаты за периоды времени.




#WINDOW - получаем дедупликацию в промежуток времени
#создаем временное окно
windowed_orders = extended_orders.withColumn("window_time",F.window(F.col("order_receive_time"),"2 minute"))
windowed_orders.printSchema()

stream = console_output(windowed_orders , 20)
stream.stop()
#устанавливаем вотермарк для очистки чекпоинта
waterwarked_windowed_orders = windowed_orders.withWatermark("window_time", "1 minute")
#удаляем дубли в каждом окне
deduplicated_windowed_orders = waterwarked_windowed_orders.drop_duplicates(["order_status", "window_time"])

stream = console_output(deduplicated_windowed_orders , 10)
stream.stop()


#SLIDING WINDOW - еще больше окон
#создаем временное окно
sliding_orders = extended_orders.withColumn("sliding_time",F.window(F.col("order_receive_time"),"1 minute","30 seconds"))
sliding_orders.printSchema()

stream = console_output(sliding_orders , 20)
stream.stop()
#устанавливаем вотермарк для очистки чекпоинта
waterwarked_sliding_orders = sliding_orders.withWatermark("sliding_time", "2 minute")
#удаляем дубли в каждом окне
deduplicated_sliding_orders = waterwarked_sliding_orders.drop_duplicates(["order_status", "sliding_time"])

stream = console_output(deduplicated_sliding_orders , 20)
stream.stop()



#OUTPUT MODES - считаем суммы
def console_output(df, freq, out_mode):
    return df.writeStream.format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .option("checkpointLocation", "checkpoints/my_watermark_console_chk2") \
        .outputMode(out_mode) \
        .start()

count_orders = waterwarked_windowed_orders.groupBy("window_time").count()
#пишем только обновляющиеся записи
stream = console_output(count_orders , 20, "update")
stream.stop()

#пишем все  записи
stream = console_output(count_orders , 20, "complete")
stream.stop()

#пишем все записи только один раз
stream = console_output(count_orders , 20, "append") #один раз в конце вотермарки
stream.stop()

sliding_orders = waterwarked_sliding_orders.groupBy("sliding_time").count()
#наблюдаем за суммами в плавающем окне
stream = console_output(sliding_orders , 20, "update")
stream.stop()







##### Задание 2. Сджойнить стрим со статикой.


##### Задание 3. Сджойнить стрим со стримом.
