# Домашние задания по курсу Spark Streaming.

### Список занятий


[Урок 1. Spark Streaming. Тестовые стримы.](/les_1/README.md)

[Урок 2. Kafka. Архитектура.](/les_2/README.md) 

[Урок 3. Spark Streaming. Чтение Kafka, чтение файлов в реальном времени.](/les_3/README.md)

[Урок 4. Spark Streaming. Sinks.](/les_4/README.md)

[Урок 5. Spark Streaming. Stateful streams.](/les_5/README.md)

[Урок 6. Spark Streaming. Cassandra.](/les_6/README.md)

[Урок 7. Spark Submit. Lambda архитектура.](/les_7/README.md)

[Урок 8. Spark Streaming + Spark ML + Cassandra. Применение ML-модели в режиме реального времени.](/les_8/README.md)

### Документация

[Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

[Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

[Spark By Examples](https://sparkbyexamples.com)

### Консолидированные знания

#### Spark Streamig

Spark streamig - инструмент для настройки маршрута сообщений. 

Для запуска консоли спарка используется sh-файл `/spark2.4/bin/pyspark`, если нужно испотзовать отдельную версию библиотеки. Или команда `pyspark`. Во втором случае запускается версия спарка, установленная на кластере.

Для выполнения файлов с кодом используется sh-файл `/spark2.4/bin/spark-submit`. Подробнее на [занятии 7](/les_7/README.md).


#### Чтение стрима

Читать можно из файлов или из Кафки (оба варианта [в занятии 3](/les_3/README.md)). Что именно читаем определяется форматом при создании стрима. 
 
* Файл: `spark.readStream.format("csv")`. Вместо `csv` можно указать  или `parquet`, `orc`, `json` и т.д.
* Кафка: `spark.readStream.format("kafka")`.
* Тестовые стримы: `spark.readStream.format("socket")`, или с указанием `rate`. Стрим `rate` зазбирался [на первом занятии](/les_1/README.md).

Следующие записи эквивалентны:

```python
spark.readStream.option("sep", ";").schema(userSchema).csv("/path/to/directory")
```

```python
spark.readStream.option("sep", ";").schema(userSchema).format("csv").load("/path/to/directory")
```

В примерах кода для чтения стримов используется функция `console_output`. Это аналог `.show()` в батчевом чтении данных. 


#### Запись стрима

Синк определяется форматом записи: `df.writeStream.format("console").start()`.
В синке `foreach batch` вместо формата указывается функция, которая будет отбрабатывать микробатчи: `.foreachBatch(foreach_batch_function)`.

Тип записи определяется форматом: `df.writeStream.format("console")`. 

Писать стрим можно в:
 - консоль (`console`, применяется почти в каждом уроке для визуализации данных в стриме);
 - в файл (`parquet`, `orc`, `json`, `csv`, etc.);
 - в Кафку `.format("kafka")`;
 - в память `.format("memory")` (используется для тестов);
 - и в `.foreachBatch(fn)`.
  
Все синки кроме консоли рассмотрены [в занятии 4](/les_4/README.md).


#### Kafka

Kafka это распределённая key-value система транспорта сообщений, гарантирующая хронологический порядок сообщения внутри одной партиции. Примеры с созданием топика, чтением и записью в консоль разбирались на втором занятии. 

Консольный клиент Кафки рассмотрен на [занятии 2](/les_2/README.md).

При чтении данных из Кафки ([занятие 3](/les_3/README.md)) и при записи данных в Кафку ([занятие 4](/les_4/README.md)) нужно указывать различные параметры.

Один брокер Кафки может читать несколько провайдеров (в опции `subscribe` указываются через запятую). При этом у топиков провайдера должна совпадать схема.


#### Lambda архитектура. 

Разделение потока данных на Speed layer и Batch layer. Стрим актуален в течение короткого времени, а аггрегаты при инициализации стрима вычисляются из медленного хранилища. Далее аггрегаты пересчитываются по данным из стрима. 

Стрим используется для мониторинга, а для отчётов используются более стабильные данные из медленного хранилища (более стабильные, потому что проще отсеять дубли).


#### Cassandra (HBase)

Cassandra key-value распределённая база данных, позволяющая создать что-то вроде индекса в хадупе. Плюсы: быстрое чтение, быстрая запись, масштабирование. 

HBase более консистентная база данных, Cassandra более доступная.

Высокая скорость чтения и записи позволяет реализовать джойн больших статических таблиц со стримом ([занятие 8](/les_8/README.md)).