# Домашние задания по курсу Spark Streaming.

### Список занятий


[Урок 1. Spark Streaming. Тестовые стримы.](/les_1/hw.md)

[Урок 2. Kafka. Архитектура.](/les_2/hw.md) 

[Урок 3. Spark Streaming. Чтение Kafka, чтение файлов в реальном времени.](/les_3/hw.md)

[Урок 4. Spark Streaming. Sinks.](/les_4/hw.md)

[Урок 5. Spark Streaming. Stateful streams.](/les_5/hw.md)

[Урок 6. Spark Streaming. Cassandra.](/les_6/hw.md)

### Документация

[Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

[Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

[Spark By Examples](https://sparkbyexamples.com)

### Консолидированные знания

**Spark-streamig** - инструмент для настройки маршрута сообщений. 

##### Чтение стрима

Читать можно из файлов или из Кафки (оба варианта в занятии 3). Что именно читаем определяется форматом при создании стрима. 
 
Файл: `spark.readStream.format("csv")`. Вместо `csv` можно указать  или `parquet`, `orc`, `json` и т.д.
Кафка: `spark.readStream.format("kafka")`.
Тестовые стримы: `spark.readStream.format("socket")`, или с указанием `rate`. Стрим `rate` зазбирался на первом занятии.

Следующие записи эквивалентны:

```python
spark.readStream.option("sep", ";").schema(userSchema).csv("/path/to/directory")
```

```python
spark.readStream.option("sep", ";").schema(userSchema).format("csv").load("/path/to/directory")
```

##### Запись стрима

Синк определяется форматом записи: `df.writeStream.format("console").start()`.
В синке foreach batch вместо формата указывается функция, которая будет отбрабатывать микробатчи: `.foreachBatch(foreach_batch_function)`.

Тип записи определяется форматом: `df.writeStream.format("console")`. 

Писать стрим можно в:
 - консоль (`console`, применяется почти в каждом уроке для визуализации данных в стриме);
 - в файл (`parquet`, `orc`, `json`, `csv`, etc.);
 - в Кафку `.format("kafka")`;
 - в память `.format("memory")` (используется для тестов);
 - и в `.foreachBatch(fn)`.
  
Все синки кроме консоли рассмотрены в занятии 4. 

##### Kafka

Kafka это распределённая key-value система транспорта сообщений, гарантирующая хронологический порядок сообщения внутри одной партиции. Примеры с созданием топика, чтением и записью в консоль разбирались на втором занятии. 

При чтении данных из Кафки (занятие 3) и при записи данных в Кафку нужно указывать различные параметры.

Один брокер Кафки может читать несколько провайдеров (в опции `subscribe` указываются через запятую). При этом у топиков провайдера должна совпадать схема.


##### Ещё немного выводов.

В примерах кода для чтения стримов используется функция `console_output`. Это аналог `.show()` в батчевом чтении данных. 

Cassandra vs HBase

Cassandra k-v база,
индекс в хадупе,
быстрое чтение, запись,
масштабирование 

HBase более консистентная БД
Cassandra более доступная

Возможность джойнить большие статические таблицы со стримом. 