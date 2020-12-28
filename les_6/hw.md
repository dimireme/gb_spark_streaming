## Урок 6. Spark Streaming. Cassandra.

##### Задание 1. Поработать с Cassandra через консоль. Протестировать инсерты, селекты с разными ключами. Работать в keyspace lesson7. Там можно создать свои таблички.


Подключаемся к серверу (worker-2).

```bash
ssh BD_274_ashadrin@89.208.197.93 -i ~/.ssh/id_rsa_gb_spark
```

Запускаем консольный клиент Cassandra. Здесь указываем внутренний ip ноды, на которой запущена кассандра (worker-2). 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-2 ~]$ /cassandra/bin/cqlsh 10.0.0.18
Connected to Test Cluster at 10.0.0.18:9042.
[cqlsh 5.0.1 | Cassandra 3.11.8 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
```

Далее все команды в терминале кассандры. 

Выбираем keyspace:

```bash
cqlsh> use lesson7;
```

Создаём новую табличку:

```bash
cqlsh:lesson7> CREATE TABLE shadrin_animals
           ... (id int, 
           ... name text,
           ... size text,
           ... primary key (id));
```

Вставка записи:

```bash
cqlsh:lesson7> insert into shadrin_animals (id, name, size)
           ... values (3, 'Deer', 'Big');
cqlsh:lesson7> select * from shadrin_animals;

 id | name | size
----+------+------
  3 | Deer |  Big

(1 rows)
```

Апдейт записи с `id = 3`:

```bash
cqlsh:lesson7> insert into shadrin_animals (id, name)
           ... values (3, 'Doe');
cqlsh:lesson7> select * from shadrin_animals;

 id | name | size
----+------+------
  3 |  Doe |  Big

(1 rows)
```

Вставка ещё одной записи:

```bash
cqlsh:lesson7> insert into shadrin_animals (id, name)
           ... values (5, 'Snake');
cqlsh:lesson7> select * from shadrin_animals;

 id | name  | size
----+-------+------
  5 | Snake | null
  3 |   Doe |  Big

(2 rows)
```

Удаление по ключу не отработает. Это особенность консольной утилиты.

```bash
cqlsh:lesson7> delete id from shadrin_animals where id = 3;
InvalidRequest: Error from server: code=2200 [Invalid query] message="Invalid identifier id for deletion (should not be a PRIMARY KEY part)"
```

Удалить запись можно, затерев старые значения.

```bash
cqlsh:lesson7> insert into shadrin_animals (id, name, size)
           ... values (3, null, null);
cqlsh:lesson7> select * from shadrin_animals;

 id | name  | size
----+-------+------
  5 | Snake | null
  3 |  null | null

(2 rows)
```

В конце удалим табличку:

```bash
cqlsh:lesson7> drop table shadrin_animals;
cqlsh:lesson7> select * from shadrin_animals;
InvalidRequest: Error from server: code=2200 [Invalid query] message="unconfigured table shadrin_animals"
cqlsh:lesson7> exit;
```

Проверим как выполняется `count` по большой таблице. 

```bash
cqlsh:lesson7> use keyspace1;
cqlsh:keyspace1> SELECT table_name FROM system_schema.tables where keyspace_name = 'keyspace1';

 table_name
---------------
       clients
    users_many
 users_unknown

cqlsh:keyspace1> select count(*) from users_many;
OperationTimedOut: errors={'10.0.0.18': 'Client request timeout. See Session.execute[_async](timeout)'}, last_host=10.0.0.18
```

Они не выполняются.

###### HBASE

Тут повторим все те же операции для другой базы. 

Пришлось переподключится к worker-0, так как на втором заканчивалась память и с HBase там поработать не получилось.

```bash
ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```

Запускаем консольный клиент:

```bash
hbase shell
```

Создаём новую табличку:

```bash
hbase(main):006:0> create 'lesson7:shadrin_animals', 'name', 'size'
Created table lesson7:shadrin_animals
Took 1.4840 seconds                                                                                                                                                                                                                                              
=> Hbase::Table - lesson7:shadrin_animals
```

Вставка записи:

```bash
hbase(main):007:0> put 'lesson7:shadrin_animals', '3', 'name', 'Deer'
Took 0.1339 seconds                                                   
hbase(main):008:0> put 'lesson7:shadrin_animals', '3', 'size', 'Big'
Took 0.0449 seconds   
hbase(main):009:0> scan 'lesson7:shadrin_animals'
ROW                                                               COLUMN+CELL                 
 3                                                                column=name:, timestamp=1609115122292, value=Deer
 3                                                                column=size:, timestamp=1609115150780, value=Big
1 row(s)
Took 0.0123 seconds  
```

Апдейт записи с `id = 3`:

```bash
hbase(main):010:0> put 'lesson7:shadrin_animals', '3', 'name', 'Doe'
Took 0.0178 seconds
hbase(main):011:0> scan 'lesson7:shadrin_animals'
ROW                                                               COLUMN+CELL
 3                                                                column=name:, timestamp=1609115487679, value=Doe
 3                                                                column=size:, timestamp=1609115150780, value=Big
1 row(s)
Took 0.0213 seconds  
```

Вставка ещё одной записи:

```bash
hbase(main):012:0> put 'lesson7:shadrin_animals', '5', 'name', 'Snake'
Took 0.0128 seconds
hbase(main):013:0> scan 'lesson7:shadrin_animals'
ROW                                                               COLUMN+CELL
 3                                                                column=name:, timestamp=1609115487679, value=Doe
 3                                                                column=size:, timestamp=1609115150780, value=Big
 5                                                                column=name:, timestamp=1609115573124, value=Snake
2 row(s)
Took 0.0107 seconds  
```

Удаление всех колонок по ключу:

```bash
hbase(main):016:0> deleteall 'lesson7:shadrin_animals', '3'
Took 0.0083 seconds
hbase(main):017:0> scan 'lesson7:shadrin_animals'
ROW                                                               COLUMN+CELL
 5                                                                column=name:, timestamp=1609115573124, value=Snake
1 row(s)
Took 0.0336 seconds 
```

В конце удалим табличку:

```bash
hbase(main):018:0> disable 'lesson7:shadrin_animals'
Took 2.4847 seconds
hbase(main):019:0> drop 'lesson7:shadrin_animals'
Took 1.4006 seconds
hbase(main):020:0> scan 'lesson7:shadrin_animals'
ROW                                                               COLUMN+CELL

ERROR: Unknown table lesson7:shadrin_animals!

hbase(main):021:0> exit
```

В HBase нету большой таблички, поэтому проверить, выполняется ли count, проверить не сможем. 

##### Задание 2. Когда cassandra станет понятна, поработать с ней через Spark.

Запускаем pyspark с указанием библиотеки для работы с cassandra.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ export SPARK_KAFKA_VERSION=0.10
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]
```

Делаем стандартные импорты и читаем табличку `lesson7.animals`. В формате чтения указываем коннектор к базе данных cassandra. Параметры подключения к БД заданы в конфигах pyspark, поэтому здесь их не указываем.

```python
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F

cass_animals_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .load()

cass_animals_df.printSchema()
```

    root
     |-- id: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- size: string (nullable = true)


Посмотрим, что есть в таблице: 

```python
cass_animals_df.show()
```

    +---+--------------+-----+                                                      
    | id|          name| size|
    +---+--------------+-----+
    |  5|         Snake| null|
    | 11|          Bull|Bddig|
    |  8|          Flea|small|
    |  2|          Doom| null|
    |  3|           Doe| null|
    |  4|Justice League|Small|
    +---+--------------+-----+

Создадим запись с ключем 11 и добавим её в таблицу.

```python
cow_df = spark.sql("""select 11 as id, "Cow" as name, "Big" as size """)
cow_df.show()
```

    +---+----+----+
    | id|name|size|
    +---+----+----+
    | 11| Cow| Big|
    +---+----+----+

Добавляем с указанием режима `append`.

```python
cow_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .mode("append") \
    .save()

cass_animals_df.show()
```

    +---+--------------+-----+
    | id|          name| size|
    +---+--------------+-----+
    |  5|         Snake| null|
    | 11|           Cow|  Big|
    |  8|          Flea|small|
    |  2|          Doom| null|
    |  3|           Doe| null|
    |  4|Justice League|Small|
    +---+--------------+-----+
    
Не смотря на то что при записи указывался режим `append`, фактически был произведён `update` записи, так как такой ключ уже существовал в таблице.

Теперь прочитаем большой большой датасет по ключу.

```python
cass_big_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_many", keyspace="keyspace1") \
    .load()

cass_big_df.filter(F.col("user_id")=="3999638244").show()
```

    +----------+------+
    |   user_id|gender|
    +----------+------+
    |3999638244|     3|
    +----------+------+

Запрос `cass_big_df.filter(F.col("gender")=="1").count()` не выполнится, так как требует прочтения всей таблицы. Это очень дорогая операция.


##### Задание 3. Проверить пушит ли спарк фильтры в касандру.

Определяем функцию `explain`, которая будет показывать план запроса.

```python
def explain(self, extended=True):
    if extended:
        print(self._jdf.queryExecution().toString())
    else:
        print(self._jdf.queryExecution().simpleString())
```



#Наблюдаем на pushedFilter в PhysicalPlan
explain(cass_big_df.filter(F.col("user_id")=="10"))

explain(cass_big_df.filter(F.col("gender")=="10"))

#between не передается в pushdown
cass_big_df.createOrReplaceTempView("cass_df")
sql_select = spark.sql("""
select * 
from cass_df
where user_id between 1999 and 2000
""")
#проверяем, что user id не попал в pushedFilter
explain(sql_select)
sql_select.show() #медленно

#in передается в pushdown
sql_select = spark.sql("""
select * 
from cass_df
where user_id in (3884632855,3562535987)
""")

#проверяем, что user id попал в pushedFilter
explain(sql_select)
sql_select.show() #быстро