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
```

##### Задание 2. Когда cassandra станет понятна, поработать с ней через Spark. Проверить пушит ли спарк фильтры в касандру.