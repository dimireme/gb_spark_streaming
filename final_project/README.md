## Урок 8. Spark Streaming + Spark ML + Cassandra. Применение ML-модели в режиме реального времени.

##### Задание. В качестве итоговой работы необходимо написать свою ML модель. Обучить ее. Затем применять на стриме. Необходимо найти / сгененрировать собственные входные данные и объяснить, какую задачу решает ваша ML модель и почему именно так.

##### Идеологическая концепция

##### Подготовка и очистка данных
В датафрейме покупок `purchases` изначально было 2396804 записей. Из их числа были отсеяны записи, где число купленных товаров или сумма заказа была равна нулю. Осталось 2379348 записи.

    user_id - id пользователя, который совершил покупку;
    item_id - id купленного товара;
    quantity - количество товаров в заказе;
    sales_value - суммарная стоимость заказа.

Здесь нет id корзины, так как мы её не используем. Идея такая что каждая запись это факт добавления пользователем товара в свою корзину.

В датафрейме фичей товаров изначально было 92353 уникальных товара. Эти фичи будем хранить в Кассандре. Для экономии места число товаров было уменьшено до 5000. Выбраны наиболее часто покупаемые товары по количеству (`quantity`) среди товаров, цена которых более 1$. Так же были отброшены все лишние фичи товаров, которые не участовуют в процессе рассчёта рекомендаций.
 
 ```python
top_5000 = purchases[purchases.sales_value / purchases.quantity >= 1] \
    .groupby('item_id')['quantity'].sum() \
    .reset_index() \
    .sort_values('quantity', ascending=False) \
    .head(5000) \
    .item_id.tolist()

item_features = item_features[item_features['product_id'].isin(top_5000)]
```

Если при применении модели, окажется что пользователь купил товар не из числа топ 5000, то будем считать что категория покупки неизвестна и мы мождем давать рекомендации среди любых категорий товаров.
 
Структура датафрейма `item_features`:
 
    product_id - id товара
    department - категория товара (21 уникальное значение)
    category - подкатегория товара (202 уникальных значения)
    
Так же был составлен датафрейм `top_5_by_category`. В этом датафрейме всего один столбец `product_id` и 5 записей. Это выжимка из `top_5000`, но только товаров из разных категорий. Этот датафрейм целиком будем хранить в Кассандре и дополнять им модель рекомендаций, на случай если не удастся сделать 5 рекомендаций товаров из разных категорий (если будут дубли по категориям). 

Так же этот список товаров будет изпользоваться как бэйзлайн рекомендаций для нового пользователя (проблема холодного старта).  
    
Все эти преобразования были сделаны на локальной машине. Весь код в файле `prepare_data.ipynb` (TODO: add link).

##### Создание таблиц и топиков в кластере

###### Копирование данных на кластер

Скопируем подготовленные файлы на удаленный сервер с помощью команды `scp`. Эта команда запускается на локальном компьютере, а не на удалённом сервере.  
```bash
scp -i ~/.ssh/id_rsa_gb_spark -r ./final_project/data BD_274_ashadrin@89.208.223.141:~/
```

Подключаемся к серверу.
```bash
ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```

Проверяем что файлы загрузились.
```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ ls -lah  data/
total 44M
drwxrwxr-x 2 BD_274_ashadrin BD_274_ashadrin 4,0K янв 16 17:50 .
drwx------ 7 BD_274_ashadrin BD_274_ashadrin 4,0K янв 16 17:50 ..
-rw-r--r-- 1 BD_274_ashadrin BD_274_ashadrin 153K янв 16 17:50 item_features.csv
-rw-r--r-- 1 BD_274_ashadrin BD_274_ashadrin  44M янв 16 17:51 purchases.csv
-rw-r--r-- 1 BD_274_ashadrin BD_274_ashadrin   49 янв 16 17:50 top_5_by_category.csv
```

Копируем файлы на HDFS.
```bash
[BD_274_ashadrin@bigdataanalytics-worker-0]$ hdfs dfs -put data ./
```

Проверим что все файлы попали на HDFS.
```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -du -h data
```

    152.6 K  457.7 K  data/item_features.csv
    43.0 M   129.1 M  data/purchases.csv
    49       147      data/top_5_by_category.csv

###### Создание таблиц в Кассандре

Запускаем консольный клиент Кассандры
```bash
[BD_274_ashadrin@bigdataanalytics-worker-2 ~]$ /cassandra/bin/cqlsh 10.0.0.18
```

Создаём новое пространство таблиц `shadrin_final`.
```sql
CREATE  KEYSPACE IF NOT EXISTS shadrin_final 
WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  };
```

Создаём таблицу топ-5 самых популярных товаров. Тут будет только одна запись.
```
CREATE TABLE IF NOT EXISTS shadrin_final.top_5 (
    name text,
    list list<int>,
    primary key (name)
);
```

Создаём таблицу собственных покупок пользователя. Эта таблица поможет посчитать точность рекомендаций `precision@5` в микробатче. 
```
CREATE TABLE IF NOT EXISTS shadrin_final.own_purchases (
    user_id int,
    item_id_list list<int>,
    primary key (user_id)
);
```

ПРОСТО ПОЛЕЗНЫЕ КОМАНДЫ. TODO: удалить
```
SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'shadrin_final';

SELECT * from shadrin_final.top_5;
drop table shadrin_final.top_5;

SELECT * from shadrin_final.own_purchases;
drop table shadrin_final.own_purchases;
```

Далее выполним код из файла `init_tables_n_topics.py`. Воспользуемся для этого новым терминалом. 

TODO: добавить команды, которые копируют файл с кодом на кластер и запуск этого файла через spark-submit.

В итоге получим заполненные таблички в Кассандре и топик в Кафке с тестовыми данными. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]
```
