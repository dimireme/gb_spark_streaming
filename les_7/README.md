## Урок 7. Spark Submit. Lambda архитектура.

##### Задание. Повторить запуск Spark приложений с такими параметрами (можно еще добавлять свои):
 
 ```bash
 /spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] my_script.py
```

Для начала скопируем все исходники в домашнюю директорию. 

```bash
scp -i ~/.ssh/id_rsa_gb_spark -r ./les_7/for_spark_submit BD_274_ashadrin@89.208.223.141:~/for_spark_submit

scp -i ~/.ssh/id_rsa_gb_spark -r ./dataframes/usa_president/ BD_274_ashadrin@89.208.223.141:~/for_stream
```

Подключаемся к серверу.

```bash
ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```

Копируем файлы, которые будут использованы как исходники для стрима, на HDFS.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -mkdir input_csv_for_stream
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -put for_stream/* input_csv_for_stream
```

Запускаем скрипт батчевой обработки csv-файлов. Сделаем это несколько раз.

 ```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ cd for_spark_submit/
[BD_274_ashadrin@bigdataanalytics-worker-0 for_spark_submit]$ /spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] 1_batch.py
```

Видим что скрипт успешно отработал три раза и при каждом запуске был создан паркет-файл в директории `my_submit_parquet_files` на HDFS.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 for_spark_submit]$ hdfs dfs -ls my_submit_parquet_files
Found 3 items
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-28 04:11 my_submit_parquet_files/p_date=20201228041057
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-28 04:14 my_submit_parquet_files/p_date=20201228041453
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-28 04:15 my_submit_parquet_files/p_date=20201228041505
```

Запустим второй скрипт. Тут чтение файлов происходит в стриминговом режиме раз в 10 секунд. Ограничений на количество файлов нет, чейкпойнт не указан.  

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 for_spark_submit]$ /spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] 2_stream.py 
```

Скрипт отработал и завершился, так как был достигнут конец файла. Микробатч ни разу не отработал. В списке файлов появился пустой файл. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 for_spark_submit]$ hdfs dfs -du -h my_submit_parquet_files
2.5 K  7.4 K  my_submit_parquet_files/p_date=20201228041057
2.5 K  7.4 K  my_submit_parquet_files/p_date=20201228041453
2.5 K  7.4 K  my_submit_parquet_files/p_date=20201228041505
0      0      my_submit_parquet_files/p_date=20201228042258
```

Запустим третий скрипт. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 for_spark_submit]$ /spark2.4/bin/spark-submit --driver-memory 512m --driver-cores 1 --master local[1] 3_stream-stable.py 
```

Скрипт не отпускает консоль. раз в 10 секунд пишется сообщение о новом микробатче. Раз в 9 секунд выводится текст `I'M STILL ALIVE`. Проверим что все файлы успено прочитались:

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -du -h my_submit_parquet_files
2.5 K  7.4 K  my_submit_parquet_files/p_date=20201228041057
2.5 K  7.4 K  my_submit_parquet_files/p_date=20201228041453
2.5 K  7.4 K  my_submit_parquet_files/p_date=20201228041505
0      0      my_submit_parquet_files/p_date=20201228042258
2.5 K  7.4 K  my_submit_parquet_files/p_date=20201228043343
``````

<details>
<summary>Для истории. Пример запуска spark-submit с параметрами.</summary>

```
spark-submit --conf spark.hadoop.hive.exec.max.dynamic.partitions=10000 \
--conf spark.hadoop.hive.exec.max.dynamic.partitions.pernode=3000 \
--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
--conf spark.hadoop.hive.eror.on.empty.partition=true \
--conf spark.hadoop.hive.exec.dynamic.partition=true \
--conf spark.sql.parquet.compression.codec=gzip \
--conf spark.sql.catalogImplementation=hive \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer=128M \
--conf spark.kryoserializer.buffer.max=2000M \
--conf spark.sql.broadcastTimeout=6000 \
--conf spark.network.timeout=600s \
--conf spark.driver.memory=20g \
--conf spark.driver.memoryOverhead=3g \
--conf spark.executor.memory=20g \
--conf spark.executor.memoryOverhead=3g \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.sql.shuffle.partitions=300 \
--conf spark.shuffle.service.enabled=true \
my_script.py
```

</details>
