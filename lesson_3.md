## Урок 3. Spark Streaming. Чтение Kafka.

##### Повторить чтение файлов со своими файлами со своей схемой.


1\.1\. Подключаемся к серверу

```bash
 ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```

1\.2\. Подготовка файлов и папок. 

Создадим папку `input_csv_for_stream` на HDFS, из который стрим будет читать файлы.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -mkdir input_csv_for_stream
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -ls
Found 2 items
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-06 19:57 .sparkStaging
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-14 09:46 input_csv_for_stream 
```

Создадим локальную директорию `for_stream`, откуда будем копировать файлы на HDFS.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ mkdir for_stream
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ ls
for_stream
```

Скопируем подготовленные файлы на удаленный сервер с помощью команды `scp`. Эта команда запускается на локальном компьютере, а не на удалённом сервере.  

```bash
scp -i ~/.ssh/id_rsa_gb_spark -r /usa_president BD_274_ashadrin@89.208.223.141:~/for_stream
```

Проверяем что файлы загрузились.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ ls  for_stream/usa_president/
usa_president_1.csv  usa_president_2.csv  usa_president_3.csv  usa_president_4.csv  usa_president_5.csv
```

Файлы содержат список президентов США, максимум 10 записей. Столбцы файлов `President`, `Took office`, `Left office`.

1\.3\. Запускаем `pyspark`. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ pyspark --master local[1]
```

1\.4\. Инициализация стрима. 

В командной строке `pyspark` импортируем нужные методы и определяем функцию `console_output` для вывода стрима в консоль. 

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()
```

Определяем схему наших файлов.
 
```python
schema = StructType() \
    .add("President", StringType()) \
    .add("Took office", StringType()) \
    .add("Left office", StringType())
``` 
 
Создаём стрим чтения из файла. Параметр `.format("csv")` определяет что чтение будет происходить из файла. В `options` указываем папку на HDFS, из которой будут читаться файлы.

```python
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()
```

Запускаем стрим. 

```python
out = console_output(raw_files, 5)
```

1\.5\. Тестирование стрима.
 
В соседнем терминале подключаемся к удалённому серверу `worker-0` и переходим в каталог с загруженными файлами.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ cd for_stream/usa_president/
[BD_274_ashadrin@bigdataanalytics-worker-0 usa_president]$ ll
total 20
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 391 дек 14 10:43 usa_president_1.csv
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 399 дек 14 10:43 usa_president_2.csv
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 416 дек 14 10:43 usa_president_3.csv
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 403 дек 14 10:43 usa_president_4.csv
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 209 дек 14 10:43 usa_president_5.csv
```

Копируем один файл на HDFS

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 usa_president]$ hdfs dfs -put usa_president_1.csv input_csv_for_stream
```

В первом терминале наблюдаем чтение данных.


    -------------------------------------------                                     
    Batch: 0
    -------------------------------------------
    +----------------------+-----------+-----------+
    |President             |Took office|Left office|
    +----------------------+-----------+-----------+
    |George Washington     |30/04/1789 |4/03/1797  |
    |John Adams            |4/03/1797  |4/03/1801  |
    |Thomas Jefferson      |4/03/1801  |4/03/1809  |
    |James Madison         |4/03/1809  |4/03/1817  |
    |James Monroe          |4/03/1817  |4/03/1825  |
    |John Quincy Adams     |4/03/1825  |4/03/1829  |
    |Andrew Jackson        |4/03/1829  |4/03/1837  |
    |Martin Van Buren      |4/03/1837  |4/03/1841  |
    |William Henry Harrison|4/03/1841  |4/04/1841  |
    |John Tyler            |4/04/1841  |4/03/1845  |
    +----------------------+-----------+-----------+

Во втором терминале скопируем все оставшиеся файлы на HDFS.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 usa_president]$ hdfs dfs -put usa_president_* input_csv_for_stream
put: 'input_csv_for_stream/usa_president_1.csv': File exists
```

В первом терминале видим что все непрочитанные ранее файлы считались.

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +---------------------------+-----------+-----------+
    |President                  |Took office|Left office|
    +---------------------------+-----------+-----------+
    |Chester A. Arthur          |19/09/1881 |4/03/1885  |
    |Grover Cleveland           |4/03/1885  |4/03/1889  |
    |Benjamin Harrison          |4/03/1889  |4/03/1893  |
    |Grover Cleveland (2nd term)|4/03/1893  |4/03/1897  |
    |William McKinley           |4/03/1897  |14/9/1901  |
    |Theodore Roosevelt         |14/9/1901  |4/3/1909   |
    |William Howard Taft        |4/3/1909   |4/03/1913  |
    |Woodrow Wilson             |4/03/1913  |4/03/1921  |
    |Warren G. Harding          |4/03/1921  |2/8/1923   |
    |Calvin Coolidge            |2/8/1923   |4/03/1929  |
    |Herbert Hoover             |4/03/1929  |4/03/1933  |
    |Franklin D. Roosevelt      |4/03/1933  |12/4/1945  |
    |Harry S. Truman            |12/4/1945  |20/01/1953 |
    |Dwight D. Eisenhower       |20/01/1953 |20/01/1961 |
    |John F. Kennedy            |20/01/1961 |22/11/1963 |
    |Lyndon B. Johnson          |22/11/1963 |20/1/1969  |
    |Richard Nixon              |20/1/1969  |9/8/1974   |
    |Gerald Ford                |9/8/1974   |20/01/1977 |
    |Jimmy Carter               |20/01/1977 |20/01/1981 |
    |Ronald Reagan              |20/01/1981 |20/01/1989 |
    +---------------------------+-----------+-----------+
    only showing top 20 rows

Завершаем стрим командой `out.stop()`. 

1\.6\. Попробуем запустить стрим с другими опциями. 

Параметр `maxFilesPerTrigger` определяет сколько файлов будет прочитано в одном батче. При этом, если необработанных файлов меньше чем `maxFilesPerTrigger`, то они не будут прочитаны и батч не появится.

```python
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream",
             header=True,
             maxFilesPerTrigger=1) \
    .load()
```

Так же добавим свою колонку `Days in power`, в которой посчитаем сколько дней каждый президент провёл у власти.

```python
extra_files = raw_files \
    .withColumn("Days in power", F.datediff(
        F.to_date(F.col("Left office"), "dd/MM/yyyy"),
        F.to_date(F.col("Took office"), "dd/MM/yyyy")
    ))
```

Запускаем стрим 

```python
out = console_output(extra_files, 5)
```

Так как все файлы уже записаны на HDFS, то они сразу считываются. В консоли наблюдаем 5 батчей (по числу файлов на HDFS). Тут приведу только последний батч. 

    -------------------------------------------
    Batch: 4
    -------------------------------------------
    +-----------------+-----------+-----------+-------------+
    |President        |Took office|Left office|Days in power|
    +-----------------+-----------+-----------+-------------+
    |George H. W. Bush|20/01/1989 |20/01/1993 |1461         |
    |Bill Clinton     |20/01/1993 |20/01/2001 |2922         |
    |George W. Bush   |20/01/2001 |20/01/2009 |2922         |
    |Barack Obama     |20/01/2009 |20/01/2017 |2922         |
    |Donald J. Trump  |20/01/2017 |null       |null         |
    +-----------------+-----------+-----------+-------------+


1\.7\. Завершение первой части.

Закрываем стим и выходим из консоли `pyspark`  

```python
out.stop()
exit()
```

Удаляем файлы из HDFS. Локально пока оставим, может пригодятся. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -rm -r input_csv_for_stream
20/12/14 11:48:10 INFO fs.TrashPolicyDefault: Moved: 'hdfs://bigdataanalytics-head-0.novalocal:8020/user/BD_274_ashadrin/input_csv_for_stream' to trash at: hdfs://bigdataanalytics-head-0.novalocal:8020/user/BD_274_ashadrin/.Trash/Current/user/BD_274_ashadrin/input_csv_for_stream
```

##### Создать свой топик/топики, загрузить туда через консоль осмысленные данные с kaggle. Лучше в формате json. Много сообщений не нужно, достаточно штук 10-100.
Прочитать свой топик так же, как на уроке.