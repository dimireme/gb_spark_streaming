## Урок 2. Kafka. Архитектура.

##### Создать свои топик в kafka. Поиграться с retention time, console-consumer, console-producer.

1. Подключаемся к серверу

```bash
ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```
 
2\. Создаем топик

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic shadrin_les2 --zookeeper bigdataanalytics-worker-0.novalocal:2181 --partitions 3 --replication-factor 2 --config retention.ms=-1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "shadrin_les2".
```

3\. Изменяем время хранения данных на 200 дней

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --zookeeper bigdataanalytics-worker-0.novalocal:2181 --alter --config retention.ms=17280000000 --topic shadrin_les2
WARNING: Altering topic configuration from this script has been deprecated and may be removed in future releases.
         Going forward, please use kafka-configs.sh for this functionality
Updated config for topic "shadrin_les2".
```

4\.

в одном терминале создали слив, который пишет в консоль
```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-consumer.sh --topic shadrin_les2 --from-beginning --bootstrap-server bigdataanalytics-worker-0.novalocal:6667

```

5\. В другом терминале создаем источник, который читает ввод с консоли

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-producer.sh --topic shadrin_les2 --broker-list bigdataanalytics-worker-0.novalocal:6667
>qwe
>asd
>zxc
>rty
>dfg
>dsfsdf
>
```

6\. В первом терминале наблюдаем пришедшие данные 

```bash
qwe
asd
zxc
rty
dfg
dsfsdf
```

7\. Удаляем топик

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --zookeeper bigdataanalytics-worker-0.novalocal:2181 --delete --topic shadrin_les2
Topic shadrin_les2 is marked for deletion.
Note: This will have no impact if delete.topic.enable is not set to true.

```