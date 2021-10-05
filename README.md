# HW3

Apache Kafka Lab 1

1. Import "Apache_Kafka_lab_1.xml" template to NiFi.
2. Upload template and run processors.
   ![img_1.png](img_1.png)
3. Connect to SSG server.
   ![img.png](img.png)
4. Upload "create-topic.sh" script in dataproc and run it.

```bash
sh create-topic.sh
```

4. Upload "build/libs/KafkaConsumer-1.0.jar" file and run it.

```bash
java -jar KafkaConsumer-1.0.jar 
```

![img_2.png](img_2.png)