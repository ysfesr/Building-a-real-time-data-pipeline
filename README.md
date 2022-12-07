# Creating a Scalable Real Time Data Pipeline for Big Data Analytics
### Real-Time Data Pipeline: Building a Scalable, High-Performance Data Pipeline to Collect, Transform, and Analyze Real-Time Data Streams

This project involves creating a scalable real time data pipeline using Apache Spark and Kafka for big data analytics. The data pipeline will be designed to ingest large volumes of data quickly and reliably, and process it in real time to produce meaningful analytics. Apache Spark will be used to process the data, and Kafka will be used to deliver the data to Spark for processing. The data pipeline will be scalable, able to handle large amounts of data in real time, and will be fault tolerant, ensuring that data is not lost in case of a failure. The analytics produced from the data pipeline will be used to provide insights into the data and to make informed decisions.


```bash
docker exec -it master spark-submit \
--packages io.delta:delta-core_2.12:1.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375 \
/opt/apps/Analysis.py
```