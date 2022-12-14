# Creating a Scalable Real Time Data Pipeline for Big Data Analytics

### Real-Time Data Pipeline: Building a Scalable, High-Performance Data Pipeline to Collect, Transform, and Analyze Real-Time Data Streams


![Architecture](./docs/streaming_pipeline.png)


Our project involves building a data pipeline to collect, transform, and analyze real-time data streams from an e-commerce website. The pipeline uses Apache Kafka to ingest the data, Apache Spark to transform and clean it, and Minio to store it in a Delta Lake format. Spark is then used again to analyze the data and extract insights from it.

The goal of the project is to create a scalable and high-performance system for handling large amounts of data in real-time. By using Apache Kafka for data ingestion, the pipeline is able to handle high volumes of data without compromising on performance. Apache Spark is used for data transformation and cleaning, making it possible to quickly and efficiently process the data and prepare it for analysis. The data is then stored in Minio using the Delta Lake format, which provides efficient storage and allows for easy access and querying of the data. Finally, Spark is used again for data analysis, providing powerful tools for extracting insights and making data-driven decisions.

```bash
docker exec -it master spark-submit \
--packages io.delta:delta-core_2.12:1.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375 \
/opt/apps/Analysis.py
```

