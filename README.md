# Building-a-real-time-data-pipeline

```bash
docker exec -it master spark-submit \
--packages io.delta:delta-core_2.12:1.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375 \
/opt/apps/Analysis.py
```