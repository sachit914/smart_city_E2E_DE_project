# smart_city_E2E_DE_project
Smart City End to End Realtime Data Engineering Project

```
source venv/bin/activate
```

```
docker compose up -d
```


```
kafka-topics --list --bootstrap-server broker:29092
```

```
kafka-console-consumer --topic vehicle_data --bootstrap-server broker:9092 --from-beginning
```


```
kafka-topics --delete --topic emergency_data --bootstrap-server broker:29092 
```

```
docker exec -it smart_city_e2e_de_project-spark-master-1 spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3,org.apache.hadoop:hadoop-aws:3.3.3,com.amazonaws:aws-java-sdk:1.12.760 jobs/spark-city.py
```

# Kafka
###  List Existing Topics
```
kafka-topics --bootstrap-server localhost:9092 --list
```

### Create a New Topic
```
kafka-topics --bootstrap-server localhost:9092 --create --topic test-topic --partitions 3 --replication-factor 1
```

### Describe a Topic
```
kafka-topics --bootstrap-server localhost:9092 --describe --topic test-topic
```

### Produce Messages to a Topic
```
kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic
```
### Consume Messages from a Topic
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### Check Broker and Topic Configuration
```
cat /etc/kafka/server.properties
```

### Check the Status of the Broker
```
tail -f /var/log/kafka/server.log
```

### Check Consumer Group Information
```
kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### Describe a Consumer Group
```
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group your-consumer-group-name
```

# spark
```
./spark-shell --master spark://spark-master:7077
```

### error
```
@dermoritz My simple resolution is to modify /opt/bitnami/spark/bin/pyspark:

$ diff --color ./pyspark.old ./pyspark
68c68
< exec "${SPARK_HOME}"/bin/spark-submit pyspark-shell-main --name "PySparkShell" "$@"
---
> exec "${SPARK_HOME}"/bin/spark-submit pyspark-shell-main "$@"
```

# aws s3 setup
- create bucket named spark-streaming-data
- disable block all  public acess
- bucket policy
 
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": "arn:aws:s3:::spark-streaming-data/*"   

        }
    ]
}
```

# aws Iam Roles
- admistratorAcess
- amazons3fullacess
- aws glueConsoleFUllAcess
