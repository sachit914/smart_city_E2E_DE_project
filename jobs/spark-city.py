# spark jobs to listen events from kafka

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from config import configuration
from pyspark.sql.functions import col, from_json

def main():
    # spark = SparkSession.builder.appName("SmartCityStreaming")\
    #     .config("spark.jars.packages",
    #             "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3,"  # spark can connect to kafka through this jar file
    #             "org.apache.hadoop:hadoop-aws:3.3.3,"              # spark can connect to aws through thsis jar file
    #             "com.amazonaws:aws-java-sdk:1.12.760")\ 
    spark = SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3,"
                "org.apache.hadoop:hadoop-aws:3.3.3,"
                "com.amazonaws:aws-java-sdk:1.12.760")\
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.acess.key",configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key",configuration.get('AWS_SECRET_KEY'))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",'org.apache.hadoop.fs.s3a.imp.SimpleAWSCredentialsProvider')\
        .getOrCreate()
        
        # adjust the log level to minimize consoler output on executors
    spark.sparkContext.setLogLevel('WARN')
    
    # vechicle schema
    vehicleSchema =  StructType([
        StructField("id",StringType(),True),
        StructField("deviceId",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("location",StringType(),True),
        StructField("speed",DoubleType(),True),
        StructField("direction",StringType(),True),
        StructField("make",StringType(),True),
        StructField("model",StringType(),True),
        StructField("year",IntegerType(),True),
        StructField("fuelType",StringType(),True),
    ])
    
    # gps schema 
    gpsSchema =  StructType([
        StructField("id",StringType(),True),
        StructField("deviceId",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("speed",DoubleType(),True),
        StructField("direction",StringType(),True),
        StructField("vehicleType",StringType(),True),
    ])
    
    #trafficSchema
    trafficSchema =  StructType([
        StructField("id",StringType(),True),
        StructField("deviceId",StringType(),True),
        StructField("cameraId",StringType(),True),
        StructField("location",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("snapshot",StringType(),True),
    ])
    
    # weatherSchema
    vehicaleSchema =  StructType([
        StructField("id",StringType(),True),
        StructField("deviceId",StringType(),True),
        StructField("location",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("temperature",DoubleType(),True),
        StructField("weatherCondition",StringType(),True),
        StructField("precipitation",DoubleType(),True),
        StructField("windSpeed",DoubleType(),True),
        StructField("humidity",IntegerType(),True),
        StructField("airQualityIndex",DoubleType(),True),
    ])
    
    # emergency schema
    emergencyschema =  StructType([
        StructField("id",StringType(),True),
        StructField("deviceId",StringType(),True),
        StructField("incidentId",StringType(),True),
        StructField("type",StringType(),True),
        StructField("timestamp",TimestampType(),True),
        StructField("location",StringType(),True),
        StructField("status",StringType(),True),
        StructField("description",StringType(),True),
    ])
    
    def read_kafka_topic(topic, schema):
        return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')  # Corrected 'values' to 'value'
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('eventTime', '2 minutes')  # Corrected syntax for withWatermark
        )

    def streamWriter(DataFrame, checkpointFolder, output):
        return input.writeStream \
            .format('parquet') \
            .option('checkpointLocation', checkpointFolder) \
            .option('path', output) \
            .outputMode('append') \
            .start()

    vehicleDF = read_kafka_topic(topic='vehicle_data', schema=vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic(topic='gps_data', schema=vehicleSchema).alias('gps')
    trafficDF = read_kafka_topic(topic='traffic_data', schema=vehicleSchema).alias('traffic')
    weatherDF = read_kafka_topic(topic='weather_data', schema=vehicleSchema).alias('weather')
    emergencyDF = read_kafka_topic(topic='emergency_data', schema=vehicleSchema).alias('emergency')       


    # query1 = streamWriter(vehicleDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/vehicle_data',
    #             output='s3a://spark-streaming-data/data/vehicle_data')

    # query2 = streamWriter(gpsDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/gps_data',
    #             output='s3a://spark-streaming-data/data/gps_data')

    # query3 = streamWriter(trafficDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/traffic_data',
    #             output='s3a://spark-streaming-data/data/traffic_data')

    # query4 = streamWriter(weatherDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/weather_data',
    #             output='s3a://spark-streaming-data/data/weather_data')

    # query5 = streamWriter(emergencyDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/emergency_data',
    #             output='s3a://spark-streaming-data/data/emergency_data')

    # query5.awaitTermination()
if __name__ == "__main__":
    main()