import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


schema = StructType([
    StructField("properties", StructType([
        StructField("stationId", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", StructType([
            StructField("value", DoubleType(), True)
        ]), True),
        StructField("relativeHumidity", StructType([
            StructField("value", DoubleType(), True)
        ]), True),
        StructField("windSpeed", StructType([
            StructField("value", DoubleType(), True)
        ]), True),
        StructField("textDescription", StringType(), True)
    ]))
])

load_dotenv('../env_file.yaml')

spark = SparkSession.builder.appName("WeatherStreamProcessor") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
                          "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0,"
                          "io.delta:delta-spark_2.12:3.3.2,"
                          "org.apache.hadoop:hadoop-aws:3.3.4"
    ) \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config("spark.mongodb.write.connection.uri", os.getenv("MONGO_URI")) \
    .config("spark.mongodb.write.database", os.getenv("MONGO_DB")) \
    .config("spark.mongodb.write.collection", os.getenv("MONGO_COLLECTION")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# check compatible versions for Structured Streaming + Kafka Integration
# print(spark.version)
# print(spark.sparkContext._jsc.sc().listJars())

raw_binary = spark.readStream.format('kafka').option(
    'kafka.bootstrap.servers', 'localhost:9092').option(
    'subscribe', 'weather_obs').option(
    'startingOffsets', 'latest'
).load()

deserialized_data =raw_binary.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col('json'), schema).alias('data')) \
    .select(
        col("data.properties.stationId").alias("station_id"),
        to_timestamp(col("data.properties.timestamp")).alias("obs_time"),
        col("data.properties.temperature.value").alias("temperature"),
        col("data.properties.relativeHumidity.value").alias("humidity"),
        col("data.properties.windSpeed.value").alias("wind_speed"),
        col("data.properties.textDescription").alias("description")
    )

# preview raw data
# raw = raw_binary.selectExpr("CAST(value AS STRING) as json")
# query = raw.writeStream.format('json').option('path', './raw_data') \
#         .option('checkpointLocation', '/tmp/checkpoints/weather').outputMode('append')

# preview dataframe in console
console_sink = deserialized_data.writeStream.format('console').outputMode(
    'append').trigger(processingTime='10 seconds').option('truncate', False)

# save data in parquet files
local_parquet_sink = deserialized_data.writeStream \
        .format('parquet') \
        .option('path', './weather_data_parquet') \
        .option('checkpointLocation', '/tmp/checkpoints/weather') \
        .outputMode('append')

s3_path = f"s3a://{os.getenv('S3_BUCKET')}/{os.getenv('S3_PATH')}"
s3_delta_sink = deserialized_data.writeStream \
    .format('delta') \
    .option('path', s3_path) \
    .option('checkpointLocation', '/tmp/checkpoints/s3_delta') \
    .outputMode('append')

mongo_sink = deserialized_data.writeStream \
    .format("mongodb") \
    .option("uri", "mongodb://localhost:27017") \
    .option("database", "weather") \
    .option("collection", "analytics") \
    .option("checkpointLocation", "/tmp/checkpoints/weather") \
    .outputMode("append")

console_sink.start().awaitTermination()
local_parquet_sink.start().awaitTermination()
mongo_sink.start().awaitTermination()
s3_delta_sink.start().awaitTermination()
spark.stop()
