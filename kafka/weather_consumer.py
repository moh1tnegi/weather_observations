import os
from dotenv import load_dotenv

from spark_driver import get_spark_session
from utils.deserialize_raw_weather_data import deserialize_weather_data

load_dotenv('../env_file.yaml')
spark = get_spark_session()

raw_binary = spark.readStream.format('kafka').option(
    'kafka.bootstrap.servers', 'localhost:9092').option(
    'subscribe', 'weather_obs').option(
    'startingOffsets', 'latest'
).load()

deserialized_data = deserialize_weather_data(raw_binary)

# preview dataframe in console
console_sink = deserialized_data.writeStream.format('console').outputMode(
    'append').trigger(processingTime='10 seconds').option('truncate', False)

# save data in parquet files
local_parquet_sink = deserialized_data.writeStream \
        .format('parquet') \
        .option('path', './weather_data_parquet') \
        .option('checkpointLocation', '/tmp/checkpoints/weather') \
        .outputMode('append')

# save to s3 delta lake
s3_path = f"s3a://{os.getenv('S3_BUCKET')}/{os.getenv('S3_PATH')}"
s3_delta_sink = deserialized_data.writeStream \
    .format('delta') \
    .option('path', s3_path) \
    .option('checkpointLocation', '/tmp/checkpoints/s3_delta') \
    .option('mergeSchema', 'true') \
    .outputMode('append')

# NoSQL store
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
