import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv


load_dotenv('../env_file.yaml')

def get_spark_session():
    return SparkSession.builder.appName("WeatherStreamProcessor") \
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