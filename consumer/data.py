from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pymongo import MongoClient


spark=SparkSession.builder.appName("school threat system").getOrCreate()

mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client.school_security

fire_alert_schema = "location STRING, alert_type STRING, severity STRING, time STRING"
intruder_detection_schema = "location STRING, activity STRING, time STRING"


# Read fire alerts from Kafka
fire_alert_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fire-alert") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Read intruder detection from Kafka
intruder_detection_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "intruder-detection") \
    .load() \
    .selectExpr("CAST(value AS STRING)")