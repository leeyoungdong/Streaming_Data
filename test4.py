# from pyspark.sql import SparkSession


# # 스파크 인스턴스 생성
# spark = SparkSession.builder.appName("udf").getOrCreate()


# # 실습을 위한 데이터
# transactions = [
#     ('찹쌀탕수육+짜장2', '2021-11-07 13:20:00', 22000, 'KRW'),
#     ('등심탕수육+크립새우+짜장면', '2021-10-24 11:19:00', 21500, 'KRW'), 
#     ('월남 쌈 2인 세트', '2021-07-25 11:12:40', 42000, 'KRW'), 
#     ('콩국수+열무비빔국수', '2021-07-10 08:20:00', 21250, 'KRW'), 
#     ('장어소금+고추장구이', '2021-07-01 05:36:00', 68700, 'KRW'), 
#     ('족발', '2020-08-19 19:04:00', 32000, 'KRW'),  
# ]

# schema = ["name", "datetime", "price", "currency"]

# df = spark.createDataFrame(data=transactions, schema=schema)
     

# df.createOrReplaceTempView("transactions")
     

# spark.sql("SELECT * FROM transactions").show()


# from __future__ import print_function

import sys
import time, json
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

kafka_bootstrap_servers = 'localhost:9092'
topic = "retweets"

spark = SparkSession \
    .builder \
    .appName("kafka_streaming") \
    .getOrCreate()
    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2")

sc = spark.sparkContext

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .load()


# conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
# sc = SparkContext(conf=conf)

# # Creating a streaming context with batch interval of 10 sec
# ssc = StreamingContext(sc, 10)