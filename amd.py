from __future__ import print_function
import mysql.connector

import sys
import time, json
from datetime import datetime
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.dstream import TransformedDStream
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# import pyspark_cassandra # saveToCassandra 함수, package 추가 필요
kafka_bootstrap_servers = "localhost:9092"
kafka_topic_name = "retweets"
customers_data_file_path = "file:////mnt/92D26AE0D26AC7D5/Python/chamath_kafka_with_pyspark/data_source/customers.csv"
mysql_driver_class = "com.mysql.jdbc.Driver"
mysql_table_name = "newtable"
mysql_user_name = "root"
mysql_password = "lgg032800"
mysql_jdbc_url = "jdbc:mysql://localhost:3306/stream?createDatabaseIfNotExist=true"
# 처리를 위해 필요한 스키마 1. Tweet 2. User 3. Quoted
tweet_schema = [
    'id',
    'created_at',
    'truncated',
    'text',
    'quote_count',
    'reply_count',
    'retweet_count',
    'favorite_count',
    'extended_tweet', # 생략 가능 # 내부 정보
    'extended_entities', # 생략 가능 # 내부 정보
    'entities', # 내부 정보
    'is_quote_status',
    'quoted_status', # 생략 가능 # 내부 정보
    'lang',
] # user, quoted_status, retweeted, retweeted_status, in_reply_to_status_id 은 따로 처리

user_schema = [
    'id', 
    'name',
    'screen_name',
    'location',
    'url',
    'description',
    'followers_count',
    'friends_count',
    'listed_count',
    'favourites_count',
    'statuses_count',
    'created_at',
    'profile_image_url',
]

quote_schema = [
    'created_at',
    'id',
    'in_reply_to_status_id', # reply인지 아닌지 판단 기준. 데이터는 user_mentions로 렌더링
    'truncated',
    'text',
    'quote_count',
    'reply_count',
    'retweet_count',
    'favorite_count',
    'extended_tweet', # 생략 가능 # 내부 정보
    'entities', # 내부 정보
] # user는 따로 처리

media_schema = [
    'id',
    'indices',
    'media_url',
    'type'
]

def retweet_filter(RDD):
    filtered = {}
    try:
        # text가 아닌 full_text인 경우 처리
        if "text" not in RDD and "full_text" in RDD:
            RDD["text"] = RDD["full_text"]

        # Reply 판단 후 처리
        if RDD["in_reply_to_status_id"]:
            filtered["reply_status"] = True
        else:
            filtered["reply_status"] = False

        # tweet_schema 처리
        tweet_lambda = lambda x : (x in tweet_schema)
        isExisting = list(filter(tweet_lambda, RDD.keys()))
        for key in isExisting:
            filtered[key] = RDD[key]

        # user_schema 처리
        filtered["user"] = {}
        for key in user_schema:
            filtered["user"][key] = RDD["user"][key]

        # quote_schema 처리
        if RDD["is_quote_status"] and "quoted_status" in RDD:
            filtered["quoted_status"] = {}
            quoted_lambda = lambda x : (x in quote_schema)
            isExisting = list(filter(quoted_lambda, RDD["quoted_status"].keys()))
            for key in isExisting:
                filtered["quoted_status"][key] = RDD["quoted_status"][key]

            filtered["quoted_status"]["user"] = {}
            for key in user_schema:
                filtered["quoted_status"]["user"][key] = RDD["quoted_status"]["user"][key]
        elif RDD["is_quote_status"] and "quoted_status" not in RDD:
            filtered["quoted_status"] = { "Error": "BLOCK" }

        # media_schema 처리
        if "media" in RDD["entities"]:
            filtered["media_status"] = True
            for index, value in enumerate(RDD["entities"]["media"]):
                new_data = {}
                for key in media_schema:
                    new_data[key] = value[key]
                if value["type"] == "video":
                    new_data["video_info"] = value["video_info"]
                filtered["entities"]["media"][index] = new_data
                filtered["extended_entities"]["media"][index] = new_data
        elif RDD["truncated"] and "media" in RDD["extended_tweet"]["entities"]:
            filtered["media_status"] = True
            for index, value in enumerate(RDD["extended_tweet"]["entities"]["media"]):
                new_data = {}
                for key in media_schema:
                    new_data[key] = value[key]
                if value["type"] == "video":
                    new_data["video_info"] = value["video_info"]
                filtered["extended_tweet"]["entities"]["media"][index] = new_data
                filtered["extended_tweet"]["extended_entities"]["media"][index] = new_data
        else:
            filtered["media_status"] = False

        # struct 변환 타입 모두 덤프로
        struct_type = ["user", "quoted_status", "entities", "extended_entities", "extended_tweet"]
        for type in struct_type:
            if type not in filtered:
                filtered[type] = None

        # date 추가
        datetime_object = datetime.strptime(filtered["created_at"], "%a %b %d %H:%M:%S %z %Y")
        filtered["date"] = datetime_object.date().__str__()
        filtered["timestamp"] = int(time.time()*1000000)

    except:
        print("**********TWEET FILTER ERROR OCCUR**********")
        print("Unexpected error:", sys.exc_info()[0])
        print(RDD)
    
    return filtered
    

# JSON 형태의 RDD 필터링 함수
def retweet_parser(RDD):
    filtered = {}
    try:
        filtered["retweeted_status"] = retweet_filter(RDD["retweeted_status"])
        filtered["retweeted"] = True
        
        filtered["id"] = RDD["id"]
        filtered["created_at"] = RDD["created_at"]
        filtered["lang"] = RDD["lang"]

        # user_schema 처리
        filtered["user"] = {}
        for key in user_schema:
            filtered["user"][key] = RDD["user"][key]

        # struct 변환 타입 모두 덤프로
        filtered["retweeted_status"] = json.dumps(filtered["retweeted_status"])

        # date 추가
        #datetime_object = datetime.strptime(filtered["created_at"], "%a %b %d %H:%M:%S %z %Y")
        filtered["date"] = datetime.now().date().__str__()
        filtered["hour"] = datetime.now().hour
        filtered["minute"] = int(datetime.now().minute / 5)
        filtered["timestamp"] = int(time.time()*1000000)

    except:
        print("**********TWEET FILTER ERROR OCCUR**********")
        print("Unexpected error:", sys.exc_info())
        print(RDD)
    
    return filtered


def save_to_mysql_database(current_df, batch_id):
    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")
    current_df_final = current_df \
        .withColumn("processed_at", lit(processed_at)) \
        .withColumn("batch_id", lit(batch_id))

    print(current_df_final.printSchema())
    print("Printing before Msql table save: " + str(batch_id))
    #새 테이블 생성
    # current_df_final \
    #     .write \
    #     .format("jdbc") \
    #     .option("driver","com.mysql.cj.jdbc.Driver") \
    #     .option("user","root") \
    #     .option("password","lgg032800") \
    #     .option("url","jdbc:mysql://localhost:3306") \
    #     .option("dbtable","stream.spark_edit") \
    #     .save()

    current_df_final.write.mode("overwrite").format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("user","root") \
            .option("password","lgg032800") \
            .option("url","jdbc:mysql://localhost:3306") \
            .option("dbtable","stream.spark_edit") \
            .save()

if __name__ == "__main__":

    kafka_bootstrap_servers = 'localhost:9092'
    topic = 'retweets'

    spark = SparkSession.builder.appName("retw_streaming").getOrCreate()

    kafka_stream = spark \
    .readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .load()



    # kafka_stream1 = spark \
    # .writeStream \
    # .format("kafka") \
    # .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    # .option("subscribe", topic) \
    # .start()

    # print(kafka_stream)
    # a = kafka_stream.textFileStream(kafka_stream)
    query = kafka_stream.writeStream.outputMode("update").foreachBatch(save_to_mysql_database).start().awaitTermination()
    kafka_stream = query.rdd
    # a = kafka_stream \
    # .writeStream \
    # .trigger(processingTime="3 seconds") \
	# .outputMode("update") \
	# .foreachBatch(save_to_mysql_database) \
	# .start() \
	# .awaitTermination() \
	

    # query = kafka_stream.writeStream.format("console").start()
    # print(type(kafka_stream))
    json_stream = kafka_stream.map(lambda topic: json.loads(topic[1]))
    # # print(json_stream)
    parsed = json_stream.map(lambda tweet: retweet_parser(tweet))  
    # parsed.foreachRDD(lambda x: x.saveToCassandra("bts", "retweet_dataset"))
    parsed.pprint()

    #Start Execution of Streams
    spark.start()
    spark.awaitTermination()