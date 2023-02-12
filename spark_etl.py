import os

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = Spark_package

import json

import math

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json, col
from pyspark import SparkContext

# from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from subprocess import check_output
from topic_subject import *


def getrows(Dataframe, rownums=None):
    return Dataframe.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])

def spark()

def func_call(df, batch_id):
        
        df.selectExpr("CAST(value AS STRING) as json")
        
        requests = df.rdd.map(lambda x: x.value).collect()
        flight = getrows(df,rownums=[0]).collect()
        
        for i in flight:
        
           print(i[1].decode("utf-8"))
           hex = i[1].decode("utf-8")        
           dictio = eval(hex)
           print(type(dictio))
      
           print("writing_to_Elasticsearch")

           es.index(
                    index="flight-realtime-project",
                    doc_type="test_doc",
                    body={
		            "hex": dictio["hex"],
		            "reg_number": dictio["reg_number"],
		            "flag": dictio["flag"],
		            "lat": dictio["lat"],
		            "lng": dictio["lng"],
		            "alt": dictio["alt"],
		            "dir": dictio["dir"],
		            "speed": dictio["speed"],
		            "flight_number": dictio["flight_number"],
		            "flight_icao": dictio["flight_icao"],
		            "flight_iata": dictio["flight_iata"],
		            "dep_icao": dictio["dep_icao"],
		            "dep_iata": dictio["dep_iata"],
		            "arr_icao": dictio["arr_icao"],
		            "arr_iata": dictio["arr_iata"],
		            "airline_icao": dictio["airline_icao"],
		            "airline_iata": dictio["airline_iata"],
		            "aircraft_icao": dictio["aircraft_icao"],
		            "updated": dictio["updated"],
		            "status": dictio["status"]                                        
                    }
                )


if __name__ == "__main__":

    es = Elasticsearch(hosts=['localhost'], port=9200)

    topic = topic_f_r

    spark_conf = SparkConf()
    spark_conf.setAll(spark_air_realtime_conf
    )


    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "flight-realtime")
        .option("enable.auto.commit", "true")
        .load()
    )


    
    query = df.writeStream \
    .format('console') \
    .foreachBatch(func_call) \
    .trigger(processingTime="30 seconds") \
    .start().awaitTermination()