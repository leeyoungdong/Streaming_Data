from pyspark.sql import SparkSession   

spark = SparkSession.builder.master("local").appName("learn-sql").getOrCreate()

stocks = [
    ('Google', 'GOOGL', 'USA', 2984, 'USD'), 
    ('Netflix', 'NFLX', 'USA', 645, 'USD'),
    ('Amazon', 'AMZN', 'USA', 3518, 'USD'),
    ('Tesla', 'TSLA', 'USA', 1222, 'USD'),
    ('Tencent', '0700', 'Hong Kong', 483, 'HKD'),
    ('Toyota', '7203', 'Japan', 2006, 'JPY'),
    ('Samsung', '005930', 'Korea', 70600, 'KRW'),
    ('Kakao', '035720', 'Korea', 125000, 'KRW'),
]

stockSchema = ["name", "ticker", "country", "price", "currency"]

df = spark.createDataFrame(data=stocks, schema=stockSchema)

df.write.format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("user","root") \
        .option("password","lgg032800") \
        .option("url","jdbc:mysql://localhost:3306") \
        .option("dbtable","stream.spark_department") \
        .save()