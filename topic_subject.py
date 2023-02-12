from subprocess import check_output


"""
스파크 패키지 목록 - {
    kafka,
    elasticsearch
}
"""
Spark_package = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.14.2 pyspark-shell"

spark_air_realtime_conf = [
        ("spark.master", "local[*]"),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", check_output(["hostname"]).decode(encoding="utf-8").strip()),
        ("spark.app.name", "Flight-infos"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
    ]

"""
Topic_list - {
    "fligt-realtime
}
"""W

topic_f_r = "flight-realtime"

