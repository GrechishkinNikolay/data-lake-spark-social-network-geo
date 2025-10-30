import os, sys
import findspark
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()

from pyspark.sql import SparkSession

def main():

    events_base_path = sys.argv[1] # "/user/kolaygrech/data/geo/events"
    output_base_path = sys.argv[2] # "/user/kolaygrech/data/analytics/geo_zones/"

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("project7") \
        .config("spark.executor.instances", "12") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "24") \
        .getOrCreate()

    events_all = (
        spark.read
        .option("basePath", events_base_path)
        .parquet(
            # f"{events_base_path}/date=2022-03-*",
            # f"{events_base_path}/date=2022-04-*",
            f"{events_base_path}/date=2022-05-*",
            f"{events_base_path}/date=2022-06-0[1-21]"
        )
        .select("event_type", "date", "lat", "lon", "event.*")
    )
    events_all.cache()

    user_last_city = spark.read.parquet("/user/kolaygrech/data/analytics/user_last_city") \
        .select("user_id", F.col("act_city").alias("city")) \
        .distinct()

    # унифицируем пользователя в одно поле user_unified
    events_all = (
        events_all
        .withColumn(
            "user_id_unified",
            F.coalesce(
                F.col("user").cast("string"),
                F.col("message_from").cast("string"),
                F.col("reaction_from").cast("string"),
                F.col("subscription_user").cast("string") # Почему-то все подписки имеют это поле NULL, поэтому непонятно откуда брать user_id здесь для джоина
            )
        )
    )

    events_geo_full = (
        events_all
        .join(user_last_city, events_all["user_id_unified"] == user_last_city["user_id"], "left")
    )

    events_geo_full.write.mode("overwrite").parquet("/user/kolaygrech/data/analytics/events_geo_full")

    w_first_event = Window.partitionBy("user_id_unified").orderBy("date")

    first_events = (
        events_geo_full
        .withColumn("event_rank", F.row_number().over(w_first_event))
        .filter(F.col("event_rank") == 1)
        .withColumn("event_type", F.lit("registration"))
        .drop("event_rank")
    )

    events_geo_full = (
        events_geo_full.unionByName(first_events)
    )

    events_geo_full = (
        events_geo_full
        .withColumn("week", F.weekofyear("date"))
        .withColumn("month", F.month("date"))
    )
    events_geo_full.cache()

    agg_week = (
        events_geo_full
        .groupBy("month", "week", "city")
        .pivot("event_type", ["message", "reaction", "subscription", "registration"])
        .agg(F.count("*"))
        .fillna(0)
        .withColumnRenamed("message", "week_message")
        .withColumnRenamed("reaction", "week_reaction")
        .withColumnRenamed("subscription", "week_subscription")
        .withColumnRenamed("registration", "week_user")
    )

    agg_month = (
        events_geo_full
        .groupBy("month", "city")
        .pivot("event_type", ["message", "reaction", "subscription", "registration"])
        .agg(F.count("*"))
        .fillna(0)
        .withColumnRenamed("message", "month_message")
        .withColumnRenamed("reaction", "month_reaction")
        .withColumnRenamed("subscription", "month_subscription")
        .withColumnRenamed("registration", "month_user")
    )

    geo_zones = agg_week.join(agg_month, on=["city", "month"], how="left")
    geo_zones.write.mode("overwrite").parquet(output_base_path)

if __name__ == "__main__":
    main()
