import os, sys
import findspark
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
# from timezonefinder import TimezoneFinder
from pyspark.sql.types import StringType
import pandas as pd

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()

from pyspark.sql import SparkSession

# @F.udf(StringType()) #Вариант рабочий только когда на всех нодах стоит TimezoneFinder
# def get_timezone(lat, lon):
#     if lat is None or lon is None:
#         return None
#     try:
#         tf = TimezoneFinder()
#         return tf.timezone_at(lat=lat, lng=lon)
#     except Exception:
#         return None

def main():

    streak_days = int(sys.argv[1])
    events_base_path = sys.argv[2] #"/user/kolaygrech/data/geo/events"
    output_base_path = sys.argv[3] #"/user/kolaygrech/data/analytics/user_activity"

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

    events_geo = (
        spark.read
        .option("basePath", events_base_path)
        .parquet(
            # f"{events_base_path}/date=2022-03-*",
            # f"{events_base_path}/date=2022-04-*",
            f"{events_base_path}/date=2022-05-*",
            f"{events_base_path}/date=2022-06-0[1-21]"
        )
        .where("event_type = 'message'")
        .select("event.message_from", "event.message_ts", "event.message_id", "date", "event_type", "lat", "lon")
    )
    events_geo.cache()

    # Закомментировал заполнение таймзон, т.к. предрасчитал заранее
    # geo_cities = (
    #     spark.read
    #     .option("delimiter", ";")
    #     .option("header", "true")
    #     .csv("/user/kolaygrech/data/geo/geo.csv")
    #     .withColumn("city_lat", F.regexp_replace(F.col("lat").cast("string"), ",", ".").cast("double"))
    #     .withColumn("city_lon", F.regexp_replace(F.col("lng").cast("string"), ",", ".").cast("double"))
    #     .drop("lat", "lng")
    # )
    #
    # geo_cities_pds = geo_cities.toPandas()
    #
    # tf = TimezoneFinder()
    # geo_cities_pds["timezone"] = geo_cities_pds.apply(
    #     lambda r: tf.timezone_at(lat=r["city_lat"], lng=r["city_lon"]), axis=1
    # )
    # geo_cities = spark.createDataFrame(geo_cities_pds)

    geo_cities = spark.read.option("header", True).csv("/user/kolaygrech/data/geo/geo_with_timezone.csv")

    R = 6371  # радиус Земли, км

    # Джойн каждого события со всеми городами
    events_with_cities = (
        events_geo
        .withColumnRenamed("lat", "event_lat")
        .withColumnRenamed("lon", "event_lon")
        .crossJoin(geo_cities)
    )

    # Переводим координаты в радианы
    events_with_cities = events_with_cities.withColumns({
        "event_lat_radian": F.radians(F.col("event_lat")),
        "event_lon_radian": F.radians(F.col("event_lon")),
        "city_lat_radian": F.radians(F.col("city_lat")),
        "city_lon_radian": F.radians(F.col("city_lon"))
    })

    events_with_cities = events_with_cities.withColumn(
        "distance_km",
        2 * R * F.asin(
            F.sqrt(
                F.pow(F.sin((F.col("city_lat_radian") - F.col("event_lat_radian")) / 2), 2)
                + F.cos(F.col("event_lat_radian")) * F.cos(F.col("city_lat_radian"))
                * F.pow(F.sin((F.col("city_lon_radian") - F.col("event_lon_radian")) / 2), 2)
            )
        )
    )

    window = Window.partitionBy(F.col("message_id")).orderBy(F.col("distance_km").asc())

    # Выбираем ближайший город
    events_closest_city = (
        events_with_cities
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .select(
            F.col("message_id"),
            F.col("message_ts"),
            F.col("message_from").alias("user_id"),
            F.col("event_lat"),
            F.col("event_lon"),
            F.col("city"),
            F.col("city_lat"),
            F.col("city_lon"),
            F.col("distance_km"),
            F.col("date"),
            F.col("timezone")
        )
    )

    events_closest_city.write.mode("overwrite").parquet("/user/kolaygrech/data/analytics/events_closest_city/")

    w_last_event = Window.partitionBy("user_id").orderBy(F.col("date").desc())

    user_last_city = (
        events_closest_city
        .withColumn("rn", F.row_number().over(w_last_event))
        .filter("rn = 1")
        .select(
            "user_id",
            F.col("city").alias("act_city"),
            "date",
            "message_ts",
            "timezone"
        )
    )

    user_last_city.write.mode("overwrite").parquet("/user/kolaygrech/data/analytics/user_last_city")

    user_city_days = (
        events_closest_city
        .select("user_id", "city", "date")
        .distinct()
    )

    w_user_city = Window.partitionBy("user_id", "city").orderBy("date")

    user_city_seq = (
        user_city_days
        .withColumn("row_num", F.row_number().over(w_user_city))
        .withColumn("date_diff", F.datediff(F.col("date"), F.lit("1970-01-01")))
        # создаём "группу" последовательности
        .withColumn("grp", F.col("date_diff") - F.col("row_num"))
    )

    user_city_streaks = (
        user_city_seq
        .groupBy("user_id", "city", "grp")
        .agg(
            F.count("*").alias("streak_days"),
            F.max("date").alias("last_day")
        )
    )

    users_homes = (
        user_city_streaks
        .filter(F.col("streak_days") >= streak_days)
    )

    w_home = Window.partitionBy("user_id").orderBy(F.col("last_day").desc())

    user_home_city = (
        users_homes
        .withColumn("rn", F.row_number().over(w_home))
        .filter("rn = 1")
        .select(
            "user_id",
            F.col("city").alias("home_city"),
            "last_day"
        )
    )

    user_activity = (
        user_last_city
        .join(user_home_city, on="user_id", how="left")
        .select(
            "user_id",
            "act_city",
            "home_city",
            "message_ts",
            "timezone",
            F.col("date").alias("last_event_date"),
            F.col("last_day").alias("home_last_day")
        )
    )

    w = Window.partitionBy("user_id").orderBy("date")

    travel_count = (
        events_closest_city
        .select("user_id", "city", "date")
        .withColumn("prev_city", F.lag("city").over(w))
        .filter((F.col("city") != F.col("prev_city")) | F.col("prev_city").isNull())
        .groupBy("user_id")
        .agg(
            F.collect_list(F.col("city")).alias("travel_array"),
            F.count("city").alias("travel_count")
        )
    )

    user_activity = (
        user_activity
        .join(travel_count, on="user_id", how="outer")
        .withColumn("local_time", F.from_utc_timestamp(F.col("message_ts"), F.col("timezone")))
    )

    user_activity.write.mode("overwrite").parquet(output_base_path)

if __name__ == "__main__":
    main()
