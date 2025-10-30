import os
import findspark
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()


from pyspark.sql import SparkSession

def main():
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

    events_geo_full = spark.read.parquet("/user/kolaygrech/data/analytics/events_geo_full")

    subscriptions = (
        events_geo_full
        .filter(F.col("event_type") == "subscription")
        .select(
            F.col("subscription_channel").alias("channel_id"),
            F.col("user_id_unified").alias("user_id")
        )
        .dropna()
        .distinct()
        .limit(2_000_000)  # Обрезаю для тестирования т.к. получился слишком большой датафрейм
    )

    subs_pairs = (
        subscriptions.alias("l")
        .join(subscriptions.alias("r"),
              (F.col("l.channel_id") == F.col("r.channel_id")) &
              (F.col("l.user_id") < F.col("r.user_id")),  # чтобы избежать дублей (A,B) = (B,A)
              "inner"
              )
        .select(
            F.col("l.user_id").alias("user_left"),
            F.col("r.user_id").alias("user_right"),
            F.col("l.channel_id")
        )
    )

    users_geo = (
        spark.read.parquet("/user/kolaygrech/data/analytics/user_last_city/")
        .select("user_id", "act_city", "user_lat", "user_lon", "timezone")
    )

    pairs_geo = (
        subs_pairs
        .join(users_geo.alias("l"), F.col("user_left") == F.col("l.user_id"), "inner")
        .join(users_geo.alias("r"), F.col("user_right") == F.col("r.user_id"), "inner")
        .filter(F.col("l.act_city") == F.col("r.act_city"))
    )

    R = 6371  # радиус Земли в км
    pairs_geo = pairs_geo.withColumns({
        "l_user_lat_radians": F.radians(F.col("l.user_lat")),
        "r_user_lat_radians": F.radians(F.col("r.user_lat")),
        "l_user_lon_radians": F.radians(F.col("l.user_lon")),
        "r_user_lon_radians": F.radians(F.col("r.user_lon")),
    })

    close_pairs = pairs_geo.withColumn(
        "distance_km",
        2 * R * F.asin(
            F.sqrt(
                F.pow(F.sin((F.col("r_user_lat_radians") - F.col("l_user_lat_radians")) / 2), 2)
                + F.cos(F.col("l_user_lat_radians")) * F.cos(F.col("r_user_lat_radians"))
                * F.pow(F.sin((F.col("r_user_lon_radians") - F.col("l_user_lon_radians")) / 2), 2)
            )
        )
    ).filter(F.col("distance_km") <= 1)

    messages = (
        events_geo_full
        .filter(F.col("event_type") == "message")
        .select("message_from", "message_to")
        .distinct()
    )

    close_pairs = (
        close_pairs
        .join(
            messages,
            ((F.col("user_left") == F.col("message_from")) & (F.col("user_right") == F.col("message_to"))) |
            ((F.col("user_left") == F.col("message_to")) & (F.col("user_right") == F.col("message_from"))),
            "leftanti"
        )
    )

    friend_recommend = (
        close_pairs
        .select(
            "user_left",
            "user_right",
            F.col("l.act_city").alias("zone_id"),
            F.from_utc_timestamp(current_timestamp(), F.col("l.timezone")).alias("local_time")
        )
        .withColumn("processed_dttm", current_timestamp())
    )

    friend_recommend.write.mode("overwrite").parquet("/user/kolaygrech/data/analytics/friend_recommend/")

if __name__ == "__main__":
    main()
