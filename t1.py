from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Execute HiveQL query
df = spark.sql ("""select vendorid, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, ratecodeid, pulocationid,
                    dolocationid, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                    total_amount, payment_type, trip_type, congestion_surcharge from greentrip2022 where vendorid in (1,2) and store_and_fwd_flag in ("N", "Y")""")
df = df.withColumn("lpep_dropoff_datetime", from_unixtime(col("lpep_dropoff_datetime") / 1000000).cast("timestamp"))
df = df.withColumn("lpep_pickup_datetime", from_unixtime(col("lpep_pickup_datetime") / 1000000).cast("timestamp"))


spark.sql("use n_database")
df.write.mode("overwrite").saveAsTable("fact")