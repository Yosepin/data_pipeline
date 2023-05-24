from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("create database n_database")

# Execute HiveQL query
df = spark.sql ("select distinct trip_type from greentrip2022")
df = df.withColumn("trip_name", when(col("trip_type") == 1.0, lit("Street-hail"))
                        .when(col("trip_type") == 2.0, lit("Dispatch")))
df = df.filter(df.trip_type.isNotNull()).select("*")


spark.sql("use n_database")
df.write.mode("overwrite").saveAsTable("dim1")
