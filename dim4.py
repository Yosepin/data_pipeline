from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Execute HiveQL query
df = spark.sql ("""select distinct ratecodeid from greentrip2022""")
df = df.withColumn("code_name", when(col("ratecodeid") == 1.0, lit("Standard rate"))
                        .when(col("ratecodeid") == 2.0, lit("JFK"))
                        .when(col("ratecodeid") == 3.0, lit("Newark"))
                        .when(col("ratecodeid") == 4.0, lit("Nassau or Westchester"))
                        .when(col("ratecodeid") == 5.0, lit("Negotiated fare"))
                        .when(col("ratecodeid") == 6.0, lit("Group Ride")))
df = df.filter(df.code_name.isNotNull()).select("*")

spark.sql("use n_database")
df.write.mode("overwrite").saveAsTable("dim4")
