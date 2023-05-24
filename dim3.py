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
df = spark.sql ("""select distinct store_and_fwd_flag from greentrip2022""")
df = df.withColumn("information", when(col("store_and_fwd_flag") == "Y", lit("Store and forward trip"))
                        .when(col("store_and_fwd_flag") == "N", lit("Not Store and forward trip"))
                        .when(col("store_and_fwd_flag").isNull(), lit(None)))

df = df.filter(df.store_and_fwd_flag.isNotNull()).select("*")


spark.sql("use n_database")
df.write.mode("overwrite").saveAsTable("dim3")
