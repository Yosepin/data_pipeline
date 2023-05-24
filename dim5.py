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
df = spark.sql ("""select distinct vendorid from greentrip2022""")
df = df.withColumn("vendor_name", when(col("vendorid") == 1, lit("Creative Mobile Technology,LLC"))
                        .when(col("vendorid") == 2, lit("Verifone Inc")))

df = df.filter(df.vendor_name.isNotNull()).select("*")

spark.sql("use n_database")
df.write.mode("overwrite").saveAsTable("dim5")
