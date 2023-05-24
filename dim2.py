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
df = spark.sql ("""select distinct payment_type from greentrip2022""")
df = df.withColumn("payment_name", when(col("payment_type") == 1.0, lit("Credit-card"))
                        .when(col("payment_type") == 2.0, lit("Cash"))
                        .when(col("payment_type") == 3.0, lit("No Charge"))
                        .when(col("payment_type") == 4.0, lit("Dispute"))
                        .when(col("payment_type") == 5.0, lit("Unknown"))
                        .when(col("payment_type") == 6.0, lit("Voided Trip"))
                        .when(col("payment_type").isNull(), lit(None)))
df = df.filter(df.payment_type.isNotNull()).select("*")

spark.sql("use n_database")
df.write.mode("overwrite").saveAsTable("dim2")