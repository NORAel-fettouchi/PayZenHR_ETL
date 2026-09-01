from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("PayZenHR_Test")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark :", spark.version)

df = spark.range(10)

df2 = (
    df
    .withColumn("double_id", F.col("id") * 2)
    .filter(F.col("id") >= 5)
)

df2.show()

print("Nombre :", df2.count())

spark.stop()