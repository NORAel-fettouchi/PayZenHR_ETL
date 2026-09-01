import os
import sys

print("Python courant :", sys.executable)
print("Python version :", sys.version)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TEST_PYTHON_WORKER")
    .master("local[2]")
    .config(
        "spark.pyspark.python",
        sys.executable
    )
    .config(
        "spark.pyspark.driver.python",
        sys.executable
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Spark :", spark.version)

df = spark.range(10)

print("COUNT =", df.count())

spark.stop()