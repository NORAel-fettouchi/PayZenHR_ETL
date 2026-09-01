import os
import sys

PYTHON_EXE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE

print("=" * 80)
print("PYTHON EXECUTABLE :", sys.executable)
print("PYSPARK_PYTHON :", os.environ["PYSPARK_PYTHON"])
print("PYSPARK_DRIVER_PYTHON :", os.environ["PYSPARK_DRIVER_PYTHON"])
print("=" * 80)

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("TestPythonWorker")
    .master("local[2]")
    .config("spark.pyspark.python", PYTHON_EXE)
    .config("spark.pyspark.driver.python", PYTHON_EXE)
    .config("spark.python.worker.reuse", "false")
    .getOrCreate()
)


print("=" * 80)
print("DRIVER PYTHON")
print(sys.executable)
print("=" * 80)


df = spark.range(10)

print(
    "COUNT =",
    df.count()
)


result = (
    df
    .rdd
    .map(
        lambda x: (
            x.id,
            sys.executable
        )
    )
    .collect()
)


print("=" * 80)
print("PYTHON WORKER RESULT")
print(result)
print("=" * 80)


spark.stop()