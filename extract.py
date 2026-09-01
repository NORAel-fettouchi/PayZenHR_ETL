import os
import sys
import subprocess

PYTHON = r"E:\etape_1\PayZenHR_ETL\venv\Scripts\python.exe"

print("=" * 80)
print("TEST PYSPARK WORKER MINIMAL")
print("=" * 80)

print("Python driver :", sys.executable)
print("Version       :", sys.version)
print()

print("PYSPARK_PYTHON        :", os.environ.get("PYSPARK_PYTHON"))
print("PYSPARK_DRIVER_PYTHON :", os.environ.get("PYSPARK_DRIVER_PYTHON"))
print()

print("TEST DIRECT PYTHON")
print("-" * 80)

p = subprocess.run(
    [
        PYTHON,
        "-c",
        """
import sys
print("CHILD START")
print(sys.executable)
print(sys.version)
print("CHILD END")
"""
    ],
    capture_output=True,
    text=True
)

print("Return code :", p.returncode)
print("STDOUT      :", repr(p.stdout))
print("STDERR      :", repr(p.stderr))

if p.returncode != 0:
    print("ERREUR : Python enfant ne fonctionne pas")
    sys.exit(1)

print()
print("CREATION SPARK")
print("-" * 80)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("MinimalWorkerTest")
    .config("spark.pyspark.python", PYTHON)
    .config("spark.pyspark.driver.python", PYTHON)
    .config("spark.executorEnv.PYSPARK_PYTHON", PYTHON)
    .config("spark.executorEnv.PYTHON", PYTHON)
    .config("spark.python.worker.faulthandler.enabled", "true")
    .config("spark.python.worker.reuse", "false")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

print()
print("CONFIGURATION")
print("-" * 80)

for key in [
    "spark.pyspark.python",
    "spark.pyspark.driver.python",
    "spark.executorEnv.PYSPARK_PYTHON",
    "spark.executorEnv.PYTHON",
    "spark.python.worker.faulthandler.enabled",
    "spark.python.worker.reuse",
    "spark.driver.host",
    "spark.driver.bindAddress",
]:
    print(f"{key} = {spark.sparkContext.getConf().get(key, '<none>')}")

print()
print("TEST RDD")
print("-" * 80)

try:
    rdd = spark.sparkContext.parallelize([1, 2, 3, 4], 1)

    result = rdd.map(
        lambda x: x * 2
    ).collect()

    print()
    print("########################################")
    print("### PYTHON WORKER OK")
    print("### RESULT =", result)
    print("########################################")

except Exception as e:
    print()
    print("########################################")
    print("### PYTHON WORKER FAILED")
    print("########################################")

    import traceback
    traceback.print_exc()

finally:
    spark.stop()