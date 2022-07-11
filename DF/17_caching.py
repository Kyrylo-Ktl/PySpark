from datetime import datetime
from random import randint
from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


@udf(returnType=IntegerType())
def dummy_operation() -> int:
    sleep(0.005)
    return randint(1, 1_000)


spark = SparkSession.builder.appName('Grouping and aggregation').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df = df.withColumn('dummy', dummy_operation())

start = datetime.now()
df.show()
print(f'Finished in {datetime.now() - start}')

start = datetime.now()
df.show()
print(f'Finished in {datetime.now() - start}')

df = df.cache()

start = datetime.now()
df.show()
print(f'Finished in {datetime.now() - start}')
