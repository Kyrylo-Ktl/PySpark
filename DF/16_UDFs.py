from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


@udf(returnType=IntegerType())
def get_total_salary(salary: int) -> int:
    return salary + 100


spark = SparkSession.builder.appName('Spark UDFs').getOrCreate()

df = spark.read.options(header='True', inferSchema='True').csv('OfficeData.csv')
df.show()

df.withColumn('total_salary', get_total_salary(df.salary)).show()
