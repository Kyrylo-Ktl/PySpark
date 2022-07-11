from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


@udf(returnType=DoubleType())
def increment_salary(state: str, salary: int, bonus: int) -> float:
    return {
        'NY': salary * 0.10 + bonus * 0.05,
        'CA': salary * 0.12 + bonus * 0.03,
    }.get(state, 0.0)


spark = SparkSession.builder.appName('Spark UDFs').getOrCreate()

df = spark.read.options(header='True', inferSchema='True').csv('OfficeData.csv')
df.show()

df.withColumn("increment", increment_salary(df.state, df.salary, df.bonus)).show()
