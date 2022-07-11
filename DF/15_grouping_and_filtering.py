from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = SparkSession.builder.appName('Grouping and filtering').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('data/StudentData.csv')
df.show()

df.filter(df.gender == 'Male').groupBy('course', 'gender').agg(
    count('*').alias('total_enrollments')
).filter(
    col('total_enrollments') > 85
).show()
