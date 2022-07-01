from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Renaming columns').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df = df.withColumnRenamed('gender', 'sex').withColumnRenamed('roll', 'roll number')
df.show()

df.select(col('name').alias('Full Name')).show()
df.show()
