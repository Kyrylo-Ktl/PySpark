from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Distinct').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df.select(df.course).distinct().show()
df.select(df.gender).distinct().show()
