from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Creating DataFrame').getOrCreate()

df = spark.read.option('header', True).csv('StudentData.csv')
df.show()
