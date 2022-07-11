from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dropping duplicates').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('data/StudentData.csv')
df.show()

df.dropDuplicates(['gender', 'course']).show()
df.dropDuplicates(['gender']).show()
