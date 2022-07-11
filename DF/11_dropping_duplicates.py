from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Counting').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df.dropDuplicates(['gender', 'course']).show()
df.dropDuplicates(['gender']).show()
