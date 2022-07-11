from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Grouping').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df.groupBy('gender').sum('marks').show()

df.groupBy('gender').count().show()
df.groupBy('course').count().show()
df.groupBy('course').sum('marks').show()

df.groupBy('gender').max('marks').show()
df.groupBy('gender').min('marks').show()

df.groupBy('age').avg('marks').show()

df.groupBy('gender').mean('marks').show()
