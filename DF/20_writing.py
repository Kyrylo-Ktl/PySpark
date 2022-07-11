from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Writing DF').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df = df.groupBy('course', 'gender').count()
df.show()

df.write.mode('overwrite').options(header=True).csv('output')

df = spark.read.options(inferSchema=True, header=True).csv('output')
df.show()
