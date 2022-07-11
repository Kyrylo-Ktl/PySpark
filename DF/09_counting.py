from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Counting').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('data/StudentData.csv')
df.show()

print(df.count())
print(df.filter(df.gender == 'Female').count(), df.filter(df.gender == 'Male').count())
print(df.filter(df.course == 'DB').count())
