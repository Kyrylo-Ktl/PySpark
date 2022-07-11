from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Converting to RDD').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

rdd = df.rdd
print(type(rdd))

print(rdd.collect())

print(rdd.filter(lambda x: x[0] == 28).collect())

print(rdd.filter(lambda x: x['gender'] == 'Male').collect())
