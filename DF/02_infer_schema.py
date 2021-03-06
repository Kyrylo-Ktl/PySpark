from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Infer DF schema').getOrCreate()

df = spark.read.options(header=True, inferSchema=True).csv('data/StudentData.csv')
df.show()
df.printSchema()
