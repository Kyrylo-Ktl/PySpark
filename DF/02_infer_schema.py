from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Infer DF schema').getOrCreate()

df = spark.read.options(header=True, inferSchema=True).csv('StudentData.csv')
df.show()
df.printSchema()
