from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName('Using withColumns').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df = df.withColumn('roll', col('roll').cast('String'))
df.printSchema()

df = df.withColumn('marks', col('marks') + 10)
df.show()

df = df.withColumn('marks', col('marks') - 10)
df.show()

df = df.withColumn('country', lit('USA'))
df.show()
