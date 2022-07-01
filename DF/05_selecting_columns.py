from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Selecting columns').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

# name, gender
df.select('name', 'gender').show()

# name, email
df.select(df.name, df.email).show()

# roll, name
df.select(col('roll'), col('name')).show()

# age, gender, name, course, roll, marks, email
df.select('*').show()

# name, course, roll, marks
df.select(df.columns[2:6]).show()
