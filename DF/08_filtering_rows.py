from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Filtering rows').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df.filter(df.course == 'DB').show()

df.filter(col('course') == 'DB').show()

df.filter((df.course == 'DB') & (df.marks > 50)).show()

courses = ['DB', 'Cloud', 'OOP', 'DSA']
df.filter(df.course.isin(courses)).show()

df.filter(df.course.startswith('DS')).show()

df.filter(df.name.endswith('se')).show()

df.filter(df.name.contains('se')).show()

df.filter(df.name.like('%s%e%')).show()
