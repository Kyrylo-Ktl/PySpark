from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Sorting').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('data/StudentData.csv')
df.show()

df.select(['age', 'course']).distinct().sort(df.age, df.course).show()

df.select(['age', 'course']).distinct().orderBy(df.age, df.course).show()

df.select(['age', 'course']).distinct().sort(df.course.asc(), df.age.desc()).show()
