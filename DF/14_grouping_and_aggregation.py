from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, min, count, round

spark = SparkSession.builder.appName('Grouping and aggregation').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('data/StudentData.csv')
df.show()

df.groupBy('course').count().show()
df.groupBy('course', 'gender').count().show()

df.groupBy('course', 'gender').agg(
    count('*').alias('total_enrollments'),
    sum('marks').alias('total_marks'),
    min('marks').alias('min_mark'),
    max('marks').alias('max_mark'),
    round(avg('marks'), 2).alias('avg_mark'),
).show()
