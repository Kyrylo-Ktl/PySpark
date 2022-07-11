from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark SQL').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('StudentData.csv')
df.show()

df.createOrReplaceTempView('Students')

query = '''
SELECT course, gender, COUNT(*) AS count
FROM Students
GROUP BY course, gender
'''

spark.sql(query).show()
df.groupBy('course', 'gender').count().show()
