from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('Providing schema').getOrCreate()

df = spark.read.options(inferSchema=True, header=True).csv('data/StudentData.csv')
df.show()
df.printSchema()

schema = StructType([
    StructField('age',    IntegerType(), True),
    StructField('gender', StringType(),  True),
    StructField('name',   StringType(),  True),
    StructField('course', StringType(),  True),
    StructField('roll',   StringType(),  True),
    StructField('marks',  IntegerType(), True),
    StructField('email',  StringType(),  True),
])

df = spark.read.options(header=True).schema(schema).csv('data/StudentData.csv')
df.show()
df.printSchema()
