from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def parse_row(row: str) -> tuple:
    age, gender, name, course, roll, marks, email = row.split(',')
    return int(age), gender, name, course, roll, int(marks), email


spark = SparkSession.builder.appName('DF from RDD').getOrCreate()

conf = SparkConf().setAppName('RDD')
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('data/StudentData.csv')
header = rdd.first()
rdd = rdd.filter(lambda x: x != header).map(parse_row)

columns = header.split(',')
df = rdd.toDF(columns)
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

df = spark.createDataFrame(rdd, schema=schema)
df.show()
df.printSchema()
