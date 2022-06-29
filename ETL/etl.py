from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StringType

from config import Config

WORDS_LIST = ArrayType(elementType=StringType(), containsNull=False)


@udf(returnType=WORDS_LIST)
def split_sentence(sentence: str) -> list:
    return sentence.strip('\n').strip().lower().split()


# Extract
spark = SparkSession.builder.appName('ETL Pipeline').config('spark.jars', 'postgresql-42.4.0.jar').getOrCreate()
rows = spark.read.text('WordData.txt')

# Transform
words = rows.withColumn('words', explode(split_sentence('value'))).select('words')
word_count = words.groupBy('words').count().orderBy(col('count').desc())
word_count.show()

# Load
properties = {
    'user': Config.DB_USER,
    'password': Config.DB_PASS,
    'driver': Config.DRIVER,
}
word_count.write.jdbc(
    mode='overwrite',
    url=Config.DB_HOST,
    table=f'{Config.DB_NAME}.WordCount',
    properties=properties,
)
