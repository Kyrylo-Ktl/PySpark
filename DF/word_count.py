from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StringType

WORDS_LIST = ArrayType(elementType=StringType(), containsNull=False)


@udf(returnType=WORDS_LIST)
def split_sentence(sentence: str) -> list:
    return sentence.strip('\n').strip().lower().split()


spark = SparkSession.builder.appName('Word Count').getOrCreate()
rows = spark.read.text('data/WordData.txt')

words = rows.withColumn('words', explode(split_sentence('value'))).select('words')
word_count = words.groupBy('words').count().orderBy(col('count').desc())
word_count.show()
