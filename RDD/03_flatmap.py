from pyspark import SparkConf, SparkContext


def split_into_words(row: str) -> list:
    return [word.removesuffix('.') for word in row.split()]


conf = SparkConf().setAppName('Mapping RDD')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('data/sample.txt')

words = rows.flatMap(split_into_words).map(str.lower)
print(words.collect())
