from operator import add

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Reducing by key')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('sample.txt')
print(rows.collect())

words = rows.flatMap(str.split).map(str.lower).map(lambda x: x.removesuffix('.'))
print(words.collect())

letters_with_ones = words.map(lambda w: (w[0], 1))
print(letters_with_ones.collect())

letter_counts = letters_with_ones.reduceByKey(add)
print(letter_counts.collect())
