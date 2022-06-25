from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Grouping by key')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('sample.txt')
print(rows.collect())

words = rows.flatMap(str.split).map(str.lower).map(lambda x: x.removesuffix('.'))
print(words.collect())

letters_with_len = words.map(lambda w: (w[0], len(w)))
print(letters_with_len.collect())

grouped_by_letter = letters_with_len.groupByKey().mapValues(list)
print(grouped_by_letter.collect())
