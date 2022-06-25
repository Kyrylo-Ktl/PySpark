from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Partitions')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('sample.txt')
rows = rows.repartition(5)
print(rows.collect())

words = rows.coalesce(3)
words = words.flatMap(str.split).map(str.lower).map(lambda x: x.removesuffix('.'))
print(words.collect())

print(rows.getNumPartitions())
rows.saveAsTextFile('output/five_partitions')
print(words.getNumPartitions())
words.saveAsTextFile('output/three_partitions')

rows = sc.textFile('output/five_partitions')
print(rows.getNumPartitions())

words = sc.textFile('output/three_partitions')
print(words.getNumPartitions())
