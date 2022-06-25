from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Partitions')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('sample.txt')
rows = rows.repartition(5)
print(rows.getNumPartitions())
print(rows.collect())

rows = rows.coalesce(3)
words = rows.flatMap(str.split).map(str.lower).map(lambda x: x.removesuffix('.'))
print(words.getNumPartitions())
print(words.collect())

rows.saveAsTextFile('output/five_partitions')
words.saveAsTextFile('output/three_partitions')

rows = sc.textFile('output/five_partitions')
print(rows.getNumPartitions())

words = sc.textFile('output/three_partitions')
print(words.getNumPartitions())
