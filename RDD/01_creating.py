from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Reading File')
sc = SparkContext.getOrCreate(conf=conf)

text = sc.textFile('sample.txt')
print(text.collect())
