from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Counting by value')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('numbers.txt')
print(rows.collect())

nums = rows.flatMap(str.split).distinct().map(int)
print(nums.collect())

nums.saveAsTextFile('output/distinct_nums')
