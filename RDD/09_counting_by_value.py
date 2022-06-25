from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Counting by value')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('numbers.txt')
print(rows.collect())

nums = rows.flatMap(str.split).map(int)
print(nums.collect())

print(nums.countByValue())
