from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Filtering')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('data/numbers.txt')
print(rows.collect())

nums = rows.flatMap(str.split).map(int)
print(nums.collect())

odd_nums = nums.filter(lambda x: x & 1)
print(odd_nums.collect())

even_nums = nums.filter(lambda x: not x & 1)
print(even_nums.collect())
