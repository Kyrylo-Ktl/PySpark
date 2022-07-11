from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Distinct')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('data/numbers.txt')
print(rows.collect())

nums = rows.flatMap(str.split).map(int)
print(nums.collect())

uniq_nums = nums.distinct()
print(uniq_nums.collect())
