from pyspark import SparkConf, SparkContext


def parse_rows(row: str) -> tuple:
    title, rating = row.split(',')
    return title, int(rating)


conf = SparkConf().setAppName('Min and Max')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('movie_ratings.csv')

movies = rows.map(parse_rows)
print(movies.collect())

minimums = movies.reduceByKey(min)
print(minimums.collect())

maximums = movies.reduceByKey(max)
print(maximums.collect())
