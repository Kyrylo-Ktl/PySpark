from pyspark import SparkConf, SparkContext


def parse_rows(row: str) -> tuple:
    title, rating = row.split(',')
    return title, int(rating)


def add_ones(row: tuple) -> tuple:
    title, rating = row
    return title, (rating, 1)


def avg_reducing(a: tuple, b: tuple) -> tuple:
    a_sum, a_count = a
    b_sum, b_count = b
    return a_sum + b_sum, a_count + b_count


def calculate_avg(row: tuple) -> tuple:
    title, (rating_sum, count) = row
    return title, round(rating_sum / count, 2)


conf = SparkConf().setAppName('Average')
sc = SparkContext.getOrCreate(conf=conf)

rows = sc.textFile('data/movie_ratings.csv')

movies = rows.map(parse_rows)
print(movies.collect())

movie_avg_ratings = movies.map(add_ones).reduceByKey(avg_reducing).map(calculate_avg)
print(movie_avg_ratings.collect())
