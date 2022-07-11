from operator import add

from pyspark import SparkConf, SparkContext


def process_row(row: str) -> tuple:
    age, gender, name, course, roll, marks, email = row.split(',')
    return int(age), gender, name, course, roll, int(marks), email


def calc_sum_and_count(a: tuple, b: tuple) -> tuple:
    a_sum, a_count = a
    b_sum, b_count = b
    return a_sum + b_sum, a_count + b_count


def calc_avg(row: tuple) -> tuple:
    course, (total_sum, count) = row
    return course, round(total_sum / count, 2)


conf = SparkConf().setAppName('Average')
sc = SparkContext.getOrCreate(conf=conf)

text = sc.textFile('StudentData.csv')
header = text.first()

students = text.filter(lambda x: x != header).map(process_row)

print('1) Total number of students')
print(students.count())

print('2) Total marks achieved by Female and Male students')
print(students.map(lambda x: (x[1], x[-2])).reduceByKey(add).collect())

print('3) Total number of students that have passed and failed. (50+ marks required)')
passed = students.filter(lambda x: int(x[5]) > 50).count()
failed = students.count() - passed
print(passed, failed)

print('4) Total number of students enrolled per course')
print(students.map(lambda x: (x[3], 1)).reduceByKey(add).collect())

print('5) Total marks that students have achieved per course')
course_marks = students.map(lambda x: (x[3], x[5]))
print(course_marks.reduceByKey(add).collect())

print('6) Average marks that students have achieved per course')
print(students.map(lambda x: (x[3], (x[5], 1))).reduceByKey(calc_sum_and_count).map(calc_avg).collect())

print('7) Minimum and maximum marks achieved per course')
minimums = course_marks.reduceByKey(min).collect()
maximums = course_marks.reduceByKey(max).collect()
print(minimums)
print(maximums)

print('8) Average age of male and female students')
print(students.map(lambda x: (x[1], (x[0], 1))).reduceByKey(calc_sum_and_count).map(calc_avg).collect())
