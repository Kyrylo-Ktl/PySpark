from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Streaming DF').getOrCreate()

word = spark.readStream.text('data/')
word = word.groupBy('value').count()
word.writeStream.format('console').outputMode('complete').start()
