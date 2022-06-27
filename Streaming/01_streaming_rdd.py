from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName('Streaming RDD')
sc = SparkContext.getOrCreate(conf=conf)
ssc = StreamingContext(sc, 1)

rdd = ssc.textFileStream('data/')
rdd = rdd.map(lambda x: (x, 1)).reduceByKey(add)

rdd.pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(60 * 60)
