import sys
from pyspark import SparkConf, SparkContext


inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('Spark Application')
sc = SparkContext(conf=conf)

rdd = sc.textFile(inputs)

data = (rdd.filter(lambda x:x!='')
        .map(lambda word:(word.lower(),1))
        .reduceByKey(lambda x,y:x+y).coalesce(1))

data.saveAsTextFile(output)
