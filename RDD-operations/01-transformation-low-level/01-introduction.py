# create rdd from textfield
import findspark
findspark.init('/home/jhoand/spark-3.2.1-bin-hadoop3.2/')
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()

rdd = spark.sparkContext.textFile('test.txt', 3)
for i in rdd.take(5): print(i)

# number of partitions
rdd.getNumPartitions()

#list of list partitions
rdd.glom().collect()

#list with len of each partitions
rdd.glom().map(len).collect()

