import findspark
findspark.init("C:\spark\spark-2.4.7-bin-hadoop2.7")
findspark.find()
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
findspark.find()

conf = SparkConf().setAppName('SparkArchitecture').setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

#Reading the text file into an RDD, performing filter, map and reduce functions on the RDD
orderitems = sc.textFile("data\\retail_db\\order_items\\part-00000")
print(orderitems.take(10))
orderitemsfiltered = orderitems.filter(lambda k: int(k.split(',')[1]) == 2)
print(orderitemsfiltered.collect())
orderitemsMap = orderitemsfiltered.map(lambda k: float(k.split(',')[4]))
print(orderitemsMap.collect())
orderitemsReduce = orderitemsMap.reduce(lambda x,y: x + y)
print(orderitemsReduce)