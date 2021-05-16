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

orderitems = sc.textFile("data\\retail_db\\order_items\\part-00000")
print(orderitems.take(10))