import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import os
spark = SparkSession.builder.appName("hdfs_test").getOrCreate()
df2 = spark.read.csv("/opt/ibm/spark/examples/src/main/resources/people.csv")
df2.show()
