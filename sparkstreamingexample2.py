#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import*
import time

#create spark session
spark = SparkSession.builder.getOrCreate()

# Connect to kafka server and read data stream
df = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "broker-2-xz4dr8d1l6m6lq49.kafka.svc07.us-south.eventstreams.cloud.ibm.com:9093") \
.option("kafka.sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='token' password='bivGCpoxmyPJTIgNB8vV3zBNP9x8K8lRyr8wFyiyFNrP';") \
.option("kafka.security.protocol", "SASL_SSL") \
.option("kafka.sasl.mechanism", "PLAIN") \
.option("kafka.ssl.protocol", "TLSv1.2") \
.option("kafka.ssl.enabled.protocols", "TLSv1.2") \
.option("kafka.ssl.endpoint.identification.algorithm", "HTTPS") \
.option("subscribe", "kafka-java-console-sample-topic") \
.load() \
.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.printSchema()

# Write the input data to memory
query = df.writeStream.outputMode("append").format("memory").queryName("testk2s").option("partition.assignment.strategy", "range").start()

query.awaitTermination(30)

query.stop()

query.status

# Query data
test_result=spark.sql("select * from testk2s")
test_result.show(10)

spark.sql("select count(*) from testk2s").show()
test_result_small = spark.sql("select * from testk2s limit 10")
test_result_small.show()
