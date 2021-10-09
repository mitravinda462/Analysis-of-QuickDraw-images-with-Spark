#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import sys
from operator import add
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
#import pandas as pd

spark = SparkSession\
	.builder\
	.appName("Task2")\
	.getOrCreate()

shape=spark.read.csv(sys.argv[3],inferSchema=True,header=True)
shape_stat=spark.read.csv(sys.argv[4],inferSchema=True,header=True)
k=int(sys.argv[2])
shape=shape.filter(shape.word==sys.argv[1])
shape_stat=shape_stat.filter(shape_stat.word==sys.argv[1])
shape_stat=shape_stat.drop(shape_stat.word)
df = shape.join(shape_stat,shape_stat.key_id==shape.key_id)
df=df.filter((df.recognized == "FALSE"))
df=df.filter( (df.Total_Strokes<k))
if(df.count()==0):
	print("0")
else:
	new_df=df.groupBy('countrycode').agg(f.count('countrycode').alias('count'))
	sorted_df = new_df.sort('countrycode')
	for row in sorted_df.rdd.collect():
		p=str(row['countrycode'])
		q=str(row['count'])
		print(p + "," + q)
spark.stop()

