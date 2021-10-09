import sys
from operator import add
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
spark = SparkSession\
	.builder\
	.appName("Task1")\
	.getOrCreate()
shape=spark.read.csv(sys.argv[2],inferSchema=True,header=True)
shape_stat=spark.read.csv(sys.argv[3],inferSchema=True,header=True)
shape=shape.filter(shape.word==sys.argv[1])
shape_stat=shape_stat.filter(shape_stat.word==sys.argv[1])
shape_stat=shape_stat.drop(shape_stat.word)
df = shape.join(shape_stat,shape_stat.key_id==shape.key_id)
agg_df=df.groupby('recognized').agg(f.avg('Total_Strokes').alias('avg'))
if(agg_df.count()==2):
	value1=agg_df.where(agg_df.recognized==True).select('avg').collect()[0]['avg']
	value2=agg_df.where(agg_df.recognized==False).select('avg').collect()[0]['avg']
	print("%.5f"%value1)
	print("%.5f"%value2)
elif(agg_df.count()==1):
	value=agg_df.select('recognized').collect()[0]['recognized']
	if(value):
		print("%.5f"%agg_df['avg'])
		print('0.00000')
	else:
		print('0.00000')
		print("%.5f"%agg_df['avg'])
else:
	print('0.00000')
	print('0.00000')
spark.stop()
