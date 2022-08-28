import pyspark

from pyspark.sql import *
from pyspark.sql.functions import *

# creating SparkSession object
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()
data="D:\\bigdata\\datasets\\donations1.csv"
rdd=spark.sparkContext.textFile(data)  #here format of file is csv and we process like textfile.
skip=rdd.first()
odata=rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata,header=True,inferSchema=True)
df.printSchema()#printing columns and its datatype in nice tree format.

df.show(5)#display top 20 rows by default.if u want to display top 5 lines use show(5)
#df=spark.read.format("csv").option("header","true").load(data)

#if u mention header true, first line of data consider as columns
#if u have amy mal records like first ine second line wrong clean that data using rdd or udf.
#skip first line second line onwards original data available.
