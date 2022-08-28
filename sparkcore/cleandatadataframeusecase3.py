from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
# importing all from pyspark.sql.function
import pyspark

from pyspark.sql import *
from pyspark.sql.functions import *

# creating SparkSession object
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()
data="D:\\bigdata\\Drivers\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)
#inferSchema .... when ur readdding data auto convert data to appropriate datatypes means value 4444 converet to int .. 4343.4 convert to double
import re
num = int(df.count())
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
# re .. replace .. except all Small letters, capital letters and number except those any other symbols if u have replace/remove

ndf =df.toDF(*cols)
#toDF used to rename all cloumns , and convert rdd to dataframe ... at that time use toDF

ndf.show(21,truncate=True)
#by default show method showing top 20rows and if any field having more than 20 chars its truncated and shows ...
ndf.printSchema()
#dataframe column names and its datattype dispsplay properly

#res=ndf.groupBy(col("gender")).agg(count(col("*").alias("cnt"))
#res.show()