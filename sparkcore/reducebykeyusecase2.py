from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\bigdata\\drivers1\\donations.csv"
drdd=sc.textFile(data)
pro = drdd.filter(lambda x: "dt" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))\
    .reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1])


for i in pro.collect():
    print(i)
#spark-submit --master local --deploy-mode client C:\Users\Legion\PycharmProjects\pythonProject1\sparkcore\reducebykeyusecase2.py