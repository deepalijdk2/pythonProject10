from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
#data = [12,32,34,4,54,56]
#drdd = spark.sparkContext.parallelize(data)
data = "D:\\bigdata\\drivers\\asl.csv"
#aslrdd = spark.sparkContext.textFile()

aslrdd = sc.textFile(data)
#select * from tab where city='hyd'
#res=aslrdd.filter(lambda x: "age" not in x).map(lambda x:x.split(",")).filter(lambda x: "hyd" in x)
res=aslrdd.filter(lambda x: "age" not in x).map(lambda x:x.split(",")).map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)
#group by ur using ... cat based col and something aggregation mandatory
#if u want to group the value first u must use reduceByKey ... its used to group the values.
#reduceByKey (any function/method ends with Key, means data must be (key, value) format
#reduceByKey ... based on same key , data process the values ..
#reduceByKey (x, y: x+y) ..4





#in sql ... olap ... sel * from tab where .....
#group by col
#join tab
for i in res.collect():
    print(i)
    '''Map: apply a logic on top of each & every element ... how many input elements u have same number of output elements u ll get.
    filter: based on bool value filter the results.'''