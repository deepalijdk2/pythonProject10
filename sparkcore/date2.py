from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
data="D:\\bigdata\\Drivers\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
def daystoyrmndays(nums):
    yrs = int(nums / 365)
    mon = int((nums % 365) / 30)
    days = int((nums % 365) % 30)
    result = yrs, "years" , mon , "months" , days, "days"
    st = ''.join(map(str, result))
    return st
udffunc = udf(daystoyrmndays)
res=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("daystoyrmon", udffunc(col("dtdiff")))
res.show()
res.printSchema()

