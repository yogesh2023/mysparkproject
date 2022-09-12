from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

sc = spark.sparkContext

data = "C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\donations.csv"
aslrdd =sc.textFile(data)

#res = aslrdd.filter(lambda x:'dt' not in x).map(lambda x:x.split(',')).map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0],ascending=False)
#res = aslrdd.filter(lambda x:'dt' not in x).map(lambda x:x.split(',')).map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
res = aslrdd.filter(lambda x:'dt' not in x).map(lambda x:x.split(',')).map(lambda x:x[0]).distinct()
for i in res.collect():
    print(i)