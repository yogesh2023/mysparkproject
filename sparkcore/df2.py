from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

sc = spark.sparkContext

#data = [1,44,5,5,56,78,98]
#drdd = spark.sparkContext.parallelize(data)
data = "C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\emailsmay4.txt"
aslrdd =sc.textFile(data)

#res = aslrdd.map(lambda x:x.split(',')).filter(lambda x:"blr" in x[2])
res = aslrdd.map(lambda x:x.split(' ')).filter(lambda x: '@' in x).map(lambda x:(x[0],x[-1]))

for i in res.collect():
    print(i)