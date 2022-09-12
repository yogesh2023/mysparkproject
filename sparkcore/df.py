from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

sc = spark.sparkContext

#data = [1,44,5,5,56,78,98]
#drdd = spark.sparkContext.parallelize(data)
data = "C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\asl.csv"
aslrdd =sc.textFile(data)

#res = aslrdd.map(lambda x:x.split(',')).filter(lambda x:"blr" in x[2])
res = aslrdd.filter(lambda x: 'age' not in x).map(lambda x:x.split(',')).toDF(['name','age','city'])
res.createOrReplaceTempView('tab')
#result=spark.sql("select * from tab where city='blr' and age<30")
result = res.where((col('age')>=30) & (col('city')=='mas'))
result.show()
#above approch used in 2015 ... in thi ur coverting rdd into df
#but today everyone using dataframe api