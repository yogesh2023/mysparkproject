from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

data='C:/venu sir/drivers-20220727T054209Z-001/drivers/bank-full.csv'
#by default seperater is ','.to change use sep
df=spark.read.format('csv').option('header','True').option('sep',';').option('inferSchema','True').load(data)

#data processing programming
#res=df.select(col('age'),col('marital')).where((col('age')>60) & (col('marital')=='married'))
#res=df.where(((col('age')>60) | (col('marital')=='married')) & (col('balance')<=400000))

#process sql freindly
df.createOrReplaceTempView('tab')  #register this dataframe as table
#res = spark.sql("select * from tab where age>60 and marital='married'")
#res=spark.sql("select marital,sum(balance) from tab group by marital")
#res=df.groupBy(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy(col("smb").desc())
#res=df.groupBy(col("marital")).count()

res.show()
res.printSchema()