from pyspark.sql import *
from pyspark.sql.functions import *

#creating sparksession object
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

#data = 'C:/venu sir/drivers-20220727T054209Z-001/drivers/donations1.csv'
#df=spark.read.format('csv').option('header','True').load(data)
#if you mention header is true,first line will cosider as header

#if you want to make 2nd line as header then first conver to rdd and then skip first line then convert rdd into df
data = 'C:/venu sir/drivers-20220727T054209Z-001/drivers/donations1.csv'
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata=rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata,header=True,inferSchema=True)  #spark by defult datatype is string
df.printSchema() #to show datatype in tree format
df.show(5) #by default show first 20 record now show first 5

