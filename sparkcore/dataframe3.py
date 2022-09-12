from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

data='C:/venu sir/drivers-20220727T054209Z-001/drivers/10000Records.csv'

df=spark.read.format('csv').option('header','True').option('inferSchema','True').load(data)

#num = int(df.count()) #to show all 10000 records
#df.show(num,truncate=False) #to full data in col. by default it takes 20 char.by default truncate is true
import re
#cols=[re.sub(' ','',c) for c in df.columns]  remove spaces
cols=[re.sub('[^a-zA-Z0-9]','',c.lower()) for c in df.columns] #remove special char.
ndf=df.toDF(*cols)  #toDF is rename all columns and convert rdd to dataframe
#ndf.show()
#ndf.printSchema()
res=ndf.withColumn('dateofbirth',to_date(col('dateofbirth'),'M/d/yyyy')).withColumn('today',current_date()).withColumn('datediff',datediff(col('today'),col('dateofbirth')))\
    .where(col('gender')=='F')
#res=ndf.groupby(col('gender')).agg((count('*')).alias('cnt'))
res.show()
res.printSchema()

#spark-submit -master local --deploy-mode client dataframe3.py