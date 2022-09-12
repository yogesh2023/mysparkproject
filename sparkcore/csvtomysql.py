from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

import configparser

from configparser import configParser



conf = configParser()
conf.read(r"D:\\config.txt")
host = conf.get("cred","host")
user = conf.get('cred','user')
pwd = conf.get('cred','password')
input = conf.get('input','data')
spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timezone","EST").getOrCreate()

#data='C:/venu sir/drivers-20220727T054209Z-001/drivers/10000Records.csv'

df=spark.read.format('csv').option('header','True').option('inferSchema','True').load(input)

#num = int(df.count()) #to show all 10000 records
#df.show(num,truncate=False) #to full data in col. by default it takes 20 char.by default truncate is true
import re
#cols=[re.sub(' ','',c) for c in df.columns]  remove spaces
cols=[re.sub('[^a-zA-Z0-9]','',c.lower()) for c in df.columns] #remove special char.
ndf=df.toDF(*cols)  #toDF is rename all columns and convert rdd to dataframe
#res=ndf.groupby(col('gender')).agg((count('*')).alias('cnt'))

ndf.show(10)
ndf.printSchema()


#host = "jdbc:mysql://yogesh.co6vzv8qx4xh.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
#uname = "myuser"
#pwd = "mypassword"

ndf.write.mode('overwrite').format('jdbc').option('url',host).option('dbtable','10000records').option('user',user).option('password',pwd).option('driver','com.mysql.jdbc.Driver').save()
