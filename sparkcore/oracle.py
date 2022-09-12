from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timezone","EST").getOrCreate()
host = "jdbc:mysql://yogesh.co6vzv8qx4xh.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
uname = "myuser"
pwd = "mypassword"

df=spark.read.format('jdbc').option('url',host).option('dbtable','emp').option('user',uname).option('password',pwd).option('driver','com.mysql.jdbc.Driver').load()

#df.show()
#df.printSchema()
#process data
res  = df.na.fill(0,['comm','mgr']).withColumn('comm',col('comm').cast(IntegerType()))\
       .withColumn('hiredate',date_format(col('hiredate'),'yyyy/MMM/dd'))

#create staging table
res.write.mode('overwrite').format('jdbc').option('url',host).option('dbtable','empclean').option('user',uname).option('password',pwd).option('driver','com.mysql.jdbc.Driver').save()


res.show()
res.printSchema()
#java.lang.ClassNotFoundException: com.mysql.jdbc.Driver
#mysql dependancy problem so pls add mysql jar and place in spark/jars folder