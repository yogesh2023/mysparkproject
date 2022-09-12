from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timezone","EST").getOrCreate()


host = "jdbc:mysql://yogesh.co6vzv8qx4xh.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
uname = "myuser"
pwd = "mypassword"


#tabs = ['dept','emp','empclean','products','offices']
#for i in tabs:
   # df=spark.read.format('jdbc').option('url',host).option('dbtable',i).option('user',uname).option('password',pwd).option('driver','com.mysql.jdbc.Driver').load()
    #df.show()
    #df.printSchema()


#qry = "(select * from emp where deptno>10)t" #where t is temporary variable

#TO IMPORT ALL TABLES AT A TIME
qry= "(select table_name from information_schema.TABLEs where TABLE_SCHEMA='mysqldb')t"
df = spark.read.format('jdbc').option('url', host).option('dbtable', qry).option('user', uname).option('password',pwd).option('driver', 'com.mysql.jdbc.Driver').load()
#tabs=[x[0] for x in df.collect() if df.count()>0]
tabs=[x[0] for x in df.collect()]
for i in tabs:
    df=spark.read.format('jdbc').option('url',host).option('dbtable',i).option('user',uname).option('password',pwd).option('driver','com.mysql.jdbc.Driver').load()
    df.show()
    df.printSchema()
df.show()
