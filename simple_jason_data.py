from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timezone","EST").getOrCreate()
host = "jdbc:mysql://yogesh.co6vzv8qx4xh.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
user = "myuser"
pwd = "mypassword"


data='C:/venu sir/drivers-20220727T054209Z-001/drivers/zips.json'

df=spark.read.format('json').option('header','True').option('inferSchema','True').load(data)
#ndf=df.withColumnRenamed('_id','id').withColumn('loc',explode(col('loc')))
ndf=df.withColumnRenamed('_id','id').withColumn('latitude',col('loc')[0]).withColumn('longitude',col('loc')[1]).drop(col('loc'))
ndf.createOrReplaceTempView('tab')
ndf1=spark.sql("select * from tab where state='CA'")
ndf1.show(truncate=False)
ndf1.printSchema()
op='C:\\venu sir\\resultjson\\'

#ndf1.write.mode('append').format('csv').option('header','True').save(op)

ndf1.write.mode('overwrite').format('jdbc').option('url',host).option('dbtable','abcd').option('user',user).option('password',pwd).option('driver','com.mysql.jdbc.Driver').save()


#simple datatypes:int,string,double,date etc.
#complex datatypes:array,struct,map
#special characters are nor recommdend in column name ex._id .....rename it id
