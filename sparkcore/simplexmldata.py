from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timezone",
                                                                       "EST").getOrCreate()

data='C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\books.xml'

df=spark.read.format('xml').option('rowTag','book').load(data)
#df.show()
#Caused by: java.lang.ClassNotFoundException: xml.DefaultSource
#download https://mvnrepository.com/artifact/com.databricks/spark-xml_2.13 dependancy and place in spark jars
res=df.withColumnRenamed('_id','id').where(col('price')>10)
res.show()
op='C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\xml'
#res.write.format('csv').option('header','true').save(op)
res.toPandas().to_csv(op)  #to save data with name