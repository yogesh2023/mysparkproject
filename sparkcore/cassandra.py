from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timezone",
                                                                       "EST").getOrCreate()

adf=spark.read.format('org.apache.spark.sql.cassandra').option('table','asl').option('keyspace','cassdb').load()
adf.show()

edf=spark.read.format('org.apache.spark.sql.cassandra').option('table','emp').option('keyspace','cassdb').load()
edf.show()


join=adf.join(edf,edf.first_name==adf.name).drop('first_name')
join.show()   #by default you get inner join