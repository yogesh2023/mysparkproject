from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()


sc=spark.sparkContext

data=[1,2,3,4]

drdd=sc.parallelize(data)
print(drdd)