from pyspark.sql import *
from pyspark.sql.functions import import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()