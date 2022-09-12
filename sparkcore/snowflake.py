from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext


# You might need to set these
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAR3P5RBDFVL6O2ZXE")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

# Set options below
sfOptions = {
  "sfURL" : "gi81579.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "KOLTEPATIL",
  "sfPassword" : "Yk@7276751361",
  "sfDatabase" : "YOGESHDB",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "SMALL"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

'''df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from CUSTOMER") \
  .load()
'''

data='C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\bank-full.csv'
df=spark.read.format('csv').option('header','true').option('inferSchema','true').option('sep',';').load(data)

df.show()

df.write.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option('dbtable','banktab') \
  .save()