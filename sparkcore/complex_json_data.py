from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timezone","EST").getOrCreate()

data='C:/venu sir/drivers-20220727T054209Z-001/drivers/world_bank.json'
df=spark.read.format('json').option('header','True').option('inferSchema','True').load(data)

ndf=df.withColumn('majorsector_percent',explode(col('majorsector_percent'))).withColumn('mjsector_namecode',explode(col('mjsector_namecode')))\
    .withColumn('mjtheme_namecode',explode(col('mjtheme_namecode')))\
    .withColumn('majorsector_percent_name',col('majorsector_percent.name'))\
    .withColumn('majorsector_percent_percent',col('majorsector_percent.percent'))\
    .withColumn('mjsector_namecode_code',col('mjsector_namecode.code'))\
    .withColumn('mjsector_namecode_name',col('mjsector_namecode.name'))\
    .withColumn('mjtheme_namecode_name',col('mjsector_namecode.name'))\
    .withColumn('mjtheme_namecode_code',col('mjsector_namecode.code'))\
    .withColumn('project_abstract_cdata',col('project_abstract.cdata'))\
    .withColumn('sector',explode(col('sector')))\
    .withColumn('sector_name',col('sector.name'))\
    .withColumn('sector4.name',col('sector4.name'))\
    .drop('sector4')\
    .withColumn('oid',col('_id.$oid'))

res=ndf.groupby(col('countrycode')).count().orderBy(col('count').desc())
res.show(truncate=False)
res.printSchema()

#i want to solve struct value ......parent_col.child_col