from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timezone","EST").getOrCreate()

data = "C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\donations.csv"

df=spark.read.format('csv').option('header','True').option('inferSchema','True').load(data)

def daystoyrmndays(nums):
      yrs = int(nums/365)
      mon = int((nums % 365) / 30)
      days = int((nums % 365) % 30)
      result = yrs, "years", mon ,"months" ,days ,"days"
      st = ''.join(map(str,result))
      return st

udffunc = udf(daystoyrmndays)

#to_date convert input date format to yyyy-MM-dd
#current_date() used to get today date based on system date
#date_add to add days in date
#date_sub to sub days in date
#last_day will give last day of each month
#next_day - next sun/mon/fri from today u will get
#date_format is uesed give ur desire date format ex- 2021-01-10 dd/MMM/yyyy/E/O/z  10/Jan/2021/Sun/GMT+5:30/IST
res = df.withColumn('dt',to_date(col('dt'),'d-M-yyyy')).withColumn('today',current_date()).withColumn('ts',current_timestamp())\
      .withColumn('dtdiff',datediff(col('today'),col('dt')))\
      .withColumn('dtadd',date_add(col('dt'),100))\
      .withColumn('dtsub',date_sub(col('dt'),1000))\
      .withColumn('lastday',last_day(col('dt')))\
      .withColumn('nextday',next_day(col('today'),'sun'))\
      .withColumn('dateformat',date_format(col('dt'),'dd/MM/yyyy'))\
      .withColumn('dateformat',date_format(col('dt'),'dd/MMM/yyyy/E/O/z '))\
      .withColumn('monlstfri',next_day(date_add(last_day(col('today')),-7),'fri'))\
      .withColumn('dayofweek',dayofweek(col('dt')))\
      .withColumn('dayofmonth',dayofmonth(col('dt')))\
      .withColumn('dayofyear',dayofyear(col('dt')))\
      .withColumn('year',year(col('dt')))\
      .withColumn('monbet',months_between(current_date(),col('dt')))\
      .withColumn('floor',floor(col('monbet')))\
      .withColumn('ceil',ceil(col('monbet')))\
      .withColumn('round',round(col('monbet')).cast(IntegerType()))\
      .withColumn('daystoyrmndays',udffunc(col('dtdiff')))




res.printSchema()
res.show(truncate=False)