from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

data = 'C:/venu sir/drivers-20220727T054209Z-001/drivers/us-500.csv'


#ndf=df.groupby(col('state')).agg(count('*').alias('cnt')).orderBy(col('cnt').desc())

#with column is used to add new col(if already not exists) or update col (if already exists)
#lit(value) = is used to give dummy value that col
#ndf=df.withColumn('age',lit(18)).withColumn('phone1',lit(99999))

#ndf=df.withColumn('fullname',concat_ws('_',df.first_name,df.last_name,df.state))\
    #.withColumn('phone1',regexp_replace(col('phone1'),'-','').cast(LongType()))\  #to change datatype
    #.withColumn('phone2',regexp_replace(col('phone2'),'-',''))\
    #.drop('email','web','city','address')


#ndf=df.withColumn('fullname',concat_ws('_',df.first_name,df.last_name,df.state))\
    #.withColumnRenamed('first_name','fname').withColumnRenamed('last_name','lname')  #rename col
    #.drop('email','web','city','address')\                         #drop unnecessory col

#ndf=df.groupby(col('state')).agg(count('*').alias('cnt'),collect_list(df.city).alias('names')).orderBy(col('cnt').desc()) #it gives list of values with duplicates
#ndf=df.groupby(col('state')).agg(count('*').alias('cnt'),collect_set(df.city).alias('names')).orderBy(col('cnt').desc()) #it gives list of values without duplicates

#ndf=df.withColumn('state',when(col('state')=='NY',"newyork").when(col('state')=='CA','california').otherwise(col('state'))) #to give condition on column

#ndf=df.withColumn('address1',when(col('address').contains('#'),'********').otherwise(col('address')))\
    #.withColumn('address2',regexp_replace(col('address'),'#','_'))

#ndf=df.withColumn('username',substring(col('email'),0,5)) #will give first 5 char.
#ndf=df.withColumn('username',substring_index(col('email'),'@',1)).withColumn('email1',substring_index(col('email'),'@',-1)) #it will use '@' as delimeter
#ndf1=ndf.groupby(col('email1')).count().orderBy(col('count').desc())

df.createOrReplaceTempView('tab')
#ndf=spark.sql("select *,concat_ws(' ',first_name,last_name) fullname,substring_index(email,'@',-1) mail from tab")
#qry="""with temp as(select *,concat_ws(' ',first_name,last_name) fullname,substring_index(email,'@',-1) mail from tab)
       #select mail,count(*) cnt from temp group by mail order by cnt desc """
#ndf=spark.sql(qry)

#create ur own function
def func(st):
    if (st=='NY'):
        return '40% off'
    elif (st=='OH'):
        return '50% off'
    else:
        return '500/- off'
#by default spark unable to underdstand python function,so convert python function to udf
#uf=udf(func)
#ndf=df.withColumn('offer',uf(col('state')))
#ndf.printSchema()
#ndf.show(truncate=False)


uf=udf(func)
spark.udf.register('offer',uf) # convert user define function to sql query
ndf=spark.sql('select *,offer(state) todayoffers from tab')
ndf.printSchema()
ndf.show(truncate=False)