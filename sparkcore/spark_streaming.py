from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)
spark.sparkContext.setLogLevel('ERROR')
#spark internally use diff contexts to create different api
#sparkcontext...to create rdd api
#sqlcontext ......to create dataframe api
#sparksession ......to create dataset api
#sparkstreamingcontext .....to create dstream api
#socketTextStream..... get data form something host/server and port no.
#create a Dstream
host='ec2-35-154-189-81.ap-south-1.compute.amazonaws.com'
line=ssc.socketTextStream(host,1234)  #to create dummy port use netcat server $nc -lk 1234
#line.pprint()           #python print

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w:w.split(','))
        cols=['name','age','city']
        df=rowRdd.toDF(['name','age','city'])
        df.show()
        ndf = df.where(col('city')=='hyd')
        host="jdbc:mysql://yogesh.co6vzv8qx4xh.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
        ndf.write.mode('append').format('jdbc').option('url',host).option('dbtable','spark').option('user','myuser').option('password','mypassword').option('driver','com.mysql.jdbc.Driver').save()

    except:
        pass

line.foreachRDD(process)
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

#java.net.ConnectException: Connection timed out: connect
#if you get this tyoe of error  this is security group issue add 0.00.00/ add 1234 port no. in security group

