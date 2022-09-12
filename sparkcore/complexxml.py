from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timezone",
                                                                       "EST").getOrCreate()
data='C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\complexxmldata.xml'

df=spark.read.format('xml').option('rowTag','catalog_item').load(data)
#df.show(truncate=False)
#df.printSchema()


def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df);
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True;
    cols = [re.sub('[^a-zA-Z0-9]', '', c.lower()) for c in df.columns]  # remove special char.
    return df.toDF(*cols);


ndf=flatten(df)
ndf.show()
ndf.printSchema()
op='C:\\venu sir\\drivers-20220727T054209Z-001\\drivers\\xml '

ndf.write.mode('overwrite').format('xml').option('rootTag','details').option('rowTag',"books").save(op)