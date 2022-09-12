# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer

# Import sys module
import sys

#importing mysql driver
import mysql.connector

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
topicName = 'amazon_user_activities3'

# Initialize consumer variable
consumer = KafkaConsumer (topicName, group_id ='cfamilyitgroup',bootstrap_servers = bootstrap_servers)

#creating mysql connection object
mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='amazon_user_activities1'
)

#creating cursor object to send sql statments
mycursor = mydb.cursor()

sql = 'insert into test(comments) values (%s)'
val = []
# Read and print message from consumer
for msg in consumer:
    #print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))
    val.append(msg.value)

mycursor.executemany(sql, val)
mydb.commit()

# Terminate the script
sys.exit()
