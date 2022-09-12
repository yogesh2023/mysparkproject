# Import KafkaProducer from Kafka library
from kafka import KafkaProducer
import os

source_dir = 'C:/kafka/data'

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name where the message will publish
topicName = 'amazon_user_activities3'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

# Publish text in defined topic
for filename in os.listdir(source_dir):
    os.chdir(source_dir)
    with open(filename, mode='r') as f:
       data = f.read()
       producer.send(topicName, bytes(f"{data}", 'utf-8'))

# Print message
print("Message Sent")

producer.flush()

producer.close()

